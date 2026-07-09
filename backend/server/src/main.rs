//! Binário orquestrador da plataforma de análise EUR/USD.
//!
//! Amarra a pipeline completa:
//!   Provider → FeedHandler → Sequencer → (mpsc) → Aggregator → (broadcast) → [Hub futuro]
//!
//! # Ordem de fiação (importante)
//! O `status_receiver()` vem do FeedHandler, que precisa do Sequencer. Por isso:
//!   canais → tick_store → Sequencer → FeedHandler → status_rx → Aggregator.

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

use common::{PlatformConfig, TickSource, Timeframe};
use aggregation_core::{
    MultiTimeframeAggregator, Sequencer, RingBuffer, CandleEvent,
};
use feed_handler::FeedHandlerService;

mod mock_provider;
use mock_provider::MockReplayProvider;

/// Capacidade do canal Sequencer → Aggregator. Grande o bastante pra absorver
/// rajadas sem estourar RAM; quando cheio, o Sequencer aplica backpressure.
const TICK_CHANNEL_CAP: usize = 65_536;

/// Capacidade do canal broadcast Aggregator → consumidores (hub/SMC).
const EVENT_CHANNEL_CAP: usize = 65_536;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Observabilidade ---
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    // --- Config (usa defaults se o arquivo não existir) ---
    let config = PlatformConfig::from_file("config.toml").unwrap_or_else(|_| {
        info!("config.toml não encontrado; usando defaults");
        PlatformConfig::default()
    });
    info!(?config, "Configuração carregada");

    // --- 1. Canais ---
    // Sequencer → Aggregator: mpsc (backpressure, sem perda de ticks).
    let (tick_tx, tick_rx) = mpsc::channel(TICK_CHANNEL_CAP);
    // Aggregator → consumidores: broadcast (fan-out para N clientes/SMC).
    let (event_tx, _event_rx) = broadcast::channel::<CandleEvent>(EVENT_CHANNEL_CAP);

    // --- 2. Tick store (ring buffer bruto, pré-alocado) ---
    let tick_store = Arc::new(RwLock::new(
        RingBuffer::new(config.tick_buffer.capacity),
    ));

    // --- 3. Sequencer (envolvido em Arc<RwLock> pro FeedHandler escrever) ---
    let sequencer = Sequencer::new(
        Arc::clone(&tick_store),
        tick_tx,
        config.tick_buffer.dedup_window,
    );
    let sequencer = Arc::new(RwLock::new(sequencer));

    // --- 4. Provider concreto ---
    // Troque MockReplayProvider por sua implementação real de FeedProvider.
    let provider = Box::new(MockReplayProvider::new(&config.symbol));

    // --- 5. FeedHandler ---
    let mut feed_handler = FeedHandlerService::new(
        provider,
        Arc::clone(&sequencer),
        TickSource::Live,
    );

    // --- 6. Pega o status_receiver ANTES de mover o handler pra task ---
    let status_rx = feed_handler.status_receiver();

    // --- 7. Aggregator, já ligado ao status (não fabrica flat em DISCONNECTED) ---
    let timeframes: &[Timeframe] = Timeframe::all_candle_timeframes();
    let (mut aggregator, _stores) = MultiTimeframeAggregator::new(
        tick_rx,
        event_tx.clone(),
        timeframes,
        config.tick_buffer.candle_capacity,
    );
    let mut aggregator = aggregator.with_status_receiver(status_rx);
    // `_stores` é o HashMap<Timeframe, CandleStore> — passe ao broadcast-hub
    // quando ele existir, pra servir snapshots iniciais.

    // --- 8. Sobe as tasks ---
    let feed_task = tokio::spawn(async move {
        if let Err(e) = feed_handler.run().await {
            error!(error = %e, "FeedHandler encerrou com erro");
        }
    });

    let agg_task = tokio::spawn(async move {
        aggregator.run().await;
    });

    info!("Pipeline no ar. Ctrl+C para encerrar.");

    // --- Shutdown gracioso ---
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C recebido, encerrando...");
        }
        r = feed_task => { info!(?r, "feed_task terminou"); }
        r = agg_task => { info!(?r, "agg_task terminou"); }
    }

    Ok(())
}