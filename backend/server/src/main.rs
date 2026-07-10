//! # Server — Ponto de entrada da plataforma
//!
//! Faz o wiring de toda a pipeline e expõe o servidor WebSocket:
//!
//! ```text
//! [FeedProvider] → FeedHandler → Sequencer → (broadcast NormalizedTick)
//!                                                     │
//!                                                     ▼
//!                                       MultiTimeframeAggregator
//!                                                     │ (broadcast CandleEvent)
//!                                                     ▼
//!                                         Hub (encode Protobuf) → WebSocket clients
//! ```
//!
//! # Integridade
//! Sem provider real configurado, NENHUM feed é iniciado: o sistema permanece
//! em estado DISCONNECTED e nenhum candle é fabricado. O feed sintético só é
//! ativado explicitamente via `USE_MOCK_FEED=1` (DEV apenas).

use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Router,
};
use tokio::sync::{broadcast, RwLock};

use aggregation_core::{CandleEvent, MultiTimeframeAggregator, RingBuffer, Sequencer};
use common::{NormalizedTick, PlatformConfig, TickSource, Timeframe};
use feed_handler::{FeedHandlerService, FeedProvider};

mod convert;
mod hub;
mod mock_provider;

use hub::Hub;

/// Capacidade dos canais broadcast internos (ticks e eventos de candle).
const CHANNEL_CAPACITY: usize = 65_536;
/// Timeframe default enviado no snapshot inicial de conexão.
const DEFAULT_TF: Timeframe = Timeframe::M1;
/// Quantidade de candles no snapshot inicial.
const SNAPSHOT_COUNT: usize = 1_500;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Configuração: tenta config.toml, cai para defaults sensatos.
    let config = PlatformConfig::from_file("config.toml").unwrap_or_else(|_| {
        tracing::info!("config.toml não encontrado — usando defaults");
        PlatformConfig::default()
    });

    let tick_capacity = config.tick_buffer.capacity;
    let candle_capacity = config.tick_buffer.candle_capacity;
    let dedup = config.tick_buffer.dedup_window;
    let symbol = config.symbol.clone();

    // === Canais internos ===
    let (tick_tx, tick_rx) = broadcast::channel::<NormalizedTick>(CHANNEL_CAPACITY);
    let (event_tx, event_rx) = broadcast::channel::<CandleEvent>(CHANNEL_CAPACITY);

    // === Tick Store (ring buffer bruto) ===
    let tick_store = Arc::new(RwLock::new(RingBuffer::new(tick_capacity)));

    // === Sequencer ===
    let sequencer = Arc::new(RwLock::new(Sequencer::new(
        tick_store.clone(),
        tick_tx.clone(),
        dedup,
    )));

    // === Aggregator multi-timeframe ===
    let tfs = Timeframe::all_candle_timeframes();
    let (mut aggregator, stores) =
        MultiTimeframeAggregator::new(tick_rx, event_tx.clone(), tfs, candle_capacity);

    tokio::spawn(async move {
        aggregator.run().await;
    });

    // === Broadcast Hub (encode Protobuf + fan-out WebSocket) ===
    let (frame_tx, _) = broadcast::channel::<Arc<Vec<u8>>>(CHANNEL_CAPACITY);
    let hub = Arc::new(Hub {
        frame_tx,
        stores,
        symbol: symbol.clone(),
        default_tf: DEFAULT_TF,
        snapshot_count: SNAPSHOT_COUNT,
    });
    hub::spawn_encoder(hub.clone(), event_rx);

    // === Feed Handler ===
    if std::env::var("USE_MOCK_FEED").is_ok() {
        tracing::warn!(
            "USE_MOCK_FEED ativo — feed SINTÉTICO (somente DEV). NUNCA use em produção."
        );
        let provider: Box<dyn FeedProvider> =
            Box::new(mock_provider::MockFeedProvider::new(&symbol));
        let mut feed = FeedHandlerService::new(provider, sequencer.clone(), TickSource::Test);
        tokio::spawn(async move {
            if let Err(e) = feed.run().await {
                tracing::error!(error = %e, "Feed handler terminou com erro");
            }
        });
    } else {
        tracing::warn!(
            "Nenhum provider real configurado — sistema em DISCONNECTED. \
             Injete um FeedProvider real ou use USE_MOCK_FEED=1 para dev."
        );
    }

    // === Servidor WebSocket ===
    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(hub.clone());

    let addr = format!("{}:{}", config.network.ws_bind_addr, config.network.ws_port);
    tracing::info!(%addr, "WebSocket server ouvindo");

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Handler de upgrade WebSocket.
async fn ws_handler(ws: WebSocketUpgrade, State(hub): State<Arc<Hub>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, hub))
}

/// Ciclo de vida de uma conexão WebSocket:
/// 1. Envia o Snapshot inicial (série histórica colunar).
/// 2. Faz stream de CandleDelta/CandleClose em tempo real.
async fn handle_socket(mut socket: WebSocket, hub: Arc<Hub>) {
    // 1. Snapshot inicial
    let snapshot = hub.build_snapshot(hub.default_tf).await;
    if socket.send(Message::Binary(snapshot)).await.is_err() {
        return;
    }

    // 2. Stream ao vivo
    let mut rx = hub.frame_tx.subscribe();
    loop {
        tokio::select! {
            frame = rx.recv() => match frame {
                Ok(bytes) => {
                    if socket.send(Message::Binary(bytes.as_ref().clone())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "Cliente WS lento — frames descartados");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                // Mensagens de controle do cliente (ex: trocar timeframe) entrariam aqui.
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            }
        }
    }
}
