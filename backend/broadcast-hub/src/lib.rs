pub mod coalescer;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use aggregation_core::proto::snapshot_message;
use aggregation_core::{CandleEvent, CandleStore};
use common::Timeframe;
use contracts_rs::market_stream::MarketMessage;

use crate::coalescer::Coalescer;

/// Janela de coalescing alinhada a 60 FPS (~16.6ms).
const FRAME_INTERVAL: Duration = Duration::from_micros(16_666);
const SNAPSHOT_CANDLES: usize = 1_500;

#[derive(Clone)]
pub struct HubConfig {
    pub bind_addr: String,
    pub port: u16,
    pub symbol: String,
}

pub struct HubState {
    pub event_tx: broadcast::Sender<CandleEvent>,
    pub stores: HashMap<Timeframe, CandleStore>,
    pub config: HubConfig,
}

pub async fn run(state: Arc<HubState>) -> std::io::Result<()> {
    let addr = format!("{}:{}", state.config.bind_addr, state.config.port);
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "Broadcast Hub ouvindo");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => { warn!(error = %e, "Falha ao aceitar conexão"); continue; }
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            debug!(%peer, "Cliente conectado");
            if let Err(e) = handle_connection(stream, state).await {
                debug!(%peer, error = %e, "Conexão encerrada");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    state: Arc<HubState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ws = tokio_tungstenite::accept_async(stream).await?;
    let (mut write, mut read) = ws.split();
    let symbol = state.config.symbol.clone();

    // 1. Snapshot inicial por timeframe.
    send_snapshots(&mut write, &state, &symbol).await?;

    // 2. Assina o stream + coalescer.
    let mut rx = state.event_tx.subscribe();
    let mut co = Coalescer::new(symbol.clone());

    // 3. Timer de frame.
    let mut frame = interval(FRAME_INTERVAL);
    frame.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            ev = rx.recv() => {
                match ev {
                    Ok(e) => co.push(e),
                    // Cliente atrasou: resync via snapshot (nunca deixa buraco).
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(missed = n, "Cliente atrasou; resync via snapshot");
                        co.reset();
                        send_snapshots(&mut write, &state, &symbol).await?;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        info!("Canal fechado; encerrando conexão");
                        break;
                    }
                }
            }
            _ = frame.tick() => {
                for msg in co.drain_frame() {
                    write.send(Message::Binary(encode(&msg))).await?;
                }
            }
            incoming = read.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(p))) => write.send(Message::Pong(p)).await?,
                    Some(Ok(_)) => {}
                    Some(Err(e)) => { debug!(error = %e, "Erro de leitura"); break; }
                }
            }
        }
    }

    let _ = write.send(Message::Close(None)).await;
    Ok(())
}

async fn send_snapshots<S>(
    write: &mut S,
    state: &HubState,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    for (&tf, store) in &state.stores {
        let candles = store.snapshot(SNAPSHOT_CANDLES).await;
        if candles.is_empty() { continue; } // sem histórico → nada (não fabricamos)
        let msg = snapshot_message(symbol, tf, &candles);
        write.send(Message::Binary(encode(&msg))).await?;
    }
    Ok(())
}

#[inline]
fn encode(msg: &MarketMessage) -> Vec<u8> {
    let mut buf = Vec::with_capacity(msg.encoded_len());
    msg.encode(&mut buf).expect("encode MarketMessage");
    buf
}