//! Broadcast Hub — consome CandleEvent, serializa em Protobuf e faz fan-out
//! para todos os clientes WebSocket conectados.
//!
//! # Design de Escalabilidade
//! O encode acontece UMA vez por evento (não por cliente). O frame binário é
//! empacotado em `Arc<Vec<u8>>` e distribuído via broadcast — 1 agregação → N
//! broadcasts, sem re-serializar por conexão.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;
use tokio::sync::broadcast;

use aggregation_core::{CandleEvent, CandleStore};
use common::Timeframe;

use crate::convert;

/// Estado compartilhado do hub, injetado no router axum.
pub struct Hub {
    /// Canal de frames binários já serializados (fan-out para clientes).
    pub frame_tx: broadcast::Sender<Arc<Vec<u8>>>,
    /// Stores históricos por timeframe (para snapshots iniciais).
    pub stores: HashMap<Timeframe, CandleStore>,
    /// Símbolo servido por esta instância.
    pub symbol: String,
    /// Timeframe default do snapshot inicial.
    pub default_tf: Timeframe,
    /// Quantidade de candles no snapshot inicial.
    pub snapshot_count: usize,
}

impl Hub {
    /// Constrói o Snapshot inicial (Protobuf binário) para um timeframe.
    pub async fn build_snapshot(&self, tf: Timeframe) -> Vec<u8> {
        let candles = match self.stores.get(&tf) {
            Some(store) => store.snapshot(self.snapshot_count).await,
            None => Vec::new(),
        };
        let msg = convert::snapshot_to_message(&self.symbol, tf, &candles);
        msg.encode_to_vec()
    }
}

/// Spawna a task que consome CandleEvent, serializa e publica frames binários.
pub fn spawn_encoder(hub: Arc<Hub>, mut event_rx: broadcast::Receiver<CandleEvent>) {
    tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(event) => {
                    let msg = convert::event_to_message(&event, &hub.symbol);
                    let bytes = Arc::new(msg.encode_to_vec());
                    // Ignora erro se não houver clientes conectados (normal).
                    let _ = hub.frame_tx.send(bytes);
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "Encoder do Hub não acompanhou o agregador");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!("Canal de eventos fechado. Encerrando encoder do Hub.");
                    break;
                }
            }
        }
    });
}