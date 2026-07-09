use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{RingBuffer, candle::Candle};
use common::Timeframe;

/// Wrapper em torno do RingBuffer especializado para armazenamento histórico de Velas.
/// 
/// Diferente do `TickStore`, que armazena os dados brutos, o `CandleStore` guarda
/// as velas agregadas fechadas. Um `CandleStore` existe para cada Timeframe suportado.
#[derive(Debug, Clone)]
pub struct CandleStore {
    timeframe: Timeframe,
    buffer: Arc<RwLock<RingBuffer<Candle>>>,
}

impl CandleStore {
    /// Inicializa um novo Store para um dado timeframe com a capacidade especificada.
    pub fn new(timeframe: Timeframe, capacity: usize) -> Self {
        Self {
            timeframe,
            buffer: Arc::new(RwLock::new(RingBuffer::new(capacity))),
        }
    }

    /// Empurra uma nova vela fechada para o armazenamento histórico.
    pub async fn push_closed_candle(&self, candle: Candle) {
        let mut b = self.buffer.write().await;
        b.push(candle);
    }

    /// Retorna um snapshot contendo as últimas N velas, em ordem cronológica
    /// (da mais antiga até a mais recente).
    pub async fn snapshot(&self, count: usize) -> Vec<Candle> {
        let b = self.buffer.read().await;
        b.snapshot(count)
    }

    /// Retorna as velas que iniciam a partir de um timestamp específico.
    pub async fn candles_since(&self, ts_ns: u64) -> Vec<Candle> {
        let b = self.buffer.read().await;
        let len = b.len();
        if len == 0 {
            return Vec::new();
        }

        let mut res = Vec::new();
        for i in (0..len).rev() {
            if let Some(c) = b.get(i) {
                if c.t >= ts_ns {
                    res.push(c.clone());
                }
            }
        }
        res
    }
    
    /// Obtém o Timeframe respectivo deste store.
    pub fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Obtém o timestamp do fechamento da última vela registrada.
    /// Útil para a inicialização e recovery.
    pub async fn last_closed_timestamp(&self) -> Option<u64> {
        let b = self.buffer.read().await;
        b.get(0).map(|c| c.t)
    }
}
