use std::sync::Arc;
use tokio::sync::RwLock;
use common::NormalizedTick;
use crate::RingBuffer;

/// Tick Store é um wrapper especializado em torno do RingBuffer genérico.
///
/// # Propósito
/// Encapsula a lógica de consultas temporais, extração de snapshots
/// e controle de concorrência com RwLock.
#[derive(Debug, Clone)]
pub struct TickStore {
    buffer: Arc<RwLock<RingBuffer<NormalizedTick>>>,
}

impl TickStore {
    /// Inicializa um novo TickStore com a capacidade dada.
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Arc::new(RwLock::new(RingBuffer::new(capacity))),
        }
    }

    /// Empurra um tick via lock de escrita. (Para uso do Sequencer).
    pub async fn push(&self, tick: NormalizedTick) {
        let mut b = self.buffer.write().await;
        b.push(tick);
    }

    /// Retorna um snapshot contendo os últimos N ticks, ordenados
    /// do mais antigo para o mais recente. (Para conectar clientes via WS).
    pub async fn snapshot(&self, count: usize) -> Vec<NormalizedTick> {
        let b = self.buffer.read().await;
        b.snapshot(count)
    }

    /// Retorna ticks que ocorreram a partir de um timestamp específico.
    pub async fn ticks_since(&self, ts_ns: u64) -> Vec<NormalizedTick> {
        let b = self.buffer.read().await;
        let len = b.len();
        if len == 0 {
            return Vec::new();
        }

        // Iterar do mais antigo ao mais recente
        let mut res = Vec::new();
        for i in (0..len).rev() {
            if let Some(tick) = b.get(i) {
                if tick.ts_ns >= ts_ns {
                    res.push(tick.clone());
                }
            }
        }
        res
    }

    /// Retorna a quantidade de itens no store.
    pub async fn len(&self) -> usize {
        self.buffer.read().await.len()
    }

    /// Estimativa de taxa de ticks por segundo com base na janela atual.
    pub async fn tick_rate(&self) -> f64 {
        let b = self.buffer.read().await;
        let len = b.len();
        if len < 2 {
            return 0.0;
        }

        // Pega o mais recente e o mais antigo
        let newest = match b.get(0) {
            Some(t) => t,
            None => return 0.0,
        };
        let oldest = match b.get(len - 1) {
            Some(t) => t,
            None => return 0.0,
        };

        let span_ns = newest.ts_ns.saturating_sub(oldest.ts_ns);
        if span_ns == 0 {
            return 0.0;
        }

        (len as f64) / (span_ns as f64 / 1_000_000_000.0)
    }
}
