use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::broadcast;

use common::{NormalizedTick, SequencerError};
use crate::RingBuffer;

#[derive(Debug, Default, Clone)]
pub struct SequencerMetrics {
    pub accepted: u64,
    pub rejected_out_of_order: u64,
    pub rejected_duplicate: u64,
}

/// Identidade de preço de um tick: (ts_ns, bid, ask). Volume não faz parte.
type TickIdentity = (u64, i64, i64);

pub struct Sequencer {
    last_ts_ns: u64,
    seq_counter: Arc<AtomicU64>,
    /// Lock SÍNCRONO: o Sequencer é o único escritor e a operação é O(1).
    /// Usar tokio::sync::RwLock aqui causava panic (blocking dentro do runtime).
    tick_store: Arc<RwLock<RingBuffer<NormalizedTick>>>,
    broadcast_tx: broadcast::Sender<NormalizedTick>,
    /// Ordem de inserção para evicção FIFO da janela de dedup.
    dedup_order: VecDeque<TickIdentity>,
    /// Set para checagem O(1) — elimina o scan O(n) e a colisão de hash XOR.
    dedup_set: HashSet<TickIdentity>,
    dedup_capacity: usize,
    metrics: SequencerMetrics,
}

impl Sequencer {
    pub fn new(
        tick_store: Arc<RwLock<RingBuffer<NormalizedTick>>>,
        broadcast_tx: broadcast::Sender<NormalizedTick>,
        dedup_capacity: usize,
    ) -> Self {
        Self {
            last_ts_ns: 0,
            seq_counter: Arc::new(AtomicU64::new(1)),
            tick_store,
            broadcast_tx,
            dedup_order: VecDeque::with_capacity(dedup_capacity),
            dedup_set: HashSet::with_capacity(dedup_capacity),
            dedup_capacity,
            metrics: SequencerMetrics::default(),
        }
    }

    pub fn process_tick(&mut self, mut tick: NormalizedTick) -> Result<u64, SequencerError> {
        // Validação 1: monotonicidade temporal.
        if tick.ts_ns < self.last_ts_ns {
            self.metrics.rejected_out_of_order += 1;
            return Err(SequencerError::OutOfOrder {
                ts_ns: tick.ts_ns,
                last_ts_ns: self.last_ts_ns,
            });
        }

        // Validação 2: deduplicação exata (sem risco de colisão).
        let identity: TickIdentity = (tick.ts_ns, tick.bid, tick.ask);
        if self.dedup_set.contains(&identity) {
            self.metrics.rejected_duplicate += 1;
            return Err(SequencerError::Duplicate {
                ts_ns: tick.ts_ns,
                bid: tick.bid,
                ask: tick.ask,
            });
        }
        if self.dedup_order.len() >= self.dedup_capacity {
            if let Some(old) = self.dedup_order.pop_front() {
                self.dedup_set.remove(&old);
            }
        }
        self.dedup_order.push_back(identity);
        self.dedup_set.insert(identity);

        // Estado de sucesso + atribuição de seq.
        self.last_ts_ns = tick.ts_ns;
        let seq = self.seq_counter.fetch_add(1, Ordering::Relaxed);
        tick.seq = seq;
        self.metrics.accepted += 1;

        // Escrita no ring buffer sob lock síncrono e curtíssimo.
        {
            let mut store = self.tick_store.write().expect("tick_store RwLock poisoned");
            store.push(tick);
        }

        let _ = self.broadcast_tx.send(tick);
        Ok(seq)
    }

    pub fn metrics(&self) -> &SequencerMetrics { &self.metrics }
    pub fn last_timestamp(&self) -> u64 { self.last_ts_ns }
    pub fn current_seq(&self) -> u64 { self.seq_counter.load(Ordering::Relaxed) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Symbol, TickSource};

    fn make_tick(ts_ns: u64, bid: i64) -> NormalizedTick {
        NormalizedTick {
            symbol: Symbol::EurUsd,
            ts_ns,
            bid,
            ask: bid + 10,
            mid: bid + 5,
            volume: None,
            seq: 0,
            source: TickSource::Test,
        }
    }

    #[test]
    fn test_sequencer_success() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        let (tx, _rx) = broadcast::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);
        assert_eq!(seq.process_tick(make_tick(100, 1000)).unwrap(), 1);
        assert_eq!(seq.process_tick(make_tick(200, 1005)).unwrap(), 2);
        assert_eq!(seq.metrics().accepted, 2);
    }

    #[test]
    fn test_sequencer_out_of_order() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        let (tx, _rx) = broadcast::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);
        seq.process_tick(make_tick(200, 1000)).unwrap();
        let res = seq.process_tick(make_tick(100, 1000));
        assert!(matches!(res, Err(SequencerError::OutOfOrder { .. })));
    }

    #[test]
    fn test_sequencer_duplicate() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        let (tx, _rx) = broadcast::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);
        let tick = make_tick(200, 1000);
        seq.process_tick(tick).unwrap();
        let res = seq.process_tick(tick);
        assert!(matches!(res, Err(SequencerError::Duplicate { .. })));
    }
}
