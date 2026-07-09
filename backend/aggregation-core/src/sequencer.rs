use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use common::{NormalizedTick, SequencerError};
use crate::RingBuffer;

/// Métricas de operação do Sequencer.
#[derive(Debug, Default, Clone)]
pub struct SequencerMetrics {
    pub accepted: u64,
    pub rejected_out_of_order: u64,
    pub rejected_duplicate: u64,
}

/// O Sequencer é o guardião da integridade temporal do sistema.
///
/// # Responsabilidades
/// 1. **Monotonicidade**: Garante que o stream de ticks progrida para frente no tempo.
/// 2. **Deduplicação**: Filtra ticks repetidos em janelas de tempo próximas.
/// 3. **Sequenciamento**: Atribui um identificador único e monotônico a cada tick aceito.
/// 4. **Armazenamento/Broadcast**: Empurra o tick validado para o Tick Store e notifica downstream.
pub struct Sequencer {
    /// O último timestamp aceito.
    last_ts_ns: u64,
    /// Contador monotônico global para ticks aceitos.
    seq_counter: Arc<AtomicU64>,
    /// Armazenamento bruto (Ring Buffer).
    tick_store: Arc<RwLock<RingBuffer<NormalizedTick>>>,
    /// Canal de broadcast para notificar Aggregators downstream.
    broadcast_tx: broadcast::Sender<NormalizedTick>,
    /// Janela de deduplicação (hashes dos últimos ticks).
    dedup_window: VecDeque<u64>,
    /// Capacidade máxima da janela de deduplicação.
    dedup_capacity: usize,
    /// Métricas internas.
    metrics: SequencerMetrics,
}

impl Sequencer {
    /// Cria um novo Sequencer.
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
            dedup_window: VecDeque::with_capacity(dedup_capacity),
            dedup_capacity,
            metrics: SequencerMetrics::default(),
        }
    }

    /// Processa um tick normalizado, aplicando validações e sequenciamento.
    ///
    /// Retorna o número de sequência atribuído em caso de sucesso.
    pub fn process_tick(&mut self, mut tick: NormalizedTick) -> Result<u64, SequencerError> {
        // Validação 1: Monotonicidade (não permitimos voltar no tempo, e ticks com o MESMO ns que não são idênticos)
        // Se ts_ns for menor que last_ts_ns, é fora de ordem.
        if tick.ts_ns < self.last_ts_ns {
            self.metrics.rejected_out_of_order += 1;
            return Err(SequencerError::OutOfOrder {
                ts_ns: tick.ts_ns,
                last_ts_ns: self.last_ts_ns,
            });
        }

        // Validação 2: Deduplicação
        // Calcula um hash rápido para (ts_ns, bid, ask). O volume não faz parte da identidade principal do preço.
        let hash = tick.ts_ns ^ (tick.bid as u64) ^ ((tick.ask as u64).rotate_left(32));
        
        if self.dedup_window.contains(&hash) {
            self.metrics.rejected_duplicate += 1;
            return Err(SequencerError::Duplicate {
                ts_ns: tick.ts_ns,
                bid: tick.bid,
                ask: tick.ask,
            });
        }

        // Atualiza a janela de deduplicação
        if self.dedup_window.len() >= self.dedup_capacity {
            self.dedup_window.pop_front();
        }
        self.dedup_window.push_back(hash);

        // Atualiza estado de sucesso
        self.last_ts_ns = tick.ts_ns;
        
        // Atribui seq (fetch_add retorna o valor antigo, que é perfeitamente sequencial 1, 2, 3...)
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        tick.seq = seq;
        
        self.metrics.accepted += 1;

        // Armazenamento
        // Utilizamos try_write ou bloqueamos brevemente. Como apenas o Sequencer escreve, o lock não será retido.
        {
            // Bloqueio breve apenas para inserir no Ring Buffer (alocação zero).
            // NOTA: Em iteração futura otimizada, remover RwLock em favor de Ring Buffer lock-free.
            let mut store = self.tick_store.blocking_write();
            store.push(tick);
        }

        // Broadcast downstream
        // Ignoramos erro se não houver assinantes (normal no startup).
        let _ = self.broadcast_tx.send(tick);

        Ok(seq)
    }

    /// Retorna as métricas atuais.
    pub fn metrics(&self) -> &SequencerMetrics {
        &self.metrics
    }

    /// Retorna o último timestamp aceito.
    pub fn last_timestamp(&self) -> u64 {
        self.last_ts_ns
    }

    /// Retorna o contador sequencial atual.
    pub fn current_seq(&self) -> u64 {
        self.seq_counter.load(Ordering::Relaxed)
    }
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

        let tick1 = make_tick(100, 1000);
        let s1 = seq.process_tick(tick1).unwrap();
        assert_eq!(s1, 1);

        let tick2 = make_tick(200, 1005);
        let s2 = seq.process_tick(tick2).unwrap();
        assert_eq!(s2, 2);

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
        assert_eq!(seq.metrics().rejected_out_of_order, 1);
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
        assert_eq!(seq.metrics().rejected_duplicate, 1);
    }
}
