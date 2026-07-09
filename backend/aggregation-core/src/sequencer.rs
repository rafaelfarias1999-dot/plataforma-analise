use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

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
/// 1. **Monotonicidade**: garante que o stream progrida para frente no tempo.
/// 2. **Deduplicação**: filtra ticks repetidos em janelas próximas.
/// 3. **Sequenciamento**: atribui um seq único e monotônico a cada tick aceito.
/// 4. **Armazenamento/Envio**: empurra ao Tick Store e envia downstream com
///    backpressure (sem perda de ticks).
///
/// # Correção crítica (vs. versão anterior)
/// - `process_tick` agora é `async` e usa `tick_store.write().await`. A versão
///   anterior chamava `blocking_write()` DENTRO de uma task Tokio, o que causa
///   panic imediato (e, com `panic = "abort"`, derruba o processo no 1º tick).
/// - O canal downstream passou de `broadcast` (lossy) para `mpsc` (com
///   backpressure). Um aggregator lento agora aplica pressão de volta em vez de
///   descartar ticks silenciosamente — o que corromperia o OHLC. Integridade > throughput.
pub struct Sequencer {
    /// O último timestamp aceito.
    last_ts_ns: u64,
    /// Contador monotônico global para ticks aceitos.
    seq_counter: Arc<AtomicU64>,
    /// Armazenamento bruto (Ring Buffer).
    tick_store: Arc<RwLock<RingBuffer<NormalizedTick>>>,
    /// Canal mpsc para o Aggregator downstream (backpressure, sem perda).
    tick_tx: mpsc::Sender<NormalizedTick>,
    /// Janela de deduplicação (hashes dos últimos ticks).
    dedup_window: VecDeque<u64>,
    /// Capacidade máxima da janela de deduplicação.
    dedup_capacity: usize,
    /// Métricas internas.
    metrics: SequencerMetrics,
}

impl Sequencer {
    /// Cria um novo Sequencer.
    ///
    /// O `tick_tx` deve ser a ponta de escrita de um `mpsc::channel`. Dimensione
    /// a capacidade do canal para absorver rajadas sem estourar memória — quando
    /// cheio, `process_tick` aguarda (backpressure) em vez de descartar.
    pub fn new(
        tick_store: Arc<RwLock<RingBuffer<NormalizedTick>>>,
        tick_tx: mpsc::Sender<NormalizedTick>,
        dedup_capacity: usize,
    ) -> Self {
        Self {
            last_ts_ns: 0,
            seq_counter: Arc::new(AtomicU64::new(1)),
            tick_store,
            tick_tx,
            dedup_window: VecDeque::with_capacity(dedup_capacity),
            dedup_capacity,
            metrics: SequencerMetrics::default(),
        }
    }

    /// Processa um tick normalizado, aplicando validações e sequenciamento.
    ///
    /// Retorna o número de sequência atribuído em caso de sucesso.
    pub async fn process_tick(&mut self, mut tick: NormalizedTick) -> Result<u64, SequencerError> {
        // Validação 1: Monotonicidade (não permitimos voltar no tempo).
        if tick.ts_ns < self.last_ts_ns {
            self.metrics.rejected_out_of_order += 1;
            return Err(SequencerError::OutOfOrder {
                ts_ns: tick.ts_ns,
                last_ts_ns: self.last_ts_ns,
            });
        }

        // Validação 2: Deduplicação.
        // Hash rápido de (ts_ns, bid, ask). Volume não faz parte da identidade do preço.
        let hash = tick.ts_ns ^ (tick.bid as u64) ^ ((tick.ask as u64).rotate_left(32));

        if self.dedup_window.contains(&hash) {
            self.metrics.rejected_duplicate += 1;
            return Err(SequencerError::Duplicate {
                ts_ns: tick.ts_ns,
                bid: tick.bid,
                ask: tick.ask,
            });
        }

        // Atualiza a janela de deduplicação.
        if self.dedup_window.len() >= self.dedup_capacity {
            self.dedup_window.pop_front();
        }
        self.dedup_window.push_back(hash);

        // Estado de sucesso.
        self.last_ts_ns = tick.ts_ns;

        // Atribui seq (fetch_add retorna o valor antigo: 1, 2, 3...).
        let seq = self.seq_counter.fetch_add(1, Ordering::SeqCst);
        tick.seq = seq;

        self.metrics.accepted += 1;

        // Armazenamento no Ring Buffer bruto.
        // Escopo explícito: NÃO seguramos o guard através do `.await` de envio.
        {
            let mut store = self.tick_store.write().await;
            store.push(tick);
        }

        // Envio downstream com backpressure. Se o canal fechou, o Aggregator
        // morreu — isso é um erro real, não um tick a ignorar.
        self.tick_tx
            .send(tick)
            .await
            .map_err(|_| SequencerError::BroadcastError("canal de agregação fechado".to_string()))?;

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

    #[tokio::test]
    async fn test_sequencer_success() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        // Mantemos `_rx` vivo: se o receiver for dropado, o send falha.
        let (tx, _rx) = mpsc::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);

        let s1 = seq.process_tick(make_tick(100, 1000)).await.unwrap();
        assert_eq!(s1, 1);

        let s2 = seq.process_tick(make_tick(200, 1005)).await.unwrap();
        assert_eq!(s2, 2);

        assert_eq!(seq.metrics().accepted, 2);
    }

    #[tokio::test]
    async fn test_sequencer_out_of_order() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        let (tx, _rx) = mpsc::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);

        seq.process_tick(make_tick(200, 1000)).await.unwrap();

        let res = seq.process_tick(make_tick(100, 1000)).await;
        assert!(matches!(res, Err(SequencerError::OutOfOrder { .. })));
        assert_eq!(seq.metrics().rejected_out_of_order, 1);
    }

    #[tokio::test]
    async fn test_sequencer_duplicate() {
        let store = Arc::new(RwLock::new(RingBuffer::new(100)));
        let (tx, _rx) = mpsc::channel(16);
        let mut seq = Sequencer::new(store, tx, 10);

        let tick = make_tick(200, 1000);
        seq.process_tick(tick).await.unwrap();

        let res = seq.process_tick(tick).await;
        assert!(matches!(res, Err(SequencerError::Duplicate { .. })));
        assert_eq!(seq.metrics().rejected_duplicate, 1);
    }
}
