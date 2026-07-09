use std::collections::HashMap;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, instrument, trace};

use common::{NormalizedTick, Timeframe};
use crate::{candle::Candle, candle_store::CandleStore};

/// Delta gerado a cada tick que muta a vela atual (Update).
#[derive(Debug, Clone)]
pub struct CandleDelta {
    pub timeframe: Timeframe,
    pub bucket_ts: u64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: Option<u64>,
}

/// Evento de publicação do agregador para a camada de Broadcast/SMC.
#[derive(Debug, Clone)]
pub enum CandleEvent {
    /// Disparado a cada tick que muta a vela em formação.
    Update(CandleDelta),
    /// Disparado quando o bucket fecha e uma nova vela é iniciada.
    Close { timeframe: Timeframe, candle: Candle },
}

/// Lógica de agregação individual para UM único Timeframe.
///
/// # Nota de arquitetura (vs. versão anterior)
/// Este tipo NÃO é mais embrulhado em `Arc<RwLock<..>>`. Apenas o loop de
/// agregação (single consumer do canal mpsc) toca estes aggregators, então
/// eles vivem como valores owned dentro de um `Vec`. Zero locks, zero spawns,
/// zero alocação por tick no caminho quente. O `fold` é só comparação de i64.
struct TimeframeAggregator {
    timeframe: Timeframe,
    store: CandleStore,
    working_candle: Option<Candle>,
    event_tx: broadcast::Sender<CandleEvent>,
}

impl TimeframeAggregator {
    fn new(timeframe: Timeframe, store: CandleStore, event_tx: broadcast::Sender<CandleEvent>) -> Self {
        Self {
            timeframe,
            store,
            working_candle: None,
            event_tx,
        }
    }

    /// Processa o tick para este timeframe específico.
    /// Retorna `true` se ocorreu o fechamento de uma vela.
    async fn process_tick(&mut self, tick: &NormalizedTick) -> bool {
        let bucket_floor = self.timeframe.bucket_floor_ns(tick.ts_ns);
        let mut candle_closed = false;

        if let Some(mut current) = self.working_candle.take() {
            if bucket_floor > current.t {
                // Tick pertence a um NOVO bucket → fecha o atual.
                trace!(
                    timeframe = ?self.timeframe,
                    old_bucket = current.t,
                    new_bucket = bucket_floor,
                    "Fechando vela por tick"
                );

                self.store.push_closed_candle(current).await;

                let _ = self.event_tx.send(CandleEvent::Close {
                    timeframe: self.timeframe,
                    candle: current,
                });

                candle_closed = true;

                // Nova vela para o novo bucket.
                self.working_candle = Some(Candle::new(bucket_floor, tick));
            } else {
                // Mesmo bucket: muta a vela (fold puro).
                current.fold(tick);
                self.working_candle = Some(current);
            }
        } else {
            // Primeira vela.
            self.working_candle = Some(Candle::new(bucket_floor, tick));
        }

        // Emite o Delta (Update) da vela em formação.
        if let Some(current) = &self.working_candle {
            let delta = CandleDelta {
                timeframe: self.timeframe,
                bucket_ts: current.t,
                high: current.h,
                low: current.l,
                close: current.c,
                volume: current.v,
            };
            let _ = self.event_tx.send(CandleEvent::Update(delta));
        }

        candle_closed
    }

    /// Fecha uma vela por timeout quando um bucket inteiro passa sem ticks
    /// (baixa liquidez), emitindo uma vela FLAT herdando o close anterior.
    ///
    /// # TODO (bug de watchdog — próximo item)
    /// Na versão anterior este método era chamado a cada tick logo após
    /// `process_tick`, o que o tornava CÓDIGO MORTO: como `process_tick` já
    /// havia criado a working candle no bucket do tick, `next_bucket` nunca era
    /// <= `reference_time_ns`. O fix correto é dirigir isto por um
    /// `tokio::time::interval` independente do stream de ticks, fechando UMA
    /// flat por bucket vazio (não pulando buckets intermediários). Mantido aqui
    /// para continuidade, mas NÃO é chamado no caminho quente.
    #[allow(dead_code)]
    async fn check_watchdog(&mut self, reference_time_ns: u64) {
        if let Some(current) = self.working_candle.take() {
            let next_bucket = current.t + self.timeframe.interval_ns();

            if reference_time_ns >= next_bucket {
                debug!(
                    timeframe = ?self.timeframe,
                    bucket = current.t,
                    "Fechamento de vela disparado por watchdog (baixa liquidez)"
                );

                self.store.push_closed_candle(current).await;

                let _ = self.event_tx.send(CandleEvent::Close {
                    timeframe: self.timeframe,
                    candle: current,
                });

                let flat_bucket = self.timeframe.bucket_floor_ns(reference_time_ns);
                self.working_candle = Some(Candle::flat_from_previous(flat_bucket, current.c));
            } else {
                self.working_candle = Some(current);
            }
        }
    }
}

/// Orquestrador do fan-out de Agregação.
///
/// Consome o canal mpsc do Sequencer (single consumer, sem perda) e aplica o
/// tick a cada Timeframe sequencialmente. Sem `tokio::spawn` por tick: a 200
/// ticks/s × 15 TFs, a versão anterior gerava ~3000 spawns/s + locks, o oposto
/// de zero-alloc. O fold é trivial e roda em microssegundos in-line.
pub struct MultiTimeframeAggregator {
    aggregators: Vec<TimeframeAggregator>,
    tick_rx: mpsc::Receiver<NormalizedTick>,
}

impl MultiTimeframeAggregator {
    pub fn new(
        tick_rx: mpsc::Receiver<NormalizedTick>,
        event_tx: broadcast::Sender<CandleEvent>,
        active_timeframes: &[Timeframe],
        candle_capacity: usize,
    ) -> (Self, HashMap<Timeframe, CandleStore>) {
        let mut aggregators = Vec::with_capacity(active_timeframes.len());
        let mut stores = HashMap::new();

        for &tf in active_timeframes {
            let store = CandleStore::new(tf, candle_capacity);
            stores.insert(tf, store.clone());
            aggregators.push(TimeframeAggregator::new(tf, store, event_tx.clone()));
        }

        (Self { aggregators, tick_rx }, stores)
    }

    /// Inicia o processamento contínuo.
    ///
    /// `mpsc::Receiver::recv` retorna `None` quando todos os senders fecham —
    /// esse é o sinal limpo de shutdown. Não existe variante `Lagged`: o ponto
    /// de trocar `broadcast` por `mpsc` é justamente NÃO perder ticks.
    #[instrument(skip(self), name = "multi_tf_aggregator")]
    pub async fn run(&mut self) {
        info!("Iniciando Motor de Agregação Multi-Timeframe...");

        while let Some(tick) = self.tick_rx.recv().await {
            for agg in &mut self.aggregators {
                agg.process_tick(&tick).await;
            }
        }

        info!("Canal de ticks fechado pelo Sequencer. Encerrando agregação.");
    }
}
