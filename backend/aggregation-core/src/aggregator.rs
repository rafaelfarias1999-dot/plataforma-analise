use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tracing::{debug, info, instrument, trace};

use common::{NormalizedTick, Timeframe};
use crate::{candle::Candle, candle_store::CandleStore};

/// Intervalo do timer de guarda do watchdog (fechamento de buckets em baixa liquidez).
const WATCHDOG_INTERVAL_MS: u64 = 200;

/// Delta gerado a cada tick modificado (Update).
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
    /// Disparado a cada tick que muta a vela atual.
    Update(CandleDelta),
    /// Disparado quando o tempo da vela termina e uma nova vela é iniciada.
    Close { timeframe: Timeframe, candle: Candle },
}

/// Lógica de agregação individual para UM único Timeframe.
///
/// # Modelo de Concorrência
/// NÃO é mais protegido por `Arc<RwLock>`. Um único consumidor (o
/// `MultiTimeframeAggregator`) chama `process_tick` sequencialmente para
/// todos os timeframes, eliminando o churn de `tokio::spawn` por tick e o
/// não-determinismo de latência associado. `fold` é CPU-trivial.
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

    /// Fecha uma vela: persiste no store histórico e emite o evento de fechamento.
    async fn close_candle(&self, candle: Candle) {
        self.store.push_closed_candle(candle).await;
        let _ = self.event_tx.send(CandleEvent::Close {
            timeframe: self.timeframe,
            candle,
        });
    }

    /// Emite o Delta (Update) da vela em formação atual.
    fn emit_update(&self) {
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
    }

    /// Processa o tick para este timeframe específico (sequencial, sem spawn).
    async fn process_tick(&mut self, tick: &NormalizedTick) {
        let interval_ns = self.timeframe.interval_ns();
        let bucket_floor = self.timeframe.bucket_floor_ns(tick.ts_ns);

        match self.working_candle.take() {
            // Tick pertence a um NOVO bucket → fecha o atual + preenche gaps.
            Some(current) if interval_ns > 0 && bucket_floor > current.t => {
                trace!(
                    timeframe = ?self.timeframe,
                    old_bucket = current.t,
                    new_bucket = bucket_floor,
                    "Fechando vela por tick"
                );

                let last_close = current.c;
                let current_t = current.t;
                self.close_candle(current).await;

                // Continuidade temporal: preenche buckets vazios entre o candle
                // fechado e o novo bucket com velas FLAT herdando o close anterior.
                // NUNCA fabrica preço — apenas propaga o último close conhecido.
                let mut gap_ts = current_t + interval_ns;
                while gap_ts < bucket_floor {
                    self.close_candle(Candle::flat_from_previous(gap_ts, last_close)).await;
                    gap_ts += interval_ns;
                }

                // Cria a nova vela em formação para o bucket atual.
                self.working_candle = Some(Candle::new(bucket_floor, tick));
            }
            // Mesmo bucket (ou timeframe Tick passthrough): muta a vela (fold).
            Some(mut current) => {
                current.fold(tick);
                self.working_candle = Some(current);
            }
            // Não havia vela: cria a primeira.
            None => {
                self.working_candle = Some(Candle::new(bucket_floor, tick));
            }
        }

        self.emit_update();
    }

    /// Verifica e força o fechamento de velas cujo tempo limite já passou,
    /// preenchendo buckets vazios de baixa liquidez.
    ///
    /// # Semântica de Integridade
    /// `reference_time_ns` é o último timestamp do PROVIDER (fonte de verdade).
    /// Durante silêncio real do feed, esse tempo não avança, então nenhuma vela
    /// flat é criada — preservando o princípio de nunca inventar preço.
    async fn check_watchdog(&mut self, reference_time_ns: u64) {
        let interval_ns = self.timeframe.interval_ns();
        if interval_ns == 0 {
            return; // Tick passthrough não tem bucket para fechar.
        }

        if let Some(current) = self.working_candle.take() {
            let next_bucket = current.t + interval_ns;

            if reference_time_ns >= next_bucket {
                debug!(
                    timeframe = ?self.timeframe,
                    bucket = current.t,
                    "Fechamento de vela disparado por watchdog (baixa liquidez)"
                );

                let last_close = current.c;
                let current_t = current.t;
                self.close_candle(current).await;

                // Preenche todos os buckets vazios até o bucket de referência.
                let target_bucket = self.timeframe.bucket_floor_ns(reference_time_ns);
                let mut gap_ts = current_t + interval_ns;
                while gap_ts < target_bucket {
                    self.close_candle(Candle::flat_from_previous(gap_ts, last_close)).await;
                    gap_ts += interval_ns;
                }

                // Inicia vela FLAT no bucket de referência, herdando o close.
                self.working_candle = Some(Candle::flat_from_previous(target_bucket, last_close));
            } else {
                // Ainda não deve fechar: devolve ao estado de working.
                self.working_candle = Some(current);
            }
        }
    }
}

/// Orquestrador principal do fan-out de Agregação.
///
/// Assina o canal de ticks do Sequencer e despacha SEQUENCIALMENTE para cada
/// timeframe. Um timer de guarda dispara o watchdog periodicamente.
pub struct MultiTimeframeAggregator {
    aggregators: HashMap<Timeframe, TimeframeAggregator>,
    tick_rx: broadcast::Receiver<NormalizedTick>,
    /// Último timestamp de referência do provider (para o watchdog).
    last_ref_ns: u64,
}

impl MultiTimeframeAggregator {
    pub fn new(
        tick_rx: broadcast::Receiver<NormalizedTick>,
        event_tx: broadcast::Sender<CandleEvent>,
        active_timeframes: &[Timeframe],
        candle_capacity: usize,
    ) -> (Self, HashMap<Timeframe, CandleStore>) {
        let mut aggregators = HashMap::new();
        let mut stores = HashMap::new();

        for &tf in active_timeframes {
            let store = CandleStore::new(tf, candle_capacity);
            stores.insert(tf, store.clone());

            let agg = TimeframeAggregator::new(tf, store, event_tx.clone());
            aggregators.insert(tf, agg);
        }

        let instance = Self {
            aggregators,
            tick_rx,
            last_ref_ns: 0,
        };

        (instance, stores)
    }

    /// Inicia o processamento contínuo.
    #[instrument(skip(self), name = "multi_tf_aggregator")]
    pub async fn run(&mut self) {
        info!("Iniciando Motor de Agregação Multi-Timeframe...");

        let mut guard = interval(Duration::from_millis(WATCHDOG_INTERVAL_MS));

        loop {
            tokio::select! {
                // Novo tick: processa sequencialmente em todos os timeframes.
                result = self.tick_rx.recv() => match result {
                    Ok(tick) => {
                        for agg in self.aggregators.values_mut() {
                            agg.process_tick(&tick).await;
                        }
                        if tick.ts_ns > self.last_ref_ns {
                            self.last_ref_ns = tick.ts_ns;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        // Integridade: NÃO ignoramos silenciosamente. Um gap no
                        // stream corrompe os candles → sinalizamos re-sync.
                        // TODO: acionar reconstrução de estado a partir do Tick Store.
                        tracing::warn!(
                            missed,
                            "Aggregator não acompanhou o Sequencer — re-sync de estado necessário"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!("Canal de ticks fechado pelo Sequencer. Terminando agregação.");
                        break;
                    }
                },
                // Timer de guarda: fecha buckets vazios em baixa liquidez.
                _ = guard.tick() => {
                    for agg in self.aggregators.values_mut() {
                        agg.check_watchdog(self.last_ref_ns).await;
                    }
                }
            }
        }
    }
}
