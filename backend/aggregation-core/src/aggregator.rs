use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use tokio::time::{self, Duration};
use tracing::{debug, info, instrument, trace};

use common::{NormalizedTick, Timeframe};
use crate::{candle::Candle, candle_store::CandleStore};

#[derive(Debug, Clone)]
pub struct CandleDelta {
    pub timeframe: Timeframe,
    pub bucket_ts: u64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum CandleEvent {
    Update(CandleDelta),
    Close { timeframe: Timeframe, candle: Candle },
}

struct TimeframeAggregator {
    timeframe: Timeframe,
    store: CandleStore,
    working_candle: Option<Candle>,
    event_tx: broadcast::Sender<CandleEvent>,
}

impl TimeframeAggregator {
    fn new(timeframe: Timeframe, store: CandleStore, event_tx: broadcast::Sender<CandleEvent>) -> Self {
        Self { timeframe, store, working_candle: None, event_tx }
    }

    async fn close_candle(&mut self, candle: Candle) {
        self.store.push_closed_candle(candle).await;
        let _ = self.event_tx.send(CandleEvent::Close { timeframe: self.timeframe, candle });
    }

    fn emit_update(&self, c: &Candle) {
        let _ = self.event_tx.send(CandleEvent::Update(CandleDelta {
            timeframe: self.timeframe,
            bucket_ts: c.t,
            high: c.h,
            low: c.l,
            close: c.c,
            volume: c.v,
        }));
    }

    async fn process_tick(&mut self, tick: &NormalizedTick) {
        let bucket_floor = self.timeframe.bucket_floor_ns(tick.ts_ns);
        match self.working_candle.take() {
            Some(mut current) => {
                if bucket_floor > current.t {
                    trace!(timeframe = ?self.timeframe, old = current.t, new = bucket_floor, "Fechando vela por tick");
                    self.close_candle(current).await;
                    self.working_candle = Some(Candle::new(bucket_floor, tick));
                } else if bucket_floor == current.t {
                    current.fold(tick);
                    self.working_candle = Some(current);
                } else {
                    // Bucket anterior (não deveria ocorrer pós-Sequencer): devolve intacta.
                    self.working_candle = Some(current);
                    return;
                }
            }
            None => self.working_candle = Some(Candle::new(bucket_floor, tick)),
        }
        if let Some(c) = &self.working_candle {
            self.emit_update(c);
        }
    }

    /// Fecha o bucket quando o tempo de PAREDE expira sem ticks (baixa liquidez).
    /// Abre uma única vela flat no bucket atual — nunca sintetiza preço nem
    /// preenche gaps enormes (ex: fim de semana no FX).
    async fn check_watchdog(&mut self, now_ns: u64) {
        let interval = self.timeframe.interval_ns();
        if interval == 0 { return; }
        if let Some(current) = self.working_candle.take() {
            if now_ns >= current.t + interval {
                debug!(timeframe = ?self.timeframe, bucket = current.t, "Fechamento por watchdog (sem liquidez)");
                let prev_close = current.c;
                self.close_candle(current).await;
                let flat_bucket = self.timeframe.bucket_floor_ns(now_ns);
                let flat = Candle::flat_from_previous(flat_bucket, prev_close);
                self.emit_update(&flat);
                self.working_candle = Some(flat);
            } else {
                self.working_candle = Some(current);
            }
        }
    }
}

pub struct MultiTimeframeAggregator {
    aggregators: HashMap<Timeframe, TimeframeAggregator>,
    tick_rx: broadcast::Receiver<NormalizedTick>,
    watchdog_period: Duration,
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
            aggregators.insert(tf, TimeframeAggregator::new(tf, store, event_tx.clone()));
        }
        (Self { aggregators, tick_rx, watchdog_period: Duration::from_millis(250) }, stores)
    }

    fn now_ns() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
    }

    #[instrument(skip(self), name = "multi_tf_aggregator")]
    pub async fn run(&mut self) {
        info!("Iniciando Motor de Agregação Multi-Timeframe...");
        let mut watchdog = time::interval(self.watchdog_period);
        loop {
            tokio::select! {
                recv = self.tick_rx.recv() => match recv {
                    Ok(tick) => {
                        // Sequencial, sem spawn, sem lock, zero alocação no hot path.
                        for agg in self.aggregators.values_mut() {
                            agg.process_tick(&tick).await;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::warn!("Aggregator perdeu {} ticks. Migrar p/ mpsc com backpressure (ver nota #4).", missed);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        tracing::error!("Canal de ticks fechado. Terminando agregação.");
                        break;
                    }
                },
                _ = watchdog.tick() => {
                    let now = Self::now_ns();
                    for agg in self.aggregators.values_mut() {
                        agg.check_watchdog(now).await;
                    }
                }
            }
        }
    }
}
