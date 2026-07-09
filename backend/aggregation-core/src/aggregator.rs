use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::{debug, info, instrument, trace, warn};

use common::{NormalizedTick, ProviderStatus, Timeframe};
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

/// Configuração do watchdog de fechamento por tempo (baixa liquidez).
#[derive(Debug, Clone, Copy)]
pub struct WatchdogConfig {
    /// Frequência de verificação. Deve ser << menor timeframe ativo.
    pub period: Duration,
    /// Período de tolerância antes de fechar um bucket já expirado.
    /// Absorve jitter de rede e ticks atrasados sem fechar cedo demais.
    pub grace: Duration,
    /// Teto de flats preenchidas por gap. Impede flood do broadcast em
    /// gaps enormes (ex.: fim de semana de mercado fechado).
    pub max_backfill_buckets: usize,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_millis(250),
            grace: Duration::from_millis(200),
            max_backfill_buckets: 5_000,
        }
    }
}

/// Relógio de parede em nanossegundos Unix (referência do watchdog).
#[inline]
fn now_unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Lógica de agregação individual para UM único Timeframe.
///
/// # Nota de arquitetura
/// NÃO é embrulhado em `Arc<RwLock<..>>`. Apenas o loop de agregação (single
/// consumer do canal mpsc) toca estes aggregators, então vivem como valores
/// owned num `Vec`. Zero locks, zero spawns, zero alocação por tick no caminho
/// quente. O `fold` é só comparação de i64.
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

                self.working_candle = Some(Candle::new(bucket_floor, tick));
            } else {
                // Mesmo bucket: muta a vela (fold puro).
                current.fold(tick);
                self.working_candle = Some(current);
            }
        } else {
            self.working_candle = Some(Candle::new(bucket_floor, tick));
        }

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

    /// Fecha buckets expirados por tempo (baixa liquidez), preenchendo UMA
    /// vela FLAT por bucket vazio, herdando o close anterior.
    ///
    /// # Correção (bug #5)
    /// A versão anterior era código morto: chamada logo após `process_tick`,
    /// `next_bucket` nunca era <= referência. Agora é dirigida por um timer
    /// independente (ver `MultiTimeframeAggregator::run`).
    ///
    /// # Integridade
    /// - Sem `working_candle` → não fabrica nada (precisa de ≥1 tick real
    ///   para estabelecer preço).
    /// - `grace_ns` evita fechar cedo demais por jitter.
    /// - `max_backfill` limita flats por gap (evita flood em mercado fechado).
    async fn watchdog_close(&mut self, now_ns: u64, grace_ns: u64, max_backfill: usize) {
        let interval_ns = self.timeframe.interval_ns();
        if interval_ns == 0 {
            return; // Timeframe::Tick não tem buckets.
        }

        let Some(current) = self.working_candle.take() else {
            return; // Nada a fechar, nada a fabricar.
        };

        let bucket_end = current.t + interval_ns;
        if now_ns < bucket_end + grace_ns {
            // Bucket atual ainda em formação (ou dentro do grace). Devolve.
            self.working_candle = Some(current);
            return;
        }

        // 1. Fecha o bucket corrente (tinha ticks reais).
        let prev_close = current.c;
        self.store.push_closed_candle(current).await;
        let _ = self.event_tx.send(CandleEvent::Close {
            timeframe: self.timeframe,
            candle: current,
        });

        debug!(
            timeframe = ?self.timeframe,
            bucket = current.t,
            "Fechamento por watchdog (baixa liquidez)"
        );

        // 2. Preenche cada bucket vazio totalmente expirado com uma flat.
        let cur_bucket = self.timeframe.bucket_floor_ns(now_ns);
        let mut b = current.t + interval_ns;
        let mut filled = 0usize;
        while b < cur_bucket {
            if filled >= max_backfill {
                warn!(
                    timeframe = ?self.timeframe,
                    "Cap de backfill atingido; pulando buckets vazios restantes"
                );
                break;
            }
            let flat = Candle::flat_from_previous(b, prev_close);
            self.store.push_closed_candle(flat).await;
            let _ = self.event_tx.send(CandleEvent::Close {
                timeframe: self.timeframe,
                candle: flat,
            });
            b += interval_ns;
            filled += 1;
        }

        // 3. Nova working candle FLAT no bucket ainda em formação.
        let working = Candle::flat_from_previous(cur_bucket, prev_close);
        self.working_candle = Some(working);
        let _ = self.event_tx.send(CandleEvent::Update(CandleDelta {
            timeframe: self.timeframe,
            bucket_ts: cur_bucket,
            high: working.h,
            low: working.l,
            close: working.c,
            volume: None,
        }));
    }
}

/// Orquestrador do fan-out de Agregação.
///
/// Consome o canal mpsc do Sequencer (single consumer, sem perda) e aplica o
/// tick a cada Timeframe sequencialmente. Um timer paralelo aciona o watchdog
/// de fechamento por tempo — desacoplado do stream de ticks.
pub struct MultiTimeframeAggregator {
    aggregators: Vec<TimeframeAggregator>,
    tick_rx: mpsc::Receiver<NormalizedTick>,
    watchdog: WatchdogConfig,
    /// Opcional: quando presente, o watchdog só preenche flats se o provider
    /// estiver `Connected` (respeita DISCONNECTED — não fabrica preço).
    status_rx: Option<watch::Receiver<ProviderStatus>>,
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

        let instance = Self {
            aggregators,
            tick_rx,
            watchdog: WatchdogConfig::default(),
            status_rx: None,
        };

        (instance, stores)
    }

    /// Ajusta a configuração do watchdog (builder).
    pub fn with_watchdog_config(mut self, cfg: WatchdogConfig) -> Self {
        self.watchdog = cfg;
        self
    }

    /// Conecta o status do provider (do `FeedHandlerService::status_receiver`).
    /// Sem isso, o watchdog roda incondicionalmente (útil em replay/testes).
    pub fn with_status_receiver(mut self, rx: watch::Receiver<ProviderStatus>) -> Self {
        self.status_rx = Some(rx);
        self
    }

    #[inline]
    fn watchdog_enabled(&self) -> bool {
        match &self.status_rx {
            Some(rx) => matches!(*rx.borrow(), ProviderStatus::Connected),
            None => true,
        }
    }

    /// Inicia o processamento contínuo.
    ///
    /// `recv()` retorna `None` quando todos os senders fecham → shutdown limpo.
    #[instrument(skip(self), name = "multi_tf_aggregator")]
    pub async fn run(&mut self) {
        info!("Iniciando Motor de Agregação Multi-Timeframe...");

        let mut ticker = interval(self.watchdog.period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let grace_ns = self.watchdog.grace.as_nanos() as u64;
        let cap = self.watchdog.max_backfill_buckets;

        loop {
            tokio::select! {
                maybe_tick = self.tick_rx.recv() => {
                    match maybe_tick {
                        Some(tick) => {
                            for agg in &mut self.aggregators {
                                agg.process_tick(&tick).await;
                            }
                        }
                        None => {
                            info!("Canal de ticks fechado pelo Sequencer. Encerrando agregação.");
                            break;
                        }
                    }
                }
                _ = ticker.tick() => {
                    if self.watchdog_enabled() {
                        let now_ns = now_unix_ns();
                        for agg in &mut self.aggregators {
                            agg.watchdog_close(now_ns, grace_ns, cap).await;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Symbol, TickSource};

    fn tick_at(ts_ns: u64, mid: i64) -> NormalizedTick {
        NormalizedTick {
            symbol: Symbol::EurUsd,
            ts_ns,
            bid: mid - 5,
            ask: mid + 5,
            mid,
            volume: None,
            seq: 1,
            source: TickSource::Test,
        }
    }

    #[tokio::test]
    async fn test_watchdog_closes_and_backfills_flats() {
        let (event_tx, mut event_rx) = broadcast::channel(256);
        let store = CandleStore::new(Timeframe::S5, 100);
        let mut agg = TimeframeAggregator::new(Timeframe::S5, store, event_tx);

        // Bucket alinhado a 5s. Um tick estabelece a vela em `base`.
        let base = 5_000_000_000u64;
        agg.process_tick(&tick_at(base + 1_000_000_000, 1000)).await;

        // Salta ~16s à frente sem ticks (grace = 0 no teste).
        let now = base + 16_000_000_000;
        agg.watchdog_close(now, 0, 5000).await;

        // Esperado: Close(base) + flats Close(base+5s) e Close(base+10s).
        // base+15s vira working (não fecha). → 3 Close.
        let mut closes = 0;
        while let Ok(ev) = event_rx.try_recv() {
            if let CandleEvent::Close { .. } = ev {
                closes += 1;
            }
        }
        assert_eq!(closes, 3, "deve fechar 1 real + 2 flats intermediárias");

        // A working candle atual é flat no bucket em formação, herdando close.
        let wc = agg.working_candle.expect("working flat");
        assert_eq!(wc.t, base + 15_000_000_000);
        assert_eq!(wc.o, 1000);
        assert_eq!(wc.c, 1000);
        assert_eq!(wc.v, None);
    }

    #[tokio::test]
    async fn test_watchdog_respects_grace_period() {
        let (event_tx, mut event_rx) = broadcast::channel(64);
        let store = CandleStore::new(Timeframe::S5, 100);
        let mut agg = TimeframeAggregator::new(Timeframe::S5, store, event_tx);

        let base = 5_000_000_000u64;
        agg.process_tick(&tick_at(base + 1_000_000_000, 1000)).await;
        let _ = event_rx.try_recv(); // descarta o Update inicial

        // Bucket termina em base+5s; com grace de 500ms, base+5.2s NÃO fecha.
        let now = base + 5_200_000_000;
        let grace = 500_000_000u64;
        agg.watchdog_close(now, grace, 5000).await;

        assert!(agg.working_candle.is_some());
        assert!(matches!(event_rx.try_recv(), Err(_)), "nada deve fechar no grace");
    }
}
