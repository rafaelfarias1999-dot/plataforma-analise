use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, instrument, trace};

use common::{NormalizedTick, Timeframe};
use crate::{candle::Candle, candle_store::CandleStore};

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

        // Se já temos uma vela em formação, vamos verificar se devemos fechá-la.
        if let Some(mut current) = self.working_candle.take() {
            if bucket_floor > current.t {
                // Tick pertence a um NOVO bucket. Precisamos fechar o atual.
                trace!(
                    timeframe = ?self.timeframe,
                    old_bucket = current.t,
                    new_bucket = bucket_floor,
                    "Fechando vela por tick"
                );
                
                // Enviar para o store histórico
                self.store.push_closed_candle(current.clone()).await;
                
                // Emitir evento de fechamento
                let _ = self.event_tx.send(CandleEvent::Close {
                    timeframe: self.timeframe,
                    candle: current,
                });
                
                candle_closed = true;

                // Cria nova vela para o novo bucket
                self.working_candle = Some(Candle::new(bucket_floor, tick));
            } else {
                // Mesmo bucket: Muta a vela (fold)
                current.fold(tick);
                self.working_candle = Some(current);
            }
        } else {
            // Não tínhamos vela. Cria a primeira.
            self.working_candle = Some(Candle::new(bucket_floor, tick));
        }

        // Se após o processamento tivermos uma vela trabalhando, enviamos o Delta (Update).
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

    /// Verifica e força o fechamento de uma vela se o tempo limite já passou.
    /// 
    /// O `reference_time_ns` é o tempo mais recente do provedor (last_ts_ns).
    /// Usamos este tempo de referência para não sofrer problemas de clock skew 
    /// com a máquina local. Apenas se o mercado andou pra frente, fechamos a vela.
    async fn check_watchdog(&mut self, reference_time_ns: u64) {
        if let Some(current) = self.working_candle.take() {
            let next_bucket = current.t + self.timeframe.interval_ns();
            
            if reference_time_ns >= next_bucket {
                // A liquidez secou e um tick posterior (talvez de outro par de moeda ou heart-beat)
                // indicou que o tempo já passou. Fechamos a vela atual.
                debug!(
                    timeframe = ?self.timeframe,
                    bucket = current.t,
                    "Fechamento de vela disparado por watchdog (baixa liquidez)"
                );

                self.store.push_closed_candle(current.clone()).await;
                
                let _ = self.event_tx.send(CandleEvent::Close {
                    timeframe: self.timeframe,
                    candle: current,
                });

                // Inicia vela FLAT baseada no fechamento da antiga.
                // O novo tempo de bucket é o bucket a que reference_time_ns pertence.
                let flat_bucket = self.timeframe.bucket_floor_ns(reference_time_ns);
                self.working_candle = Some(Candle::flat_from_previous(flat_bucket, current.c));
            } else {
                // Retorna a vela ao estado de working, pois ainda não deve ser fechada
                self.working_candle = Some(current);
            }
        }
    }
}

/// Orquestrador principal do fan-out de Agregação.
///
/// Assina o canal de ticks do Sequencer e despacha para cada um dos
/// Timeframes configurados em paralelo.
pub struct MultiTimeframeAggregator {
    aggregators: HashMap<Timeframe, Arc<RwLock<TimeframeAggregator>>>,
    tick_rx: broadcast::Receiver<NormalizedTick>,
    event_tx: broadcast::Sender<CandleEvent>,
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
            aggregators.insert(tf, Arc::new(RwLock::new(agg)));
        }

        let instance = Self {
            aggregators,
            tick_rx,
            event_tx,
        };

        (instance, stores)
    }

    /// Inicia o processamento contínuo.
    #[instrument(skip(self), name = "multi_tf_aggregator")]
    pub async fn run(&mut self) {
        info!("Iniciando Motor de Agregação Multi-Timeframe...");

        loop {
            match self.tick_rx.recv().await {
                Ok(tick) => {
                    // Ignora o Timeframe::Tick em si na agregação, pois ele é apenas passthrough.
                    // Distribui para todos os outros timeframes concorrentemente.
                    let mut handles = Vec::new();

                    for (tf, agg_lock) in &self.aggregators {
                        let agg_clone = Arc::clone(agg_lock);
                        let tick_clone = tick; // NormalizedTick is Copy
                        let tf_val = *tf;

                        handles.push(tokio::spawn(async move {
                            let mut agg = agg_clone.write().await;
                            
                            // 1. Processa o Tick e atualiza o estado OHL(C)
                            agg.process_tick(&tick_clone).await;

                            // 2. Aciona o watchdog interno passando o tempo do provedor 
                            //    como referência absoluta para fechamento flat de liquidez
                            agg.check_watchdog(tick_clone.ts_ns).await;
                        }));
                    }

                    // Aguarda processamento do tick em todas as janelas antes de prosseguir
                    for handle in handles {
                        let _ = handle.await;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!("Aggregator não acompanhou o Sequencer e perdeu {} ticks", missed);
                    // Em produção, isso pode indicar a necessidade de reconstrução de estado via re-sync
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!("Canal de ticks fechado pelo Sequencer. Terminando agregação.");
                    break;
                }
            }
        }
    }
}
