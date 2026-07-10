use std::sync::Arc;
use tokio::sync::{RwLock, watch};
use tracing::{info, warn, error, instrument};

use common::{ProviderStatus, FeedError, TickSource};
use aggregation_core::Sequencer;

use crate::provider::FeedProvider;
use crate::normalizer::TickNormalizer;

/// Parâmetros de reconexão com backoff exponencial.
const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 30_000;
const BACKOFF_MULTIPLIER: f64 = 2.0;
const MAX_RECONNECT_ATTEMPTS: u32 = 50;

/// Serviço principal de ingestão de dados de mercado.
///
/// # Arquitetura
/// Orquestra o loop de ingestão:
/// 1. Conecta ao FeedProvider
/// 2. Consome ticks via next_tick()
/// 3. Normaliza via TickNormalizer (float → micropips)
/// 4. Envia ao Sequencer (validação + ring buffer)
/// 5. Em caso de desconexão, reconecta com backoff exponencial
///
/// # Concorrência
/// Roda como uma task Tokio dedicada. O Sequencer é protegido por
/// Arc<RwLock<>> para permitir acesso concorrente do Aggregator.
pub struct FeedHandlerService {
    provider: Box<dyn FeedProvider>,
    normalizer: TickNormalizer,
    sequencer: Arc<RwLock<Sequencer>>,
    status_tx: watch::Sender<ProviderStatus>,
    status_rx: watch::Receiver<ProviderStatus>,
    max_reconnect_attempts: u32,
}

impl FeedHandlerService {
    pub fn new(
        provider: Box<dyn FeedProvider>,
        sequencer: Arc<RwLock<Sequencer>>,
        source: TickSource,
    ) -> Self {
        let (status_tx, status_rx) = watch::channel(ProviderStatus::Disconnected);
        Self {
            provider,
            normalizer: TickNormalizer::new(source),
            sequencer,
            status_tx,
            status_rx,
            max_reconnect_attempts: MAX_RECONNECT_ATTEMPTS,
        }
    }

    /// Retorna um receiver para observar mudanças de status do feed.
    pub fn status_receiver(&self) -> watch::Receiver<ProviderStatus> {
        self.status_rx.clone()
    }

    /// Inicia o loop de ingestão.
    /// Esta função roda indefinidamente, reconectando em caso de falha.
    #[instrument(skip(self), name = "feed_handler")]
    pub async fn run(&mut self) -> Result<(), FeedError> {
        let mut reconnect_attempts: u32 = 0;
        let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;

        loop {
            // Fase 1: Conectar
            self.update_status(ProviderStatus::Connecting);
            info!(provider = self.provider.provider_name(), "Conectando ao provider...");

            match self.provider.connect().await {
                Ok(()) => {
                    reconnect_attempts = 0;
                    backoff_ms = INITIAL_BACKOFF_MS;
                    self.update_status(ProviderStatus::Connected);
                    info!("Provider conectado com sucesso");
                }
                Err(e) => {
                    error!(error = %e, "Falha ao conectar");
                    reconnect_attempts += 1;
                    if reconnect_attempts >= self.max_reconnect_attempts {
                        error!("Máximo de tentativas de reconexão atingido");
                        self.update_status(ProviderStatus::Error);
                        return Err(e);
                    }
                    self.update_status(ProviderStatus::Reconnecting { attempt: reconnect_attempts });
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = ((backoff_ms as f64) * BACKOFF_MULTIPLIER) as u64;
                    backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
                    continue;
                }
            }

            // Fase 2: Loop de ingestão
            match self.ingest_loop().await {
                Ok(()) => {
                    info!("Stream do provider terminou gracefully");
                    break;
                }
                Err(e) => {
                    warn!(error = %e, "Erro durante ingestão, tentando reconectar...");
                    reconnect_attempts += 1;
                    if reconnect_attempts >= self.max_reconnect_attempts {
                        error!("Máximo de tentativas de reconexão atingido");
                        self.update_status(ProviderStatus::Error);
                        return Err(e);
                    }
                    self.update_status(ProviderStatus::Reconnecting { attempt: reconnect_attempts });
                    let _ = self.provider.disconnect().await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = ((backoff_ms as f64) * BACKOFF_MULTIPLIER) as u64;
                    backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
                }
            }
        }

        Ok(())
    }

    /// Loop interno de ingestão de ticks.
    async fn ingest_loop(&mut self) -> Result<(), FeedError> {
        loop {
            let raw_tick = match self.provider.next_tick().await? {
                Some(tick) => tick,
                None => return Ok(()), // Stream terminou
            };

            // Normalizar tick (float → micropips)
            let normalized = match self.normalizer.normalize(&raw_tick) {
                Ok(tick) => tick,
                Err(e) => {
                    warn!(error = %e, "Tick rejeitado pelo normalizer");
                    continue; // Pula tick inválido, não derruba o loop
                }
            };

            // Enviar ao Sequencer (validação + armazenamento).
            // process_tick agora é async — usa .write().await internamente.
            let mut sequencer = self.sequencer.write().await;
            match sequencer.process_tick(normalized).await {
                Ok(seq) => {
                    tracing::trace!(seq = seq, ts_ns = normalized.ts_ns, "Tick processado");
                }
                Err(e) => {
                    tracing::trace!(error = %e, "Tick rejeitado pelo sequencer");
                    // Ticks fora de ordem ou duplicados são esperados em cenários de
                    // reconexão; logamos mas não derrubamos o loop.
                }
            }
        }
    }

    /// Atualiza o status do feed e notifica observadores.
    fn update_status(&self, status: ProviderStatus) {
        let _ = self.status_tx.send(status);
    }
}
