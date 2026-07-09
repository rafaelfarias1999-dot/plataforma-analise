//! # Módulo Provider
//!
//! Define a interface abstrata para conectores de dados de mercado
//! e o tipo de tick bruto (`RawTick`) que eles produzem.
//!
//! ## Princípio de Extensibilidade (OCP)
//!
//! Cada provedor (ex: broker X, broker Y, feed CSV para backtesting)
//! implementa a trait [`FeedProvider`]. O [`FeedHandlerService`] opera
//! sobre a trait — nunca sobre implementações concretas.
//!
//! Trocar de provedor = trocar a implementação injetada, sem recompilar
//! ou alterar o handler.

use async_trait::async_trait;
use common::error::FeedError;
use common::types::ProviderStatus;

/// Tick bruto do provedor — antes de qualquer normalização.
///
/// # Decisão de Design
///
/// Mantemos os preços como `f64` aqui porque é o formato que provedores
/// externos entregam (JSON, FIX, WebSocket). A conversão para micropips
/// (`i64`) acontece exclusivamente no [`TickNormalizer`], que é a fronteira
/// de precisão do sistema.
///
/// ## Campo `volume`
///
/// `volume` é `Option<u64>` porque a grande maioria dos provedores Forex
/// **não fornece volume real** (apenas tick volume ou nada). NUNCA fabricamos
/// volume — `None` é preservado fielmente por toda a pipeline.
///
/// ## Campo `symbol`
///
/// É `String` (não `Symbol`) porque o provedor pode enviar qualquer texto.
/// A validação e conversão para o enum `Symbol` ocorre na normalização.
#[derive(Debug, Clone)]
pub struct RawTick {
    /// Identificador textual do instrumento (ex: "EURUSD", "EUR/USD").
    /// Será mapeado para `Symbol` pelo normalizer.
    pub symbol: String,

    /// Timestamp em nanossegundos desde Unix epoch.
    /// Provedores que entregam millisegundos devem ser convertidos antes.
    pub timestamp_ns: u64,

    /// Preço bid (melhor oferta de compra) em formato float do provedor.
    pub bid: f64,

    /// Preço ask (melhor oferta de venda) em formato float do provedor.
    pub ask: f64,

    /// Volume negociado, se disponível.
    /// `None` quando o provedor não fornece essa informação.
    pub volume: Option<u64>,
}

/// Interface abstrata para conectores de dados de mercado.
///
/// # Contrato de Integridade
///
/// Implementações **DEVEM** retornar apenas dados reais do provedor.
/// **Simulação de preços é PROIBIDA.** Se a conexão cair, retorne
/// [`FeedError::Disconnected`]. Se houver timeout, retorne
/// [`FeedError::Timeout`].
///
/// # Ciclo de Vida
///
/// ```text
/// Disconnected → connect() → Connected → next_tick()* → disconnect() → Disconnected
///                    ↑                        │
///                    └── Reconnecting ←───────┘ (em caso de erro)
/// ```
///
/// # Requisitos de Thread Safety
///
/// A trait exige `Send + Sync` para permitir que o handler mova o provider
/// entre tasks Tokio e compartilhe referências quando necessário.
#[async_trait]
pub trait FeedProvider: Send + Sync {
    /// Estabelece conexão com o provedor de dados.
    ///
    /// # Erros
    ///
    /// - [`FeedError::ConnectionFailed`] se a conexão não puder ser estabelecida.
    /// - [`FeedError::AuthError`] se as credenciais forem inválidas.
    /// - [`FeedError::Timeout`] se o provedor não responder a tempo.
    async fn connect(&mut self) -> Result<(), FeedError>;

    /// Aguarda e retorna o próximo tick do provedor.
    ///
    /// Esta função é **blocking** no sentido assíncrono — ela aguarda
    /// até que um novo tick esteja disponível ou ocorra um erro.
    ///
    /// # Retorno
    ///
    /// - `Ok(Some(tick))` — tick recebido com sucesso.
    /// - `Ok(None)` — o stream do provedor terminou gracefully
    ///   (ex: fim de dados históricos, desconexão limpa).
    /// - `Err(FeedError)` — erro de conexão, timeout ou protocolo.
    async fn next_tick(&mut self) -> Result<Option<RawTick>, FeedError>;

    /// Desconecta do provedor de dados de forma limpa.
    ///
    /// Implementações devem liberar recursos (sockets, handles) e
    /// garantir que o provedor possa ser reconectado posteriormente.
    async fn disconnect(&mut self) -> Result<(), FeedError>;

    /// Retorna o status atual da conexão.
    ///
    /// Este método é síncrono e barato — usado pelo handler para
    /// decisões de reconexão e por observadores para telemetria.
    fn status(&self) -> ProviderStatus;

    /// Retorna o nome identificador do provedor (para logging/telemetria).
    ///
    /// Deve ser um identificador estável e legível (ex: "binance-ws",
    /// "dukascopy-csv", "oanda-rest").
    fn provider_name(&self) -> &str;
}
