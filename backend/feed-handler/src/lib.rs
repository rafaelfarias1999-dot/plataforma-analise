//! # Feed Handler
//!
//! Crate responsável pela ingestão de dados de mercado em tempo real.
//!
//! ## Arquitetura da Pipeline de Ingestão
//!
//! ```text
//! [Provedor Externo]
//!        │
//!        ▼
//!   FeedProvider  (trait — abstrai o conector específico)
//!        │
//!        ▼  RawTick (f64, formato do provedor)
//!   TickNormalizer (fronteira float → micropips)
//!        │
//!        ▼  NormalizedTick (i64 micropips, formato interno)
//!   Sequencer     (ordenação + ring buffer)
//!        │
//!        ▼
//!   [Aggregation Core / Consumidores]
//! ```
//!
//! ## Princípios de Design
//!
//! - **Separação de responsabilidades**: cada módulo tem exatamente uma função.
//! - **Nunca simular preços**: o sistema opera exclusivamente com dados reais.
//! - **Micropips como unidade canônica**: toda aritmética interna usa `i64`,
//!   eliminando erros de ponto flutuante após a normalização.
//! - **Resiliência**: reconexão automática com backoff exponencial.
//! - **Observabilidade**: tracing estruturado em todos os pontos críticos.

pub mod provider;
pub mod normalizer;
pub mod handler;

// Re-exportações para ergonomia — consumidores do crate não precisam
// navegar na hierarquia de módulos para acessar os tipos principais.
pub use provider::{FeedProvider, RawTick};
pub use normalizer::TickNormalizer;
pub use handler::FeedHandlerService;
