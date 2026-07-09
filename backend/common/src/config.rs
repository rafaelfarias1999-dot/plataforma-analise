use serde::Deserialize;

/// Configuração principal da plataforma.
///
/// Carregável a partir de arquivo TOML. Valores sensatos de default
/// são fornecidos para desenvolvimento rápido.
#[derive(Debug, Clone, Deserialize)]
pub struct PlatformConfig {
    /// Configuração do ring buffer de ticks.
    #[serde(default)]
    pub tick_buffer: TickBufferConfig,

    /// Configuração de rede.
    #[serde(default)]
    pub network: NetworkConfig,

    /// Símbolo principal (default: EURUSD).
    #[serde(default = "default_symbol")]
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TickBufferConfig {
    /// Capacidade do ring buffer de ticks brutos.
    /// Default: 1_000_000 (últimos ~1M ticks).
    #[serde(default = "default_tick_capacity")]
    pub capacity: usize,

    /// Capacidade do ring buffer de candles por timeframe.
    /// Default: 10_000 candles por TF.
    #[serde(default = "default_candle_capacity")]
    pub candle_capacity: usize,

    /// Janela de deduplicação do sequencer (últimos N ticks para hash check).
    /// Default: 1000.
    #[serde(default = "default_dedup_window")]
    pub dedup_window: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    /// Endereço de bind do servidor WebSocket.
    #[serde(default = "default_ws_bind")]
    pub ws_bind_addr: String,

    /// Porta do servidor WebSocket.
    #[serde(default = "default_ws_port")]
    pub ws_port: u16,
}

// Default functions
fn default_symbol() -> String { "EURUSD".to_string() }
fn default_tick_capacity() -> usize { 1_000_000 }
fn default_candle_capacity() -> usize { 10_000 }
fn default_dedup_window() -> usize { 1_000 }
fn default_ws_bind() -> String { "0.0.0.0".to_string() }
fn default_ws_port() -> u16 { 8080 }

impl Default for PlatformConfig {
    fn default() -> Self {
        Self {
            tick_buffer: TickBufferConfig::default(),
            network: NetworkConfig::default(),
            symbol: default_symbol(),
        }
    }
}

impl Default for TickBufferConfig {
    fn default() -> Self {
        Self {
            capacity: default_tick_capacity(),
            candle_capacity: default_candle_capacity(),
            dedup_window: default_dedup_window(),
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            ws_bind_addr: default_ws_bind(),
            ws_port: default_ws_port(),
        }
    }
}

impl PlatformConfig {
    /// Carrega configuração de um arquivo TOML.
    pub fn from_file(path: &str) -> Result<Self, crate::error::ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| crate::error::ConfigError::IoError(e.to_string()))?;
        toml::from_str(&content)
            .map_err(|e| crate::error::ConfigError::ParseError(e.to_string()))
    }

    /// Carrega configuração de uma string TOML.
    pub fn from_str(content: &str) -> Result<Self, crate::error::ConfigError> {
        toml::from_str(content)
            .map_err(|e| crate::error::ConfigError::ParseError(e.to_string()))
    }
}
