use thiserror::Error;

/// Erros relacionados ao feed de dados (provider/conector).
#[derive(Error, Debug)]
pub enum FeedError {
    #[error("Falha na conexão com o provider: {0}")]
    ConnectionFailed(String),

    #[error("Provider desconectado inesperadamente: {0}")]
    Disconnected(String),

    #[error("Timeout aguardando tick do provider: {0}ms")]
    Timeout(u64),

    #[error("Erro de protocolo do provider: {0}")]
    ProtocolError(String),

    #[error("Tick inválido recebido: {0}")]
    InvalidTick(String),

    #[error("Erro de autenticação: {0}")]
    AuthError(String),
}

/// Erros do Sequencer (validação e ordenação de ticks).
#[derive(Error, Debug)]
pub enum SequencerError {
    #[error("Tick fora de ordem: ts_ns={ts_ns} <= último={last_ts_ns}")]
    OutOfOrder { ts_ns: u64, last_ts_ns: u64 },

    #[error("Tick duplicado detectado: ts_ns={ts_ns}, bid={bid}, ask={ask}")]
    Duplicate { ts_ns: u64, bid: i64, ask: i64 },

    #[error("Falha ao enviar tick para downstream: {0}")]
    BroadcastError(String),
}

/// Erros de configuração.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Erro de I/O ao ler configuração: {0}")]
    IoError(String),

    #[error("Erro ao parsear configuração TOML: {0}")]
    ParseError(String),

    #[error("Valor de configuração inválido: {field} = {value} — {reason}")]
    InvalidValue {
        field: String,
        value: String,
        reason: String,
    },
}

/// Erros de normalização de ticks.
#[derive(Error, Debug)]
pub enum NormalizationError {
    #[error("Preço bid inválido: {0} (deve ser > 0)")]
    InvalidBid(f64),

    #[error("Preço ask inválido: ask={ask} < bid={bid}")]
    InvalidSpread { bid: f64, ask: f64 },

    #[error("Timestamp inválido: {0} (deve ser > 0)")]
    InvalidTimestamp(u64),

    #[error("Símbolo não reconhecido: {0}")]
    UnknownSymbol(String),

    #[error("Spread anormal detectado: {spread_pips:.1} pips (limite: {max_pips:.1})")]
    AbnormalSpread { spread_pips: f64, max_pips: f64 },

    #[error("Timestamp no futuro detectado: tick_ts={tick_ts}, now={now}")]
    FutureTimestamp { tick_ts: u64, now: u64 },
}
