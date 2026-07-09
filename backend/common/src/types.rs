use std::fmt;

/// Fator de escala de preço (re-exportado de contracts_rs por conveniência).
pub const PRICE_SCALE: i64 = contracts_rs::PRICE_SCALE;

/// Símbolos de pares de moedas suportados pela plataforma.
///
/// # Decisão de Design
/// Usamos um enum tipado em vez de String para:
/// - Validação em tempo de compilação
/// - Zero-allocation na comparação (não aloca String)
/// - Pattern matching exaustivo
///
/// Novos pares são adicionados aqui e o compilador aponta
/// todos os match que precisam ser atualizados (OCP via enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Symbol {
    EurUsd,
}

impl Symbol {
    /// Retorna a representação canônica do símbolo como string.
    pub fn as_str(&self) -> &'static str {
        match self {
            Symbol::EurUsd => "EURUSD",
        }
    }

    /// Tenta converter uma string para o enum Symbol.
    pub fn from_str_symbol(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "EURUSD" | "EUR/USD" | "EUR_USD" => Some(Symbol::EurUsd),
            _ => None,
        }
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Timeframes suportados pela plataforma de análise.
///
/// # Bucketing
/// Cada timeframe define um intervalo em nanossegundos.
/// A função `bucket_floor_ns` calcula o início do bucket para qualquer timestamp:
///   bucket = floor(ts_ns / interval_ns) * interval_ns
///
/// # Decisão Crítica
/// Todos os timeframes derivam do MESMO stream ordenado de ticks.
/// Nunca se agrega TF maior a partir de TF menor já agregado
/// (evita erro de arredondamento acumulado).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Timeframe {
    Tick,
    S5,
    S10,
    S15,
    S30,
    M1,
    M2,
    M3,
    M5,
    M10,
    M15,
    M30,
    H1,
    H2,
    H4,
    D1,
}

impl Timeframe {
    /// Retorna o intervalo do bucket em nanossegundos.
    ///
    /// Tick retorna 0 (passthrough — cada tick É seu próprio "candle").
    pub const fn interval_ns(&self) -> u64 {
        const SECOND_NS: u64 = 1_000_000_000;
        const MINUTE_NS: u64 = 60 * SECOND_NS;
        const HOUR_NS: u64 = 60 * MINUTE_NS;
        match self {
            Timeframe::Tick => 0,
            Timeframe::S5 => 5 * SECOND_NS,
            Timeframe::S10 => 10 * SECOND_NS,
            Timeframe::S15 => 15 * SECOND_NS,
            Timeframe::S30 => 30 * SECOND_NS,
            Timeframe::M1 => MINUTE_NS,
            Timeframe::M2 => 2 * MINUTE_NS,
            Timeframe::M3 => 3 * MINUTE_NS,
            Timeframe::M5 => 5 * MINUTE_NS,
            Timeframe::M10 => 10 * MINUTE_NS,
            Timeframe::M15 => 15 * MINUTE_NS,
            Timeframe::M30 => 30 * MINUTE_NS,
            Timeframe::H1 => HOUR_NS,
            Timeframe::H2 => 2 * HOUR_NS,
            Timeframe::H4 => 4 * HOUR_NS,
            Timeframe::D1 => 24 * HOUR_NS,
        }
    }

    /// Calcula o timestamp de início do bucket para um dado timestamp.
    ///
    /// # Correção Matemática
    /// bucket_floor = floor(ts_ns / interval_ns) × interval_ns
    /// Um tick pertence a EXATAMENTE um bucket por timeframe.
    /// Para Tick, retorna o próprio ts_ns (passthrough).
    pub const fn bucket_floor_ns(&self, ts_ns: u64) -> u64 {
        let interval = self.interval_ns();
        if interval == 0 {
            return ts_ns;
        }
        (ts_ns / interval) * interval
    }

    /// Retorna o nome legível do timeframe.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Timeframe::Tick => "Tick",
            Timeframe::S5 => "5s",
            Timeframe::S10 => "10s",
            Timeframe::S15 => "15s",
            Timeframe::S30 => "30s",
            Timeframe::M1 => "1m",
            Timeframe::M2 => "2m",
            Timeframe::M3 => "3m",
            Timeframe::M5 => "5m",
            Timeframe::M10 => "10m",
            Timeframe::M15 => "15m",
            Timeframe::M30 => "30m",
            Timeframe::H1 => "1h",
            Timeframe::H2 => "2h",
            Timeframe::H4 => "4h",
            Timeframe::D1 => "1D",
        }
    }

    /// Retorna todos os timeframes suportados (exceto Tick).
    pub fn all_candle_timeframes() -> &'static [Timeframe] {
        &[
            Timeframe::S5, Timeframe::S10, Timeframe::S15, Timeframe::S30,
            Timeframe::M1, Timeframe::M2, Timeframe::M3, Timeframe::M5,
            Timeframe::M10, Timeframe::M15, Timeframe::M30,
            Timeframe::H1, Timeframe::H2, Timeframe::H4, Timeframe::D1,
        ]
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Tick normalizado — representação interna de alta performance.
///
/// # Decisão de Memória
/// Todos os campos são tipos primitivos Copy, permitindo armazenamento
/// em arrays contíguos (ring buffers) sem indireção de ponteiro.
/// O tamanho total é 56 bytes (alinhado a cache-line de 64 bytes).
///
/// # Integridade
/// - `bid`, `ask`, `mid`: preços em micropips (i64). NUNCA float.
/// - `volume`: Option<u64> — None quando provider não fornece. NUNCA fabricado.
/// - `seq`: atribuído pelo Sequencer, monotonicamente crescente.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizedTick {
    pub symbol: Symbol,
    pub ts_ns: u64,
    pub bid: i64,
    pub ask: i64,
    pub mid: i64,
    pub volume: Option<u64>,
    pub seq: u64,
    pub source: TickSource,
}

impl Default for NormalizedTick {
    fn default() -> Self {
        Self {
            symbol: Symbol::EurUsd,
            ts_ns: 0,
            bid: 0,
            ask: 0,
            mid: 0,
            volume: None,
            seq: 0,
            source: TickSource::Test,
        }
    }
}

/// Origem do tick — rastreabilidade de proveniência.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TickSource {
    /// Feed ao vivo de provider real.
    Live,
    /// Dados históricos carregados para replay.
    Historical,
    /// Fonte de teste (testes unitários/integração).
    Test,
}

/// Status de conexão do provider de dados.
///
/// # Princípio Inegociável
/// Quando `Disconnected`, o sistema NÃO gera candles sintéticos.
/// A UI deve comunicar explicitamente a ausência de feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderStatus {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting { attempt: u32 },
    Error,
}

impl fmt::Display for ProviderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderStatus::Disconnected => write!(f, "DISCONNECTED"),
            ProviderStatus::Connecting => write!(f, "CONNECTING"),
            ProviderStatus::Connected => write!(f, "CONNECTED"),
            ProviderStatus::Reconnecting { attempt } => write!(f, "RECONNECTING (attempt {})", attempt),
            ProviderStatus::Error => write!(f, "ERROR"),
        }
    }
}
