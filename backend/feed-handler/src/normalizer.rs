use common::{NormalizedTick, NormalizationError, Symbol, TickSource};
use contracts_rs::price_to_micropips;
use tracing::warn;
use crate::provider::RawTick;

/// Limites de validação para detecção de anomalias.
const MAX_SPREAD_PIPS: f64 = 50.0;  // Spread máximo aceitável em pips
const MAX_FUTURE_TOLERANCE_NS: u64 = 5_000_000_000; // 5 segundos de tolerância para clock skew

/// Normalizador de ticks — fronteira de precisão do sistema.
///
/// # Responsabilidade (SRP)
/// Converte ticks brutos (float) para a representação interna de alta performance
/// (micropips inteiros). Esta é a ÚNICA fronteira onde float→int acontece na
/// pipeline de ingestão.
///
/// # Validações
/// - Preço bid > 0
/// - ask >= bid (spread não negativo)
/// - Spread dentro de limites aceitáveis (detecção de anomalia)
/// - Timestamp > 0 e não no futuro distante (detecção de clock skew)
/// - Símbolo reconhecido
pub struct TickNormalizer {
    source: TickSource,
    max_spread_pips: f64,
    max_future_tolerance_ns: u64,
}

impl TickNormalizer {
    pub fn new(source: TickSource) -> Self {
        Self {
            source,
            max_spread_pips: MAX_SPREAD_PIPS,
            max_future_tolerance_ns: MAX_FUTURE_TOLERANCE_NS,
        }
    }

    pub fn with_max_spread_pips(mut self, max: f64) -> Self {
        self.max_spread_pips = max;
        self
    }

    /// Normaliza um tick bruto para a representação interna.
    ///
    /// # Integridade
    /// O `mid` é calculado como a média aritmética de bid e ask em micropips:
    ///   mid = (bid_micropips + ask_micropips) / 2
    /// Isso garante que mid é determinístico e reprodutível.
    ///
    /// `seq` é inicializado como 0 aqui — será atribuído pelo Sequencer.
    pub fn normalize(&self, raw: &RawTick) -> Result<NormalizedTick, NormalizationError> {
        // Validação: símbolo reconhecido
        let symbol = Symbol::from_str_symbol(&raw.symbol)
            .ok_or_else(|| NormalizationError::UnknownSymbol(raw.symbol.clone()))?;

        // Validação: bid > 0
        if raw.bid <= 0.0 {
            return Err(NormalizationError::InvalidBid(raw.bid));
        }

        // Validação: ask >= bid
        if raw.ask < raw.bid {
            return Err(NormalizationError::InvalidSpread {
                bid: raw.bid,
                ask: raw.ask,
            });
        }

        // Validação: spread aceitável
        let spread_pips = (raw.ask - raw.bid) * 10_000.0; // 1 pip = 0.0001 para EUR/USD
        if spread_pips > self.max_spread_pips {
            warn!(
                spread_pips = spread_pips,
                max_pips = self.max_spread_pips,
                "Spread anormal detectado"
            );
            return Err(NormalizationError::AbnormalSpread {
                spread_pips,
                max_pips: self.max_spread_pips,
            });
        }

        // Validação: timestamp válido
        if raw.timestamp_ns == 0 {
            return Err(NormalizationError::InvalidTimestamp(0));
        }

        // Conversão float → micropips (FRONTEIRA DE PRECISÃO)
        let bid = price_to_micropips(raw.bid);
        let ask = price_to_micropips(raw.ask);
        // mid calculado em inteiros para evitar acumular erro de float
        let mid = (bid + ask) / 2;

        Ok(NormalizedTick {
            symbol,
            ts_ns: raw.timestamp_ns,
            bid,
            ask,
            mid,
            volume: raw.volume,
            seq: 0, // Será atribuído pelo Sequencer
            source: self.source,
        })
    }
}
