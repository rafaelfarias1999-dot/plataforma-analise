use std::time::{SystemTime, UNIX_EPOCH};

use common::{NormalizedTick, NormalizationError, Symbol, TickSource};
use contracts_rs::price_to_micropips;
use tracing::warn;
use crate::provider::RawTick;

const MAX_SPREAD_PIPS: f64 = 50.0;
const MAX_FUTURE_TOLERANCE_NS: u64 = 5_000_000_000; // 5s de tolerância p/ clock skew

/// Normalizador de ticks — fronteira de precisão do sistema (float → micropips).
///
/// # Validações
/// - bid > 0
/// - ask >= bid (spread não negativo)
/// - spread dentro do limite (detecção de anomalia)
/// - timestamp > 0 e não no futuro além da tolerância (detecção de clock skew)
/// - símbolo reconhecido
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

    pub fn with_max_future_tolerance_ns(mut self, ns: u64) -> Self {
        self.max_future_tolerance_ns = ns;
        self
    }

    /// Relógio de parede em ns Unix.
    #[inline]
    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    /// Normaliza usando o relógio real como referência de "agora".
    pub fn normalize(&self, raw: &RawTick) -> Result<NormalizedTick, NormalizationError> {
        self.normalize_with_now(raw, Self::now_ns())
    }

    /// Versão pura e testável: recebe `now_ns` explícito.
    ///
    /// # Correção (bug #6)
    /// A validação de timestamp futuro agora é aplicada de fato. Um tick com
    /// `ts_ns` além de `now_ns + tolerância` é rejeitado (clock skew do provider),
    /// em vez de contaminar a monotonicidade do Sequencer com um timestamp irreal.
    pub fn normalize_with_now(
        &self,
        raw: &RawTick,
        now_ns: u64,
    ) -> Result<NormalizedTick, NormalizationError> {
        let symbol = Symbol::from_str_symbol(&raw.symbol)
            .ok_or_else(|| NormalizationError::UnknownSymbol(raw.symbol.clone()))?;

        if raw.bid <= 0.0 {
            return Err(NormalizationError::InvalidBid(raw.bid));
        }

        if raw.ask < raw.bid {
            return Err(NormalizationError::InvalidSpread { bid: raw.bid, ask: raw.ask });
        }

        let spread_pips = (raw.ask - raw.bid) * 10_000.0; // 1 pip = 0.0001 (EUR/USD)
        if spread_pips > self.max_spread_pips {
            warn!(spread_pips, max_pips = self.max_spread_pips, "Spread anormal detectado");
            return Err(NormalizationError::AbnormalSpread {
                spread_pips,
                max_pips: self.max_spread_pips,
            });
        }

        if raw.timestamp_ns == 0 {
            return Err(NormalizationError::InvalidTimestamp(0));
        }

        // NOVO: rejeita timestamp no futuro além da tolerância (clock skew).
        // `now_ns == 0` significa relógio indisponível → não valida (fail-open p/ não
        // travar ingestão por falha de clock local; a monotonicidade ainda protege).
        if now_ns != 0 && raw.timestamp_ns > now_ns.saturating_add(self.max_future_tolerance_ns) {
            warn!(
                tick_ts = raw.timestamp_ns,
                now = now_ns,
                "Timestamp no futuro além da tolerância; tick rejeitado"
            );
            return Err(NormalizationError::FutureTimestamp {
                tick_ts: raw.timestamp_ns,
                now: now_ns,
            });
        }

        // Conversão float → micropips (FRONTEIRA DE PRECISÃO).
        let bid = price_to_micropips(raw.bid);
        let ask = price_to_micropips(raw.ask);
        let mid = (bid + ask) / 2; // em inteiros, sem acumular erro de float

        Ok(NormalizedTick {
            symbol,
            ts_ns: raw.timestamp_ns,
            bid,
            ask,
            mid,
            volume: raw.volume,
            seq: 0, // atribuído pelo Sequencer
            source: self.source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(ts_ns: u64) -> RawTick {
        RawTick {
            symbol: "EURUSD".to_string(),
            timestamp_ns: ts_ns,
            bid: 1.08420,
            ask: 1.08422,
            volume: None,
        }
    }

    #[test]
    fn test_accepts_present_timestamp() {
        let n = TickNormalizer::new(TickSource::Test);
        let now = 1_000_000_000_000u64;
        assert!(n.normalize_with_now(&raw(now), now).is_ok());
    }

    #[test]
    fn test_accepts_within_tolerance() {
        let n = TickNormalizer::new(TickSource::Test);
        let now = 1_000_000_000_000u64;
        assert!(n.normalize_with_now(&raw(now + 3_000_000_000), now).is_ok());
    }

    #[test]
    fn test_rejects_future_beyond_tolerance() {
        let n = TickNormalizer::new(TickSource::Test);
        let now = 1_000_000_000_000u64;
        let res = n.normalize_with_now(&raw(now + 10_000_000_000), now);
        assert!(matches!(res, Err(NormalizationError::FutureTimestamp { .. })));
    }

    #[test]
    fn test_clock_unavailable_fails_open() {
        let n = TickNormalizer::new(TickSource::Test);
        assert!(n.normalize_with_now(&raw(999), 0).is_ok());
    }
}
