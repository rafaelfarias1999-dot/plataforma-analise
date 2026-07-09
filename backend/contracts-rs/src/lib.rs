//! # Contracts RS
//!
//! Crate dedicada à geração e re-exportação dos tipos Protobuf para a plataforma
//! de análise EUR/USD. Serve como fonte única de verdade (Single Source of Truth)
//! para os contratos de comunicação entre backend e frontend.
//!
//! ## Decisão Arquitetural
//!
//! Isolamos a geração Protobuf em uma crate separada seguindo o padrão "Proto Crate":
//! - Compilação dos .proto acontece uma única vez
//! - Todos os outros crates dependem desta crate
//! - Mudanças no schema propagam atomicamente

/// Módulo gerado a partir de `tick.proto`.
/// Contém o `TickEnvelope` — representação normalizada do tick bruto.
pub mod tick {
    include!(concat!(env!("OUT_DIR"), "/tick.rs"));
}

/// Módulo gerado a partir de `market_stream.proto`.
/// Contém as mensagens do protocolo WebSocket (backend → frontend).
pub mod market_stream {
    include!(concat!(env!("OUT_DIR"), "/market_stream.rs"));
}

// === Constantes de Precisão ===

/// Fator de escala para conversão de preço float para micropips.
/// Exemplo: 1.08423 × 1_000_000 = 1_084_230
///
/// Justificativa: representação em inteiros elimina erros de ponto flutuante
/// na construção de candles. A conversão float↔int ocorre APENAS nas bordas
/// do sistema (Feed Handler na entrada, Broadcast Hub na saída).
pub const PRICE_SCALE: i64 = 1_000_000;

/// Converte preço float (ex: 1.08423) para micropips inteiros (1084230).
///
/// # Precisão
/// Usa arredondamento para o inteiro mais próximo para minimizar drift.
#[inline(always)]
pub fn price_to_micropips(price: f64) -> i64 {
    (price * PRICE_SCALE as f64).round() as i64
}

/// Converte micropips inteiros (1084230) de volta para preço float (1.08423).
///
/// # Uso
/// Chamado apenas na borda de saída (serialização para o cliente).
#[inline(always)]
pub fn micropips_to_price(micropips: i64) -> f64 {
    micropips as f64 / PRICE_SCALE as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_roundtrip() {
        let original = 1.08423;
        let micropips = price_to_micropips(original);
        assert_eq!(micropips, 1_084_230);
        let recovered = micropips_to_price(micropips);
        assert!((original - recovered).abs() < 1e-9);
    }

    #[test]
    fn test_price_scale_precision() {
        // Verifica que a escala suporta 6 casas decimais (pip + frações)
        let prices = [1.08423, 1.12345, 0.99999, 1.50000];
        for &p in &prices {
            let mp = price_to_micropips(p);
            let back = micropips_to_price(mp);
            assert!((p - back).abs() < 1e-6, "Falha roundtrip para {}", p);
        }
    }

    #[test]
    fn test_tick_envelope_creation() {
        // Verifica que o tipo gerado pelo prost compila e pode ser instanciado
        let envelope = tick::TickEnvelope {
            symbol: "EURUSD".to_string(),
            ts_ns: 1751990400_123456789,
            bid: price_to_micropips(1.08423),
            ask: price_to_micropips(1.08425),
            mid: price_to_micropips(1.08424),
            volume: Some(100),
            seq: 1,
        };
        assert_eq!(envelope.symbol, "EURUSD");
        assert_eq!(envelope.bid, 1_084_230);
    }

    #[test]
    fn test_market_message_creation() {
        use market_stream::*;
        let delta = CandleDelta {
            symbol: "EURUSD".to_string(),
            timeframe: Timeframe::TfS5 as i32,
            bucket_ts: 1751990400,
            high: price_to_micropips(1.08430),
            low: price_to_micropips(1.08419),
            close: price_to_micropips(1.08424),
            volume: None,
            seq: 84213377,
        };
        let msg = MarketMessage {
            payload: Some(market_message::Payload::CandleDelta(delta)),
        };
        assert!(msg.payload.is_some());
    }
}
