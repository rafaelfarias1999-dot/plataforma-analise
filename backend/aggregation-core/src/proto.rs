//! Conversões dos tipos internos (Candle, CandleDelta, NormalizedTick) para os
//! tipos Protobuf do wire, e helpers que já embrulham num `MarketMessage`.
//!
//! # Sobre `symbol` e `seq`
//! O `CandleDelta` interno não carrega `symbol` nem `seq` (economia no hot path
//! de agregação). Quem serializa (broadcast-hub) conhece o símbolo do shard e
//! mantém um contador `seq` por (symbol, timeframe, bucket) — ambos entram aqui
//! como parâmetro.

use common::proto::timeframe_to_i32;
use common::{NormalizedTick, Timeframe};
use contracts_rs::{market_stream as ms, tick as pt, volume_to_sentinel};

use crate::aggregator::CandleDelta;
use crate::candle::Candle;

/// `NormalizedTick` → `tick.TickEnvelope`.
pub fn tick_to_proto(t: &NormalizedTick) -> pt::TickEnvelope {
    pt::TickEnvelope {
        symbol: t.symbol.as_str().to_string(),
        ts_ns: t.ts_ns,
        bid: t.bid,
        ask: t.ask,
        mid: t.mid,
        volume: t.volume,
        seq: t.seq,
    }
}

/// `Candle` interno → `market_stream.Candle`.
pub fn candle_to_proto(c: &Candle) -> ms::Candle {
    ms::Candle { t: c.t, o: c.o, h: c.h, l: c.l, c: c.c, v: c.v }
}

/// `CandleDelta` interno → `market_stream.CandleDelta` (precisa de symbol/seq).
pub fn candle_delta_to_proto(symbol: &str, seq: u64, d: &CandleDelta) -> ms::CandleDelta {
    ms::CandleDelta {
        symbol: symbol.to_string(),
        timeframe: timeframe_to_i32(d.timeframe),
        bucket_ts: d.bucket_ts,
        high: d.high,
        low: d.low,
        close: d.close,
        volume: d.volume,
        seq,
    }
}

/// `Candle` fechada → `market_stream.CandleClose`.
/// `next_bucket_ts` é derivado do timeframe (fonte única do intervalo).
pub fn candle_close_to_proto(symbol: &str, tf: Timeframe, c: &Candle) -> ms::CandleClose {
    ms::CandleClose {
        symbol: symbol.to_string(),
        timeframe: timeframe_to_i32(tf),
        candle: Some(candle_to_proto(c)),
        next_bucket_ts: c.t + tf.interval_ns(),
    }
}

/// Série de candles → `market_stream.Snapshot` (formato colunar).
/// Volume ausente vira `-1` (sentinela), NUNCA `0` falso.
pub fn snapshot_to_proto(symbol: &str, tf: Timeframe, candles: &[Candle]) -> ms::Snapshot {
    let n = candles.len();
    let mut timestamps = Vec::with_capacity(n);
    let mut opens = Vec::with_capacity(n);
    let mut highs = Vec::with_capacity(n);
    let mut lows = Vec::with_capacity(n);
    let mut closes = Vec::with_capacity(n);
    let mut volumes = Vec::with_capacity(n);

    for c in candles {
        timestamps.push(c.t);
        opens.push(c.o);
        highs.push(c.h);
        lows.push(c.l);
        closes.push(c.c);
        volumes.push(volume_to_sentinel(c.v));
    }

    ms::Snapshot {
        symbol: symbol.to_string(),
        timeframe: timeframe_to_i32(tf),
        count: n as u32,
        timestamps,
        opens,
        highs,
        lows,
        closes,
        volumes,
    }
}

// === Helpers que já embrulham num MarketMessage (pronto pra prost::encode) ===

fn wrap(payload: ms::market_message::Payload) -> ms::MarketMessage {
    ms::MarketMessage { payload: Some(payload) }
}

pub fn tick_message(t: &NormalizedTick) -> ms::MarketMessage {
    wrap(ms::market_message::Payload::Tick(tick_to_proto(t)))
}

pub fn delta_message(symbol: &str, seq: u64, d: &CandleDelta) -> ms::MarketMessage {
    wrap(ms::market_message::Payload::CandleDelta(candle_delta_to_proto(symbol, seq, d)))
}

pub fn close_message(symbol: &str, tf: Timeframe, c: &Candle) -> ms::MarketMessage {
    wrap(ms::market_message::Payload::CandleClose(candle_close_to_proto(symbol, tf, c)))
}

pub fn snapshot_message(symbol: &str, tf: Timeframe, candles: &[Candle]) -> ms::MarketMessage {
    wrap(ms::market_message::Payload::Snapshot(snapshot_to_proto(symbol, tf, candles)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::proto::timeframe_from_i32;

    fn candle(t: u64, o: i64, h: i64, l: i64, c: i64, v: Option<u64>) -> Candle {
        Candle { t, o, h, l, c, v }
    }

    #[test]
    fn test_candle_delta_carries_symbol_and_seq() {
        let d = CandleDelta {
            timeframe: Timeframe::S5,
            bucket_ts: 1_000,
            high: 1_084_300,
            low: 1_084_190,
            close: 1_084_240,
            volume: None,
        };
        let p = candle_delta_to_proto("EURUSD", 42, &d);
        assert_eq!(p.symbol, "EURUSD");
        assert_eq!(p.seq, 42);
        assert_eq!(timeframe_from_i32(p.timeframe), Some(Timeframe::S5));
        assert_eq!(p.volume, None);
    }

    #[test]
    fn test_close_derives_next_bucket() {
        let c = candle(2_000_000_000, 1000, 1010, 990, 1005, Some(7));
        let p = candle_close_to_proto("EURUSD", Timeframe::S5, &c);
        assert_eq!(p.next_bucket_ts, 2_000_000_000 + Timeframe::S5.interval_ns());
        assert_eq!(p.candle.unwrap().v, Some(7));
    }

    #[test]
    fn test_snapshot_volume_sentinel() {
        let candles = [
            candle(1, 10, 12, 9, 11, None),
            candle(2, 11, 13, 10, 12, Some(5)),
        ];
        let s = snapshot_to_proto("EURUSD", Timeframe::M1, &candles);
        assert_eq!(s.count, 2);
        assert_eq!(s.volumes, vec![-1, 5]); // None → -1
        assert_eq!(s.opens, vec![10, 11]);
    }

    #[test]
    fn test_market_message_wrapping() {
        let c = candle(1, 10, 12, 9, 11, None);
        let msg = close_message("EURUSD", Timeframe::M1, &c);
        assert!(matches!(
            msg.payload,
            Some(ms::market_message::Payload::CandleClose(_))
        ));
    }
}
