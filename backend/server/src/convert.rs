//! Conversões dos tipos internos (aggregation-core) para os contratos Protobuf.
//! Esta é a fronteira de saída: aqui o estado interno vira mensagem de rede.

use aggregation_core::{Candle, CandleEvent};
use common::Timeframe;
use contracts_rs::market_stream as ms;

/// Sentinela de "sem volume" no formato colunar do Snapshot (int64).
const NO_VOLUME_SENTINEL: i64 = -1;

/// Mapeia o Timeframe interno para o enum Protobuf.
pub fn tf_to_proto(tf: Timeframe) -> i32 {
    use ms::Timeframe as P;
    let v = match tf {
        Timeframe::Tick => P::TfTick,
        Timeframe::S5 => P::TfS5,
        Timeframe::S10 => P::TfS10,
        Timeframe::S15 => P::TfS15,
        Timeframe::S30 => P::TfS30,
        Timeframe::M1 => P::TfM1,
        Timeframe::M2 => P::TfM2,
        Timeframe::M3 => P::TfM3,
        Timeframe::M5 => P::TfM5,
        Timeframe::M10 => P::TfM10,
        Timeframe::M15 => P::TfM15,
        Timeframe::M30 => P::TfM30,
        Timeframe::H1 => P::TfH1,
        Timeframe::H2 => P::TfH2,
        Timeframe::H4 => P::TfH4,
        Timeframe::D1 => P::TfD1,
    };
    v as i32
}

/// Converte um Candle interno para o Candle Protobuf.
pub fn candle_to_proto(c: &Candle) -> ms::Candle {
    ms::Candle {
        t: c.t,
        o: c.o,
        h: c.h,
        l: c.l,
        c: c.c,
        v: c.v,
    }
}

/// Converte um CandleEvent do agregador em MarketMessage pronto para o wire.
pub fn event_to_message(ev: &CandleEvent, symbol: &str) -> ms::MarketMessage {
    let payload = match ev {
        CandleEvent::Update(d) => ms::market_message::Payload::CandleDelta(ms::CandleDelta {
            symbol: symbol.to_string(),
            timeframe: tf_to_proto(d.timeframe),
            bucket_ts: d.bucket_ts,
            high: d.high,
            low: d.low,
            close: d.close,
            volume: d.volume,
            // TODO: propagar seq do tick através do delta para dedup no cliente.
            seq: 0,
        }),
        CandleEvent::Close { timeframe, candle } => {
            let next_bucket_ts = candle.t + timeframe.interval_ns();
            ms::market_message::Payload::CandleClose(ms::CandleClose {
                symbol: symbol.to_string(),
                timeframe: tf_to_proto(*timeframe),
                candle: Some(candle_to_proto(candle)),
                next_bucket_ts,
            })
        }
    };

    ms::MarketMessage {
        payload: Some(payload),
    }
}

/// Monta o Snapshot colunar a partir de uma fatia de candles fechados.
pub fn snapshot_to_message(symbol: &str, tf: Timeframe, candles: &[Candle]) -> ms::MarketMessage {
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
        // -1 = sem volume (equivalente a None). NUNCA fabricamos 0.
        volumes.push(c.v.map(|v| v as i64).unwrap_or(NO_VOLUME_SENTINEL));
    }

    let snap = ms::Snapshot {
        symbol: symbol.to_string(),
        timeframe: tf_to_proto(tf),
        count: n as u32,
        timestamps,
        opens,
        highs,
        lows,
        closes,
        volumes,
    };

    ms::MarketMessage {
        payload: Some(ms::market_message::Payload::Snapshot(snap)),
    }
}