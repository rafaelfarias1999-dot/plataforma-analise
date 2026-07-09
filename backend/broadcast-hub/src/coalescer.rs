use std::collections::HashMap;

use aggregation_core::proto::{close_message, delta_message};
use aggregation_core::{Candle, CandleDelta, CandleEvent};
use common::Timeframe;
use contracts_rs::market_stream::MarketMessage;

pub struct Coalescer {
    symbol: String,
    pending_deltas: HashMap<(Timeframe, u64), CandleDelta>,
    pending_closes: Vec<(Timeframe, Candle)>,
    seq: HashMap<(Timeframe, u64), u64>,
}

impl Coalescer {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            pending_deltas: HashMap::new(),
            pending_closes: Vec::new(),
            seq: HashMap::new(),
        }
    }

    /// Absorve um evento na janela atual.
    pub fn push(&mut self, ev: CandleEvent) {
        match ev {
            // Último delta vence para o bucket.
            CandleEvent::Update(d) => {
                self.pending_deltas.insert((d.timeframe, d.bucket_ts), d);
            }
            // Close nunca é descartado; delta pendente do bucket vira redundante.
            CandleEvent::Close { timeframe, candle } => {
                let key = (timeframe, candle.t);
                self.pending_deltas.remove(&key);
                self.seq.remove(&key);
                self.pending_closes.push((timeframe, candle));
            }
        }
    }

    /// Esvazia a janela: Closes (cronológicos) primeiro, depois deltas coalescidos.
    pub fn drain_frame(&mut self) -> Vec<MarketMessage> {
        let mut out =
            Vec::with_capacity(self.pending_closes.len() + self.pending_deltas.len());

        for (tf, candle) in self.pending_closes.drain(..) {
            out.push(close_message(&self.symbol, tf, &candle));
        }

        let mut deltas: Vec<((Timeframe, u64), CandleDelta)> =
            self.pending_deltas.drain().collect();
        deltas.sort_by_key(|((tf, bucket), _)| (*tf as u16, *bucket));

        for ((tf, bucket), d) in deltas {
            let s = self.seq.entry((tf, bucket)).or_insert(0);
            *s += 1;
            out.push(delta_message(&self.symbol, *s, &d));
        }

        out
    }

    /// Descarta pendências (usado no resync por snapshot). `seq` é preservado.
    pub fn reset(&mut self) {
        self.pending_deltas.clear();
        self.pending_closes.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_rs::market_stream::market_message::Payload;

    fn delta(tf: Timeframe, bucket: u64, close: i64) -> CandleEvent {
        CandleEvent::Update(CandleDelta {
            timeframe: tf, bucket_ts: bucket,
            high: close + 5, low: close - 5, close, volume: None,
        })
    }
    fn candle(t: u64, c: i64) -> Candle {
        Candle { t, o: c, h: c + 5, l: c - 5, c, v: None }
    }

    #[test]
    fn test_deltas_coalesce_last_wins() {
        let mut co = Coalescer::new("EURUSD");
        co.push(delta(Timeframe::S5, 1000, 100));
        co.push(delta(Timeframe::S5, 1000, 101));
        co.push(delta(Timeframe::S5, 1000, 102));
        let frame = co.drain_frame();
        assert_eq!(frame.len(), 1);
        if let Some(Payload::CandleDelta(d)) = &frame[0].payload {
            assert_eq!(d.close, 102);
            assert_eq!(d.seq, 1);
        } else { panic!("esperava CandleDelta"); }
    }

    #[test]
    fn test_close_never_dropped_and_ordered_first() {
        let mut co = Coalescer::new("EURUSD");
        co.push(delta(Timeframe::S5, 1000, 100));
        co.push(CandleEvent::Close { timeframe: Timeframe::S5, candle: candle(1000, 105) });
        co.push(delta(Timeframe::S5, 2000, 106));
        let frame = co.drain_frame();
        assert_eq!(frame.len(), 2);
        assert!(matches!(frame[0].payload, Some(Payload::CandleClose(_))));
        assert!(matches!(frame[1].payload, Some(Payload::CandleDelta(_))));
    }

    #[test]
    fn test_seq_monotonic_per_bucket() {
        let mut co = Coalescer::new("EURUSD");
        co.push(delta(Timeframe::S5, 1000, 100));
        let f1 = co.drain_frame();
        co.push(delta(Timeframe::S5, 1000, 101));
        let f2 = co.drain_frame();
        let seq_of = |f: &[MarketMessage]| match &f[0].payload {
            Some(Payload::CandleDelta(d)) => d.seq, _ => panic!(),
        };
        assert_eq!(seq_of(&f1), 1);
        assert_eq!(seq_of(&f2), 2);
    }
}