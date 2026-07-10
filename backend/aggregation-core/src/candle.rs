use common::NormalizedTick;

/// Representação de uma vela (Candle) em alta performance.
///
/// # Design
/// Utiliza primitivos `i64` para preços (micropips) a fim de evitar
/// erros de arredondamento inerentes a ponto flutuante. Esta estrutura
/// é compacta e projetada para viver em ring buffers contíguos (zero-allocation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candle {
    /// Timestamp de início do bucket em nanossegundos
    pub t: u64,
    /// Preço de abertura em micropips
    pub o: i64,
    /// Preço máximo atingido em micropips
    pub h: i64,
    /// Preço mínimo atingido em micropips
    pub l: i64,
    /// Preço de fechamento atual em micropips
    pub c: i64,
    /// Volume acumulado (se provido pelo feed)
    pub v: Option<u64>,
}

impl Default for Candle {
    fn default() -> Self {
        Self {
            t: 0,
            o: 0,
            h: 0,
            l: 0,
            c: 0,
            v: None,
        }
    }
}

impl Candle {
    /// Cria uma nova vela a partir de um tick inicial e do timestamp do bucket.
    pub fn new(bucket_ts_ns: u64, tick: &NormalizedTick) -> Self {
        Self {
            t: bucket_ts_ns,
            o: tick.mid,
            h: tick.mid,
            l: tick.mid,
            c: tick.mid,
            v: tick.volume,
        }
    }

    /// Cria uma vela "flat" a partir do fechamento de uma vela anterior.
    /// Utilizado pelo Watchdog quando um bucket inteiro passa sem nenhum tick
    /// por conta de baixa liquidez. NUNCA simula volume.
    pub fn flat_from_previous(bucket_ts_ns: u64, prev_close: i64) -> Self {
        Self {
            t: bucket_ts_ns,
            o: prev_close,
            h: prev_close,
            l: prev_close,
            c: prev_close,
            v: None,
        }
    }

    /// Função pura determinística de agregação.
    /// Muta o estado interno desta vela aplicando as regras de OHLC para um novo tick.
    pub fn fold(&mut self, tick: &NormalizedTick) {
        if tick.mid > self.h {
            self.h = tick.mid;
        }
        if tick.mid < self.l {
            self.l = tick.mid;
        }

        self.c = tick.mid; // O fechamento é sempre o mid do último tick recebido

        // Acumula volume apenas quando o tick fornece. Se ausente, preserva o
        // estado atual (nunca fabrica).
        if let Some(tick_vol) = tick.volume {
            self.v = Some(self.v.unwrap_or(0) + tick_vol);
        }
    }

    /// Valida a coerência OHLC: Low é o piso, High é o teto de O/C, e High >= Low.
    pub fn is_valid(&self) -> bool {
        self.h >= self.l
            && self.l <= self.o
            && self.l <= self.c
            && self.h >= self.o
            && self.h >= self.c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Symbol, TickSource};

    fn dummy_tick(ts: u64, mid: i64, vol: Option<u64>) -> NormalizedTick {
        NormalizedTick {
            symbol: Symbol::EurUsd,
            ts_ns: ts,
            bid: mid - 5,
            ask: mid + 5,
            mid,
            volume: vol,
            seq: 1,
            source: TickSource::Test,
        }
    }

    #[test]
    fn test_candle_creation_and_folding() {
        let t1 = dummy_tick(1_000_000_000, 1084200, Some(10));
        let mut candle = Candle::new(1_000_000_000, &t1);

        assert_eq!(candle.o, 1084200);
        assert_eq!(candle.h, 1084200);
        assert_eq!(candle.l, 1084200);
        assert_eq!(candle.c, 1084200);
        assert_eq!(candle.v, Some(10));

        let t2 = dummy_tick(1_005_000_000, 1084250, Some(5)); // Novo High
        candle.fold(&t2);

        assert_eq!(candle.o, 1084200); // Open imutável
        assert_eq!(candle.h, 1084250);
        assert_eq!(candle.l, 1084200);
        assert_eq!(candle.c, 1084250);
        assert_eq!(candle.v, Some(15));

        let t3 = dummy_tick(1_008_000_000, 1084150, None); // Novo Low, Sem Volume
        candle.fold(&t3);

        assert_eq!(candle.o, 1084200);
        assert_eq!(candle.h, 1084250);
        assert_eq!(candle.l, 1084150);
        assert_eq!(candle.c, 1084150);
        assert_eq!(candle.v, Some(15)); // Volume não altera já que o provider omitiu

        assert!(candle.is_valid());
    }

    #[test]
    fn test_flat_candle() {
        let candle = Candle::flat_from_previous(2_000_000_000, 1084200);
        assert_eq!(candle.t, 2_000_000_000);
        assert_eq!(candle.o, 1084200);
        assert_eq!(candle.h, 1084200);
        assert_eq!(candle.l, 1084200);
        assert_eq!(candle.c, 1084200);
        assert_eq!(candle.v, None);
        assert!(candle.is_valid());
    }
}
