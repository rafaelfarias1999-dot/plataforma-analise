//! Conversões entre `common::Timeframe` e o enum Protobuf gerado.
//!
//! Fonte única do mapeamento TF interno ↔ TF do wire. Usado pelo
//! aggregation-core e pelo broadcast-hub ao serializar mensagens.

use crate::Timeframe;
use contracts_rs::market_stream::Timeframe as ProtoTf;

/// TF interno → enum Protobuf.
pub fn timeframe_to_proto(tf: Timeframe) -> ProtoTf {
    match tf {
        Timeframe::Tick => ProtoTf::TfTick,
        Timeframe::S5 => ProtoTf::TfS5,
        Timeframe::S10 => ProtoTf::TfS10,
        Timeframe::S15 => ProtoTf::TfS15,
        Timeframe::S30 => ProtoTf::TfS30,
        Timeframe::M1 => ProtoTf::TfM1,
        Timeframe::M2 => ProtoTf::TfM2,
        Timeframe::M3 => ProtoTf::TfM3,
        Timeframe::M5 => ProtoTf::TfM5,
        Timeframe::M10 => ProtoTf::TfM10,
        Timeframe::M15 => ProtoTf::TfM15,
        Timeframe::M30 => ProtoTf::TfM30,
        Timeframe::H1 => ProtoTf::TfH1,
        Timeframe::H2 => ProtoTf::TfH2,
        Timeframe::H4 => ProtoTf::TfH4,
        Timeframe::D1 => ProtoTf::TfD1,
    }
}

/// enum Protobuf → TF interno.
pub fn timeframe_from_proto(p: ProtoTf) -> Timeframe {
    match p {
        ProtoTf::TfTick => Timeframe::Tick,
        ProtoTf::TfS5 => Timeframe::S5,
        ProtoTf::TfS10 => Timeframe::S10,
        ProtoTf::TfS15 => Timeframe::S15,
        ProtoTf::TfS30 => Timeframe::S30,
        ProtoTf::TfM1 => Timeframe::M1,
        ProtoTf::TfM2 => Timeframe::M2,
        ProtoTf::TfM3 => Timeframe::M3,
        ProtoTf::TfM5 => Timeframe::M5,
        ProtoTf::TfM10 => Timeframe::M10,
        ProtoTf::TfM15 => Timeframe::M15,
        ProtoTf::TfM30 => Timeframe::M30,
        ProtoTf::TfH1 => Timeframe::H1,
        ProtoTf::TfH2 => Timeframe::H2,
        ProtoTf::TfH4 => Timeframe::H4,
        ProtoTf::TfD1 => Timeframe::D1,
    }
}

/// TF interno → i32 (representação do campo no struct prost).
#[inline]
pub fn timeframe_to_i32(tf: Timeframe) -> i32 {
    timeframe_to_proto(tf) as i32
}

/// i32 do wire → TF interno. `None` se o valor for desconhecido.
#[inline]
pub fn timeframe_from_i32(v: i32) -> Option<Timeframe> {
    ProtoTf::try_from(v).ok().map(timeframe_from_proto)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeframe_roundtrip_all() {
        let all = [
            Timeframe::Tick, Timeframe::S5, Timeframe::S10, Timeframe::S15,
            Timeframe::S30, Timeframe::M1, Timeframe::M2, Timeframe::M3,
            Timeframe::M5, Timeframe::M10, Timeframe::M15, Timeframe::M30,
            Timeframe::H1, Timeframe::H2, Timeframe::H4, Timeframe::D1,
        ];
        for tf in all {
            let back = timeframe_from_i32(timeframe_to_i32(tf));
            assert_eq!(back, Some(tf), "roundtrip falhou para {tf:?}");
        }
    }

    #[test]
    fn test_unknown_i32_is_none() {
        assert_eq!(timeframe_from_i32(9999), None);
    }
}
