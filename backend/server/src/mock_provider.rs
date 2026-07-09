//! Provider de teste (replay sintético) — APENAS para smoke-test da pipeline.
//! NÃO usar em produção. Substitua por um FeedProvider real (broker/CSV).

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use common::error::FeedError;
use common::types::ProviderStatus;
use feed_handler::{FeedProvider, RawTick};

pub struct MockReplayProvider {
    symbol: String,
    status: ProviderStatus,
    last_mid: f64,
    seed: u64,
}

impl MockReplayProvider {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            status: ProviderStatus::Disconnected,
            last_mid: 1.08420,
            seed: 0x9E3779B97F4A7C15,
        }
    }

    /// PRNG determinístico (xorshift) — passeio de preço reprodutível.
    fn next_rand(&mut self) -> f64 {
        self.seed ^= self.seed << 13;
        self.seed ^= self.seed >> 7;
        self.seed ^= self.seed << 17;
        (self.seed as f64 / u64::MAX as f64) - 0.5 // [-0.5, 0.5)
    }
}

#[async_trait]
impl FeedProvider for MockReplayProvider {
    async fn connect(&mut self) -> Result<(), FeedError> {
        self.status = ProviderStatus::Connected;
        Ok(())
    }

    async fn next_tick(&mut self) -> Result<Option<RawTick>, FeedError> {
        // ~50 ticks/s
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Passeio aleatório de ±0.2 pip sobre o último mid.
        self.last_mid += self.next_rand() * 0.00004;

        let ts_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        let spread = 0.00002; // 0.2 pip
        Ok(Some(RawTick {
            symbol: self.symbol.clone(),
            timestamp_ns: ts_ns,
            bid: self.last_mid - spread / 2.0,
            ask: self.last_mid + spread / 2.0,
            volume: None, // FX real raramente fornece — NUNCA fabricamos
        }))
    }

    async fn disconnect(&mut self) -> Result<(), FeedError> {
        self.status = ProviderStatus::Disconnected;
        Ok(())
    }

    fn status(&self) -> ProviderStatus {
        self.status
    }

    fn provider_name(&self) -> &str {
        "mock-replay"
    }
}
