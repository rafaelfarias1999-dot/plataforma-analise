//! # MockFeedProvider — Feed SINTÉTICO para DESENVOLVIMENTO
//!
//! ⚠️  ATENÇÃO: este provider FABRICA preços (random walk). Ele existe
//! exclusivamente como harness de desenvolvimento/teste e é marcado como
//! `TickSource::Test` em toda a pipeline. NUNCA deve ser usado em produção
//! nem apresentado como feed real. Só é ativado via `USE_MOCK_FEED=1`.

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use common::error::FeedError;
use common::types::ProviderStatus;
use feed_handler::{FeedProvider, RawTick};

pub struct MockFeedProvider {
    symbol: String,
    status: ProviderStatus,
    last_mid: f64,
    rng: StdRng,
}

impl MockFeedProvider {
    pub fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            status: ProviderStatus::Disconnected,
            last_mid: 1.08420,
            rng: StdRng::seed_from_u64(0xEA9_F00D),
        }
    }
}

#[async_trait]
impl FeedProvider for MockFeedProvider {
    async fn connect(&mut self) -> Result<(), FeedError> {
        self.status = ProviderStatus::Connected;
        Ok(())
    }

    async fn next_tick(&mut self) -> Result<Option<RawTick>, FeedError> {
        // Cadência sintética ~5ms (≈200 ticks/s).
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

        // Random walk em torno do último mid.
        let step: f64 = self.rng.gen_range(-0.00005..0.00005);
        self.last_mid += step;

        let spread = 0.00010; // 1 pip
        let bid = self.last_mid - spread / 2.0;
        let ask = self.last_mid + spread / 2.0;

        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| FeedError::ProtocolError(e.to_string()))?
            .as_nanos() as u64;

        Ok(Some(RawTick {
            symbol: self.symbol.clone(),
            timestamp_ns,
            bid,
            ask,
            volume: None, // NUNCA fabricamos volume.
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
        "mock-dev-synthetic"
    }
}
