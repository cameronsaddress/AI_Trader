pub mod market_neutral;
pub mod cex_arb;
pub mod copy_bot;
pub mod atomic_arb;
pub mod obi_scalper;
pub mod control;
pub mod market_data;
pub mod simulation;

use async_trait::async_trait;

#[async_trait]
pub trait Strategy {
    async fn run(&self, redis_client: redis::Client);
}
