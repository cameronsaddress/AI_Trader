pub mod market_neutral;
pub mod cex_arb;
pub mod copy_bot;
pub mod atomic_arb;
pub mod obi_scalper;
pub mod graph_arb;
pub mod convergence_carry;
pub mod maker_mm;
pub mod btc_5m_lag;
pub mod as_market_maker;
pub mod longshot_bias;
pub mod control;
pub mod market_data;
pub mod simulation;
pub mod vol_regime;
pub mod implied_vol;

use async_trait::async_trait;

#[async_trait]
pub trait Strategy {
    async fn run(&self, redis_client: redis::Client);
}
