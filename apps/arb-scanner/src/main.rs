use tokio::time::{sleep, Duration};
use log::{info, error};
use std::env;

mod engine;
mod strategies;

use crate::strategies::Strategy;
use crate::strategies::market_neutral::MarketNeutralStrategy;
use crate::strategies::cex_arb::CexArbStrategy;
use crate::strategies::copy_bot::SyndicateStrategy;
use crate::strategies::atomic_arb::AtomicArbStrategy;
use crate::strategies::obi_scalper::ObiScalperStrategy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    let strategy_type = env::var("STRATEGY_TYPE").unwrap_or_else(|_| "BTC_15M".to_string());
    info!("Starting Arb Scanner with Strategy: {}", strategy_type);

    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_string());
    
    // Connect Redis
    let client = loop {
        match redis::Client::open(redis_url.clone()) {
            Ok(c) => break c,
            Err(e) => {
                error!("Invalid Redis URL: {}", e);
                sleep(Duration::from_secs(2)).await;
            }
        }
    };

    let strategy: Box<dyn Strategy + Send + Sync> = match strategy_type.as_str() {
        "BTC_15M" => Box::new(MarketNeutralStrategy::new("BTC".to_string())),
        "ETH_15M" => Box::new(MarketNeutralStrategy::new("ETH".to_string())),
        "SOL_15M" => Box::new(MarketNeutralStrategy::new("SOL".to_string())),
        "CEX_ARB" | "CEX_SNIPER" => Box::new(CexArbStrategy::new()),
        "COPY_BOT" | "SYNDICATE" => Box::new(SyndicateStrategy::new()),
        "ATOMIC_ARB" => Box::new(AtomicArbStrategy::new()),
        "OBI_SCALPER" => Box::new(ObiScalperStrategy::new()),
        _ => {
            error!("Unknown Strategy Type. Defaulting to BTC_15M");
            Box::new(MarketNeutralStrategy::new("BTC".to_string()))
        }
    };

    tokio::spawn(async move {
        strategy.run(client).await;
    });

    // Keep alive
    loop {
        sleep(Duration::from_secs(60)).await;
        info!("Heartbeat [{}]...", strategy_type);
    }
}
