#![recursion_limit = "256"]

use tokio::time::{sleep, Duration};
use tokio::signal;
use log::{error, info, warn};
use redis::AsyncCommands;
use std::env;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

mod engine;
mod strategies;

use crate::strategies::Strategy;
use crate::strategies::market_neutral::MarketNeutralStrategy;
use crate::strategies::cex_arb::CexArbStrategy;
use crate::strategies::copy_bot::SyndicateStrategy;
use crate::strategies::atomic_arb::AtomicArbStrategy;
use crate::strategies::obi_scalper::ObiScalperStrategy;
use crate::strategies::graph_arb::GraphArbStrategy;
use crate::strategies::convergence_carry::ConvergenceCarryStrategy;
use crate::strategies::maker_mm::MakerMmStrategy;
use crate::strategies::btc_5m_lag::Btc5mLagStrategy;
use crate::strategies::as_market_maker::AsMarketMakerStrategy;
use crate::strategies::longshot_bias::LongshotBiasStrategy;

const STRATEGY_LOCK_TTL_MS: i64 = 30_000;
const STRATEGY_LOCK_REFRESH_MS: u64 = 10_000;

fn canonical_strategy_id(input: &str) -> &'static str {
    match input {
        "BTC_5M" => "BTC_5M",
        "BTC_15M" => "BTC_15M",
        "ETH_15M" => "ETH_15M",
        "SOL_15M" => "SOL_15M",
        "CEX_ARB" | "CEX_SNIPER" => "CEX_SNIPER",
        "COPY_BOT" | "SYNDICATE" => "SYNDICATE",
        "ATOMIC_ARB" => "ATOMIC_ARB",
        "OBI_SCALPER" => "OBI_SCALPER",
        "GRAPH_ARB" => "GRAPH_ARB",
        "CONVERGENCE_CARRY" => "CONVERGENCE_CARRY",
        "MAKER_MM" => "MAKER_MM",
        "AS_MARKET_MAKER" => "AS_MARKET_MAKER",
        "LONGSHOT_BIAS" => "LONGSHOT_BIAS",
        _ => "BTC_15M",
    }
}

fn process_owner_token(strategy_id: &str) -> String {
    let hostname = env::var("HOSTNAME").unwrap_or_else(|_| "scanner".to_string());
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{}:{}:{}:{}", strategy_id, hostname, process::id(), now_ms)
}

async fn acquire_strategy_lock(
    client: &redis::Client,
    strategy_id: &str,
    owner: &str,
) -> Result<bool, redis::RedisError> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let lock_key = format!("strategy:process_lock:{}", strategy_id);
    let result: Option<String> = redis::cmd("SET")
        .arg(&lock_key)
        .arg(owner)
        .arg("NX")
        .arg("PX")
        .arg(STRATEGY_LOCK_TTL_MS)
        .query_async(&mut conn)
        .await?;
    Ok(result.is_some())
}

async fn release_strategy_lock(
    client: &redis::Client,
    strategy_id: &str,
    owner: &str,
) -> Result<(), redis::RedisError> {
    let mut conn = client.get_multiplexed_async_connection().await?;
    let lock_key = format!("strategy:process_lock:{}", strategy_id);
    let release_script = r#"
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
    "#;
    let _: i32 = redis::cmd("EVAL")
        .arg(release_script)
        .arg(1)
        .arg(&lock_key)
        .arg(owner)
        .query_async(&mut conn)
        .await?;
    Ok(())
}

fn spawn_strategy_lock_lease(client: redis::Client, strategy_id: String, owner: String) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(STRATEGY_LOCK_REFRESH_MS)).await;
            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(error) => {
                    error!("Strategy lock lease reconnect failed for {}: {}", strategy_id, error);
                    process::exit(1);
                }
            };

            let lock_key = format!("strategy:process_lock:{}", strategy_id);
            let renew_script = r#"
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    redis.call("PEXPIRE", KEYS[1], ARGV[2])
                    return 1
                end
                return 0
            "#;

            let renewed = redis::cmd("EVAL")
                .arg(renew_script)
                .arg(1)
                .arg(&lock_key)
                .arg(&owner)
                .arg(STRATEGY_LOCK_TTL_MS)
                .query_async::<i32>(&mut conn)
                .await
                .unwrap_or(0);

            if renewed != 1 {
                warn!("Strategy lock renewal failed for {}, retrying...", strategy_id);
                let mut recovered = false;
                for retry in 1..=3 {
                    sleep(Duration::from_secs(10)).await;
                    if let Ok(mut retry_conn) = client.get_multiplexed_async_connection().await {
                        let r: i32 = redis::cmd("EVAL")
                            .arg(renew_script)
                            .arg(1)
                            .arg(&lock_key)
                            .arg(&owner)
                            .arg(STRATEGY_LOCK_TTL_MS)
                            .query_async(&mut retry_conn)
                            .await
                            .unwrap_or(0);
                        if r == 1 {
                            info!("Strategy lock recovered for {} on retry {}", strategy_id, retry);
                            recovered = true;
                            break;
                        }
                    }
                    warn!("Lock retry {}/3 failed for {}", retry, strategy_id);
                }
                if !recovered {
                    error!("Strategy lock permanently lost for {}. Exiting.", strategy_id);
                    if let Ok(mut alert_conn) = client.get_multiplexed_async_connection().await {
                        let _: () = alert_conn.publish::<_, _, ()>(
                            "system:alert",
                            format!(r#"{{"level":"CRITICAL","strategy":"{}","msg":"LOCK_LOST_EXIT"}}"#, strategy_id),
                        ).await.unwrap_or(());
                    }
                    process::exit(1);
                }
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    let strategy_type = env::var("STRATEGY_TYPE").unwrap_or_else(|_| "BTC_15M".to_string());
    let strategy_id = canonical_strategy_id(&strategy_type).to_string();
    info!("Starting Arb Scanner with Strategy: {} ({})", strategy_type, strategy_id);

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

    let owner = process_owner_token(&strategy_id);
    let lock_acquired = acquire_strategy_lock(&client, &strategy_id, &owner).await?;
    if !lock_acquired {
        warn!(
            "Another scanner process already holds strategy lock for {}. Exiting duplicate worker.",
            strategy_id
        );
        return Ok(());
    }
    info!("Acquired strategy process lock for {}", strategy_id);
    spawn_strategy_lock_lease(client.clone(), strategy_id.clone(), owner.clone());

    let strategy: Box<dyn Strategy + Send + Sync> = match strategy_type.as_str() {
        "BTC_5M" => Box::new(Btc5mLagStrategy::new()),
        "BTC_15M" => Box::new(MarketNeutralStrategy::new("BTC".to_string())),
        "ETH_15M" => Box::new(MarketNeutralStrategy::new("ETH".to_string())),
        "SOL_15M" => Box::new(MarketNeutralStrategy::new("SOL".to_string())),
        "CEX_ARB" | "CEX_SNIPER" => Box::new(CexArbStrategy::new()),
        "COPY_BOT" | "SYNDICATE" => Box::new(SyndicateStrategy::new()),
        "ATOMIC_ARB" => Box::new(AtomicArbStrategy::new()),
        "OBI_SCALPER" => Box::new(ObiScalperStrategy::new()),
        "GRAPH_ARB" => Box::new(GraphArbStrategy::new()),
        "CONVERGENCE_CARRY" => Box::new(ConvergenceCarryStrategy::new()),
        "MAKER_MM" => Box::new(MakerMmStrategy::new()),
        "AS_MARKET_MAKER" => Box::new(AsMarketMakerStrategy::new()),
        "LONGSHOT_BIAS" => Box::new(LongshotBiasStrategy::new()),
        _ => {
            error!("Unknown Strategy Type. Defaulting to BTC_15M");
            Box::new(MarketNeutralStrategy::new("BTC".to_string()))
        }
    };

    // Graceful shutdown: listen for SIGTERM/SIGINT and stop strategy cleanly.
    let shutdown_strategy_id = strategy_id.clone();
    let shutdown = async move {
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to register SIGTERM handler");
        let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
            .expect("Failed to register SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => info!("[{}] SIGTERM received", shutdown_strategy_id),
            _ = sigint.recv() => info!("[{}] SIGINT received", shutdown_strategy_id),
        }
    };

    tokio::select! {
        _ = strategy.run(client.clone()) => {
            info!("[{}] Strategy run completed", strategy_id);
        },
        _ = shutdown => {
            info!("[{}] Shutdown signal received, cleaning up...", strategy_id);
        },
    }

    let _ = release_strategy_lock(&client, &strategy_id, &owner).await;
    info!("[{}] Lock released, exiting cleanly.", strategy_id);

    Ok(())
}
