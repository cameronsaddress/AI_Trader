use chrono::{DateTime, Utc};
use log::{error, info, warn};
use reqwest::Client as HttpClient;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashSet;

// Constants
pub const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com/events";
pub const CLOB_API_URL: &str = "https://clob.polymarket.com/markets";
pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Clone)]
pub struct MarketTarget {
    pub market_id: String,
    pub slug: String,
    pub yes_token: String,
    pub no_token: String,
    pub question: String,
    pub expiry_ts: Option<i64>,
    pub volume: Option<f64>,
    pub liquidity: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

pub struct PolymarketClient {
    pub http_client: HttpClient,
}

fn first_two_from_array(values: &[Value]) -> Option<(String, String)> {
    let mut ids: Vec<String> = values
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
        .filter(|s| !s.is_empty())
        .take(2)
        .collect();

    if ids.len() < 2 {
        return None;
    }

    let second = ids.pop().unwrap_or_default();
    let first = ids.pop().unwrap_or_default();
    if first.is_empty() || second.is_empty() {
        None
    } else {
        Some((first, second))
    }
}

fn parse_clob_token_ids(value: &Value) -> Option<(String, String)> {
    if let Some(arr) = value.as_array() {
        return first_two_from_array(arr);
    }

    if let Some(raw) = value.as_str() {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }

        // Gamma often returns clobTokenIds as a JSON-encoded string.
        if let Ok(parsed) = serde_json::from_str::<Vec<String>>(trimmed) {
            let values: Vec<Value> = parsed.into_iter().map(Value::String).collect();
            return first_two_from_array(&values);
        }
    }

    None
}

fn parse_tokens_array(value: &Value) -> Option<(String, String)> {
    let arr = value.as_array()?;
    let ids: Vec<String> = arr
        .iter()
        .filter_map(|entry| {
            entry.get("token_id")
                .and_then(|v| v.as_str())
                .or_else(|| entry.get("tokenId").and_then(|v| v.as_str()))
                .map(|s| s.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .take(2)
        .collect();

    if ids.len() < 2 {
        return None;
    }

    Some((ids[0].clone(), ids[1].clone()))
}

fn parse_bool(value: Option<&Value>) -> Option<bool> {
    let value = value?;
    if let Some(parsed) = value.as_bool() {
        return Some(parsed);
    }
    if let Some(parsed) = value.as_i64() {
        return Some(parsed != 0);
    }
    if let Some(raw) = value.as_str() {
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized == "true" || normalized == "1" {
            return Some(true);
        }
        if normalized == "false" || normalized == "0" {
            return Some(false);
        }
    }
    None
}

fn parse_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse::<f64>().ok()))
}

fn parse_expiry_ts(raw: Option<&Value>) -> Option<i64> {
    let raw = raw?.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.timestamp())
        .ok()
}

fn extract_token_pair(market: &Value) -> Option<(String, String)> {
    if let Some(clob_token_ids) = market.get("clobTokenIds") {
        if let Some(pair) = parse_clob_token_ids(clob_token_ids) {
            return Some(pair);
        }
    }

    if let Some(tokens) = market.get("tokens") {
        if let Some(pair) = parse_tokens_array(tokens) {
            return Some(pair);
        }
    }

    None
}

fn is_tradeable_market(market: &Value) -> bool {
    if let Some(active) = parse_bool(market.get("active")) {
        if !active {
            return false;
        }
    }

    if let Some(closed) = parse_bool(market.get("closed")) {
        if closed {
            return false;
        }
    }

    if let Some(accepting_orders) = parse_bool(
        market
            .get("accepting_orders")
            .or_else(|| market.get("acceptingOrders"))
            .or_else(|| market.get("accepting_order")),
    ) {
        if !accepting_orders {
            return false;
        }
    }

    true
}

fn market_condition_id(market: &Value) -> String {
    market
        .get("conditionId")
        .and_then(|s| s.as_str())
        .or_else(|| market.get("condition_id").and_then(|s| s.as_str()))
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn market_slug(market: &Value, default_slug: Option<&str>) -> String {
    market
        .get("slug")
        .and_then(|s| s.as_str())
        .or_else(|| market.get("market_slug").and_then(|s| s.as_str()))
        .or(default_slug)
        .unwrap_or("UNKNOWN")
        .trim()
        .to_string()
}

fn market_question(market: &Value) -> String {
    market
        .get("question")
        .and_then(|s| s.as_str())
        .unwrap_or("UNKNOWN")
        .trim()
        .to_string()
}

fn build_market_target(market: &Value, default_slug: Option<&str>) -> Option<MarketTarget> {
    if !is_tradeable_market(market) {
        return None;
    }

    let market_id = market_condition_id(market);
    if market_id.is_empty() {
        return None;
    }

    let (yes_token, no_token) = extract_token_pair(market)?;
    if yes_token.is_empty() || no_token.is_empty() {
        return None;
    }

    let expiry_ts = parse_expiry_ts(market.get("endDate"))
        .or_else(|| parse_expiry_ts(market.get("end_date_iso")))
        .or_else(|| parse_expiry_ts(market.get("game_start_time")));

    Some(MarketTarget {
        market_id,
        slug: market_slug(market, default_slug),
        yes_token,
        no_token,
        question: market_question(market),
        expiry_ts,
        volume: parse_f64(market.get("volume")),
        liquidity: parse_f64(market.get("liquidity")),
        best_bid: parse_f64(market.get("bestBid")).or_else(|| parse_f64(market.get("best_bid"))),
        best_ask: parse_f64(market.get("bestAsk")).or_else(|| parse_f64(market.get("best_ask"))),
    })
}

impl PolymarketClient {
    pub fn new() -> Self {
        let client = HttpClient::builder()
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            .build()
            .unwrap_or_else(|_| HttpClient::new());
            
        warn!("Rust live execution modules are disabled in this scanner build; strategies run in signal/paper-safe mode only.");

        Self {
            http_client: client,
        }
    }

    // Helper to fetch tokens from CLOB if missing in Gamma
    pub async fn fetch_tokens_from_clob(&self, condition_id: &str) -> Option<(String, String)> {
        let url = format!("{}?condition_id={}", CLOB_API_URL, condition_id);
        info!("Falling back to CLOB API: {}", url);
        match self.http_client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(text) = resp.text().await {
                    if let Ok(val) = serde_json::from_str::<Value>(&text) {
                        let markets = val
                            .get("data")
                            .and_then(|d| d.as_array())
                            .or_else(|| val.as_array());

                        if let Some(market) = markets.and_then(|m| m.first()) {
                            if let Some(pair) = extract_token_pair(market) {
                                return Some(pair);
                            }
                        }
                    }
                }
            }
            Err(e) => error!("CLOB API Fallback failed: {}", e),
        }
        None
    }

    pub async fn fetch_active_binary_markets(&self, limit: usize) -> Vec<MarketTarget> {
        let capped_limit = limit.clamp(1, 128);
        let fetch_events = (capped_limit * 6).clamp(30, 400);
        let url = format!(
            "{}?limit={}&active=true&closed=false",
            GAMMA_API_URL, fetch_events
        );

        let mut targets: Vec<MarketTarget> = Vec::new();
        let mut seen_conditions: HashSet<String> = HashSet::new();

        match self.http_client.get(&url).send().await {
            Ok(resp) => {
                let text = resp.text().await.unwrap_or_default();
                if let Ok(events) = serde_json::from_str::<Value>(&text) {
                    if let Some(event_arr) = events.as_array() {
                        for event in event_arr {
                            let default_slug = event.get("slug").and_then(|v| v.as_str());
                            let event_markets = event.get("markets").and_then(|v| v.as_array());
                            let Some(markets) = event_markets else {
                                continue;
                            };

                            for market in markets {
                                if targets.len() >= capped_limit.saturating_mul(2) {
                                    break;
                                }

                                let target = if let Some(parsed) = build_market_target(market, default_slug) {
                                    parsed
                                } else {
                                    let fallback_condition = market_condition_id(market);
                                    if fallback_condition.is_empty() || !is_tradeable_market(market) {
                                        continue;
                                    }
                                    let (yes_token, no_token) = match self.fetch_tokens_from_clob(&fallback_condition).await {
                                        Some(pair) => pair,
                                        None => continue,
                                    };
                                    MarketTarget {
                                        market_id: fallback_condition,
                                        slug: market_slug(market, default_slug),
                                        yes_token,
                                        no_token,
                                        question: market_question(market),
                                        expiry_ts: parse_expiry_ts(market.get("endDate"))
                                            .or_else(|| parse_expiry_ts(market.get("end_date_iso")))
                                            .or_else(|| parse_expiry_ts(market.get("game_start_time"))),
                                        volume: parse_f64(market.get("volume")),
                                        liquidity: parse_f64(market.get("liquidity")),
                                        best_bid: parse_f64(market.get("bestBid")).or_else(|| parse_f64(market.get("best_bid"))),
                                        best_ask: parse_f64(market.get("bestAsk")).or_else(|| parse_f64(market.get("best_ask"))),
                                    }
                                };

                                if seen_conditions.insert(target.market_id.clone()) {
                                    targets.push(target);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("Active market universe fetch failed: {}", e),
        }

        targets.sort_by(|a, b| {
            let a_expiry = a.expiry_ts.unwrap_or(i64::MAX);
            let b_expiry = b.expiry_ts.unwrap_or(i64::MAX);
            match a_expiry.cmp(&b_expiry) {
                Ordering::Equal => {
                    let a_liq = a.liquidity.or(a.volume).unwrap_or(0.0);
                    let b_liq = b.liquidity.or(b.volume).unwrap_or(0.0);
                    b_liq.partial_cmp(&a_liq).unwrap_or(Ordering::Equal)
                }
                ordering => ordering,
            }
        });
        targets.truncate(capped_limit);

        if !targets.is_empty() {
            info!("Loaded {} active binary markets for scanner universe", targets.len());
        }

        targets
    }

    async fn fetch_market_by_slug(&self, slug: &str) -> Option<MarketTarget> {
        let trimmed = slug.trim();
        if trimmed.is_empty() {
            return None;
        }

        info!("Looking for market slug: {}", trimmed);
        let url = format!("{}?slug={}", GAMMA_API_URL, trimmed);

        match self.http_client.get(&url).send().await {
            Ok(resp) => {
                let text = resp.text().await.unwrap_or_default();
                if let Ok(events) = serde_json::from_str::<Value>(&text) {
                    if let Some(event_arr) = events.as_array() {
                        if let Some(event) = event_arr.first() {
                            if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                for market in markets {
                                    if let Some(parsed) = build_market_target(market, Some(trimmed)) {
                                        info!("Found Active Market: {}", parsed.question);
                                        return Some(parsed);
                                    }

                                    let condition_id = market_condition_id(market);
                                    if condition_id.is_empty() || !is_tradeable_market(market) {
                                        continue;
                                    }

                                    warn!(
                                        "Gamma missing tokens, trying CLOB fallback for condition_id: {}",
                                        condition_id
                                    );
                                    if let Some((yes_token, no_token)) =
                                        self.fetch_tokens_from_clob(&condition_id).await
                                    {
                                        return Some(MarketTarget {
                                            market_id: condition_id,
                                            slug: trimmed.to_string(),
                                            yes_token,
                                            no_token,
                                            question: market_question(market),
                                            expiry_ts: parse_expiry_ts(market.get("endDate"))
                                                .or_else(|| parse_expiry_ts(market.get("end_date_iso")))
                                                .or_else(|| parse_expiry_ts(market.get("game_start_time"))),
                                            volume: parse_f64(market.get("volume")),
                                            liquidity: parse_f64(market.get("liquidity")),
                                            best_bid: parse_f64(market.get("bestBid"))
                                                .or_else(|| parse_f64(market.get("best_bid"))),
                                            best_ask: parse_f64(market.get("bestAsk"))
                                                .or_else(|| parse_f64(market.get("best_ask"))),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => error!("API Refresh failed: {}", e),
        }

        None
    }

    pub async fn fetch_current_market_window(&self, asset: &str, window_seconds: i64) -> Option<MarketTarget> {
        if asset.trim().is_empty() {
            return None;
        }

        let window = window_seconds.clamp(60, 24 * 60 * 60);
        let minutes = (window / 60).max(1);
        let now = Utc::now();
        let ts = now.timestamp() - (now.timestamp().rem_euclid(window));
        // slug pattern: btc-updown-15m-TIMESTAMP, btc-updown-5m-TIMESTAMP, etc.
        let slug = format!("{}-updown-{}m-{}", asset.to_lowercase(), minutes, ts);
        self.fetch_market_by_slug(&slug).await
    }

    // Backwards-compatible fetcher for the 15m up/down markets per asset.
    pub async fn fetch_current_market(&self, asset: &str) -> Option<MarketTarget> {
        self.fetch_current_market_window(asset, 900).await
    }
}
