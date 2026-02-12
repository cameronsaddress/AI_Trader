use log::{info, error, warn};
use reqwest::Client as HttpClient;
use chrono::Utc;
use serde_json::Value;

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

    // Generic fetcher for 15m markets based on asset
    // asset: "btc", "eth", "sol"
    pub async fn fetch_current_market(&self, asset: &str) -> Option<MarketTarget> {
        let now = Utc::now();
        let ts = now.timestamp() - (now.timestamp().rem_euclid(900));
        // slug pattern: btc-updown-15m-TIMESTAMP
        let slug = format!("{}-updown-15m-{}", asset.to_lowercase(), ts);
        
        info!("Looking for market slug: {}", slug);
        
        let url = format!("{}?slug={}", GAMMA_API_URL, slug);
        
        match self.http_client.get(&url).send().await {
            Ok(resp) => {
                let text = resp.text().await.unwrap_or_default();
                if let Ok(events) = serde_json::from_str::<Value>(&text) {
                     if let Some(event_arr) = events.as_array() {
                        if let Some(event) = event_arr.first() {
                            if let Some(markets) = event.get("markets").and_then(|m| m.as_array()) {
                                for market in markets {
                                    let id = market.get("conditionId").and_then(|s| s.as_str()).unwrap_or_default().to_string();
                                    let (yes_token, no_token) = match extract_token_pair(market) {
                                        Some(pair) => pair,
                                        None => {
                                            warn!("Gamma missing tokens, trying CLOB fallback for condition_id: {}", id);
                                            self.fetch_tokens_from_clob(&id).await.unwrap_or(("".to_string(), "".to_string()))
                                        }
                                    };

                                    if !yes_token.is_empty() && !no_token.is_empty() {
                                        let question = market.get("question").and_then(|s| s.as_str()).unwrap_or("UNKNOWN");
                                        info!("Found Active Market: {}", question);
                                        return Some(MarketTarget {
                                            market_id: id,
                                            slug: slug.clone(),
                                            yes_token,
                                            no_token,
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
}
