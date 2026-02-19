use chrono::{TimeZone, Utc};
use log::{debug, warn};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

const DERIBIT_BOOK_SUMMARY_URL: &str =
    "https://www.deribit.com/api/v2/public/get_book_summary_by_currency";
const YEAR_MS: f64 = 31_536_000_000.0;

#[derive(Debug, Clone, Default)]
pub struct ImpliedVolSnapshot {
    pub annualized_iv: f64,
    pub timestamp_ms: i64,
    pub sample_count: usize,
    pub median_tte_years: f64,
    pub source: String,
}

fn env_u64(name: &str, fallback: u64, min: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(fallback)
        .clamp(min, max)
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(v) => v
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| v.as_f64()),
        None => None,
    }
}

fn parse_deribit_expiry_ms(raw: &str) -> Option<i64> {
    // Deribit instrument format: BTC-28FEB26-80000-C
    if raw.len() != 7 {
        return None;
    }
    let day = raw[0..2].parse::<u32>().ok()?;
    let month = match &raw[2..5].to_ascii_uppercase()[..] {
        "JAN" => 1,
        "FEB" => 2,
        "MAR" => 3,
        "APR" => 4,
        "MAY" => 5,
        "JUN" => 6,
        "JUL" => 7,
        "AUG" => 8,
        "SEP" => 9,
        "OCT" => 10,
        "NOV" => 11,
        "DEC" => 12,
        _ => return None,
    };
    let year_suffix = raw[5..7].parse::<i32>().ok()?;
    let year = 2000 + year_suffix;
    let dt = Utc.with_ymd_and_hms(year, month, day, 8, 0, 0).single()?;
    Some(dt.timestamp_millis())
}

fn summarize_iv_rows(rows: &[Value], spot: f64, now_ms: i64) -> Option<ImpliedVolSnapshot> {
    if !spot.is_finite() || spot <= 0.0 {
        return None;
    }

    let min_tte_years = 1.0 / (365.0 * 24.0); // 1 hour
    let max_tte_years = 1.5; // keep short-dated surface only
    let mut candidates: Vec<(f64, f64, f64)> = Vec::new(); // (score, weight, iv)
    let mut ttes: Vec<f64> = Vec::new();

    for row in rows {
        let instrument = row.get("instrument_name").and_then(|v| v.as_str()).unwrap_or_default();
        let mut parts = instrument.split('-');
        let _asset = parts.next();
        let expiry_raw = parts.next().unwrap_or_default();
        let strike = parts.next().and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
        if strike <= 0.0 {
            continue;
        }
        let expiry_ms = match parse_deribit_expiry_ms(expiry_raw) {
            Some(value) => value,
            None => continue,
        };
        let tte_years = (expiry_ms - now_ms) as f64 / YEAR_MS;
        if !tte_years.is_finite() || tte_years < min_tte_years || tte_years > max_tte_years {
            continue;
        }

        let mut mark_iv = parse_number(row.get("mark_iv"))
            .or_else(|| parse_number(row.get("bid_iv")))
            .or_else(|| parse_number(row.get("ask_iv")))
            .unwrap_or(0.0);
        if mark_iv > 3.0 {
            mark_iv /= 100.0;
        }
        if !mark_iv.is_finite() || !(0.03..=4.0).contains(&mark_iv) {
            continue;
        }

        let moneyness = ((strike - spot) / spot).abs();
        if !moneyness.is_finite() || moneyness > 0.35 {
            continue;
        }
        let open_interest = parse_number(row.get("open_interest")).unwrap_or(0.0).max(0.0);
        let oi_boost = 1.0 + (open_interest.sqrt().min(40.0) / 40.0);
        let score = moneyness + (0.20 * tte_years);
        let weight = (1.0 / (0.02 + score)).clamp(0.1, 50.0) * oi_boost;
        candidates.push((score, weight, mark_iv));
        ttes.push(tte_years);
    }

    if candidates.is_empty() {
        return None;
    }

    candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let keep = candidates.len().min(24);
    let filtered = &candidates[..keep];

    let mut weighted_sum = 0.0;
    let mut total_weight = 0.0;
    for (_, weight, iv) in filtered {
        weighted_sum += weight * iv;
        total_weight += weight;
    }
    if total_weight <= 0.0 {
        return None;
    }

    ttes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = ttes.len() / 2;
    let median_tte = if ttes.len() % 2 == 1 {
        ttes[mid]
    } else {
        (ttes[mid.saturating_sub(1)] + ttes[mid]) / 2.0
    };

    Some(ImpliedVolSnapshot {
        annualized_iv: (weighted_sum / total_weight).clamp(0.03, 4.0),
        timestamp_ms: now_ms,
        sample_count: filtered.len(),
        median_tte_years: median_tte.max(0.0),
        source: "DERIBIT_OPTION_SURFACE".to_string(),
    })
}

async fn fetch_deribit_iv_snapshot(
    http: &Client,
    currency: &str,
    spot: f64,
    now_ms: i64,
) -> Option<ImpliedVolSnapshot> {
    let response = http
        .get(DERIBIT_BOOK_SUMMARY_URL)
        .query(&[("currency", currency), ("kind", "option")])
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }

    let payload = response.json::<Value>().await.ok()?;
    let rows = payload.get("result").and_then(|v| v.as_array())?;
    summarize_iv_rows(rows, spot, now_ms)
}

pub fn blend_sigma(realized: Option<f64>, implied: Option<f64>, implied_weight: f64, fallback: f64) -> f64 {
    let w = implied_weight.clamp(0.0, 1.0);
    let blended = match (realized, implied) {
        (Some(rv), Some(iv)) => ((1.0 - w) * rv) + (w * iv),
        (Some(rv), None) => rv,
        (None, Some(iv)) => iv,
        (None, None) => fallback,
    };
    blended.clamp(0.05, 3.50)
}

pub fn spawn_deribit_iv_feed(
    currency: &'static str,
    spot: Arc<RwLock<(f64, i64)>>,
    sink: Arc<RwLock<Option<ImpliedVolSnapshot>>>,
) {
    tokio::spawn(async move {
        let refresh_ms = env_u64("DERIBIT_IV_REFRESH_MS", 12_000, 1_000, 120_000);
        let timeout_ms = env_u64("DERIBIT_IV_HTTP_TIMEOUT_MS", 4_000, 500, 20_000);
        let max_spot_age_ms = env_u64("DERIBIT_IV_MAX_SPOT_AGE_MS", 15_000, 2_000, 120_000) as i64;
        let min_samples = env_u64("DERIBIT_IV_MIN_SAMPLES", 4, 1, 64) as usize;

        let http = match Client::builder()
            .user_agent("ai-trader/deribit-iv")
            .timeout(Duration::from_millis(timeout_ms))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                warn!("Deribit IV feed setup failed for {}: {}", currency, error);
                return;
            }
        };

        loop {
            let now_ms = Utc::now().timestamp_millis();
            let (spot_px, spot_ts) = *spot.read().await;
            let spot_is_fresh = spot_px > 0.0 && now_ms.saturating_sub(spot_ts) <= max_spot_age_ms;
            if !spot_is_fresh {
                sleep(Duration::from_millis(refresh_ms)).await;
                continue;
            }

            match fetch_deribit_iv_snapshot(&http, currency, spot_px, now_ms).await {
                Some(snapshot) if snapshot.sample_count >= min_samples => {
                    let mut writer = sink.write().await;
                    *writer = Some(snapshot);
                }
                Some(snapshot) => {
                    debug!(
                        "Deribit IV {} sample skipped: {} rows < min {}",
                        currency, snapshot.sample_count, min_samples
                    );
                }
                None => {}
            }
            sleep(Duration::from_millis(refresh_ms)).await;
        }
    });
}
