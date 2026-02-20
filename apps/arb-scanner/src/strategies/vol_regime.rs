/// Volatility regime detection module.
/// Strategies query this to adjust edge thresholds and position sizing.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Regime {
    Trending,      // Hurst > 0.55 — momentum strategies preferred
    MeanReverting, // Hurst < 0.45 — reversion strategies preferred
    Choppy,        // 0.45 <= Hurst <= 0.55 — reduce size, widen thresholds
}

#[derive(Debug, Clone)]
pub struct VolRegime {
    pub regime: Regime,
    pub hurst_exponent: f64,
    pub parkinson_vol: f64, // Annualized close-to-close volatility proxy
}

/// Detect the current volatility regime from a time series of (timestamp_ms, price) tuples.
/// Requires at least 50 data points for meaningful estimation.
pub fn detect_regime(price_history: &[(i64, f64)]) -> Option<VolRegime> {
    if price_history.len() < 50 {
        return None;
    }

    let prices: Vec<f64> = price_history.iter().map(|(_, p)| *p).collect();

    // Compute Hurst exponent via R/S analysis (simplified)
    let hurst = estimate_hurst(&prices);

    // Close-to-close volatility proxy (high/low candles are unavailable here).
    let parkinson = estimate_close_to_close_vol(&prices);

    let regime = if hurst > 0.55 {
        Regime::Trending
    } else if hurst < 0.45 {
        Regime::MeanReverting
    } else {
        Regime::Choppy
    };

    Some(VolRegime {
        regime,
        hurst_exponent: hurst,
        parkinson_vol: parkinson,
    })
}

/// Returns a sizing multiplier based on the detected regime.
/// Trending: 1.2x (momentum-friendly), MeanReverting: 1.0x, Choppy: 0.7x (reduce risk)
pub fn regime_sizing_multiplier(regime: &VolRegime) -> f64 {
    match regime.regime {
        Regime::Trending => 1.2,
        Regime::MeanReverting => 1.0,
        Regime::Choppy => 0.7,
    }
}

/// Returns an edge threshold adjustment in basis points.
/// Trending: -5 bps (easier entries), Choppy: +10 bps (harder entries)
pub fn regime_edge_adjustment_bps(regime: &VolRegime) -> f64 {
    match regime.regime {
        Regime::Trending => -0.0005,
        Regime::MeanReverting => 0.0,
        Regime::Choppy => 0.001,
    }
}

/// Returns (tp_multiplier, sl_multiplier) for adapting exit thresholds to the regime.
/// Trending: widen TP (1.5x), keep SL. Choppy: tighten both (0.7x). MeanReverting: standard.
pub fn regime_exit_multipliers(regime: &VolRegime) -> (f64, f64) {
    match regime.regime {
        Regime::Trending => (1.5, 1.0),      // Let winners run in trends
        Regime::MeanReverting => (1.0, 1.0),  // Standard exits
        Regime::Choppy => (0.7, 0.7),         // Tighten both in choppy markets
    }
}

/// Simplified R/S Hurst exponent estimation.
/// H > 0.5: trending/persistent, H < 0.5: mean-reverting, H ≈ 0.5: random walk.
fn estimate_hurst(prices: &[f64]) -> f64 {
    let n = prices.len();
    if n < 20 {
        return 0.5;
    }

    let returns: Vec<f64> = prices.windows(2)
        .map(|w| if w[0] > 0.0 { (w[1] / w[0]).ln() } else { 0.0 })
        .collect();

    let mut rs_values: Vec<(f64, f64)> = Vec::new(); // (log(n), log(R/S))

    let mut chunk_sizes = Vec::new();
    let mut chunk_size = 8usize;
    let max_chunk_size = (returns.len() / 2).max(chunk_size);
    while chunk_size <= max_chunk_size {
        if returns.len() / chunk_size >= 4 {
            chunk_sizes.push(chunk_size);
        }
        let next = ((chunk_size as f64) * 1.4).round() as usize;
        chunk_size = if next <= chunk_size { chunk_size + 1 } else { next };
    }

    for chunk_size in chunk_sizes {
        if chunk_size > returns.len() { continue; }
        let mut rs_sum = 0.0;
        let mut count = 0;
        for chunk in returns.chunks(chunk_size) {
            if chunk.len() < chunk_size { continue; }
            let mean: f64 = chunk.iter().sum::<f64>() / chunk.len() as f64;
            let deviations: Vec<f64> = chunk.iter().map(|r| r - mean).collect();
            let cumulative: Vec<f64> = deviations.iter()
                .scan(0.0, |acc, &x| { *acc += x; Some(*acc) })
                .collect();
            let range = cumulative.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                - cumulative.iter().cloned().fold(f64::INFINITY, f64::min);
            let std_dev = (chunk.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / chunk.len() as f64).sqrt();
            if std_dev > 1e-12 {
                rs_sum += range / std_dev;
                count += 1;
            }
        }
        if count > 0 {
            let avg_rs = rs_sum / count as f64;
            rs_values.push(((chunk_size as f64).ln(), avg_rs.ln()));
        }
    }

    if rs_values.len() < 2 {
        return 0.5;
    }

    // Linear regression: log(R/S) = H * log(n) + c
    let n_pts = rs_values.len() as f64;
    let sum_x: f64 = rs_values.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = rs_values.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = rs_values.iter().map(|(x, y)| x * y).sum();
    let sum_xx: f64 = rs_values.iter().map(|(x, _)| x * x).sum();

    let denom = n_pts * sum_xx - sum_x * sum_x;
    if denom.abs() < 1e-12 {
        return 0.5;
    }

    let hurst = (n_pts * sum_xy - sum_x * sum_y) / denom;
    hurst.clamp(0.0, 1.0)
}

/// Annualized close-to-close volatility estimator.
/// This module receives only close prices, so true Parkinson
/// high/low range volatility cannot be computed here.
/// Returns annualized volatility.
fn estimate_close_to_close_vol(prices: &[f64]) -> f64 {
    if prices.len() < 10 {
        return 0.0;
    }

    // Use rolling windows to estimate volatility
    let returns: Vec<f64> = prices.windows(2)
        .map(|w| if w[0] > 0.0 { (w[1] / w[0]).ln() } else { 0.0 })
        .collect();

    let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance: f64 = returns.iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>() / returns.len() as f64;

    // Annualize: assuming 5-minute bars, 288 per day, 365 days
    let daily_factor = (288.0_f64).sqrt();
    let annual_factor = (365.0_f64).sqrt();
    variance.sqrt() * daily_factor * annual_factor
}
