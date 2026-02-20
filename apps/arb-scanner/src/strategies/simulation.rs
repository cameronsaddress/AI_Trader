use std::env;

#[derive(Debug, Clone, Copy)]
pub struct SimCostModel {
    pub fee_bps_per_side: f64,
    pub slippage_bps_per_side: f64,
    pub maker_rebate_bps_per_side: f64,
    pub maker_adverse_bps_per_side: f64,
}

impl Default for SimCostModel {
    fn default() -> Self {
        Self {
            fee_bps_per_side: 10.0,
            slippage_bps_per_side: 8.0,
            maker_rebate_bps_per_side: 1.5,
            maker_adverse_bps_per_side: 2.0,
        }
    }
}

impl SimCostModel {
    pub fn from_env() -> Self {
        let mut model = Self::default();
        if let Ok(raw) = env::var("SIM_FEE_BPS_PER_SIDE") {
            if let Ok(parsed) = raw.parse::<f64>() {
                if parsed.is_finite() && parsed >= 0.0 {
                    model.fee_bps_per_side = parsed;
                }
            }
        }
        if let Ok(raw) = env::var("SIM_SLIPPAGE_BPS_PER_SIDE") {
            if let Ok(parsed) = raw.parse::<f64>() {
                if parsed.is_finite() && parsed >= 0.0 {
                    model.slippage_bps_per_side = parsed;
                }
            }
        }
        if let Ok(raw) = env::var("SIM_MAKER_REBATE_BPS_PER_SIDE") {
            if let Ok(parsed) = raw.parse::<f64>() {
                if parsed.is_finite() && parsed >= 0.0 {
                    model.maker_rebate_bps_per_side = parsed;
                }
            }
        }
        if let Ok(raw) = env::var("SIM_MAKER_ADVERSE_BPS_PER_SIDE") {
            if let Ok(parsed) = raw.parse::<f64>() {
                if parsed.is_finite() && parsed >= 0.0 {
                    model.maker_adverse_bps_per_side = parsed;
                }
            }
        }
        model
    }

    pub fn per_side_cost_rate(self) -> f64 {
        (self.fee_bps_per_side + self.slippage_bps_per_side) / 10_000.0
    }

    pub fn maker_side_cost_rate(self) -> f64 {
        // Passive fills earn rebate but can suffer adverse selection while resting.
        let effective_bps = (self.fee_bps_per_side + self.maker_adverse_bps_per_side)
            - self.maker_rebate_bps_per_side;
        effective_bps.max(0.0) / 10_000.0
    }

    pub fn round_trip_cost_rate(self) -> f64 {
        self.per_side_cost_rate() * 2.0
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FillStyle {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy)]
pub struct MakerFillOutcome {
    pub fill_probability: f64,
    pub filled: bool,
    pub execution_price: f64,
    pub adverse_bps: f64,
}

pub fn side_cost_rate(model: SimCostModel, style: FillStyle) -> f64 {
    match style {
        FillStyle::Maker => model.maker_side_cost_rate(),
        FillStyle::Taker => model.per_side_cost_rate(),
    }
}

pub fn net_return_after_cost(gross_return: f64, model: SimCostModel) -> f64 {
    gross_return - model.round_trip_cost_rate()
}

pub fn estimate_maker_fill_probability(spread: f64, book_age_ms: i64, expected_net_return: f64) -> f64 {
    let spread_factor = ((spread - 0.01) / 0.10).clamp(0.0, 1.0);
    let freshness_factor = (1.0 - ((book_age_ms.max(0) as f64) / 2_500.0)).clamp(0.0, 1.0);
    let edge_factor = (expected_net_return / 0.01).clamp(0.0, 1.5) / 1.5;
    (0.10 + (0.45 * spread_factor) + (0.30 * freshness_factor) + (0.15 * edge_factor)).clamp(0.05, 0.95)
}

pub fn simulate_maker_entry_fill(
    entry_price: f64,
    spread: f64,
    book_age_ms: i64,
    expected_net_return: f64,
    model: SimCostModel,
    uniform_sample: f64,
) -> MakerFillOutcome {
    let fill_probability = estimate_maker_fill_probability(spread, book_age_ms, expected_net_return);
    let sampled = uniform_sample.clamp(0.0, 1.0);
    if sampled > fill_probability {
        return MakerFillOutcome {
            fill_probability,
            filled: false,
            execution_price: entry_price,
            adverse_bps: 0.0,
        };
    }

    let stale_factor = ((book_age_ms.max(0) as f64) / 2_500.0).clamp(0.0, 1.0);
    let adverse_confidence = ((1.0 - fill_probability) * 0.65 + stale_factor * 0.35).clamp(0.0, 1.0);
    let adverse_bps = (model.maker_adverse_bps_per_side * (0.4 + adverse_confidence)).clamp(0.0, 50.0);
    let adverse_rate = adverse_bps / 10_000.0;
    let spread_slip = spread.max(0.0) * adverse_confidence * 0.50;
    let execution_price = (entry_price * (1.0 + adverse_rate) + spread_slip).clamp(0.001, 0.999);

    MakerFillOutcome {
        fill_probability,
        filled: true,
        execution_price,
        adverse_bps,
    }
}

pub fn realized_pnl(size_usd: f64, gross_return: f64, model: SimCostModel) -> f64 {
    if !size_usd.is_finite() || size_usd <= 0.0 {
        return 0.0;
    }

    let bounded_gross = gross_return.max(-0.999);
    let net_return = net_return_after_cost(bounded_gross, model);
    size_usd * net_return
}

pub fn realized_pnl_with_fill(
    size_usd: f64,
    gross_return: f64,
    model: SimCostModel,
    entry_style: FillStyle,
    exit_style: FillStyle,
) -> f64 {
    if !size_usd.is_finite() || size_usd <= 0.0 {
        return 0.0;
    }

    let bounded_gross = gross_return.max(-0.999);
    let total_cost = side_cost_rate(model, entry_style) + side_cost_rate(model, exit_style);
    size_usd * (bounded_gross - total_cost)
}

pub fn pair_net_edge_after_cost(gross_edge: f64, model: SimCostModel) -> f64 {
    // Atomic pair enters two legs; apply entry-side costs to each leg.
    gross_edge - (2.0 * model.per_side_cost_rate())
}

/// Polymarket dynamic taker fee for binary option contracts.
/// Formula: fee(p) = p * (1-p) * base_fee_rate
/// Maximum at p=0.50 (= 25% of base_rate), approaches 0 at price extremes.
/// For 15-min crypto markets, base_rate ≈ 2% → max fee ≈ 50 bps at 0.50.
pub fn polymarket_taker_fee(price: f64, base_fee_rate: f64) -> f64 {
    if !price.is_finite() || price <= 0.0 || price >= 1.0 || !base_fee_rate.is_finite() {
        return 0.0;
    }
    price * (1.0 - price) * base_fee_rate
}

/// Adaptive slippage that blends static model with recent observed slippage.
/// `base_bps`: static slippage estimate from SimCostModel
/// `hour_utc`: current hour (0-23) for time-of-day adjustment
/// `recent_slippage_bps`: observed slippage from recent trades (0.0 if unknown)
pub fn adaptive_slippage(base_bps: f64, hour_utc: u32, recent_slippage_bps: f64) -> f64 {
    // Time-of-day multiplier: higher slippage during thin liquidity periods
    let time_mult = match hour_utc {
        0..=4 => 1.3,    // Asian quiet hours
        5..=7 => 1.1,    // Pre-London
        8..=10 => 0.9,   // London open (deep liquidity)
        11..=13 => 1.0,  // Mid-day
        14..=16 => 0.85, // US open (deepest liquidity)
        17..=20 => 1.0,  // US afternoon
        21..=23 => 1.2,  // Post-US close
        _ => 1.0,
    };

    if recent_slippage_bps > 0.0 {
        // Blend: 70% recent data, 30% static model
        let blended = 0.7 * recent_slippage_bps + 0.3 * base_bps;
        (blended * time_mult).max(2.0)
    } else {
        (base_bps * time_mult).max(2.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_cost_is_two_sides() {
        let model = SimCostModel {
            fee_bps_per_side: 10.0,
            slippage_bps_per_side: 15.0,
            maker_rebate_bps_per_side: 1.5,
            maker_adverse_bps_per_side: 2.0,
        };
        assert!((model.per_side_cost_rate() - 0.0025).abs() < 1e-12);
        assert!((model.round_trip_cost_rate() - 0.0050).abs() < 1e-12);
    }

    #[test]
    fn realized_pnl_includes_cost() {
        let model = SimCostModel {
            fee_bps_per_side: 10.0,
            slippage_bps_per_side: 10.0,
            maker_rebate_bps_per_side: 1.5,
            maker_adverse_bps_per_side: 2.0,
        };
        let pnl = realized_pnl(1000.0, 0.01, model);
        // 1.00% gross - 0.40% cost = 0.60% net => $6
        assert!((pnl - 6.0).abs() < 1e-9);
    }

    #[test]
    fn pair_edge_after_cost_reduces_edge() {
        let model = SimCostModel {
            fee_bps_per_side: 12.0,
            slippage_bps_per_side: 8.0,
            maker_rebate_bps_per_side: 1.5,
            maker_adverse_bps_per_side: 2.0,
        };
        let net = pair_net_edge_after_cost(0.01, model);
        // Two entry legs at 20 bps each => 40 bps total.
        assert!((net - 0.006).abs() < 1e-12);
    }

    #[test]
    fn maker_entry_reduces_cost_relative_to_taker() {
        let model = SimCostModel {
            fee_bps_per_side: 10.0,
            slippage_bps_per_side: 10.0,
            maker_rebate_bps_per_side: 2.0,
            maker_adverse_bps_per_side: 1.0,
        };
        let taker_round_trip = realized_pnl_with_fill(
            1000.0,
            0.01,
            model,
            FillStyle::Taker,
            FillStyle::Taker,
        );
        let maker_entry_round_trip = realized_pnl_with_fill(
            1000.0,
            0.01,
            model,
            FillStyle::Maker,
            FillStyle::Taker,
        );
        assert!(maker_entry_round_trip > taker_round_trip);
    }

    #[test]
    fn polymarket_fee_curve_peaks_at_half() {
        let rate = 0.02; // 2% base
        let fee_50 = polymarket_taker_fee(0.50, rate);
        let fee_30 = polymarket_taker_fee(0.30, rate);
        let fee_10 = polymarket_taker_fee(0.10, rate);
        // At p=0.50: 0.50*0.50*0.02 = 0.005 (50 bps)
        assert!((fee_50 - 0.005).abs() < 1e-12);
        // At p=0.30: 0.30*0.70*0.02 = 0.0042 (42 bps)
        assert!((fee_30 - 0.0042).abs() < 1e-12);
        // At p=0.10: 0.10*0.90*0.02 = 0.0018 (18 bps)
        assert!((fee_10 - 0.0018).abs() < 1e-12);
        // Fee at 0.50 > 0.30 > 0.10
        assert!(fee_50 > fee_30);
        assert!(fee_30 > fee_10);
        // Edge cases
        assert_eq!(polymarket_taker_fee(0.0, rate), 0.0);
        assert_eq!(polymarket_taker_fee(1.0, rate), 0.0);
    }

    #[test]
    fn maker_fill_probability_prefers_wider_fresh_books() {
        let wide_fresh = estimate_maker_fill_probability(0.08, 200, 0.003);
        let tight_stale = estimate_maker_fill_probability(0.01, 3_500, 0.003);
        assert!(wide_fresh > tight_stale);
    }

    #[test]
    fn maker_fill_simulation_returns_miss_when_sample_exceeds_probability() {
        let model = SimCostModel::default();
        let outcome = simulate_maker_entry_fill(0.42, 0.02, 200, 0.001, model, 0.99);
        assert!(!outcome.filled);
        assert!((outcome.execution_price - 0.42).abs() < 1e-9);
    }

    #[test]
    fn maker_fill_simulation_applies_adverse_price_on_fill() {
        let model = SimCostModel {
            fee_bps_per_side: 10.0,
            slippage_bps_per_side: 8.0,
            maker_rebate_bps_per_side: 1.5,
            maker_adverse_bps_per_side: 4.0,
        };
        let outcome = simulate_maker_entry_fill(0.40, 0.06, 1_500, 0.004, model, 0.0);
        assert!(outcome.filled);
        assert!(outcome.execution_price >= 0.40);
        assert!(outcome.adverse_bps > 0.0);
    }
}
