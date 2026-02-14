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

pub fn side_cost_rate(model: SimCostModel, style: FillStyle) -> f64 {
    match style {
        FillStyle::Maker => model.maker_side_cost_rate(),
        FillStyle::Taker => model.per_side_cost_rate(),
    }
}

pub fn net_return_after_cost(gross_return: f64, model: SimCostModel) -> f64 {
    gross_return - model.round_trip_cost_rate()
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
}
