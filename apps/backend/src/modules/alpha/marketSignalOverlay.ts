type NullableNumber = number | null;

function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

function finite(value: NullableNumber): value is number {
    return value !== null && Number.isFinite(value);
}

function median(values: number[]): number | null {
    if (values.length === 0) {
        return null;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) {
        return (sorted[middle - 1] + sorted[middle]) / 2;
    }
    return sorted[middle];
}

function asFinite(value: unknown): number | null {
    const parsed = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function firstFinite(input: Array<unknown>): number | null {
    for (const candidate of input) {
        const numeric = asFinite(candidate);
        if (numeric !== null) {
            return numeric;
        }
    }
    return null;
}

export type FundingBasisVenueSnapshot = {
    venue: string;
    funding_rate_8h: number | null;
    basis_bps: number | null;
    updated_at: number;
};

export type FundingBasisAssetSnapshot = {
    asset: string;
    funding_rate_8h: number | null;
    basis_bps: number | null;
    sample_count: number;
    venues: FundingBasisVenueSnapshot[];
    updated_at: number;
};

export type FundingBasisSignal = {
    bias: -1 | 0 | 1;
    strength: number;
    crowding_bps: number;
    reason: string;
};

export function aggregateFundingBasisAsset(
    asset: string,
    venueSnapshots: FundingBasisVenueSnapshot[],
    now = Date.now(),
): FundingBasisAssetSnapshot {
    const filtered = venueSnapshots
        .filter((snapshot) => snapshot.venue.trim().length > 0)
        .map((snapshot) => ({
            venue: snapshot.venue.trim().toUpperCase(),
            funding_rate_8h: finite(snapshot.funding_rate_8h) ? snapshot.funding_rate_8h : null,
            basis_bps: finite(snapshot.basis_bps) ? snapshot.basis_bps : null,
            updated_at: Number.isFinite(snapshot.updated_at) ? snapshot.updated_at : now,
        }));
    const fundingMedian = median(
        filtered
            .map((snapshot) => snapshot.funding_rate_8h)
            .filter(finite),
    );
    const basisMedian = median(
        filtered
            .map((snapshot) => snapshot.basis_bps)
            .filter(finite),
    );
    return {
        asset: asset.trim().toUpperCase(),
        funding_rate_8h: fundingMedian,
        basis_bps: basisMedian,
        sample_count: filtered.length,
        venues: filtered,
        updated_at: now,
    };
}

export function computeFundingBasisSignal(
    snapshot: FundingBasisAssetSnapshot,
    options?: {
        crowding_threshold_bps?: number;
        basis_weight?: number;
    },
): FundingBasisSignal {
    const thresholdBps = Math.max(0.5, options?.crowding_threshold_bps ?? 6);
    const basisWeight = clamp(options?.basis_weight ?? 0.6, 0, 2);
    const fundingBps = finite(snapshot.funding_rate_8h)
        ? snapshot.funding_rate_8h * 10_000
        : null;
    const basisBps = finite(snapshot.basis_bps)
        ? snapshot.basis_bps
        : null;

    if (fundingBps === null && basisBps === null) {
        return {
            bias: 0,
            strength: 0,
            crowding_bps: 0,
            reason: 'no funding/basis sample',
        };
    }

    const crowdingBps = (fundingBps ?? 0) + ((basisBps ?? 0) * basisWeight);
    const crowdingAbs = Math.abs(crowdingBps);
    if (crowdingAbs < thresholdBps) {
        return {
            bias: 0,
            strength: crowdingAbs / thresholdBps,
            crowding_bps: crowdingBps,
            reason: `crowding ${crowdingBps.toFixed(2)}bps below threshold`,
        };
    }

    const bias: -1 | 1 = crowdingBps > 0 ? -1 : 1;
    return {
        bias,
        strength: Math.min(1, crowdingAbs / thresholdBps),
        crowding_bps: crowdingBps,
        reason: crowdingBps > 0
            ? `crowded longs ${crowdingBps.toFixed(2)}bps`
            : `crowded shorts ${crowdingBps.toFixed(2)}bps`,
    };
}

export function directionalOverlayMultiplier(
    directionSign: number,
    biasSign: number,
    strength: number,
    maxBoost: number,
    maxPenalty: number,
): number {
    const direction = Math.sign(directionSign);
    const bias = Math.sign(biasSign);
    if (direction === 0 || bias === 0) {
        return 1;
    }
    const safeStrength = clamp(strength, 0, 1);
    const boost = Math.max(1, maxBoost);
    const penalty = clamp(maxPenalty, 0.05, 1);
    if (direction === bias) {
        return 1 + ((boost - 1) * safeStrength);
    }
    return 1 - ((1 - penalty) * safeStrength);
}

export type PolymarketBookSnapshot = {
    yes_bid: number;
    yes_ask: number;
    no_bid: number;
    no_ask: number;
    timestamp: number;
};

function clampProbability(value: number): number {
    return clamp(value, 0, 1);
}

function bookMidProbability(snapshot: PolymarketBookSnapshot): number {
    const yesMid = (snapshot.yes_bid + snapshot.yes_ask) / 2;
    const noMid = (snapshot.no_bid + snapshot.no_ask) / 2;
    const impliedFromNo = 1 - noMid;
    return clampProbability((yesMid + impliedFromNo) / 2);
}

function averageSpread(snapshot: PolymarketBookSnapshot): number {
    const yesSpread = Math.max(0, snapshot.yes_ask - snapshot.yes_bid);
    const noSpread = Math.max(0, snapshot.no_ask - snapshot.no_bid);
    return (yesSpread + noSpread) / 2;
}

export function extractPolymarketBookSnapshot(
    meta: Record<string, number> | undefined,
    timestamp: number,
): PolymarketBookSnapshot | null {
    if (!meta) {
        return null;
    }
    const yesAsk = firstFinite([
        meta.meta_yes_ask,
        meta.meta_best_ask_yes,
        meta.meta_poly_yes_ask,
    ]);
    const noAsk = firstFinite([
        meta.meta_no_ask,
        meta.meta_poly_no_ask,
    ]);
    const yesBidDirect = firstFinite([
        meta.meta_yes_bid,
        meta.meta_poly_yes_bid,
    ]);
    const noBidDirect = firstFinite([
        meta.meta_no_bid,
        meta.meta_poly_no_bid,
    ]);
    const yesMid = firstFinite([meta.meta_yes_mid]);
    const noMid = firstFinite([meta.meta_no_mid]);
    const yesSpread = firstFinite([meta.meta_yes_spread]);
    const noSpread = firstFinite([meta.meta_no_spread]);

    let yesBid = yesBidDirect;
    let noBid = noBidDirect;
    let normalizedYesAsk = yesAsk;
    let normalizedNoAsk = noAsk;

    if (normalizedYesAsk === null && yesMid !== null && yesSpread !== null) {
        normalizedYesAsk = yesMid + (yesSpread / 2);
    }
    if (normalizedNoAsk === null && noMid !== null && noSpread !== null) {
        normalizedNoAsk = noMid + (noSpread / 2);
    }
    if (yesBid === null && yesMid !== null && normalizedYesAsk !== null) {
        yesBid = (2 * yesMid) - normalizedYesAsk;
    }
    if (noBid === null && noMid !== null && normalizedNoAsk !== null) {
        noBid = (2 * noMid) - normalizedNoAsk;
    }
    if (yesBid === null && normalizedYesAsk !== null && yesSpread !== null) {
        yesBid = normalizedYesAsk - yesSpread;
    }
    if (noBid === null && normalizedNoAsk !== null && noSpread !== null) {
        noBid = normalizedNoAsk - noSpread;
    }

    if (
        yesBid === null
        || normalizedYesAsk === null
        || noBid === null
        || normalizedNoAsk === null
    ) {
        return null;
    }

    const yesBidClamped = clampProbability(yesBid);
    const yesAskClamped = clampProbability(normalizedYesAsk);
    const noBidClamped = clampProbability(noBid);
    const noAskClamped = clampProbability(normalizedNoAsk);
    if (
        yesAskClamped < yesBidClamped
        || noAskClamped < noBidClamped
    ) {
        return null;
    }
    return {
        yes_bid: yesBidClamped,
        yes_ask: yesAskClamped,
        no_bid: noBidClamped,
        no_ask: noAskClamped,
        timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
    };
}

export type PolymarketMicrostructureSignal = {
    direction: -1 | 0 | 1;
    strength: number;
    mid_shift_bps: number;
    spread_bps: number;
    reason: string;
};

export function computePolymarketMicrostructureSignal(
    previous: PolymarketBookSnapshot | null,
    current: PolymarketBookSnapshot,
    options?: {
        min_shift_bps?: number;
        max_spread_bps?: number;
    },
): PolymarketMicrostructureSignal {
    const minShiftBps = Math.max(1, options?.min_shift_bps ?? 30);
    const maxSpreadBps = Math.max(10, options?.max_spread_bps ?? 450);
    const spreadBps = averageSpread(current) * 10_000;
    if (spreadBps > maxSpreadBps) {
        return {
            direction: 0,
            strength: 0,
            mid_shift_bps: 0,
            spread_bps: spreadBps,
            reason: `spread ${spreadBps.toFixed(1)}bps exceeds max`,
        };
    }

    const currentMid = bookMidProbability(current);
    const currentBias = currentMid - 0.5;
    const currentBiasStrength = clamp(Math.abs(currentBias) / 0.12, 0, 1);
    const currentBiasDirection = Math.sign(currentBias) as -1 | 0 | 1;

    if (!previous) {
        return {
            direction: currentBiasDirection,
            strength: currentBiasStrength * 0.40,
            mid_shift_bps: 0,
            spread_bps: spreadBps,
            reason: 'bootstrap snapshot',
        };
    }

    if (current.timestamp <= previous.timestamp) {
        return {
            direction: 0,
            strength: 0,
            mid_shift_bps: 0,
            spread_bps: spreadBps,
            reason: 'non-monotonic timestamp',
        };
    }

    const previousMid = bookMidProbability(previous);
    const midShiftBps = (currentMid - previousMid) * 10_000;
    const shiftStrength = clamp(Math.abs(midShiftBps) / minShiftBps, 0, 1);
    const shiftDirection = Math.sign(midShiftBps) as -1 | 0 | 1;

    let direction: -1 | 0 | 1 = 0;
    if (shiftStrength >= 0.4) {
        direction = shiftDirection;
    }
    if (currentBiasStrength >= 0.35) {
        if (direction === 0) {
            direction = currentBiasDirection;
        } else if (direction !== currentBiasDirection && currentBiasStrength > shiftStrength) {
            direction = currentBiasDirection;
        }
    }

    const agreement = shiftDirection !== 0 && shiftDirection === currentBiasDirection ? 0.15 : 0;
    const strength = clamp((shiftStrength * 0.65) + (currentBiasStrength * 0.35) + agreement, 0, 1);

    if (direction === 0 || strength < 0.20) {
        return {
            direction: 0,
            strength,
            mid_shift_bps: midShiftBps,
            spread_bps: spreadBps,
            reason: 'weak directional signal',
        };
    }

    return {
        direction,
        strength,
        mid_shift_bps: midShiftBps,
        spread_bps: spreadBps,
        reason: Math.abs(midShiftBps) >= minShiftBps
            ? 'sweep impulse'
            : 'book bias',
    };
}
