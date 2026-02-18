# Paper Soak Report

This script runs a paper-only soak and grades launch readiness for:

1. preflight reliability
2. shortfall/netting block rates
3. edge capture quality (gross vs net return)
4. strategy allocator posture (base multiplier + meta overlay)

## Run

Use backend container token automatically:

```bash
scripts/run_paper_soak_report.sh
```

Or run directly:

```bash
CONTROL_PLANE_TOKEN=... \
API_URL=http://localhost:5114 \
SOAK_DURATION_SECONDS=86400 \
SOAK_SAMPLE_INTERVAL_SECONDS=5 \
node scripts/paper_soak_report.js
```

## Outputs

1. JSON report: `reports/paper_soak_report.json`
2. Markdown summary: `reports/paper_soak_report.md`
3. sampled stats stream: `reports/paper_soak_stats.jsonl`

## Useful Env Overrides

1. `SOAK_DURATION_SECONDS` default `86400`
2. `SOAK_SAMPLE_INTERVAL_SECONDS` default `5`
3. `SOAK_BASELINE_BANKROLL` default `1000`
4. `SOAK_MIN_PREFLIGHT_PASS_RATE_PCT` default `96`
5. `SOAK_MAX_SHORTFALL_BLOCK_RATE_PCT` default `18`
6. `SOAK_MAX_NETTING_BLOCK_RATE_PCT` default `12`
7. `SOAK_MIN_EDGE_CAPTURE_RATIO` default `0.55`
8. `SOAK_OUTPUT_PATH` default `reports/paper_soak_report.json`

## Notes

1. Script enforces `PAPER` mode at start and end.
2. Script resets simulation, validation trades, and rejected-signal logs at start.
3. For conservative rollout defaults, use `config/live_canary_baseline.env`.
