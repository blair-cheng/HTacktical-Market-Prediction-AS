# Real-Data Portfolio Research Pipeline

This folder contains the real-data rebuild of the Kaggle historical-regime retrieval prototype.

The goal is to move from anonymized competition features to auditable market data:
ETF prices, Treasury yields, CPI, VIX, and sentiment proxies.
The pipeline builds daily market-state features, generates historical fragments, searches similar regimes, and evaluates portfolio exposure decisions with walk-forward tests.

## What It Does

```text
Download real market data
        |
        v
Build daily market-state features
        |
        v
Generate historical fragments
        |
        v
Search similar historical regimes
        |
        v
Convert matched outcomes into exposure decisions
        |
        v
Evaluate rolling / recent windows
```

## Key Scripts

- `scripts/download_p0_data.py`
  Downloads and merges ETF prices, Treasury yields, CPI, VIX, and Fear & Greed data.
- `scripts/generate_fragments.py`
  Builds the fragment library and 5D feature embeddings.
- `scripts/run_p0_walk_forward.py`
  Runs top-1 historical-regime retrieval and exposure rules over time.
- `scripts/score_p0_predictions.py`
  Scores outputs with a Kaggle-style adjusted Sharpe formula and rolling windows.

## Core Modules

- `src/mirror_portfolio/features.py`
  Feature transforms and market forward excess return construction.
- `src/mirror_portfolio/fragments.py`
  Historical fragment generation.
- `src/mirror_portfolio/similarity.py`
  Cosine / Euclidean retrieval over fragment features.
- `src/mirror_portfolio/allocator.py`
  Rule-based allocation helpers.
- `src/mirror_portfolio/walk_forward.py`
  Walk-forward evaluation utilities.

## Current Public Outputs

Raw market data, generated fragments, embeddings, and CSV outputs are intentionally not committed.
The public repo keeps the reproducible code path and the project explanation clean.

## Current Finding

The first full-period score was misleading because the early real-data version had too little searchable history.
The model behaves more meaningfully once the historical library has enough fragments, so the current evaluation focuses on recent rolling windows and minimum history depth rather than a single full-period metric.

## AI Workflow

See `docs/AI_WORKFLOW.md` for how I used Codex and Claude Code together:
implementation agent, review agent, persistent decision log, and repeated audit cycles.

## Disclaimer

This is a research and engineering work sample, not investment advice.
