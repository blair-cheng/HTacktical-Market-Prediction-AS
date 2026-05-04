# Historical Regime Retrieval for Market Decisions

This repository documents a market-decision system I built around historical analogy retrieval: encode the current market state, retrieve similar historical fragments, and translate the matched future outcomes into decision support.

The most recent version was adapted for the Kaggle Hull Tactical Market Prediction competition, where I built a 5D market-state embedding pipeline, cosine-similarity retrieval over historical fragments, and a low-latency Polars inference path for portfolio exposure decisions. The earlier code in this repository shows the BigQuery / SQL prototype behind the same idea: a "Mirror of History" engine that retrieved comparable historical market narratives and generated multi-scenario decision briefs.

## Why This Build Matters

- It turns a vague decision problem into a working retrieval system:
  current state -> historical analogs -> future outcomes -> action.
- It combines product thinking and engineering:
  data pipeline, feature design, vector retrieval, inference latency, and post-launch evaluation.
- It exposed a useful systems lesson after submission:
  monthly reruns showed regime overfitting, so I audited the data pipeline and began rebuilding the approach on real market data with stricter rolling-window evaluation.

## My Contribution

I designed and implemented the core system logic:

- feature engineering for market state representation;
- historical fragment generation and future-outcome labeling;
- vector / embedding retrieval logic;
- decision rules that convert matched outcomes into portfolio exposure;
- BigQuery SQL prototype for large-scale historical retrieval;
- Kaggle-style inference flow optimized for low-latency evaluation;
- post-submission analysis of regime mismatch and model degradation.

## System Shape

```text
Market / macro / sentiment data
        |
        v
Market-state features
        |
        v
Historical fragment library
        |
        v
Similarity search
        |
        v
Matched future outcomes
        |
        v
Decision / scenario output
```

## Repository Map

```text
real_data_portfolio/
  src/                         real-data feature, fragment, retrieval, and evaluation code
  scripts/                     data download, fragment generation, walk-forward, scoring
  docs/                        AI workflow notes

sql/
  01_raw_ingest/              raw market data loading
  02_feature_engineering/     fused historical state construction
  03_embedding_generation/    embedding table generation
  04_inference/               historical mirror retrieval and generation
  05_output/                  export scripts

v1v3prompt report compare, 2023-08-21 to 2023-10-16/
  comparison analysis, report generation, and validation scripts
```

## Earlier Prototype: Mirror of History

The original prototype was a BigQuery-native historical analogy agent for financial decision support.
It generated multi-scenario briefings by retrieving similar historical periods and comparing what happened afterward.

That version was intentionally more narrative and strategic.
The Kaggle Hull version compressed the same core idea into a stricter prediction/inference pipeline:
5D embeddings, fragment matching, and portfolio exposure output.

## Post-Submission Learning

After the Kaggle submission, monthly rerun scores changed sharply.
I treated that as a regime-mismatch signal rather than just a leaderboard movement.

That debugging process became the next iteration:
separate competition-artifact performance from real out-of-sample robustness,
verify data-source coverage, avoid leakage, and evaluate on rolling recent windows instead of relying on a single full-period score.

The real-data rebuild code is in `real_data_portfolio/`.

## Status

This repo is a public work-sample snapshot.
The BigQuery SQL prototype is preserved here; the real-data portfolio rebuild is included as a compact research pipeline under `real_data_portfolio/`.

## Disclaimer

This project is for research and decision-system demonstration only.
It is not investment advice.
