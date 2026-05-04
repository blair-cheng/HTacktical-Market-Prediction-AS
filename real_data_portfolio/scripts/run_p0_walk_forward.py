#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.mirror_portfolio.features import (
    MarketStateFeatureBuilder,
    add_market_forward_excess_columns,
    default_specs,
)
from src.mirror_portfolio.similarity import SimilaritySearcher


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run old-style P0 walk-forward over real fragments.")
    parser.add_argument("--fragments", default="data/processed/fragments.csv")
    parser.add_argument("--features", default="data/processed/feature_columns.txt")
    parser.add_argument("--market-state", default="data/processed/market_state.csv")
    parser.add_argument("--output", default="data/processed/p0_walk_forward.csv")
    parser.add_argument("--summary", default="data/processed/p0_walk_forward_summary.csv")
    parser.add_argument("--min-history", type=int, default=252)
    parser.add_argument("--forward-window", type=int, default=20)
    parser.add_argument("--holding-window", type=int, default=1)
    parser.add_argument("--threshold", type=float, default=0.001)
    parser.add_argument("--asset", default="SPY")
    parser.add_argument("--market-excess-mode", choices=["rolling", "expanding"], default="rolling")
    parser.add_argument("--market-excess-min-periods", type=int, default=None)
    parser.add_argument("--mad-criterion", type=float, default=4.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fragments = pd.read_csv(args.fragments)
    fragments["start_date"] = pd.to_datetime(fragments["start_date"])
    fragments["end_date"] = pd.to_datetime(fragments["end_date"])
    fragments = fragments.sort_values("end_date").reset_index(drop=True)
    market_state = pd.read_csv(args.market_state)
    market_state["date"] = pd.to_datetime(market_state["date"])
    market_state = market_state.sort_values("date").reset_index(drop=True)
    market_state = add_market_forward_excess_columns(
        market_state,
        price_col=args.asset,
        min_periods=args.market_excess_min_periods,
        mad_criterion=args.mad_criterion,
        baseline_mode=args.market_excess_mode,
        rfr_col="yield_3m",
    )
    asset_prices = pd.to_numeric(market_state[args.asset], errors="coerce")
    market_state[f"{args.asset}_return_1d"] = asset_prices / asset_prices.shift(1) - 1.0
    features = MarketStateFeatureBuilder(default_specs()).transform(market_state)
    features["date"] = pd.to_datetime(features["date"])

    feature_cols = Path(args.features).read_text().splitlines()
    expected_col = f"future_{args.forward_window}d_excess_returns"
    if expected_col not in fragments.columns:
        raise ValueError(f"Missing expected excess return column: {expected_col}")

    rows: list[dict[str, object]] = []
    for _, query in features.sort_values("date").iterrows():
        today = query["date"]
        # future_20d_excess_returns uses target rows t+1..t+20.
        # Each target row is itself a forward return, so the final target day is
        # only observable one business day later.
        cutoff = today - pd.tseries.offsets.BDay(args.forward_window + 1)
        history = fragments[fragments["end_date"] <= cutoff]
        if len(history) < args.min_history:
            continue

        matches = SimilaritySearcher(history, feature_cols).search(query, top_k=1)
        match = matches.iloc[0]
        expected_return = float(match[expected_col])
        position_2state = 2.0 if expected_return > args.threshold else 0.0
        position_3state = (
            2.0
            if expected_return > args.threshold
            else 0.0
            if expected_return < -args.threshold
            else 1.0
        )
        realized_return = realized_holding_return(market_state, today, args.asset, args.holding_window)
        if realized_return is None:
            continue

        rows.append(
            {
                "date": today.date().isoformat(),
                "matched_date_range_id": match["date_range_id"],
                "matched_end_date": pd.Timestamp(match["end_date"]).date().isoformat(),
                "similarity": float(match["similarity"]),
                "expected_20d_excess_returns": expected_return,
                "realized_holding_return": realized_return,
                "position_2state": position_2state,
                "position_3state": position_3state,
                "strategy_2state_return": position_2state * realized_return,
                "strategy_3state_return": position_3state * realized_return,
                "benchmark_return": realized_return,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        raise ValueError("No walk-forward rows generated")

    periods_per_year = 252 / args.holding_window
    summary = pd.DataFrame(
        [
            summarize(out, "strategy_2state_return", "2state_threshold", periods_per_year),
            summarize(out, "strategy_3state_return", "3state_threshold", periods_per_year),
            summarize(out, "benchmark_return", f"{args.asset}_{args.holding_window}d_benchmark", periods_per_year),
        ]
    )
    summary["threshold"] = args.threshold
    summary["holding_window"] = args.holding_window
    summary["rows"] = len(out)
    summary["start_date"] = out["date"].iloc[0]
    summary["end_date"] = out["date"].iloc[-1]
    summary["mean_similarity"] = out["similarity"].mean()
    summary["median_similarity"] = out["similarity"].median()
    summary["hit_rate_expected_positive"] = (out["expected_20d_excess_returns"] > args.threshold).mean()
    summary["realized_positive_rate"] = (out["realized_holding_return"] > 0).mean()

    output_path = Path(args.output)
    summary_path = Path(args.summary)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    summary.to_csv(summary_path, index=False)

    print(f"wrote {output_path}")
    print(f"wrote {summary_path}")
    print(summary.to_string(index=False))


def realized_holding_return(
    market_state: pd.DataFrame,
    end_date: pd.Timestamp,
    asset: str,
    holding_window: int,
) -> float | None:
    return_col = f"{asset}_return_1d"
    matches = market_state.index[market_state["date"] == end_date].to_list()
    if not matches:
        return None
    start_idx = matches[0] + 1
    end_idx = start_idx + holding_window
    if end_idx > len(market_state):
        return None
    daily_returns = market_state.iloc[start_idx:end_idx][return_col].dropna()
    if len(daily_returns) < holding_window:
        return None
    return float((1.0 + daily_returns).prod() - 1.0)


def summarize(
    df: pd.DataFrame,
    col: str,
    name: str,
    periods_per_year: float,
) -> dict[str, float | str]:
    returns = df[col].astype(float)
    mean_return = returns.mean()
    volatility = returns.std(ddof=0)
    sharpe = mean_return / volatility * np.sqrt(periods_per_year) if volatility > 1e-12 else np.nan
    equity = (1.0 + returns).cumprod()
    drawdown = equity / equity.cummax() - 1.0
    return {
        "strategy": name,
        "mean_period_return": mean_return,
        "period_volatility": volatility,
        "annualized_sharpe": sharpe,
        "cumulative_return": equity.iloc[-1] - 1.0,
        "max_drawdown": drawdown.min(),
        "positive_period_rate": (returns > 0).mean(),
    }


if __name__ == "__main__":
    main()
