#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score P0 walk-forward predictions with the old Kaggle formula.")
    parser.add_argument("--predictions", default="data/processed/p0_walk_forward.csv")
    parser.add_argument("--market-state", default="data/processed/market_state.csv")
    parser.add_argument("--asset", default="SPY")
    parser.add_argument("--summary-output", default="data/processed/p0_score_calculator_results.csv")
    parser.add_argument("--windows-output", default="data/processed/p0_score_by_window.csv")
    parser.add_argument("--risk-free-col", default="auto")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    predictions = pd.read_csv(args.predictions)
    market_state = pd.read_csv(args.market_state)
    predictions["date"] = pd.to_datetime(predictions["date"])
    market_state["date"] = pd.to_datetime(market_state["date"])
    market_state = market_state.sort_values("date").reset_index(drop=True)

    if args.asset not in market_state.columns:
        raise ValueError(f"Missing asset column: {args.asset}")

    prices = pd.to_numeric(market_state[args.asset], errors="coerce")
    solution = market_state[["date"]].copy()
    solution["forward_returns"] = prices.shift(-1) / prices - 1.0
    risk_free_col = resolve_risk_free_col(market_state, args.risk_free_col)
    solution["risk_free_rate"] = to_daily_risk_free_rate(market_state[risk_free_col])
    solution = solution.merge(predictions, on="date", how="inner")
    solution = solution.dropna(subset=["forward_returns", "risk_free_rate"]).reset_index(drop=True)

    if solution.empty:
        raise ValueError("No aligned scoring rows")

    strategy_positions = {
        "cash": pd.Series(0.0, index=solution.index),
        args.asset: pd.Series(1.0, index=solution.index),
    }
    for col in ["position_2state", "position_3state"]:
        if col in solution.columns:
            strategy_positions[col] = pd.to_numeric(solution[col], errors="coerce")

    summary_rows = [
        score_position(solution, name, position)
        for name, position in strategy_positions.items()
    ]
    summary = pd.DataFrame(summary_rows)
    summary["rows"] = len(solution)
    summary["start_date"] = solution["date"].min().date().isoformat()
    summary["end_date"] = solution["date"].max().date().isoformat()
    summary["risk_free_col"] = risk_free_col

    windows = build_windows(solution)
    window_rows = []
    for window_name, window_df in windows:
        for name, position in strategy_positions.items():
            sub_position = position.loc[window_df.index]
            row = score_position(window_df, name, sub_position)
            row["window"] = window_name
            row["rows"] = len(window_df)
            row["start_date"] = window_df["date"].min().date().isoformat()
            row["end_date"] = window_df["date"].max().date().isoformat()
            row["risk_free_col"] = risk_free_col
            window_rows.append(row)
    by_window = pd.DataFrame(window_rows)

    summary_path = Path(args.summary_output)
    windows_path = Path(args.windows_output)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(summary_path, index=False)
    by_window.to_csv(windows_path, index=False)

    print(f"wrote {summary_path}")
    print(summary.to_string(index=False))
    print(f"wrote {windows_path}")
    key_windows = by_window[by_window["window"].isin(["all_current", "year_2025", "last_180_rows"])]
    print(key_windows.to_string(index=False))


def resolve_risk_free_col(market_state: pd.DataFrame, requested: str) -> str:
    if requested != "auto":
        if requested not in market_state.columns:
            raise ValueError(f"Missing requested risk-free column: {requested}")
        return requested
    for col in ["risk_free_rate", "fed_funds", "yield_3m", "yield_10y"]:
        if col in market_state.columns:
            return col
    raise ValueError("No risk-free proxy found. Expected one of risk_free_rate, fed_funds, yield_3m, yield_10y.")


def to_daily_risk_free_rate(raw: pd.Series) -> pd.Series:
    values = pd.to_numeric(raw, errors="coerce").ffill()
    median_abs = values.abs().median()
    if pd.isna(median_abs):
        return values
    if median_abs > 0.02:
        return values / 100.0 / 252.0
    return values


def score_position(solution: pd.DataFrame, name: str, position: pd.Series) -> dict[str, float | str]:
    frame = solution[["forward_returns", "risk_free_rate"]].copy()
    frame["position"] = pd.to_numeric(position, errors="coerce")
    frame = frame.dropna()
    if frame.empty:
        return {"strategy": name, "score": np.nan}
    if frame["position"].max() > 2.0 or frame["position"].min() < 0.0:
        raise ValueError(f"{name} position outside [0, 2]")

    strategy_returns = frame["risk_free_rate"] + frame["position"] * (
        frame["forward_returns"] - frame["risk_free_rate"]
    )
    strategy_excess_returns = frame["position"] * (frame["forward_returns"] - frame["risk_free_rate"])
    strategy_excess_cumulative = (1.0 + strategy_excess_returns).prod()
    strategy_mean_excess_return = strategy_excess_cumulative ** (1.0 / len(frame)) - 1.0
    strategy_std = strategy_returns.std()

    trading_days_per_year = 252
    raw_sharpe = (
        strategy_mean_excess_return / strategy_std * np.sqrt(trading_days_per_year)
        if strategy_std and strategy_std > 0
        else np.nan
    )

    market_excess_returns = frame["forward_returns"] - frame["risk_free_rate"]
    market_excess_cumulative = (1.0 + market_excess_returns).prod()
    market_mean_excess_return = market_excess_cumulative ** (1.0 / len(frame)) - 1.0
    market_std = frame["forward_returns"].std()

    excess_vol = max(0.0, strategy_std / market_std - 1.2) if market_std and market_std > 0 else 0.0
    vol_penalty = 1.0 + excess_vol
    return_gap = max(0.0, (market_mean_excess_return - strategy_mean_excess_return) * 100.0 * trading_days_per_year)
    return_penalty = 1.0 + (return_gap**2) / 100.0
    score = raw_sharpe / (vol_penalty * return_penalty) if raw_sharpe == raw_sharpe else np.nan
    strategy_cumulative = (1.0 + strategy_returns).prod() - 1.0
    equity = (1.0 + strategy_returns).cumprod()
    max_drawdown = (equity / equity.cummax() - 1.0).min()

    return {
        "strategy": name,
        "score": min(float(score), 1_000_000) if score == score else np.nan,
        "raw_sharpe": float(raw_sharpe) if raw_sharpe == raw_sharpe else np.nan,
        "vol_penalty": float(vol_penalty),
        "return_penalty": float(return_penalty),
        "cumulative_return": float(strategy_cumulative),
        "max_drawdown": float(max_drawdown),
        "avg_position": float(frame["position"].mean()),
    }


def build_windows(solution: pd.DataFrame) -> list[tuple[str, pd.DataFrame]]:
    windows: list[tuple[str, pd.DataFrame]] = [("all_current", solution)]
    for year in sorted(solution["date"].dt.year.unique()):
        year_df = solution[solution["date"].dt.year == year]
        if len(year_df) >= 20:
            windows.append((f"year_{year}", year_df))
    if len(solution) >= 180:
        windows.append(("last_180_rows", solution.tail(180)))
    return windows


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise
