#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.mirror_portfolio.features import (
    MarketStateFeatureBuilder,
    add_market_forward_excess_columns,
    default_specs,
)
from src.mirror_portfolio.fragments import FragmentBuilder, FragmentConfig


DEFAULT_ASSETS = ("SPY", "QQQ", "IWM", "TLT", "IEF", "SHY", "GLD", "UUP")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate real-portfolio fragments and embeddings.")
    parser.add_argument("--market-state", default="data/processed/market_state.csv")
    parser.add_argument("--output-dir", default="data/processed")
    parser.add_argument("--step", type=int, default=5)
    parser.add_argument("--lookback-window", type=int, default=5)
    parser.add_argument("--min-history", type=int, default=1260)
    parser.add_argument("--forward-windows", default="20")
    parser.add_argument("--future-metrics", default="return")
    parser.add_argument("--assets", default=",".join(DEFAULT_ASSETS))
    parser.add_argument("--market-excess-mode", choices=["rolling", "expanding"], default="rolling")
    parser.add_argument("--market-excess-min-periods", type=int, default=None)
    parser.add_argument("--mad-criterion", type=float, default=4.0)
    return parser.parse_args()


def parse_int_list(value: str) -> tuple[int, ...]:
    out = tuple(int(x.strip()) for x in value.split(",") if x.strip())
    if not out:
        raise ValueError("Expected at least one forward window")
    return out


def parse_str_list(value: str) -> list[str]:
    out = [x.strip() for x in value.split(",") if x.strip()]
    if not out:
        raise ValueError("Expected at least one asset")
    return out


def build_returns(market_state: pd.DataFrame, assets: list[str]) -> pd.DataFrame:
    missing = sorted(set(assets) - set(market_state.columns))
    if missing:
        raise ValueError(f"Missing asset columns: {missing}")
    out = pd.DataFrame({"date": market_state["date"]})
    for asset in assets:
        prices = pd.to_numeric(market_state[asset], errors="coerce")
        out[asset] = prices / prices.shift(1) - 1.0
    return out


def main() -> None:
    args = parse_args()
    market_state_path = Path(args.market_state)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    market_state = pd.read_csv(market_state_path)
    market_state["date"] = pd.to_datetime(market_state["date"])
    market_state = market_state.sort_values("date").reset_index(drop=True)

    numeric_cols = [c for c in market_state.columns if c != "date"]
    market_state[numeric_cols] = market_state[numeric_cols].apply(pd.to_numeric, errors="coerce")
    market_state[numeric_cols] = market_state[numeric_cols].ffill()
    derived_cols = {"market_forward_excess_returns", "lagged_market_forward_excess_returns"}
    raw_required_cols = sorted(({spec.source for spec in default_specs()} - derived_cols) | {"SPY"})
    missing_source_cols = sorted(set(raw_required_cols) - set(market_state.columns))
    if missing_source_cols:
        raise ValueError(f"market_state missing required source columns: {missing_source_cols}")
    market_state = market_state.dropna(subset=raw_required_cols).reset_index(drop=True)
    market_state = add_market_forward_excess_columns(
        market_state,
        price_col="SPY",
        min_periods=args.market_excess_min_periods,
        mad_criterion=args.mad_criterion,
        baseline_mode=args.market_excess_mode,
        rfr_col="yield_3m",
    )

    assets = parse_str_list(args.assets)
    forward_windows = parse_int_list(args.forward_windows)
    future_metrics = tuple(parse_str_list(args.future_metrics))

    features = MarketStateFeatureBuilder(default_specs()).transform(market_state)
    returns = build_returns(market_state, assets)
    market_forward_excess = market_state[["date", "market_forward_excess_returns"]].copy()

    config = FragmentConfig(
        lookback_window=args.lookback_window,
        forward_windows=forward_windows,
        future_metrics=future_metrics,
        min_history=args.min_history,
        step=args.step,
    )
    fragments = FragmentBuilder(config).build(
        features=features,
        returns=returns,
        asset_cols=assets,
        target=market_forward_excess,
        target_col="market_forward_excess_returns",
    )

    feature_cols = [c for c in features.columns if c != "date"]
    meta_cols = ["date_range_id", "start_date", "end_date"]
    target_cols = [
        c
        for window in forward_windows
        for c in (f"future_{window}d_excess_returns", f"future_{window}d_volatility")
        if c in fragments.columns
    ]
    fragments = fragments.dropna(subset=target_cols + feature_cols).reset_index(drop=True)
    if fragments.empty:
        raise ValueError("No complete fragments generated after dropping rows with missing targets/features")
    asset_fpr_cols = [c for c in fragments.columns if c.startswith("fpR_")]
    ordered_cols = meta_cols + target_cols + feature_cols + asset_fpr_cols
    fragments = fragments[ordered_cols]

    scaler = StandardScaler()
    embeddings = scaler.fit_transform(fragments[feature_cols].astype(float).to_numpy())

    fragments_path = output_dir / "fragments.csv"
    embeddings_path = output_dir / "embeddings.npy"
    scaler_path = output_dir / "scaler.pkl"
    feature_cols_path = output_dir / "feature_columns.txt"

    fragments.to_csv(fragments_path, index=False)
    np.save(embeddings_path, embeddings)
    with scaler_path.open("wb") as f:
        pickle.dump(scaler, f)
    feature_cols_path.write_text("\n".join(feature_cols) + "\n")

    print(f"market_state: {market_state.shape} from {market_state['date'].min().date()} to {market_state['date'].max().date()}")
    print(f"features: {features.shape}")
    print(f"fragments: {fragments.shape} -> {fragments_path}")
    print(f"embeddings: {embeddings.shape} -> {embeddings_path}")
    print(f"scaler: {scaler_path}")
    print(f"feature columns: {len(feature_cols)} -> {feature_cols_path}")


if __name__ == "__main__":
    main()
