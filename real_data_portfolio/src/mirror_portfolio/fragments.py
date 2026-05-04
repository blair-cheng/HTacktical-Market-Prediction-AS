from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FragmentConfig:
    lookback_window: int = 5
    forward_windows: tuple[int, ...] = (20,)
    future_metrics: tuple[str, ...] = ("return",)
    min_history: int = 252
    step: int = 5


class FragmentBuilder:
    """Create fpL/fpR historical fragments without future leakage."""

    def __init__(self, config: FragmentConfig | None = None, date_col: str = "date"):
        self.config = config or FragmentConfig()
        self.date_col = date_col

    def build(
        self,
        features: pd.DataFrame,
        returns: pd.DataFrame,
        asset_cols: Sequence[str],
        target: pd.DataFrame | None = None,
        target_col: str | None = None,
    ) -> pd.DataFrame:
        if self.date_col not in features.columns or self.date_col not in returns.columns:
            raise ValueError(f"Both inputs must contain {self.date_col}")
        if target is not None and self.date_col not in target.columns:
            raise ValueError(f"Target input must contain {self.date_col}")
        if target is not None and target_col is None:
            raise ValueError("target_col is required when target is provided")
        if target_col is not None and target is None:
            raise ValueError("target is required when target_col is provided")

        feature_cols = [c for c in features.columns if c != self.date_col]
        missing_assets = sorted(set(asset_cols) - set(returns.columns))
        if missing_assets:
            raise ValueError(f"Missing return columns: {missing_assets}")
        if target is not None and target_col not in target.columns:
            raise ValueError(f"Missing target column: {target_col}")

        df = features.merge(returns[[self.date_col, *asset_cols]], on=self.date_col, how="inner")
        if target is not None and target_col is not None:
            df = df.merge(target[[self.date_col, target_col]], on=self.date_col, how="inner")
        df[self.date_col] = pd.to_datetime(df[self.date_col])
        df = df.sort_values(self.date_col).reset_index(drop=True)

        allowed_metrics = {"return", "volatility", "drawdown"}
        unknown_metrics = sorted(set(self.config.future_metrics) - allowed_metrics)
        if unknown_metrics:
            raise ValueError(f"Unsupported future metrics: {unknown_metrics}")

        max_forward = max(self.config.forward_windows)
        start_idx = max(self.config.min_history, self.config.lookback_window - 1)
        target_buffer = 1 if target_col is not None else 0
        end_idx_exclusive = len(df) - max_forward - target_buffer

        rows: list[dict[str, object]] = []
        for end_idx in range(start_idx, end_idx_exclusive, self.config.step):
            start_idx_window = end_idx - self.config.lookback_window + 1
            start_date = df.at[start_idx_window, self.date_col]
            end_date = df.at[end_idx, self.date_col]
            row: dict[str, object] = {
                "date_range_id": f"{pd.Timestamp(start_date).date()}_{pd.Timestamp(end_date).date()}",
                "start_date": start_date,
                "end_date": end_date,
            }

            for col in feature_cols:
                row[col] = float(df.at[end_idx, col])

            for window in self.config.forward_windows:
                if target_col is not None:
                    future_target = df.loc[end_idx + 1 : end_idx + window, target_col].astype(float).dropna()
                    target_mean = float(future_target.mean()) if len(future_target) == window else np.nan
                    target_vol = float(future_target.std(ddof=0)) if len(future_target) == window else np.nan
                    row[f"future_{window}d_excess_returns"] = target_mean
                    row[f"future_{window}d_volatility"] = target_vol
                future = df.loc[end_idx + 1 : end_idx + window, asset_cols].astype(float)
                for asset in asset_cols:
                    series = future[asset].dropna()
                    if "return" in self.config.future_metrics:
                        row[f"fpR_{window}d_{asset}_return"] = self._compound_return(series)
                    if "volatility" in self.config.future_metrics:
                        row[f"fpR_{window}d_{asset}_volatility"] = float(series.std(ddof=0)) if len(series) else np.nan
                    if "drawdown" in self.config.future_metrics:
                        row[f"fpR_{window}d_{asset}_drawdown"] = self._max_drawdown(series)
            rows.append(row)

        return pd.DataFrame(rows)

    @staticmethod
    def _compound_return(returns: pd.Series) -> float:
        if returns.empty:
            return np.nan
        return float((1.0 + returns).prod() - 1.0)

    @staticmethod
    def _max_drawdown(returns: pd.Series) -> float:
        if returns.empty:
            return np.nan
        equity = (1.0 + returns).cumprod()
        peak = equity.cummax()
        return float((equity / peak - 1.0).min())
