from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

import numpy as np
import pandas as pd

Transform = Literal["level", "delta", "return", "ratio", "zscore"]
MarketExcessBaseline = Literal["rolling", "expanding"]


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    source: str
    transform: Transform
    short_window: int = 5
    long_window: int = 252
    fill_value: float = 0.0


class MarketStateFeatureBuilder:
    """Build visible market-state features from real daily data."""

    def __init__(self, specs: Iterable[FeatureSpec], date_col: str = "date"):
        self.specs = list(specs)
        self.date_col = date_col

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        missing = sorted({spec.source for spec in self.specs} - set(data.columns))
        if missing:
            raise ValueError(f"Missing source columns: {missing}")
        if self.date_col not in data.columns:
            raise ValueError(f"Missing date column: {self.date_col}")

        df = data.copy()
        df[self.date_col] = pd.to_datetime(df[self.date_col])
        df = df.sort_values(self.date_col).reset_index(drop=True)

        out = pd.DataFrame({self.date_col: df[self.date_col]})
        for spec in self.specs:
            s = pd.to_numeric(df[spec.source], errors="coerce").ffill()
            out[spec.name] = self._build_one(s, spec).replace([np.inf, -np.inf], np.nan)
            out[spec.name] = out[spec.name].fillna(spec.fill_value)
        return out

    def _build_one(self, s: pd.Series, spec: FeatureSpec) -> pd.Series:
        if spec.transform == "level":
            return s
        if spec.transform == "delta":
            return s.diff()
        if spec.transform == "return":
            return s / s.shift(1) - 1.0
        if spec.transform == "ratio":
            short = s.rolling(spec.short_window, min_periods=spec.short_window).mean()
            long = s.rolling(spec.long_window, min_periods=spec.long_window).mean()
            long = long.mask(long.abs() < 1e-12)
            return short / long
        if spec.transform == "zscore":
            mean = s.rolling(spec.long_window, min_periods=spec.long_window).mean()
            std = s.rolling(spec.long_window, min_periods=spec.long_window).std()
            std = std.mask(std.abs() < 1e-12)
            return (s - mean) / std
        raise ValueError(f"Unsupported transform: {spec.transform}")


def default_specs() -> list[FeatureSpec]:
    """P0 embedding features with current long-history data only."""
    return [
        FeatureSpec("cpi_ratio", "cpi", "ratio"),
        FeatureSpec("vix_delta", "VIX", "delta"),
        FeatureSpec("yield_3m_ratio", "yield_3m", "ratio"),
        FeatureSpec("lagged_market_forward_excess_returns_delta", "lagged_market_forward_excess_returns", "delta"),
        FeatureSpec("spy_price_ratio", "SPY", "ratio"),
    ]


def fred_rate_specs() -> list[FeatureSpec]:
    """Compatibility helper: P0 uses one Treasury yield feature."""
    return [
        FeatureSpec("yield_10y_ratio", "yield_10y", "ratio"),
    ]


def sentiment_filter_specs() -> list[FeatureSpec]:
    """Short-history sentiment features for filters/diagnostics, not P0 embedding."""
    return [
        FeatureSpec("fear_greed_delta", "fear_greed_score", "delta"),
        FeatureSpec("fear_greed_zscore", "fear_greed_score", "zscore"),
    ]


def news_specs() -> list[FeatureSpec]:
    """Future news features. P0 does not have these columns."""
    return [
        FeatureSpec("news_tone_delta", "news_tone", "delta"),
        FeatureSpec("news_volume_ratio", "news_count", "ratio"),
    ]


def filter_specs_for_columns(specs: Iterable[FeatureSpec], columns: Iterable[str]) -> list[FeatureSpec]:
    available = set(columns)
    return [spec for spec in specs if spec.source in available]


def add_market_forward_excess_columns(
    data: pd.DataFrame,
    price_col: str = "SPY",
    date_col: str = "date",
    long_window: int = 1260,
    min_periods: int | None = None,
    mad_criterion: float = 4.0,
    baseline_mode: MarketExcessBaseline = "rolling",
    rfr_col: str | None = None,
) -> pd.DataFrame:
    """Add Kaggle-style market excess labels and their one-day lag.

    The lagged column is the only target-like column allowed in fpL.
    """
    if price_col not in data.columns:
        raise ValueError(f"Missing price column: {price_col}")
    if date_col not in data.columns:
        raise ValueError(f"Missing date column: {date_col}")
    if min_periods is None:
        min_periods = long_window

    out = data.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    out = out.sort_values(date_col).reset_index(drop=True)

    prices = pd.to_numeric(out[price_col], errors="coerce")
    forward_returns = prices.shift(-1) / prices - 1.0
    if rfr_col is not None:
        if rfr_col not in out.columns:
            raise ValueError(f"Missing risk-free rate column: {rfr_col}")
        rfr_annual = pd.to_numeric(out[rfr_col], errors="coerce").ffill()
        baseline = (1.0 + rfr_annual / 100.0) ** (1.0 / 252.0) - 1.0
    elif baseline_mode == "rolling":
        baseline = forward_returns.rolling(long_window, min_periods=min_periods).mean()
    elif baseline_mode == "expanding":
        baseline = forward_returns.expanding(min_periods=min_periods).mean()
    else:
        raise ValueError(f"Unsupported market excess baseline mode: {baseline_mode}")
    excess = forward_returns - baseline

    if mad_criterion and mad_criterion > 0:
        median = excess.rolling(long_window, min_periods=min_periods).median()
        mad = (excess - median).abs().rolling(long_window, min_periods=min_periods).median()
        lower = median - mad_criterion * mad
        upper = median + mad_criterion * mad
        winsorized = excess.mask(excess < lower, lower).mask(excess > upper, upper)
    else:
        winsorized = excess

    out["market_forward_excess_returns"] = winsorized
    out["lagged_market_forward_excess_returns"] = winsorized.shift(1)
    return out
