from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AllocationResult:
    weights: dict[str, float]
    expected_returns: dict[str, float]
    confidence: float
    selected_assets: list[str]


class RuleBasedAllocator:
    """Convert matched fpR evidence into long-only portfolio weights."""

    def __init__(
        self,
        assets: Sequence[str],
        forward_window: int = 20,
        cash_asset: str = "CASH",
        max_gross: float = 1.0,
        min_edge: float = 0.0,
        confidence_floor: float = 0.25,
        max_asset_weight: float = 0.6,
        drawdown_floor: float = -0.15,
    ):
        self.assets = list(assets)
        self.forward_window = forward_window
        self.cash_asset = cash_asset
        self.max_gross = max_gross
        self.min_edge = min_edge
        self.confidence_floor = confidence_floor
        self.max_asset_weight = max_asset_weight
        self.drawdown_floor = drawdown_floor

    def allocate(self, matches: pd.DataFrame) -> AllocationResult:
        if matches.empty:
            return self._cash_result()
        if "similarity" not in matches.columns:
            raise ValueError("matches must contain similarity")

        sim = np.clip(matches["similarity"].astype(float).to_numpy(), 0.0, None)
        if sim.sum() <= 1e-12:
            return self._cash_result()
        weights = sim / sim.sum()
        confidence = float(sim.mean())

        expected = self._weighted_metric(matches, weights, "return")
        drawdown = self._weighted_metric(matches, weights, "drawdown")

        scores = {}
        for asset in self.assets:
            dd = drawdown.get(asset, 0.0)
            dd_penalty = 0.5 if dd < self.drawdown_floor else 1.0
            scores[asset] = max(0.0, expected.get(asset, 0.0) - self.min_edge) * dd_penalty

        positive = {asset: score for asset, score in scores.items() if score > 0}
        if not positive:
            return AllocationResult(
                weights={self.cash_asset: 1.0, **{asset: 0.0 for asset in self.assets}},
                expected_returns=expected,
                confidence=confidence,
                selected_assets=[],
            )

        confidence_scale = np.clip(
            (confidence - self.confidence_floor) / max(1e-12, 1.0 - self.confidence_floor),
            0.0,
            1.0,
        )
        risky_budget = self.max_gross * float(confidence_scale)
        score_sum = sum(positive.values())

        asset_weights = {
            asset: min(self.max_asset_weight, risky_budget * score / score_sum)
            for asset, score in positive.items()
        }
        total = sum(asset_weights.values())
        if total > risky_budget and total > 0:
            asset_weights = {asset: weight * risky_budget / total for asset, weight in asset_weights.items()}

        final_weights = {asset: 0.0 for asset in self.assets}
        final_weights.update(asset_weights)
        final_weights[self.cash_asset] = max(0.0, 1.0 - sum(final_weights.values()))

        return AllocationResult(
            weights=final_weights,
            expected_returns=expected,
            confidence=confidence,
            selected_assets=sorted(positive),
        )

    def _weighted_metric(self, matches: pd.DataFrame, weights: np.ndarray, metric: str) -> dict[str, float]:
        out = {}
        for asset in self.assets:
            col = f"fpR_{self.forward_window}d_{asset}_{metric}"
            if col not in matches.columns:
                out[asset] = 0.0
                continue
            values = matches[col].astype(float).fillna(0.0).to_numpy()
            out[asset] = float(np.dot(weights, values))
        return out

    def _cash_result(self) -> AllocationResult:
        return AllocationResult(
            weights={self.cash_asset: 1.0, **{asset: 0.0 for asset in self.assets}},
            expected_returns={asset: 0.0 for asset in self.assets},
            confidence=0.0,
            selected_assets=[],
        )
