from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

from .allocator import RuleBasedAllocator
from .similarity import SimilaritySearcher


@dataclass(frozen=True)
class WalkForwardConfig:
    top_k: int = 5
    min_history_fragments: int = 252
    forward_window: int = 20


class WalkForwardEvaluator:
    """Leak-safe daily evaluation over prebuilt fragments and features."""

    def __init__(
        self,
        feature_cols: Sequence[str],
        allocator: RuleBasedAllocator,
        config: WalkForwardConfig | None = None,
    ):
        self.feature_cols = list(feature_cols)
        self.allocator = allocator
        self.config = config or WalkForwardConfig()

    def run(self, fragments: pd.DataFrame, features: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
        if "end_date" not in fragments.columns:
            raise ValueError("fragments must contain end_date")
        if date_col not in features.columns:
            raise ValueError(f"features must contain {date_col}")

        fragments = fragments.copy()
        features = features.copy()
        fragments["end_date"] = pd.to_datetime(fragments["end_date"])
        features[date_col] = pd.to_datetime(features[date_col])

        rows = []
        for _, query in features.sort_values(date_col).iterrows():
            today = query[date_col]
            cutoff = today - pd.tseries.offsets.BDay(self.config.forward_window)
            history = fragments[fragments["end_date"] <= cutoff]
            if len(history) < self.config.min_history_fragments:
                continue

            searcher = SimilaritySearcher(history, self.feature_cols)
            matches = searcher.search(query, top_k=self.config.top_k)
            allocation = self.allocator.allocate(matches)

            row = {
                date_col: today,
                "confidence": allocation.confidence,
                "selected_assets": ",".join(allocation.selected_assets),
            }
            row.update({f"weight_{asset}": weight for asset, weight in allocation.weights.items()})
            row.update({f"expected_{asset}": value for asset, value in allocation.expected_returns.items()})
            rows.append(row)

        return pd.DataFrame(rows)
