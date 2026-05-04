from __future__ import annotations

from typing import Literal, Sequence

import numpy as np
import pandas as pd

Metric = Literal["cosine", "euclidean"]


class SimilaritySearcher:
    """Search similar historical fpL states."""

    def __init__(
        self,
        fragments: pd.DataFrame,
        feature_cols: Sequence[str],
        metric: Metric = "cosine",
        standardize: bool = True,
        clip_value: float = 10.0,
    ):
        self.fragments = fragments.reset_index(drop=True).copy()
        self.feature_cols = list(feature_cols)
        self.metric = metric
        self.standardize = standardize
        self.clip_value = clip_value
        self._fit_matrix()

    def search(self, query: pd.Series | dict[str, float], top_k: int = 5) -> pd.DataFrame:
        q = np.array([float(query[col]) for col in self.feature_cols], dtype=float)
        q = self._transform(q.reshape(1, -1))[0]
        q = np.nan_to_num(q, nan=0.0, posinf=0.0, neginf=0.0)
        q = np.clip(q, -self.clip_value, self.clip_value)

        if self.metric == "cosine":
            q_norm = q / (np.linalg.norm(q) + 1e-12)
            scores = np.sum(self.matrix_normalized * q_norm, axis=1)
        elif self.metric == "euclidean":
            distances = np.linalg.norm(self.matrix - q, axis=1)
            scores = 1.0 / (1.0 + distances)
        else:
            raise ValueError(f"Unsupported metric: {self.metric}")

        scores = np.nan_to_num(scores, nan=-1.0, posinf=-1.0, neginf=-1.0)
        n = min(top_k, len(scores))
        idx = np.argsort(scores)[::-1][:n]
        out = self.fragments.iloc[idx].copy()
        out["similarity"] = scores[idx]
        return out.reset_index(drop=True)

    def _fit_matrix(self) -> None:
        missing = sorted(set(self.feature_cols) - set(self.fragments.columns))
        if missing:
            raise ValueError(f"Missing feature columns: {missing}")
        raw = self.fragments[self.feature_cols].astype(float).to_numpy()
        self.mean = np.nanmean(raw, axis=0)
        self.std = np.nanstd(raw, axis=0)
        self.std[self.std < 1e-8] = 1.0
        self.matrix = self._transform(raw)
        self.matrix = np.nan_to_num(self.matrix, nan=0.0, posinf=0.0, neginf=0.0)
        self.matrix = np.clip(self.matrix, -self.clip_value, self.clip_value)
        norms = np.linalg.norm(self.matrix, axis=1, keepdims=True)
        self.matrix_normalized = self.matrix / (norms + 1e-12)

    def _transform(self, raw: np.ndarray) -> np.ndarray:
        if not self.standardize:
            return raw
        return (raw - self.mean) / self.std
