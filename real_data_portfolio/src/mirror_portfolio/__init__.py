"""Mirror of History real-portfolio core."""

from .allocator import AllocationResult, RuleBasedAllocator
from .features import (
    FeatureSpec,
    MarketStateFeatureBuilder,
    default_specs,
    filter_specs_for_columns,
    fred_rate_specs,
    news_specs,
    sentiment_filter_specs,
)
from .fragments import FragmentBuilder, FragmentConfig
from .similarity import SimilaritySearcher
from .walk_forward import WalkForwardConfig, WalkForwardEvaluator

__all__ = [
    "AllocationResult",
    "FeatureSpec",
    "FragmentBuilder",
    "FragmentConfig",
    "MarketStateFeatureBuilder",
    "RuleBasedAllocator",
    "SimilaritySearcher",
    "WalkForwardConfig",
    "WalkForwardEvaluator",
    "default_specs",
    "filter_specs_for_columns",
    "fred_rate_specs",
    "news_specs",
    "sentiment_filter_specs",
]
