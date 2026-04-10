from collections.abc import Callable

from src.config import TheoryConfig
from src.features import (
    bibliographic_coupling,
    direct_citation,
    graph_path,
    semantic,
    temporal,
    topical,
)
from src.ingest.doi_resolver import NormalizedOpenAlexRecord

FeatureFunction = Callable[[NormalizedOpenAlexRecord, NormalizedOpenAlexRecord, TheoryConfig], float | None]

FEATURE_FUNCTIONS: dict[str, FeatureFunction] = {
    "bibliographic_coupling": bibliographic_coupling.score,
    "direct_citation": direct_citation.score,
    "topical": topical.score,
    "temporal": temporal.score,
    "semantic": semantic.score,
    "graph_path": graph_path.score,
}

__all__ = ["FEATURE_FUNCTIONS", "FeatureFunction"]
