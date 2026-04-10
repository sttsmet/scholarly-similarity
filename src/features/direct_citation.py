from __future__ import annotations

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


def score(seed: NormalizedOpenAlexRecord, candidate: NormalizedOpenAlexRecord, theory: TheoryConfig) -> float:
    """Return 1.0 when a direct citation edge is present, otherwise 0.0."""

    del theory
    if candidate.openalex_id in seed.referenced_works:
        return 1.0
    if seed.openalex_id in candidate.referenced_works:
        return 1.0
    return 0.0
