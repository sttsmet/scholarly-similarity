from __future__ import annotations

from math import sqrt

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


def score(seed: NormalizedOpenAlexRecord, candidate: NormalizedOpenAlexRecord, theory: TheoryConfig) -> float:
    """Return bounded bibliographic coupling over referenced OpenAlex identifiers."""

    del theory
    seed_refs = set(seed.referenced_works)
    candidate_refs = set(candidate.referenced_works)
    overlap = len(seed_refs & candidate_refs)
    denominator = sqrt(max(1, len(seed_refs)) * max(1, len(candidate_refs)))
    return round(overlap / denominator, 6)
