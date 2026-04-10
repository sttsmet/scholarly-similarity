from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from src.models import PaperRecord


@dataclass(frozen=True, slots=True)
class LocalNeighborhood:
    """Small graph neighborhood summary around a seed paper."""

    seed: PaperRecord
    candidate_ids: tuple[str, ...]
    shared_concepts: frozenset[str]


class LocalNeighborhoodBuilder:
    """Placeholder local neighborhood builder for future graph-based features."""

    def build(self, seed: PaperRecord, candidates: Iterable[PaperRecord]) -> LocalNeighborhood:
        candidate_ids = tuple(candidate.openalex_id or candidate.title for candidate in candidates)
        return LocalNeighborhood(
            seed=seed,
            candidate_ids=candidate_ids,
            shared_concepts=frozenset(seed.concept_names),
        )
