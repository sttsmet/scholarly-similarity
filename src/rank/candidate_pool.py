from __future__ import annotations

from src.config import TheoryConfig
from src.models import PaperRecord


class CandidatePoolBuilder:
    """Build a deterministic candidate pool from in-memory records."""

    def __init__(self, theory: TheoryConfig) -> None:
        self.theory = theory

    def build(self, seed: PaperRecord, candidates: list[PaperRecord]) -> list[PaperRecord]:
        ordered: dict[str, PaperRecord] = {}
        for candidate in candidates:
            candidate_key = self._candidate_key(candidate)
            if not candidate_key:
                continue
            if seed.openalex_id and candidate.openalex_id == seed.openalex_id:
                continue
            ordered.setdefault(candidate_key, candidate)

        limit = self.theory.candidate_pool.max_candidates
        return list(ordered.values())[:limit]

    def _candidate_key(self, candidate: PaperRecord) -> str:
        dedupe_key = self.theory.candidate_pool.dedupe_key
        if dedupe_key == "openalex_id" and candidate.openalex_id:
            return candidate.openalex_id
        if candidate.doi:
            return candidate.doi
        return candidate.title.lower()

