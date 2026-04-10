from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PaperRecord:
    """Minimal normalized paper record used across the scaffold."""

    title: str
    openalex_id: str | None = None
    doi: str | None = None
    publication_year: int | None = None
    cited_by_count: int = 0
    referenced_openalex_ids: tuple[str, ...] = ()
    concept_names: tuple[str, ...] = ()
    abstract: str | None = None
    source: str = "openalex"


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    """Ranked candidate record returned by the deterministic scorer."""

    paper: PaperRecord
    similarity_score: float
    confidence_score: float
    total_score: float
    feature_scores: dict[str, float] = field(default_factory=dict)
    explanation_lines: tuple[str, ...] = ()
