from __future__ import annotations

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


def score(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    theory: TheoryConfig,
) -> float | None:
    """Return a bounded topical similarity over primary and secondary topics."""

    del theory
    component_scores: list[float] = []

    if seed.primary_topic and candidate.primary_topic:
        component_scores.append(1.0 if seed.primary_topic == candidate.primary_topic else 0.0)

    seed_topics = set(seed.topics)
    candidate_topics = set(candidate.topics)
    if seed_topics and candidate_topics:
        overlap = len(seed_topics & candidate_topics)
        union = len(seed_topics | candidate_topics)
        component_scores.append(overlap / union if union else 0.0)

    if not component_scores:
        return None
    return round(sum(component_scores) / len(component_scores), 6)
