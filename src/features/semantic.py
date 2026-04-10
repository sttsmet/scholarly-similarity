from __future__ import annotations

import re

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")


def score(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    theory: TheoryConfig,
) -> float | None:
    """Return a deterministic lexical similarity over title and abstract text."""

    del theory
    seed_tokens = _tokenize_record(seed)
    candidate_tokens = _tokenize_record(candidate)
    if not seed_tokens or not candidate_tokens:
        return None

    overlap = len(seed_tokens & candidate_tokens)
    union = len(seed_tokens | candidate_tokens)
    if not union:
        return None
    return round(overlap / union, 6)


def _tokenize_record(record: NormalizedOpenAlexRecord) -> set[str]:
    parts = [record.title]
    if record.abstract_text:
        parts.append(record.abstract_text)
    return {
        token.lower()
        for token in TOKEN_PATTERN.findall(" ".join(parts))
        if len(token) > 1
    }
