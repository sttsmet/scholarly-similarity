from __future__ import annotations

from math import exp

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


def score(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    theory: TheoryConfig,
) -> float | None:
    """Return an exponential decay score over publication-year distance."""

    if seed.publication_year is None or candidate.publication_year is None:
        return None
    year_gap = abs(seed.publication_year - candidate.publication_year)
    tau = theory.sim_parameters.temporal_tau
    return round(exp(-year_gap / tau), 6)
