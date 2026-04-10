from __future__ import annotations

from math import exp
from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


CONFIDENCE_EXCLUDED_FEATURES = {"graph_path"}


class ConfidenceBreakdown(BaseModel):
    """Simple deterministic confidence components for one seed-candidate pair."""

    model_config = ConfigDict(extra="forbid")

    coverage: float = Field(ge=0.0, le=1.0)
    support: float = Field(ge=0.0, le=1.0)
    maturity: float = Field(ge=0.0, le=1.0)
    score: float = Field(ge=0.0, le=1.0)


def score(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    feature_values: Mapping[str, float | None],
    theory: TheoryConfig,
) -> ConfidenceBreakdown:
    """Score confidence from coverage, support, and maturity.

    coverage = available active features / active features
    support = max(direct citation signal, shared_refs / (shared_refs + support_eta))
    maturity = mean(1 - exp(-age / maturity_tau)) over available seed/candidate ages
    """

    active_feature_names = [
        name
        for name, weight in theory.sim_weights.model_dump().items()
        if float(weight) > 0.0 and name not in CONFIDENCE_EXCLUDED_FEATURES
    ]
    available_count = sum(1 for name in active_feature_names if feature_values.get(name) is not None)
    coverage = (available_count / len(active_feature_names)) if active_feature_names else 0.0

    shared_refs = len(set(seed.referenced_works) & set(candidate.referenced_works))
    direct_signal = 1.0 if feature_values.get("direct_citation") == 1.0 else 0.0
    support_eta = theory.confidence_parameters.support_eta
    shared_signal = shared_refs / (shared_refs + support_eta) if shared_refs > 0 else 0.0
    support = max(direct_signal, shared_signal)

    maturity_signals: list[float] = []
    observation_year = theory.confidence_parameters.observation_year
    maturity_tau = theory.confidence_parameters.maturity_tau
    for publication_year in (seed.publication_year, candidate.publication_year):
        if publication_year is None:
            continue
        age = max(0, observation_year - publication_year)
        maturity_signals.append(1.0 - exp(-age / maturity_tau))
    maturity = (sum(maturity_signals) / len(maturity_signals)) if maturity_signals else 0.0

    factors = theory.confidence_factors
    weighted = (
        coverage * factors.coverage
        + support * factors.support
        + maturity * factors.maturity
    )
    max_weight = factors.coverage + factors.support + factors.maturity
    confidence_score = round(weighted / max_weight, 6) if max_weight else 0.0
    return ConfidenceBreakdown(
        coverage=round(coverage, 6),
        support=round(support, 6),
        maturity=round(maturity, 6),
        score=confidence_score,
    )
