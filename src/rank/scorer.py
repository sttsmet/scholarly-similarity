from __future__ import annotations

from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field

from src.config import TheoryConfig
from src.features import FEATURE_FUNCTIONS
from src.features.confidence import ConfidenceBreakdown, score as confidence_score
from src.features.explanation import StructuredExplanation, build_explanation
from src.features.graph_path import analyze as analyze_graph_path
from src.graph.bridge_graph import BridgeGraphContext, build_bridge_graph_context
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


class ScoredCandidateRecord(BaseModel):
    """Scored candidate artifact for the local corpus ranking slice."""

    model_config = ConfigDict(extra="forbid")

    openalex_id: str
    title: str
    publication_year: int | None
    sim: float = Field(ge=0.0, le=1.0)
    conf: float = Field(ge=0.0, le=1.0)
    rank: int = Field(ge=0)
    candidate_origins: list[str] = Field(default_factory=list)
    feature_values: dict[str, float | None]
    exp: StructuredExplanation
    confidence_breakdown: ConfidenceBreakdown


class CandidateScorer:
    """Config-driven deterministic scorer for seed-candidate pairs."""

    def __init__(
        self,
        theory: TheoryConfig,
        *,
        seed: NormalizedOpenAlexRecord | None = None,
        local_records: list[NormalizedOpenAlexRecord] | None = None,
    ) -> None:
        self.theory = theory
        self._graph_context: BridgeGraphContext | None = None
        if (
            seed is not None
            and local_records is not None
            and float(theory.sim_weights.graph_path) > 0.0
        ):
            self._graph_context = build_bridge_graph_context(
                seed=seed,
                candidates=list(local_records),
                parameters=theory.sim_parameters.graph_path,
            )

    def for_local_records(
        self,
        *,
        seed: NormalizedOpenAlexRecord,
        local_records: list[NormalizedOpenAlexRecord],
    ) -> CandidateScorer:
        if float(self.theory.sim_weights.graph_path) <= 0.0:
            return self
        return CandidateScorer(
            self.theory,
            seed=seed,
            local_records=local_records,
        )

    def score(
        self,
        seed: NormalizedOpenAlexRecord,
        candidate: NormalizedOpenAlexRecord,
    ) -> ScoredCandidateRecord:
        feature_weights = self.theory.sim_weights.model_dump()
        feature_scores: dict[str, float | None] = {}
        feature_notes: dict[str, str] = {}

        for feature_name, weight in feature_weights.items():
            if float(weight) <= 0.0:
                feature_scores[feature_name] = None
                continue
            if feature_name == "graph_path":
                graph_result = analyze_graph_path(
                    seed,
                    candidate,
                    self.theory,
                    context=self._graph_context,
                )
                feature_scores[feature_name] = graph_result.score
                if graph_result.explanation_note is not None:
                    feature_notes[feature_name] = graph_result.explanation_note
                continue
            feature_fn = FEATURE_FUNCTIONS[feature_name]
            feature_scores[feature_name] = feature_fn(seed, candidate, self.theory)

        similarity_score = _compute_similarity(feature_scores, feature_weights)
        confidence = confidence_score(seed, candidate, feature_scores, self.theory)
        explanation = build_explanation(
            feature_scores=feature_scores,
            feature_weights=feature_weights,
            explanation_config=self.theory.explanation,
            feature_notes=feature_notes,
        )
        return ScoredCandidateRecord(
            openalex_id=candidate.openalex_id,
            title=candidate.title,
            publication_year=candidate.publication_year,
            sim=similarity_score,
            conf=confidence.score,
            rank=0,
            candidate_origins=list(candidate.candidate_origins),
            feature_values=feature_scores,
            exp=explanation,
            confidence_breakdown=confidence,
        )


def _compute_similarity(
    feature_scores: Mapping[str, float | None],
    feature_weights: Mapping[str, float],
) -> float:
    available_weight = 0.0
    weighted_score = 0.0
    for feature_name, raw_score in feature_scores.items():
        if raw_score is None:
            continue
        weight = float(feature_weights.get(feature_name, 0.0))
        if weight <= 0.0:
            continue
        weighted_score += raw_score * weight
        available_weight += weight

    if available_weight <= 0.0:
        return 0.0
    return round(weighted_score / available_weight, 6)
