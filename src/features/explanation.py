from __future__ import annotations

from typing import Mapping

from pydantic import BaseModel, ConfigDict, Field

from src.config import ExplanationConfig


class ExplanationFactor(BaseModel):
    """Top weighted factor exposed in the structured explanation object."""

    model_config = ConfigDict(extra="forbid")

    name: str
    value: float = Field(ge=0.0, le=1.0)
    contribution_share: float = Field(ge=0.0, le=1.0)
    reason: str
    weighted_contribution: float | None = Field(default=None, ge=0.0)


class StructuredExplanation(BaseModel):
    """Structured explanation for one ranked seed-candidate comparison."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    top_factors: list[ExplanationFactor]
    masked_features: list[str]


def build_explanation(
    feature_scores: Mapping[str, float | None],
    feature_weights: Mapping[str, float],
    explanation_config: ExplanationConfig,
    feature_notes: Mapping[str, str] | None = None,
) -> StructuredExplanation:
    """Build a structured explanation from weighted feature contributions."""

    contributions: list[tuple[float, str, float]] = []
    note_map = dict(feature_notes or {})
    masked_features = sorted(
        feature_name
        for feature_name, raw_score in feature_scores.items()
        if raw_score is None and float(feature_weights.get(feature_name, 0.0)) > 0.0
    )
    for feature_name, raw_score in feature_scores.items():
        if raw_score is None:
            continue
        weight = float(feature_weights.get(feature_name, 0.0))
        contribution = raw_score * weight
        contributions.append((contribution, feature_name, raw_score))

    contributions.sort(key=lambda item: (-item[0], item[1]))
    total_positive_contribution = sum(max(0.0, contribution) for contribution, _, _ in contributions)

    top_factors: list[ExplanationFactor] = []
    for contribution, feature_name, raw_score in contributions[: explanation_config.top_k_features]:
        share = contribution / total_positive_contribution if total_positive_contribution > 0 else 0.0
        top_factors.append(
            ExplanationFactor(
                name=feature_name,
                value=round(raw_score, 6),
                contribution_share=round(share, 6),
                reason=_build_reason(feature_name, raw_score, note_map.get(feature_name)),
                weighted_contribution=round(contribution, 6) if explanation_config.include_raw_scores else None,
            )
        )

    summary = _build_summary(top_factors, masked_features, explanation_config)
    return StructuredExplanation(
        summary=summary,
        top_factors=top_factors,
        masked_features=masked_features,
    )


def _build_summary(
    top_factors: list[ExplanationFactor],
    masked_features: list[str],
    explanation_config: ExplanationConfig,
) -> str:
    if not top_factors:
        summary = "No positive similarity factors were available for this candidate."
    else:
        factor_names = ", ".join(factor.name for factor in top_factors)
        summary = f"Top signals: {factor_names}."
    if masked_features and explanation_config.include_notes:
        return f"{summary} Masked features: {', '.join(masked_features)}."
    return summary


def _build_reason(feature_name: str, value: float, note: str | None = None) -> str:
    if feature_name == "bibliographic_coupling":
        reason = (
            "shared references overlap the seed bibliography"
            if value > 0
            else "reference lists do not overlap"
        )
        return _append_note(reason, note)
    if feature_name == "direct_citation":
        reason = (
            "seed and candidate are directly connected by citation"
            if value > 0
            else "no direct citation link is present"
        )
        return _append_note(reason, note)
    if feature_name == "topical":
        reason = (
            "topic labels and the primary topic align"
            if value > 0
            else "topic metadata is present but overlap is limited"
        )
        return _append_note(reason, note)
    if feature_name == "temporal":
        reason = (
            "publication years are close"
            if value >= 0.5
            else "publication years are relatively far apart"
        )
        return _append_note(reason, note)
    if feature_name == "semantic":
        reason = "title and abstract tokens overlap" if value > 0 else "lexical overlap is limited"
        return _append_note(reason, note)
    if feature_name == "graph_path":
        reason = (
            "short bridge paths connect the seed and candidate"
            if value > 0
            else "no short bridge paths were found"
        )
        return _append_note(reason, note)
    return _append_note("feature contributed to the similarity score", note)


def _append_note(reason: str, note: str | None) -> str:
    if note is None or not note.strip():
        return reason
    return f"{reason} ({note.strip()})"
