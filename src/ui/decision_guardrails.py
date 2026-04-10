from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.ui.comparison import ComparisonMetricSummary


MIN_COMMON_COMPLETED_FOR_PASS = 3
TIE_TOLERANCE = 1e-9


@dataclass(frozen=True, slots=True)
class DecisionGuardrailAssessment:
    verdict: str
    selected_metric: str | None
    common_doi_count: int
    common_completed_seed_count: int
    paired_seed_count: int
    wins: int
    losses: int
    ties: int
    primary_mean: float | None
    primary_median: float | None
    secondary_mean: float | None
    secondary_median: float | None
    raw_delta_mean: float | None
    raw_delta_median: float | None
    improvement_delta_mean: float | None
    improvement_delta_median: float | None
    reasons: tuple[str, ...]
    policy_summary: dict[str, Any]
    evaluation_mode: str | None = None
    metric_scope: str | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    benchmark_maturity_tier: str | None = None
    promotion_ready: bool = False
    promotion_ineligibility_reasons: tuple[str, ...] = ()
    promotion_eligible: bool = True


def evaluate_decision_guardrails(
    *,
    selected_metric: str | None,
    common_doi_count: int,
    common_completed_seed_count: int,
    summary: ComparisonMetricSummary | dict[str, Any] | None,
    paired_seed_count: int | None = None,
    evaluation_mode: str | None = None,
    metric_scope: str | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    benchmark_maturity_tier: str | None = None,
    promotion_ready: bool | None = None,
    promotion_ineligibility_reasons: list[str] | tuple[str, ...] | None = None,
    comparison_benchmark_dataset_id: str | None = None,
    comparison_benchmark_labels_sha256: str | None = None,
    comparison_benchmark_maturity_tier: str | None = None,
    comparison_promotion_ready: bool | None = None,
    comparison_promotion_ineligibility_reasons: list[str] | tuple[str, ...] | None = None,
) -> DecisionGuardrailAssessment:
    normalized_metric = _optional_str(selected_metric)
    summary_payload = _summary_payload(summary)
    wins = int(summary_payload.get("wins") or 0)
    losses = int(summary_payload.get("losses") or 0)
    ties = int(summary_payload.get("ties") or 0)
    effective_paired_seed_count = (
        int(paired_seed_count)
        if paired_seed_count is not None
        else wins + losses + ties
    )
    improvement_delta_mean = _optional_float(summary_payload.get("improvement_delta_mean"))
    improvement_delta_median = _optional_float(summary_payload.get("improvement_delta_median"))
    normalized_evaluation_mode = _optional_str(evaluation_mode)
    normalized_metric_scope = _optional_str(metric_scope)
    normalized_benchmark_dataset_id = _optional_str(benchmark_dataset_id)
    normalized_benchmark_labels_sha256 = _optional_str(benchmark_labels_sha256)
    normalized_benchmark_maturity_tier = _optional_str(benchmark_maturity_tier)
    normalized_comparison_benchmark_dataset_id = _optional_str(comparison_benchmark_dataset_id)
    normalized_comparison_benchmark_labels_sha256 = _optional_str(comparison_benchmark_labels_sha256)
    normalized_comparison_benchmark_maturity_tier = _optional_str(comparison_benchmark_maturity_tier)
    normalized_promotion_ready = promotion_ready if isinstance(promotion_ready, bool) else None
    normalized_comparison_promotion_ready = (
        comparison_promotion_ready if isinstance(comparison_promotion_ready, bool) else None
    )
    normalized_promotion_ineligibility_reasons = _normalize_reason_list(
        promotion_ineligibility_reasons
    )
    normalized_comparison_promotion_ineligibility_reasons = _normalize_reason_list(
        comparison_promotion_ineligibility_reasons
    )

    fail_reasons: list[str] = []
    if normalized_metric is None:
        fail_reasons.append("No selected comparison metric is available.")
    if common_completed_seed_count <= 0:
        fail_reasons.append("There are zero common completed seeds.")
    if effective_paired_seed_count <= 0:
        fail_reasons.append("The selected metric is unavailable for paired comparison.")
    if improvement_delta_mean is not None and improvement_delta_mean < -TIE_TOLERANCE:
        fail_reasons.append("Mean improvement delta is negative.")
    if losses > wins:
        fail_reasons.append("Losses exceed wins.")
    if (
        effective_paired_seed_count > 0
        and wins == 0
        and losses == 0
        and ties == effective_paired_seed_count
        and not _is_positive(improvement_delta_mean)
        and not _is_positive(improvement_delta_median)
    ):
        fail_reasons.append("All paired seeds are ties with no measurable improvement.")

    promotion_eligibility_reasons: list[str] = []
    promotion_eligible = True
    if normalized_evaluation_mode != "independent_benchmark":
        promotion_eligible = False
        if normalized_evaluation_mode is None:
            promotion_eligibility_reasons.append(
                "Promotion evidence is missing evaluation_mode metadata."
            )
        else:
            promotion_eligibility_reasons.append(
                "Promotion evidence is not based on independent_benchmark mode."
            )
    if normalized_benchmark_dataset_id is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append("Promotion evidence is missing benchmark_dataset_id.")
    if normalized_benchmark_labels_sha256 is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Promotion evidence is missing benchmark_labels_sha256."
        )
    if normalized_benchmark_maturity_tier is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Promotion evidence is missing benchmark_maturity_tier."
        )
    if normalized_promotion_ready is not True:
        promotion_eligible = False
        if normalized_promotion_ready is None:
            promotion_eligibility_reasons.append("Promotion evidence is missing promotion_ready.")
        else:
            promotion_eligibility_reasons.append(
                "Promotion evidence benchmark is not marked promotion_ready."
            )
    promotion_eligibility_reasons.extend(normalized_promotion_ineligibility_reasons)
    if normalized_comparison_benchmark_dataset_id is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Comparison evidence is missing benchmark_dataset_id."
        )
    elif normalized_benchmark_dataset_id != normalized_comparison_benchmark_dataset_id:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Primary and secondary benchmark_dataset_id values do not match."
        )
    if normalized_comparison_benchmark_labels_sha256 is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Comparison evidence is missing benchmark_labels_sha256."
        )
    elif normalized_benchmark_labels_sha256 != normalized_comparison_benchmark_labels_sha256:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Primary and secondary benchmark_labels_sha256 values do not match."
        )
    if normalized_comparison_benchmark_maturity_tier is None:
        promotion_eligible = False
        promotion_eligibility_reasons.append(
            "Comparison evidence is missing benchmark_maturity_tier."
        )
    if normalized_comparison_promotion_ready is not True:
        promotion_eligible = False
        if normalized_comparison_promotion_ready is None:
            promotion_eligibility_reasons.append(
                "Comparison evidence is missing promotion_ready."
            )
        else:
            promotion_eligibility_reasons.append(
                "Comparison evidence benchmark is not marked promotion_ready."
            )
    promotion_eligibility_reasons.extend(
        normalized_comparison_promotion_ineligibility_reasons
    )

    if fail_reasons:
        return _build_assessment(
            verdict="fail",
            selected_metric=normalized_metric,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_seed_count,
            paired_seed_count=effective_paired_seed_count,
            summary_payload=summary_payload,
            wins=wins,
            losses=losses,
            ties=ties,
            reasons=fail_reasons,
            evaluation_mode=normalized_evaluation_mode,
            metric_scope=normalized_metric_scope,
            benchmark_dataset_id=normalized_benchmark_dataset_id,
            benchmark_labels_sha256=normalized_benchmark_labels_sha256,
            benchmark_maturity_tier=normalized_benchmark_maturity_tier,
            promotion_ready=bool(normalized_promotion_ready),
            promotion_eligible=promotion_eligible,
            promotion_eligibility_reasons=promotion_eligibility_reasons,
        )

    if (
        common_completed_seed_count >= MIN_COMMON_COMPLETED_FOR_PASS
        and _is_positive(improvement_delta_mean)
        and wins > losses
    ):
        return _build_assessment(
            verdict="pass",
            selected_metric=normalized_metric,
            common_doi_count=common_doi_count,
            common_completed_seed_count=common_completed_seed_count,
            paired_seed_count=effective_paired_seed_count,
            summary_payload=summary_payload,
            wins=wins,
            losses=losses,
            ties=ties,
            reasons=[
                f"At least {MIN_COMMON_COMPLETED_FOR_PASS} common completed seeds are available.",
                "Mean improvement delta is positive.",
                "Wins exceed losses.",
            ],
            evaluation_mode=normalized_evaluation_mode,
            metric_scope=normalized_metric_scope,
            benchmark_dataset_id=normalized_benchmark_dataset_id,
            benchmark_labels_sha256=normalized_benchmark_labels_sha256,
            benchmark_maturity_tier=normalized_benchmark_maturity_tier,
            promotion_ready=bool(normalized_promotion_ready),
            promotion_eligible=promotion_eligible,
            promotion_eligibility_reasons=promotion_eligibility_reasons,
        )

    weak_reasons: list[str] = []
    if common_completed_seed_count < MIN_COMMON_COMPLETED_FOR_PASS:
        weak_reasons.append(
            f"Fewer than {MIN_COMMON_COMPLETED_FOR_PASS} common completed seeds are available."
        )
    if improvement_delta_mean is None or abs(improvement_delta_mean) <= TIE_TOLERANCE:
        weak_reasons.append("Mean improvement delta is neutral.")
    elif improvement_delta_mean > 0 and wins <= losses:
        weak_reasons.append("Positive mean improvement is not supported by the win/loss balance.")
    if wins == losses:
        weak_reasons.append("Wins and losses are tied.")
    if effective_paired_seed_count > 0 and ties >= max(wins + losses, 1):
        weak_reasons.append("Ties dominate the paired comparisons.")
    if not weak_reasons:
        weak_reasons.append("Evidence is mixed and not clearly positive.")

    return _build_assessment(
        verdict="weak",
        selected_metric=normalized_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        paired_seed_count=effective_paired_seed_count,
        summary_payload=summary_payload,
        wins=wins,
        losses=losses,
        ties=ties,
        reasons=weak_reasons,
        evaluation_mode=normalized_evaluation_mode,
        metric_scope=normalized_metric_scope,
        benchmark_dataset_id=normalized_benchmark_dataset_id,
        benchmark_labels_sha256=normalized_benchmark_labels_sha256,
        benchmark_maturity_tier=normalized_benchmark_maturity_tier,
        promotion_ready=bool(normalized_promotion_ready),
        promotion_eligible=promotion_eligible,
        promotion_eligibility_reasons=promotion_eligibility_reasons,
    )


def requires_explicit_promotion_override(
    assessment: DecisionGuardrailAssessment | None,
) -> bool:
    if assessment is None:
        return True
    return assessment.verdict != "pass" or not assessment.promotion_eligible


def build_guardrail_artifact_fields(
    assessment: DecisionGuardrailAssessment | None,
    *,
    override_used: bool = False,
    override_reason: str | None = None,
) -> dict[str, Any]:
    return {
        "guardrail_verdict": assessment.verdict if assessment is not None else None,
        "guardrail_reasons": list(assessment.reasons) if assessment is not None else [],
        "benchmark_maturity_tier": (
            assessment.benchmark_maturity_tier if assessment is not None else None
        ),
        "promotion_ready": assessment.promotion_ready if assessment is not None else False,
        "promotion_ineligibility_reasons": (
            list(assessment.promotion_ineligibility_reasons) if assessment is not None else []
        ),
        "promotion_eligible": assessment.promotion_eligible if assessment is not None else False,
        "policy_summary": dict(assessment.policy_summary) if assessment is not None else {},
        "override_used": bool(override_used),
        "override_reason": _optional_str(override_reason),
    }


def _build_assessment(
    *,
    verdict: str,
    selected_metric: str | None,
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_seed_count: int,
    summary_payload: dict[str, Any],
    wins: int,
    losses: int,
    ties: int,
    reasons: list[str],
    evaluation_mode: str | None,
    metric_scope: str | None,
    benchmark_dataset_id: str | None,
    benchmark_labels_sha256: str | None,
    benchmark_maturity_tier: str | None,
    promotion_ready: bool,
    promotion_eligible: bool,
    promotion_eligibility_reasons: list[str],
) -> DecisionGuardrailAssessment:
    return DecisionGuardrailAssessment(
        verdict=verdict,
        selected_metric=selected_metric,
        common_doi_count=int(common_doi_count),
        common_completed_seed_count=int(common_completed_seed_count),
        paired_seed_count=int(max(paired_seed_count, 0)),
        wins=wins,
        losses=losses,
        ties=ties,
        primary_mean=_optional_float(summary_payload.get("primary_mean")),
        primary_median=_optional_float(summary_payload.get("primary_median")),
        secondary_mean=_optional_float(summary_payload.get("secondary_mean")),
        secondary_median=_optional_float(summary_payload.get("secondary_median")),
        raw_delta_mean=_optional_float(summary_payload.get("raw_delta_mean")),
        raw_delta_median=_optional_float(summary_payload.get("raw_delta_median")),
        improvement_delta_mean=_optional_float(summary_payload.get("improvement_delta_mean")),
        improvement_delta_median=_optional_float(summary_payload.get("improvement_delta_median")),
        evaluation_mode=evaluation_mode,
        metric_scope=metric_scope,
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=benchmark_labels_sha256,
        benchmark_maturity_tier=benchmark_maturity_tier,
        promotion_ready=promotion_ready,
        promotion_ineligibility_reasons=tuple(
            dict.fromkeys(reason for reason in promotion_eligibility_reasons if reason)
        ),
        promotion_eligible=promotion_eligible,
        reasons=tuple(dict.fromkeys(reason for reason in reasons if reason)),
        policy_summary={
            "selected_metric": selected_metric,
            "min_common_completed_for_pass": MIN_COMMON_COMPLETED_FOR_PASS,
            "common_doi_count": int(common_doi_count),
            "common_completed_seed_count": int(common_completed_seed_count),
            "paired_seed_count": int(max(paired_seed_count, 0)),
            "wins": wins,
            "losses": losses,
            "ties": ties,
            "primary_mean": _optional_float(summary_payload.get("primary_mean")),
            "primary_median": _optional_float(summary_payload.get("primary_median")),
            "secondary_mean": _optional_float(summary_payload.get("secondary_mean")),
            "secondary_median": _optional_float(summary_payload.get("secondary_median")),
            "raw_delta_mean": _optional_float(summary_payload.get("raw_delta_mean")),
            "raw_delta_median": _optional_float(summary_payload.get("raw_delta_median")),
            "improvement_delta_mean": _optional_float(summary_payload.get("improvement_delta_mean")),
            "improvement_delta_median": _optional_float(summary_payload.get("improvement_delta_median")),
            "evaluation_mode": evaluation_mode,
            "metric_scope": metric_scope,
            "benchmark_dataset_id": benchmark_dataset_id,
            "benchmark_labels_sha256": benchmark_labels_sha256,
            "benchmark_maturity_tier": benchmark_maturity_tier,
            "promotion_ready": promotion_ready,
            "promotion_eligible": promotion_eligible,
            "promotion_eligibility_reasons": list(
                dict.fromkeys(reason for reason in promotion_eligibility_reasons if reason)
            ),
        },
    )


def _summary_payload(
    summary: ComparisonMetricSummary | dict[str, Any] | None,
) -> dict[str, Any]:
    if isinstance(summary, ComparisonMetricSummary):
        return {
            "primary_mean": summary.primary_mean,
            "primary_median": summary.primary_median,
            "secondary_mean": summary.secondary_mean,
            "secondary_median": summary.secondary_median,
            "raw_delta_mean": summary.raw_delta_mean,
            "raw_delta_median": summary.raw_delta_median,
            "improvement_delta_mean": summary.improvement_delta_mean,
            "improvement_delta_median": summary.improvement_delta_median,
            "wins": summary.wins,
            "losses": summary.losses,
            "ties": summary.ties,
        }
    if isinstance(summary, dict):
        return dict(summary)
    return {}


def _optional_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _is_positive(value: float | None) -> bool:
    return value is not None and value > TIE_TOLERANCE


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_reason_list(value: list[str] | tuple[str, ...] | None) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return list(
        dict.fromkeys(
            reason
            for reason in (_optional_str(item) for item in value)
            if reason is not None
        )
    )
