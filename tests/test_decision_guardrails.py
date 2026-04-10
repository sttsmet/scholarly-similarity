from __future__ import annotations

from src.ui.comparison import ComparisonMetricSummary
from src.ui.decision_guardrails import (
    build_guardrail_artifact_fields,
    evaluate_decision_guardrails,
    requires_explicit_promotion_override,
)


def _summary(
    *,
    improvement_delta_mean: float | None,
    improvement_delta_median: float | None,
    wins: int,
    losses: int,
    ties: int,
) -> ComparisonMetricSummary:
    return ComparisonMetricSummary(
        primary_mean=0.7,
        primary_median=0.7,
        secondary_mean=0.8,
        secondary_median=0.8,
        raw_delta_mean=0.1 if improvement_delta_mean is not None else None,
        raw_delta_median=0.1 if improvement_delta_median is not None else None,
        improvement_delta_mean=improvement_delta_mean,
        improvement_delta_median=improvement_delta_median,
        wins=wins,
        losses=losses,
        ties=ties,
    )


def test_evaluate_decision_guardrails_pass_case() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary(
            improvement_delta_mean=0.05,
            improvement_delta_median=0.04,
            wins=3,
            losses=1,
            ties=0,
        ),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )

    assert assessment.verdict == "pass"
    assert assessment.reasons
    assert requires_explicit_promotion_override(assessment) is False


def test_evaluate_decision_guardrails_weak_tie_only_neutral_case() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary=_summary(
            improvement_delta_mean=0.0,
            improvement_delta_median=0.0,
            wins=1,
            losses=1,
            ties=4,
        ),
        paired_seed_count=6,
    )

    assert assessment.verdict == "weak"
    assert "Mean improvement delta is neutral." in assessment.reasons
    assert requires_explicit_promotion_override(assessment) is True


def test_evaluate_decision_guardrails_fail_case() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=3,
        common_completed_seed_count=3,
        summary=_summary(
            improvement_delta_mean=-0.02,
            improvement_delta_median=-0.01,
            wins=1,
            losses=2,
            ties=0,
        ),
        paired_seed_count=3,
    )

    assert assessment.verdict == "fail"
    assert "Mean improvement delta is negative." in assessment.reasons
    assert "Losses exceed wins." in assessment.reasons


def test_evaluate_decision_guardrails_fail_when_metric_unavailable() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary=None,
        paired_seed_count=0,
    )

    assert assessment.verdict == "fail"
    assert "The selected metric is unavailable for paired comparison." in assessment.reasons


def test_build_guardrail_artifact_fields_includes_override_metadata() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary=_summary(
            improvement_delta_mean=0.0,
            improvement_delta_median=0.0,
            wins=0,
            losses=0,
            ties=2,
        ),
        paired_seed_count=2,
    )

    fields = build_guardrail_artifact_fields(
        assessment,
        override_used=True,
        override_reason="Expert override after qualitative review.",
    )

    assert fields["guardrail_verdict"] == "fail"
    assert fields["override_used"] is True
    assert fields["override_reason"] == "Expert override after qualitative review."
    assert fields["policy_summary"]["selected_metric"] == "ndcg_at_k"


def test_evaluate_decision_guardrails_blocks_promotion_when_benchmark_not_promotion_ready() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary(
            improvement_delta_mean=0.05,
            improvement_delta_median=0.04,
            wins=3,
            losses=1,
            ties=0,
        ),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
        promotion_ineligibility_reasons=["Dataset is still prototype maturity."],
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="prototype",
        comparison_promotion_ready=False,
    )

    assert assessment.verdict == "pass"
    assert assessment.promotion_eligible is False
    assert assessment.promotion_ready is False
    assert "Dataset is still prototype maturity." in assessment.promotion_ineligibility_reasons
    assert requires_explicit_promotion_override(assessment) is True


def test_evaluate_decision_guardrails_blocks_promotion_when_maturity_fields_are_missing() -> None:
    assessment = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary(
            improvement_delta_mean=0.05,
            improvement_delta_median=0.04,
            wins=3,
            losses=1,
            ties=0,
        ),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
    )

    assert assessment.verdict == "pass"
    assert assessment.promotion_eligible is False
    assert "missing benchmark_maturity_tier" in " ".join(assessment.promotion_ineligibility_reasons)
    assert "missing promotion_ready" in " ".join(assessment.promotion_ineligibility_reasons)
