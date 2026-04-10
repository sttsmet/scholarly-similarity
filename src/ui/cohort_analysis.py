from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.ui.comparison import (
    COMPARISON_METRICS,
    COMPARISON_STATUS_OPTIONS,
    align_common_seed_rows,
    common_completed_seed_count,
    common_numeric_metrics,
    comparison_metric_summary,
    paired_metric_rows,
)
from src.ui.decision_guardrails import evaluate_decision_guardrails
from src.ui.experiment_matrix import ExperimentBatchRow


PAIRWISE_STATUS_MODE = COMPARISON_STATUS_OPTIONS[0]
DEFAULT_COHORT_ANALYSIS_METRIC_ORDER = (
    "ndcg_at_k",
    "precision_at_k",
    "recall_at_k",
    "brier_score",
    "expected_calibration_error",
)


@dataclass(frozen=True, slots=True)
class CohortPairwiseAnalysisRow:
    reference_batch_id: str
    candidate_batch_id: str
    candidate_batch_dir: Path
    candidate_status: str | None
    candidate_completed_seed_count: int | None
    candidate_failed_seed_count: int | None
    candidate_theory_config: str | None
    candidate_launch_source_type: str | None
    accepted_baseline_id: str | None
    benchmark_preset_id: str | None
    eval_preset_id: str | None
    launch_profile_id: str | None
    source_curation_id: str | None
    cohort_key: str | None
    cohort_summary: str
    selected_metric: str
    status_mode: str
    common_doi_count: int
    common_completed_seed_count: int
    paired_seed_count: int
    reference_mean: float | None
    reference_median: float | None
    candidate_mean: float | None
    candidate_median: float | None
    raw_delta_mean: float | None
    raw_delta_median: float | None
    improvement_delta_mean: float | None
    improvement_delta_median: float | None
    wins: int
    losses: int
    ties: int
    tie_rate: float | None
    pairwise_status: str
    available_metrics: tuple[str, ...]
    guardrail_verdict: str | None
    guardrail_reasons: tuple[str, ...]
    reference_run_context_summary: dict[str, Any] | None
    candidate_run_context_summary: dict[str, Any] | None
    summary_payload: dict[str, Any]


def choose_default_reference_batch_id(
    rows: list[ExperimentBatchRow],
    *,
    current_primary_batch_id: str | None = None,
) -> str | None:
    if not rows:
        return None
    if current_primary_batch_id is not None:
        for row in rows:
            if row.batch_id == current_primary_batch_id:
                return row.batch_id

    completed_rows = [row for row in rows if _optional_str(row.status) == "completed"]
    if completed_rows:
        return max(
            completed_rows,
            key=lambda row: (row.timestamp_sort_key, row.batch_id.lower()),
        ).batch_id

    return max(rows, key=lambda row: (row.timestamp_sort_key, row.batch_id.lower())).batch_id


def pairwise_metric_availability_counts(
    *,
    reference_seed_rows: list[dict[str, Any]],
    candidate_seed_rows_by_id: dict[str, list[dict[str, Any]]],
) -> dict[str, int]:
    counts = {metric_name: 0 for metric_name in COMPARISON_METRICS}
    for candidate_seed_rows in candidate_seed_rows_by_id.values():
        aligned_rows = align_common_seed_rows(reference_seed_rows, candidate_seed_rows)
        available_metrics = common_numeric_metrics(aligned_rows)
        for metric_name in available_metrics:
            paired_rows = paired_metric_rows(
                aligned_rows,
                metric_name=metric_name,
                status_mode=PAIRWISE_STATUS_MODE,
            )
            if paired_rows:
                counts[metric_name] += 1
    return {
        metric_name: count
        for metric_name, count in counts.items()
        if count > 0
    }


def choose_default_cohort_analysis_metric(
    metric_counts: dict[str, int],
    *,
    candidate_count: int,
    preferred_metric: str | None = None,
) -> str | None:
    available_metrics = [
        metric_name
        for metric_name in DEFAULT_COHORT_ANALYSIS_METRIC_ORDER
        if metric_counts.get(metric_name, 0) > 0
    ]
    if preferred_metric in available_metrics:
        return preferred_metric
    if not available_metrics:
        return None

    ndcg_count = metric_counts.get("ndcg_at_k", 0)
    if candidate_count > 0 and ndcg_count > 0 and (ndcg_count * 2) > candidate_count:
        return "ndcg_at_k"

    return min(
        available_metrics,
        key=lambda metric_name: (
            -metric_counts.get(metric_name, 0),
            DEFAULT_COHORT_ANALYSIS_METRIC_ORDER.index(metric_name),
        ),
    )


def build_pairwise_analysis_rows(
    *,
    reference_row: ExperimentBatchRow,
    reference_seed_rows: list[dict[str, Any]],
    candidate_rows: list[ExperimentBatchRow],
    candidate_seed_rows_by_id: dict[str, list[dict[str, Any]]],
    selected_metric: str,
) -> list[CohortPairwiseAnalysisRow]:
    analysis_rows: list[CohortPairwiseAnalysisRow] = []
    for candidate_row in candidate_rows:
        if candidate_row.batch_id == reference_row.batch_id:
            continue
        candidate_seed_rows = candidate_seed_rows_by_id.get(candidate_row.batch_id)
        if candidate_seed_rows is None:
            continue
        analysis_rows.append(
            build_pairwise_analysis_row(
                reference_row=reference_row,
                reference_seed_rows=reference_seed_rows,
                candidate_row=candidate_row,
                candidate_seed_rows=candidate_seed_rows,
                selected_metric=selected_metric,
            )
        )
    return analysis_rows


def build_pairwise_analysis_row(
    *,
    reference_row: ExperimentBatchRow,
    reference_seed_rows: list[dict[str, Any]],
    candidate_row: ExperimentBatchRow,
    candidate_seed_rows: list[dict[str, Any]],
    selected_metric: str,
) -> CohortPairwiseAnalysisRow:
    aligned_rows = align_common_seed_rows(reference_seed_rows, candidate_seed_rows)
    common_doi_count = len(aligned_rows)
    common_completed_count = common_completed_seed_count(aligned_rows)
    available_metrics = tuple(common_numeric_metrics(aligned_rows))
    paired_rows = (
        paired_metric_rows(
            aligned_rows,
            metric_name=selected_metric,
            status_mode=PAIRWISE_STATUS_MODE,
        )
        if selected_metric in available_metrics
        else []
    )
    summary = comparison_metric_summary(paired_rows) if paired_rows else None
    assessment = evaluate_decision_guardrails(
        selected_metric=selected_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_count,
        summary=summary,
        paired_seed_count=len(paired_rows),
    )
    pairwise_status = "usable" if paired_rows and common_completed_count > 0 else "unusable"
    tie_rate = (
        float(assessment.ties) / float(len(paired_rows))
        if paired_rows
        else None
    )
    summary_payload = {
        "selected_metric": selected_metric,
        "status_mode": PAIRWISE_STATUS_MODE,
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_count,
        "paired_seed_count": len(paired_rows),
        "available_metrics": list(available_metrics),
        "reference_batch_id": reference_row.batch_id,
        "candidate_batch_id": candidate_row.batch_id,
        "primary_mean": assessment.primary_mean,
        "primary_median": assessment.primary_median,
        "secondary_mean": assessment.secondary_mean,
        "secondary_median": assessment.secondary_median,
        "raw_delta_mean": assessment.raw_delta_mean,
        "raw_delta_median": assessment.raw_delta_median,
        "improvement_delta_mean": assessment.improvement_delta_mean,
        "improvement_delta_median": assessment.improvement_delta_median,
        "wins": assessment.wins,
        "losses": assessment.losses,
        "ties": assessment.ties,
        "tie_rate": tie_rate,
        "pairwise_status": pairwise_status,
        "guardrail_verdict": assessment.verdict,
        "guardrail_reasons": list(assessment.reasons),
    }
    return CohortPairwiseAnalysisRow(
        reference_batch_id=reference_row.batch_id,
        candidate_batch_id=candidate_row.batch_id,
        candidate_batch_dir=candidate_row.batch_dir,
        candidate_status=candidate_row.status,
        candidate_completed_seed_count=candidate_row.completed_seed_count,
        candidate_failed_seed_count=candidate_row.failed_seed_count,
        candidate_theory_config=candidate_row.theory_config,
        candidate_launch_source_type=candidate_row.launch_source_type,
        accepted_baseline_id=candidate_row.accepted_baseline_id,
        benchmark_preset_id=candidate_row.benchmark_preset_id,
        eval_preset_id=candidate_row.eval_preset_id,
        launch_profile_id=candidate_row.launch_profile_id,
        source_curation_id=candidate_row.source_curation_id,
        cohort_key=candidate_row.cohort_key,
        cohort_summary=candidate_row.cohort_summary,
        selected_metric=selected_metric,
        status_mode=PAIRWISE_STATUS_MODE,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_count,
        paired_seed_count=len(paired_rows),
        reference_mean=assessment.primary_mean,
        reference_median=assessment.primary_median,
        candidate_mean=assessment.secondary_mean,
        candidate_median=assessment.secondary_median,
        raw_delta_mean=assessment.raw_delta_mean,
        raw_delta_median=assessment.raw_delta_median,
        improvement_delta_mean=assessment.improvement_delta_mean,
        improvement_delta_median=assessment.improvement_delta_median,
        wins=assessment.wins,
        losses=assessment.losses,
        ties=assessment.ties,
        tie_rate=tie_rate,
        pairwise_status=pairwise_status,
        available_metrics=available_metrics,
        guardrail_verdict=assessment.verdict,
        guardrail_reasons=tuple(assessment.reasons),
        reference_run_context_summary=_copy_optional_dict(reference_row.run_context_summary),
        candidate_run_context_summary=_copy_optional_dict(candidate_row.run_context_summary),
        summary_payload=summary_payload,
    )


def filter_pairwise_analysis_rows(
    rows: list[CohortPairwiseAnalysisRow],
    *,
    search_text: str | None = None,
) -> list[CohortPairwiseAnalysisRow]:
    needle = _optional_str(search_text)
    if needle is None:
        return list(rows)
    lowered_needle = needle.lower()
    return [
        row
        for row in rows
        if lowered_needle in _pairwise_search_blob(row)
    ]


def sort_pairwise_analysis_rows(
    rows: list[CohortPairwiseAnalysisRow],
) -> list[CohortPairwiseAnalysisRow]:
    def sort_key(row: CohortPairwiseAnalysisRow) -> tuple[Any, ...]:
        improvement_delta = row.improvement_delta_mean
        if improvement_delta is None:
            delta_sort_value = float("inf")
        else:
            delta_sort_value = -float(improvement_delta)
        return (
            row.pairwise_status != "usable",
            delta_sort_value,
            -(row.common_completed_seed_count or 0),
            -(row.wins or 0),
            row.candidate_batch_id.lower(),
        )

    return sorted(rows, key=sort_key)


def find_pairwise_analysis_row(
    rows: list[CohortPairwiseAnalysisRow],
    candidate_batch_id: str | None,
) -> CohortPairwiseAnalysisRow | None:
    needle = _optional_str(candidate_batch_id)
    if needle is None:
        return None
    for row in rows:
        if row.candidate_batch_id == needle:
            return row
    return None


def build_pairwise_analysis_table_rows(
    rows: list[CohortPairwiseAnalysisRow],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_batch_id": row.candidate_batch_id,
            "candidate_status": row.candidate_status,
            "pairwise_status": row.pairwise_status,
            "candidate_completed_seed_count": row.candidate_completed_seed_count,
            "common_doi_count": row.common_doi_count,
            "common_completed_seed_count": row.common_completed_seed_count,
            "selected_metric": row.selected_metric,
            "reference_mean": row.reference_mean,
            "candidate_mean": row.candidate_mean,
            "improvement_delta_mean": row.improvement_delta_mean,
            "improvement_delta_median": row.improvement_delta_median,
            "wins": row.wins,
            "losses": row.losses,
            "ties": row.ties,
            "tie_rate": row.tie_rate,
            "guardrail_verdict": row.guardrail_verdict,
            "accepted_baseline_id": row.accepted_baseline_id,
            "benchmark_preset_id": row.benchmark_preset_id,
            "eval_preset_id": row.eval_preset_id,
            "launch_profile_id": row.launch_profile_id,
        }
        for row in rows
    ]


def build_pairwise_analysis_detail(
    row: CohortPairwiseAnalysisRow,
) -> dict[str, Any]:
    return {
        "identity": {
            "reference_batch_id": row.reference_batch_id,
            "candidate_batch_id": row.candidate_batch_id,
            "candidate_batch_dir": row.candidate_batch_dir,
            "candidate_status": row.candidate_status,
            "cohort_key": row.cohort_key,
            "cohort_summary": row.cohort_summary,
            "pairwise_status": row.pairwise_status,
        },
        "summary": dict(row.summary_payload),
        "candidate_provenance": {
            "candidate_theory_config": row.candidate_theory_config,
            "candidate_launch_source_type": row.candidate_launch_source_type,
            "accepted_baseline_id": row.accepted_baseline_id,
            "benchmark_preset_id": row.benchmark_preset_id,
            "eval_preset_id": row.eval_preset_id,
            "launch_profile_id": row.launch_profile_id,
            "source_curation_id": row.source_curation_id,
        },
        "reference_run_context_summary": _copy_optional_dict(row.reference_run_context_summary),
        "candidate_run_context_summary": _copy_optional_dict(row.candidate_run_context_summary),
    }


def _copy_optional_dict(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return dict(value)


def _pairwise_search_blob(row: CohortPairwiseAnalysisRow) -> str:
    return " ".join(
        value.lower()
        for value in (
            row.candidate_batch_id,
            row.candidate_theory_config,
            row.accepted_baseline_id,
            row.benchmark_preset_id,
            row.eval_preset_id,
            row.launch_profile_id,
            row.candidate_launch_source_type,
        )
        if isinstance(value, str) and value.strip()
    )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
