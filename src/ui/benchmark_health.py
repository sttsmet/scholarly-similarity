from __future__ import annotations

import json
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.comparison import (
    COMPARISON_METRICS,
    ComparisonMetricSummary,
    TIE_TOLERANCE,
    movement_diagnostics_payload,
)


SATURATION_METRICS = {
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
}
SATURATION_THRESHOLD = 0.999
NONTRIVIAL_STD_THRESHOLD = 1e-6
MIN_COMPLETED_FOR_USABLE = 3
HIGH_FAILURE_RATE_THRESHOLD = 0.25
OVERWHELMING_SATURATION_RATE = 0.95
WEAK_TIE_RATE_THRESHOLD = 0.8
NEAR_ZERO_DELTA_THRESHOLD = TIE_TOLERANCE


class BenchmarkAuditError(ValueError):
    """Raised when a benchmark audit cannot be created."""


@dataclass(frozen=True, slots=True)
class BatchMetricHealth:
    metric_name: str
    count: int
    missing_count: int
    mean: float | None
    median: float | None
    std: float | None
    min: float | None
    max: float | None
    saturation_rate: float | None
    saturated_seed_count: int
    low_variance: bool
    overwhelming_saturation: bool


@dataclass(frozen=True, slots=True)
class PrimaryBatchHealthAssessment:
    verdict: str
    batch_id: str | None
    batch_dir: str | None
    seed_count: int
    completed_seed_count: int
    failed_seed_count: int
    failure_rate: float | None
    available_metrics: tuple[str, ...]
    completed_seeds_with_numeric_metrics: int
    metric_summaries: tuple[BatchMetricHealth, ...]
    reasons: tuple[str, ...]
    policy_summary: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ComparisonDiscriminativenessAssessment:
    verdict: str
    selected_metric: str | None
    common_doi_count: int
    common_completed_seed_count: int
    paired_seed_count: int
    wins: int
    losses: int
    ties: int
    tie_rate: float | None
    near_zero_delta_fraction: float | None
    improvement_delta_mean: float | None
    improvement_delta_median: float | None
    improvement_delta_std: float | None
    primary_mean: float | None
    primary_median: float | None
    secondary_mean: float | None
    secondary_median: float | None
    reasons: tuple[str, ...]
    policy_summary: dict[str, Any]


@dataclass(frozen=True, slots=True)
class BenchmarkAuditExportRequest:
    audit_id: str
    reviewer: str | None
    notes: str | None
    include_markdown_summary: bool


@dataclass(frozen=True, slots=True)
class BenchmarkAuditExportResult:
    audit_id: str
    audit_dir: Path
    manifest_path: Path
    primary_batch_health_path: Path
    comparison_discriminativeness_path: Path | None
    report_path: Path | None
    seed_quality_table_path: Path | None


def evaluate_primary_batch_health(
    *,
    batch_id: str | None,
    batch_dir: str | Path | None,
    seed_count: int,
    completed_seed_count: int,
    failed_seed_count: int,
    metric_aggregates: dict[str, Any],
    seed_rows: list[dict[str, Any]],
) -> PrimaryBatchHealthAssessment:
    completed_rows = [row for row in seed_rows if str(row.get("status", "")).strip().lower() == "completed"]
    completed_with_numeric_metrics = sum(
        1
        for row in completed_rows
        if any(_numeric_value(row.get(metric_name)) is not None for metric_name in COMPARISON_METRICS)
    )

    metric_summaries: list[BatchMetricHealth] = []
    for metric_name in COMPARISON_METRICS:
        stats = metric_aggregates.get(metric_name)
        count = _int_value(_stats_value(stats, "count")) or 0
        mean = _numeric_value(_stats_value(stats, "mean"))
        median = _numeric_value(_stats_value(stats, "median"))
        std = _numeric_value(_stats_value(stats, "std"))
        min_value = _numeric_value(_stats_value(stats, "min"))
        max_value = _numeric_value(_stats_value(stats, "max"))
        metric_values = [
            value
            for value in (_numeric_value(row.get(metric_name)) for row in completed_rows)
            if value is not None
        ]
        missing_count = max(int(completed_seed_count) - len(metric_values), 0)
        saturation_rate = None
        saturated_seed_count = 0
        if metric_name in SATURATION_METRICS and metric_values:
            saturated_seed_count = sum(1 for value in metric_values if value >= SATURATION_THRESHOLD)
            saturation_rate = saturated_seed_count / len(metric_values)
        low_variance = len(metric_values) >= 2 and (std is None or abs(std) <= NONTRIVIAL_STD_THRESHOLD)
        overwhelming_saturation = saturation_rate is not None and saturation_rate >= OVERWHELMING_SATURATION_RATE
        metric_summaries.append(
            BatchMetricHealth(
                metric_name=metric_name,
                count=count,
                missing_count=missing_count,
                mean=mean,
                median=median,
                std=std,
                min=min_value,
                max=max_value,
                saturation_rate=saturation_rate,
                saturated_seed_count=saturated_seed_count,
                low_variance=low_variance,
                overwhelming_saturation=overwhelming_saturation,
            )
        )

    available_metrics = tuple(
        metric.metric_name
        for metric in metric_summaries
        if metric.count > 0
    )
    discriminative_metrics = [
        metric.metric_name
        for metric in metric_summaries
        if metric.count >= 2
        and not metric.low_variance
        and not metric.overwhelming_saturation
    ]
    failure_rate = (float(failed_seed_count) / float(seed_count)) if int(seed_count) > 0 else None

    reasons: list[str] = []
    if int(completed_seed_count) <= 0:
        verdict = "degenerate"
        reasons.append("There are zero completed seeds in the current batch.")
    elif completed_with_numeric_metrics <= 0 or not available_metrics:
        verdict = "degenerate"
        reasons.append("No usable numeric metrics are available for completed seeds.")
    elif not discriminative_metrics:
        verdict = "degenerate"
        reasons.append("Available numeric metrics are effectively constant or saturated.")
    elif (
        int(completed_seed_count) >= MIN_COMPLETED_FOR_USABLE
        and (failure_rate is None or failure_rate <= HIGH_FAILURE_RATE_THRESHOLD)
        and discriminative_metrics
    ):
        verdict = "usable"
        reasons.append(f"At least {MIN_COMPLETED_FOR_USABLE} completed seeds are available.")
        reasons.append("Failure rate is low enough for a useful readout.")
        reasons.append(
            "Non-trivial variation exists for: "
            + ", ".join(discriminative_metrics)
            + "."
        )
    else:
        verdict = "weak"
        if int(completed_seed_count) < MIN_COMPLETED_FOR_USABLE:
            reasons.append(
                f"Fewer than {MIN_COMPLETED_FOR_USABLE} completed seeds are available."
            )
        if failure_rate is not None and failure_rate > HIGH_FAILURE_RATE_THRESHOLD:
            reasons.append("Failure rate is elevated for a benchmark-quality readout.")
        saturated_metrics = [
            metric.metric_name
            for metric in metric_summaries
            if metric.overwhelming_saturation
        ]
        if saturated_metrics:
            reasons.append(
                "Metric saturation is heavy for: "
                + ", ".join(saturated_metrics)
                + "."
            )
        low_variance_metrics = [
            metric.metric_name
            for metric in metric_summaries
            if metric.count >= 2 and metric.low_variance
        ]
        if low_variance_metrics:
            reasons.append(
                "Metric variance is very low for: "
                + ", ".join(low_variance_metrics)
                + "."
            )
        if not reasons:
            reasons.append("Some signal exists, but the benchmark looks only weakly discriminative.")

    policy_summary = {
        "min_completed_for_usable": MIN_COMPLETED_FOR_USABLE,
        "high_failure_rate_threshold": HIGH_FAILURE_RATE_THRESHOLD,
        "overwhelming_saturation_rate": OVERWHELMING_SATURATION_RATE,
        "nontrivial_std_threshold": NONTRIVIAL_STD_THRESHOLD,
        "batch_id": _optional_str(batch_id),
        "batch_dir": _serialize_path(batch_dir),
        "seed_count": int(seed_count),
        "completed_seed_count": int(completed_seed_count),
        "failed_seed_count": int(failed_seed_count),
        "failure_rate": failure_rate,
        "completed_seeds_with_numeric_metrics": completed_with_numeric_metrics,
        "available_metrics": list(available_metrics),
        "discriminative_metrics": list(discriminative_metrics),
    }
    return PrimaryBatchHealthAssessment(
        verdict=verdict,
        batch_id=_optional_str(batch_id),
        batch_dir=_serialize_path(batch_dir),
        seed_count=int(seed_count),
        completed_seed_count=int(completed_seed_count),
        failed_seed_count=int(failed_seed_count),
        failure_rate=failure_rate,
        available_metrics=available_metrics,
        completed_seeds_with_numeric_metrics=completed_with_numeric_metrics,
        metric_summaries=tuple(metric_summaries),
        reasons=tuple(dict.fromkeys(reason for reason in reasons if reason)),
        policy_summary=policy_summary,
    )


def evaluate_comparison_discriminativeness(
    *,
    selected_metric: str | None,
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_rows: list[dict[str, Any]],
    summary: ComparisonMetricSummary | dict[str, Any] | None,
) -> ComparisonDiscriminativenessAssessment:
    normalized_metric = _optional_str(selected_metric)
    summary_payload = _summary_payload(summary)
    wins = _int_value(summary_payload.get("wins")) or 0
    losses = _int_value(summary_payload.get("losses")) or 0
    ties = _int_value(summary_payload.get("ties")) or 0
    improvement_values = [
        value
        for value in (_numeric_value(row.get("improvement_delta")) for row in paired_rows)
        if value is not None
    ]
    paired_seed_count = len(improvement_values)
    tie_rate = (float(ties) / float(paired_seed_count)) if paired_seed_count > 0 else None
    near_zero_delta_fraction = (
        float(sum(1 for value in improvement_values if abs(value) <= NEAR_ZERO_DELTA_THRESHOLD)) / float(paired_seed_count)
        if paired_seed_count > 0
        else None
    )
    improvement_delta_mean = _numeric_value(summary_payload.get("improvement_delta_mean"))
    improvement_delta_median = _numeric_value(summary_payload.get("improvement_delta_median"))
    improvement_delta_std = _population_std(improvement_values)

    reasons: list[str] = []
    if normalized_metric is None:
        verdict = "degenerate"
        reasons.append("No selected comparison metric is available.")
    elif int(common_completed_seed_count) <= 0:
        verdict = "degenerate"
        reasons.append("There are zero common completed seeds for paired comparison.")
    elif paired_seed_count <= 0:
        verdict = "degenerate"
        reasons.append("The selected metric is unavailable for paired comparison.")
    elif (
        paired_seed_count > 0
        and ties == paired_seed_count
        and all(abs(value) <= NEAR_ZERO_DELTA_THRESHOLD for value in improvement_values)
    ):
        verdict = "degenerate"
        reasons.append("All paired seeds are ties with no measurable improvement.")
    elif (
        int(common_completed_seed_count) >= MIN_COMPLETED_FOR_USABLE
        and (tie_rate is None or tie_rate < WEAK_TIE_RATE_THRESHOLD)
        and (near_zero_delta_fraction is None or near_zero_delta_fraction < WEAK_TIE_RATE_THRESHOLD)
        and (
            wins != losses
            or (improvement_delta_std is not None and improvement_delta_std > NONTRIVIAL_STD_THRESHOLD)
        )
    ):
        verdict = "usable"
        reasons.append(f"At least {MIN_COMPLETED_FOR_USABLE} common completed seeds are available.")
        reasons.append("Tie rate is not overwhelming.")
        reasons.append("The paired comparison shows non-trivial discriminatory signal.")
    else:
        verdict = "weak"
        if int(common_completed_seed_count) < MIN_COMPLETED_FOR_USABLE:
            reasons.append(
                f"Fewer than {MIN_COMPLETED_FOR_USABLE} common completed seeds are available."
            )
        if tie_rate is not None and tie_rate >= WEAK_TIE_RATE_THRESHOLD:
            reasons.append("Ties dominate the paired comparison.")
        if near_zero_delta_fraction is not None and near_zero_delta_fraction >= WEAK_TIE_RATE_THRESHOLD:
            reasons.append("Improvement deltas are mostly near zero.")
        if wins == losses:
            reasons.append("Wins and losses are tied.")
        if improvement_delta_std is not None and improvement_delta_std <= NONTRIVIAL_STD_THRESHOLD:
            reasons.append("Improvement variation is very small.")
        if not reasons:
            reasons.append("The comparison exists, but its discriminatory signal is weak.")

    policy_summary = {
        "min_common_completed_for_usable": MIN_COMPLETED_FOR_USABLE,
        "weak_tie_rate_threshold": WEAK_TIE_RATE_THRESHOLD,
        "near_zero_delta_threshold": NEAR_ZERO_DELTA_THRESHOLD,
        "nontrivial_std_threshold": NONTRIVIAL_STD_THRESHOLD,
        "selected_metric": normalized_metric,
        "common_doi_count": int(common_doi_count),
        "common_completed_seed_count": int(common_completed_seed_count),
        "paired_seed_count": paired_seed_count,
        "wins": wins,
        "losses": losses,
        "ties": ties,
        "tie_rate": tie_rate,
        "near_zero_delta_fraction": near_zero_delta_fraction,
        "improvement_delta_mean": improvement_delta_mean,
        "improvement_delta_median": improvement_delta_median,
        "improvement_delta_std": improvement_delta_std,
        "primary_mean": _numeric_value(summary_payload.get("primary_mean")),
        "primary_median": _numeric_value(summary_payload.get("primary_median")),
        "secondary_mean": _numeric_value(summary_payload.get("secondary_mean")),
        "secondary_median": _numeric_value(summary_payload.get("secondary_median")),
    }
    return ComparisonDiscriminativenessAssessment(
        verdict=verdict,
        selected_metric=normalized_metric,
        common_doi_count=int(common_doi_count),
        common_completed_seed_count=int(common_completed_seed_count),
        paired_seed_count=paired_seed_count,
        wins=wins,
        losses=losses,
        ties=ties,
        tie_rate=tie_rate,
        near_zero_delta_fraction=near_zero_delta_fraction,
        improvement_delta_mean=improvement_delta_mean,
        improvement_delta_median=improvement_delta_median,
        improvement_delta_std=improvement_delta_std,
        primary_mean=_numeric_value(summary_payload.get("primary_mean")),
        primary_median=_numeric_value(summary_payload.get("primary_median")),
        secondary_mean=_numeric_value(summary_payload.get("secondary_mean")),
        secondary_median=_numeric_value(summary_payload.get("secondary_median")),
        reasons=tuple(dict.fromkeys(reason for reason in reasons if reason)),
        policy_summary=policy_summary,
    )


def build_benchmark_audit_export_request(
    *,
    audit_id: str,
    reviewer: str,
    notes: str,
    include_markdown_summary: bool,
) -> BenchmarkAuditExportRequest:
    normalized_audit_id = _normalize_directory_name(audit_id, label="Audit ID")
    return BenchmarkAuditExportRequest(
        audit_id=normalized_audit_id,
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        include_markdown_summary=bool(include_markdown_summary),
    )


def build_primary_batch_health_payload(
    assessment: PrimaryBatchHealthAssessment,
) -> dict[str, Any]:
    return {
        "verdict": assessment.verdict,
        "batch_id": assessment.batch_id,
        "batch_dir": assessment.batch_dir,
        "seed_count": assessment.seed_count,
        "completed_seed_count": assessment.completed_seed_count,
        "failed_seed_count": assessment.failed_seed_count,
        "failure_rate": assessment.failure_rate,
        "available_metrics": list(assessment.available_metrics),
        "completed_seeds_with_numeric_metrics": assessment.completed_seeds_with_numeric_metrics,
        "metric_summaries": [asdict(metric) for metric in assessment.metric_summaries],
        "reasons": list(assessment.reasons),
        "policy_summary": dict(assessment.policy_summary),
    }


def build_comparison_discriminativeness_payload(
    assessment: ComparisonDiscriminativenessAssessment,
) -> dict[str, Any]:
    return {
        "verdict": assessment.verdict,
        "selected_metric": assessment.selected_metric,
        "common_doi_count": assessment.common_doi_count,
        "common_completed_seed_count": assessment.common_completed_seed_count,
        "paired_seed_count": assessment.paired_seed_count,
        "wins": assessment.wins,
        "losses": assessment.losses,
        "ties": assessment.ties,
        "tie_rate": assessment.tie_rate,
        "near_zero_delta_fraction": assessment.near_zero_delta_fraction,
        "improvement_delta_mean": assessment.improvement_delta_mean,
        "improvement_delta_median": assessment.improvement_delta_median,
        "improvement_delta_std": assessment.improvement_delta_std,
        "primary_mean": assessment.primary_mean,
        "primary_median": assessment.primary_median,
        "secondary_mean": assessment.secondary_mean,
        "secondary_median": assessment.secondary_median,
        "reasons": list(assessment.reasons),
        "policy_summary": dict(assessment.policy_summary),
    }


def build_seed_quality_rows(seed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed_row in seed_rows:
        row = {
            "doi": seed_row.get("doi"),
            "status": seed_row.get("status"),
        }
        for metric_name in COMPARISON_METRICS:
            value = _numeric_value(seed_row.get(metric_name))
            row[metric_name] = value
            if metric_name in SATURATION_METRICS:
                row[f"saturated_{metric_name}"] = bool(
                    value is not None and value >= SATURATION_THRESHOLD
                )
        rows.append(row)
    return rows


def build_benchmark_audit_manifest_payload(
    *,
    request: BenchmarkAuditExportRequest,
    audit_dir: str | Path,
    created_at: str,
    context_metadata: dict[str, Any],
    primary_batch_health: PrimaryBatchHealthAssessment,
    comparison_discriminativeness: ComparisonDiscriminativenessAssessment | None,
    output_paths: dict[str, str | None],
) -> dict[str, Any]:
    return {
        "audit_id": request.audit_id,
        "audit_dir": _serialize_path(audit_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "primary_batch": context_metadata.get("primary_batch"),
        "secondary_batch": context_metadata.get("secondary_batch"),
        "selected_metric": context_metadata.get("selected_metric"),
        "accepted_baseline": context_metadata.get("accepted_baseline"),
        "benchmark_preset": context_metadata.get("benchmark_preset"),
        "evaluation_preset": context_metadata.get("evaluation_preset"),
        "launch_profile": context_metadata.get("launch_profile"),
        "primary_batch_health_verdict": primary_batch_health.verdict,
        "primary_batch_health_reasons": list(primary_batch_health.reasons),
        "primary_batch_health_summary": dict(primary_batch_health.policy_summary),
        "comparison_discriminativeness_verdict": (
            comparison_discriminativeness.verdict if comparison_discriminativeness is not None else None
        ),
        "comparison_discriminativeness_reasons": (
            list(comparison_discriminativeness.reasons) if comparison_discriminativeness is not None else []
        ),
        "comparison_discriminativeness_summary": (
            dict(comparison_discriminativeness.policy_summary)
            if comparison_discriminativeness is not None
            else None
        ),
        "output_paths": dict(output_paths),
    }


def build_benchmark_audit_report_markdown(
    *,
    audit_id: str,
    reviewer: str | None,
    notes: str | None,
    created_at: str,
    context_metadata: dict[str, Any],
    primary_batch_health: PrimaryBatchHealthAssessment,
    comparison_discriminativeness: ComparisonDiscriminativenessAssessment | None,
) -> str:
    primary_batch = context_metadata.get("primary_batch") or {}
    secondary_batch = context_metadata.get("secondary_batch") or {}
    benchmark_preset = context_metadata.get("benchmark_preset") or {}
    evaluation_preset = context_metadata.get("evaluation_preset") or {}
    accepted_baseline = context_metadata.get("accepted_baseline") or {}
    launch_profile = context_metadata.get("launch_profile") or {}

    lines = [
        f"# Benchmark Audit: {audit_id}",
        "",
        f"- Created at: `{created_at}`",
        f"- Reviewer: `{reviewer or 'n/a'}`",
        f"- Notes: `{notes or 'n/a'}`",
        "",
        "## Primary Batch Health",
        f"- Batch ID: `{primary_batch.get('batch_id') or primary_batch_health.batch_id or 'n/a'}`",
        f"- Verdict: `{primary_batch_health.verdict}`",
        f"- Completed / Failed / Total: `{primary_batch_health.completed_seed_count}` / `{primary_batch_health.failed_seed_count}` / `{primary_batch_health.seed_count}`",
        f"- Failure Rate: `{primary_batch_health.failure_rate}`",
        f"- Available Metrics: `{', '.join(primary_batch_health.available_metrics) if primary_batch_health.available_metrics else 'n/a'}`",
    ]
    for reason in primary_batch_health.reasons:
        lines.append(f"- {reason}")

    if comparison_discriminativeness is not None:
        lines.extend(
            [
                "",
                "## Comparison Discriminativeness",
                f"- Secondary Batch ID: `{secondary_batch.get('batch_id') or 'n/a'}`",
                f"- Selected Metric: `{comparison_discriminativeness.selected_metric or 'n/a'}`",
                f"- Verdict: `{comparison_discriminativeness.verdict}`",
                f"- Common Completed: `{comparison_discriminativeness.common_completed_seed_count}`",
                f"- Wins / Losses / Ties: `{comparison_discriminativeness.wins}` / `{comparison_discriminativeness.losses}` / `{comparison_discriminativeness.ties}`",
                f"- Tie Rate: `{comparison_discriminativeness.tie_rate}`",
                f"- Improvement Delta Mean / Median / Std: `{comparison_discriminativeness.improvement_delta_mean}` / `{comparison_discriminativeness.improvement_delta_median}` / `{comparison_discriminativeness.improvement_delta_std}`",
            ]
        )
        for reason in comparison_discriminativeness.reasons:
            lines.append(f"- {reason}")

    if benchmark_preset or evaluation_preset or accepted_baseline or launch_profile:
        lines.extend(["", "## Current Selections"])
        if accepted_baseline:
            lines.append(f"- Accepted Baseline: `{accepted_baseline.get('baseline_id') or 'n/a'}`")
        if benchmark_preset:
            lines.append(f"- Benchmark Preset: `{benchmark_preset.get('benchmark_preset_id') or 'n/a'}`")
        if evaluation_preset:
            lines.append(f"- Evaluation Preset: `{evaluation_preset.get('eval_preset_id') or 'n/a'}`")
        if launch_profile:
            lines.append(f"- Launch Profile: `{launch_profile.get('launch_profile_id') or 'n/a'}`")

    return "\n".join(lines) + "\n"


def export_benchmark_audit(
    *,
    base_dir: str | Path,
    request: BenchmarkAuditExportRequest,
    context_metadata: dict[str, Any],
    primary_batch_health: PrimaryBatchHealthAssessment,
    comparison_discriminativeness: ComparisonDiscriminativenessAssessment | None,
    seed_quality_rows: list[dict[str, Any]] | None = None,
) -> BenchmarkAuditExportResult:
    if _optional_str(primary_batch_health.batch_id) is None:
        raise BenchmarkAuditError("A primary batch must be loaded before exporting a benchmark audit.")

    audit_root = Path(base_dir)
    audit_root.mkdir(parents=True, exist_ok=True)
    audit_dir = audit_root / request.audit_id
    if audit_dir.exists():
        raise BenchmarkAuditError(f"Benchmark audit directory already exists: {audit_dir}")

    created_at = _utc_timestamp()
    audit_dir.mkdir(parents=False, exist_ok=False)
    manifest_path = audit_dir / "benchmark_audit_manifest.json"
    primary_batch_health_path = audit_dir / "primary_batch_health.json"
    comparison_discriminativeness_path = (
        audit_dir / "comparison_discriminativeness.json"
        if comparison_discriminativeness is not None
        else None
    )
    report_path = audit_dir / "benchmark_audit_report.md" if request.include_markdown_summary else None
    seed_quality_table_path = (
        audit_dir / "seed_quality_table.jsonl"
        if seed_quality_rows
        else None
    )

    output_paths = {
        "benchmark_audit_manifest_json": _serialize_path(manifest_path),
        "primary_batch_health_json": _serialize_path(primary_batch_health_path),
        "comparison_discriminativeness_json": _serialize_path(comparison_discriminativeness_path)
        if comparison_discriminativeness_path is not None
        else None,
        "benchmark_audit_report_md": _serialize_path(report_path) if report_path is not None else None,
        "seed_quality_table_jsonl": _serialize_path(seed_quality_table_path) if seed_quality_table_path is not None else None,
    }
    _write_json(
        manifest_path,
        build_benchmark_audit_manifest_payload(
            request=request,
            audit_dir=audit_dir,
            created_at=created_at,
            context_metadata=context_metadata,
            primary_batch_health=primary_batch_health,
            comparison_discriminativeness=comparison_discriminativeness,
            output_paths=output_paths,
        ),
    )
    _write_json(primary_batch_health_path, build_primary_batch_health_payload(primary_batch_health))
    if comparison_discriminativeness_path is not None and comparison_discriminativeness is not None:
        _write_json(
            comparison_discriminativeness_path,
            build_comparison_discriminativeness_payload(comparison_discriminativeness),
        )
    if report_path is not None:
        report_path.write_text(
            build_benchmark_audit_report_markdown(
                audit_id=request.audit_id,
                reviewer=request.reviewer,
                notes=request.notes,
                created_at=created_at,
                context_metadata=context_metadata,
                primary_batch_health=primary_batch_health,
                comparison_discriminativeness=comparison_discriminativeness,
            ),
            encoding="utf-8",
        )
    if seed_quality_table_path is not None and seed_quality_rows:
        _write_jsonl(seed_quality_table_path, seed_quality_rows)

    return BenchmarkAuditExportResult(
        audit_id=request.audit_id,
        audit_dir=audit_dir,
        manifest_path=manifest_path,
        primary_batch_health_path=primary_batch_health_path,
        comparison_discriminativeness_path=comparison_discriminativeness_path,
        report_path=report_path,
        seed_quality_table_path=seed_quality_table_path,
    )


def _summary_payload(summary: ComparisonMetricSummary | dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(summary, ComparisonMetricSummary):
        return {
            "primary_mean": summary.primary_mean,
            "primary_median": summary.primary_median,
            "secondary_mean": summary.secondary_mean,
            "secondary_median": summary.secondary_median,
            "improvement_delta_mean": summary.improvement_delta_mean,
            "improvement_delta_median": summary.improvement_delta_median,
            "wins": summary.wins,
            "losses": summary.losses,
            "ties": summary.ties,
            "movement_diagnostics": movement_diagnostics_payload(summary.movement_diagnostics),
        }
    if isinstance(summary, dict):
        return dict(summary)
    return {}


def _stats_value(stats: Any, field_name: str) -> Any:
    if isinstance(stats, dict):
        return stats.get(field_name)
    return getattr(stats, field_name, None)


def _int_value(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _numeric_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _population_std(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return float(statistics.pstdev(values))


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise BenchmarkAuditError(f"{label} is required.")
    if normalized in {".", ".."}:
        raise BenchmarkAuditError(f"{label} must not be '.' or '..'.")
    invalid_characters = set('/\\:*?"<>|')
    if any(character in invalid_characters for character in normalized):
        raise BenchmarkAuditError(f"{label} contains invalid path characters.")
    return normalized


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: str | Path | None) -> str | None:
    if value in (None, ""):
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
