from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ui.benchmark_health import (
    BenchmarkAuditError,
    build_benchmark_audit_export_request,
    build_benchmark_audit_manifest_payload,
    build_seed_quality_rows,
    evaluate_comparison_discriminativeness,
    evaluate_primary_batch_health,
    export_benchmark_audit,
)
from src.ui.comparison import ComparisonMetricSummary


def _metric_stats(
    *,
    count: int,
    mean: float,
    median: float,
    std: float,
    min_value: float,
    max_value: float,
) -> SimpleNamespace:
    return SimpleNamespace(
        count=count,
        mean=mean,
        median=median,
        std=std,
        min=min_value,
        max=max_value,
    )


def _comparison_summary(
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


def test_evaluate_primary_batch_health_usable_case() -> None:
    assessment = evaluate_primary_batch_health(
        batch_id="batch_009",
        batch_dir="runs/batches/batch_009",
        seed_count=4,
        completed_seed_count=4,
        failed_seed_count=0,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=4,
                mean=0.72,
                median=0.73,
                std=0.08,
                min_value=0.61,
                max_value=0.81,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.61},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 0.69},
            {"doi": "10.1000/3", "status": "completed", "ndcg_at_k": 0.77},
            {"doi": "10.1000/4", "status": "completed", "ndcg_at_k": 0.81},
        ],
    )

    assert assessment.verdict == "usable"
    assert "ndcg_at_k" in assessment.available_metrics
    assert assessment.completed_seeds_with_numeric_metrics == 4


def test_evaluate_primary_batch_health_weak_case_when_saturation_is_heavy() -> None:
    assessment = evaluate_primary_batch_health(
        batch_id="batch_010",
        batch_dir="runs/batches/batch_010",
        seed_count=3,
        completed_seed_count=2,
        failed_seed_count=1,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=2,
                mean=0.75,
                median=0.75,
                std=0.05,
                min_value=0.7,
                max_value=0.8,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.7},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 0.8},
            {"doi": "10.1000/3", "status": "failed"},
        ],
    )

    assert assessment.verdict == "weak"
    assert "Fewer than 3 completed seeds are available." in assessment.reasons


def test_evaluate_primary_batch_health_degenerate_when_saturation_is_total() -> None:
    assessment = evaluate_primary_batch_health(
        batch_id="batch_010",
        batch_dir="runs/batches/batch_010",
        seed_count=3,
        completed_seed_count=2,
        failed_seed_count=1,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=2,
                mean=1.0,
                median=1.0,
                std=0.0,
                min_value=1.0,
                max_value=1.0,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 1.0},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 1.0},
            {"doi": "10.1000/3", "status": "failed"},
        ],
    )

    assert assessment.verdict == "degenerate"
    assert "constant or saturated" in assessment.reasons[0]


def test_evaluate_primary_batch_health_degenerate_without_numeric_metrics() -> None:
    assessment = evaluate_primary_batch_health(
        batch_id="batch_011",
        batch_dir="runs/batches/batch_011",
        seed_count=2,
        completed_seed_count=2,
        failed_seed_count=0,
        metric_aggregates={},
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed"},
            {"doi": "10.1000/2", "status": "completed"},
        ],
    )

    assert assessment.verdict == "degenerate"
    assert "No usable numeric metrics" in assessment.reasons[0]


def test_evaluate_comparison_discriminativeness_usable_case() -> None:
    assessment = evaluate_comparison_discriminativeness(
        selected_metric="ndcg_at_k",
        common_doi_count=4,
        common_completed_seed_count=4,
        paired_rows=[
            {"improvement_delta": 0.10},
            {"improvement_delta": 0.07},
            {"improvement_delta": -0.02},
            {"improvement_delta": 0.04},
        ],
        summary=_comparison_summary(
            improvement_delta_mean=0.0475,
            improvement_delta_median=0.055,
            wins=3,
            losses=1,
            ties=0,
        ),
    )

    assert assessment.verdict == "usable"
    assert assessment.tie_rate == 0.0


def test_evaluate_comparison_discriminativeness_weak_case() -> None:
    assessment = evaluate_comparison_discriminativeness(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        paired_rows=[
            {"improvement_delta": 0.01},
            {"improvement_delta": -0.01},
        ],
        summary=_comparison_summary(
            improvement_delta_mean=0.0,
            improvement_delta_median=0.0,
            wins=1,
            losses=1,
            ties=0,
        ),
    )

    assert assessment.verdict == "weak"
    assert "Fewer than 3 common completed seeds are available." in assessment.reasons


def test_evaluate_comparison_discriminativeness_weak_all_ties_case() -> None:
    assessment = evaluate_comparison_discriminativeness(
        selected_metric="ndcg_at_k",
        common_doi_count=3,
        common_completed_seed_count=2,
        paired_rows=[
            {"improvement_delta": 0.0},
            {"improvement_delta": 0.0},
        ],
        summary=_comparison_summary(
            improvement_delta_mean=0.0,
            improvement_delta_median=0.0,
            wins=0,
            losses=0,
            ties=2,
        ),
    )

    assert assessment.verdict == "degenerate"
    assert "All paired seeds are ties" in assessment.reasons[0]


def test_evaluate_comparison_discriminativeness_degenerate_without_metric() -> None:
    assessment = evaluate_comparison_discriminativeness(
        selected_metric=None,
        common_doi_count=2,
        common_completed_seed_count=2,
        paired_rows=[],
        summary=None,
    )

    assert assessment.verdict == "degenerate"
    assert "No selected comparison metric" in assessment.reasons[0]


def test_build_seed_quality_rows_marks_saturation_flags() -> None:
    rows = build_seed_quality_rows(
        [
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 1.0, "precision_at_k": 0.5},
        ]
    )

    assert rows[0]["saturated_ndcg_at_k"] is True
    assert rows[0]["saturated_precision_at_k"] is False


def test_build_benchmark_audit_manifest_payload_includes_verdicts() -> None:
    primary_assessment = evaluate_primary_batch_health(
        batch_id="batch_009",
        batch_dir="runs/batches/batch_009",
        seed_count=3,
        completed_seed_count=3,
        failed_seed_count=0,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=3,
                mean=0.75,
                median=0.75,
                std=0.05,
                min_value=0.7,
                max_value=0.8,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.7},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 0.75},
            {"doi": "10.1000/3", "status": "completed", "ndcg_at_k": 0.8},
        ],
    )
    comparison_assessment = evaluate_comparison_discriminativeness(
        selected_metric="ndcg_at_k",
        common_doi_count=3,
        common_completed_seed_count=3,
        paired_rows=[
            {"improvement_delta": 0.1},
            {"improvement_delta": 0.0},
            {"improvement_delta": 0.05},
        ],
        summary=_comparison_summary(
            improvement_delta_mean=0.05,
            improvement_delta_median=0.05,
            wins=2,
            losses=0,
            ties=1,
        ),
    )
    request = build_benchmark_audit_export_request(
        audit_id="audit_001",
        reviewer="Alice",
        notes="smoke benchmark audit",
        include_markdown_summary=True,
    )

    payload = build_benchmark_audit_manifest_payload(
        request=request,
        audit_dir="runs/benchmark_audits/audit_001",
        created_at="2026-03-30T12:00:00Z",
        context_metadata={"primary_batch": {"batch_id": "batch_009"}, "selected_metric": "ndcg_at_k"},
        primary_batch_health=primary_assessment,
        comparison_discriminativeness=comparison_assessment,
        output_paths={"benchmark_audit_manifest_json": "runs/benchmark_audits/audit_001/benchmark_audit_manifest.json"},
    )

    assert payload["primary_batch_health_verdict"] == "usable"
    assert payload["comparison_discriminativeness_verdict"] == "usable"
    assert payload["selected_metric"] == "ndcg_at_k"


def test_export_benchmark_audit_refuses_overwrite(tmp_path: Path) -> None:
    primary_assessment = evaluate_primary_batch_health(
        batch_id="batch_009",
        batch_dir="runs/batches/batch_009",
        seed_count=3,
        completed_seed_count=3,
        failed_seed_count=0,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=3,
                mean=0.75,
                median=0.75,
                std=0.05,
                min_value=0.7,
                max_value=0.8,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.7},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 0.75},
            {"doi": "10.1000/3", "status": "completed", "ndcg_at_k": 0.8},
        ],
    )
    request = build_benchmark_audit_export_request(
        audit_id="audit_002",
        reviewer="",
        notes="",
        include_markdown_summary=False,
    )

    export_benchmark_audit(
        base_dir=tmp_path / "runs" / "benchmark_audits",
        request=request,
        context_metadata={"primary_batch": {"batch_id": "batch_009"}},
        primary_batch_health=primary_assessment,
        comparison_discriminativeness=None,
    )

    with pytest.raises(BenchmarkAuditError):
        export_benchmark_audit(
            base_dir=tmp_path / "runs" / "benchmark_audits",
            request=request,
            context_metadata={"primary_batch": {"batch_id": "batch_009"}},
            primary_batch_health=primary_assessment,
            comparison_discriminativeness=None,
        )


def test_export_benchmark_audit_writes_expected_files(tmp_path: Path) -> None:
    primary_assessment = evaluate_primary_batch_health(
        batch_id="batch_009",
        batch_dir="runs/batches/batch_009",
        seed_count=3,
        completed_seed_count=3,
        failed_seed_count=0,
        metric_aggregates={
            "ndcg_at_k": _metric_stats(
                count=3,
                mean=0.75,
                median=0.75,
                std=0.05,
                min_value=0.7,
                max_value=0.8,
            )
        },
        seed_rows=[
            {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.7},
            {"doi": "10.1000/2", "status": "completed", "ndcg_at_k": 0.75},
            {"doi": "10.1000/3", "status": "completed", "ndcg_at_k": 0.8},
        ],
    )
    comparison_assessment = evaluate_comparison_discriminativeness(
        selected_metric="ndcg_at_k",
        common_doi_count=3,
        common_completed_seed_count=3,
        paired_rows=[
            {"improvement_delta": 0.1},
            {"improvement_delta": 0.0},
            {"improvement_delta": 0.05},
        ],
        summary=_comparison_summary(
            improvement_delta_mean=0.05,
            improvement_delta_median=0.05,
            wins=2,
            losses=0,
            ties=1,
        ),
    )
    request = build_benchmark_audit_export_request(
        audit_id="audit_003",
        reviewer="Alice",
        notes="smoke benchmark audit",
        include_markdown_summary=True,
    )

    result = export_benchmark_audit(
        base_dir=tmp_path / "runs" / "benchmark_audits",
        request=request,
        context_metadata={
            "primary_batch": {"batch_id": "batch_009"},
            "secondary_batch": {"batch_id": "batch_010"},
            "selected_metric": "ndcg_at_k",
        },
        primary_batch_health=primary_assessment,
        comparison_discriminativeness=comparison_assessment,
        seed_quality_rows=build_seed_quality_rows(
            [
                {"doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.7},
            ]
        ),
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))

    assert result.audit_dir == tmp_path / "runs" / "benchmark_audits" / "audit_003"
    assert result.primary_batch_health_path.exists()
    assert result.comparison_discriminativeness_path is not None
    assert result.comparison_discriminativeness_path.exists()
    assert result.report_path is not None
    assert result.report_path.exists()
    assert result.seed_quality_table_path is not None
    assert result.seed_quality_table_path.exists()
    assert manifest["primary_batch_health_verdict"] == "usable"
    assert manifest["comparison_discriminativeness_verdict"] == "usable"
