from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.benchmark_curation import (
    BenchmarkCurationError,
    build_benchmark_curation_export_request,
    build_seed_curation_rows,
    build_seed_decision_rows,
    export_benchmark_curation,
    filter_curation_rows,
    normalize_curation_decisions,
    summarize_curation_decisions,
)


def test_failed_seed_is_suggested_for_exclusion() -> None:
    rows = build_seed_curation_rows(
        [
            {
                "batch_index": 1,
                "doi": "10.1000/failed",
                "status": "failed",
                "failed_stage": "evaluation",
                "error_message": "boom",
            }
        ]
    )

    assert rows[0]["suggested_decision"] == "exclude"
    assert "failed" in rows[0]["reason_summary"].lower()


def test_saturated_and_tie_like_seed_is_suggested_for_review() -> None:
    rows = build_seed_curation_rows(
        [
            {
                "batch_index": 1,
                "doi": "10.1000/saturated",
                "status": "completed",
                "ndcg_at_k": 1.0,
                "precision_at_k": 1.0,
            }
        ],
        comparison_rows_by_doi={
            "10.1000/saturated": {"improvement_delta": 0.0}
        },
        selected_metric="ndcg_at_k",
    )

    assert rows[0]["suggested_decision"] == "review"
    assert rows[0]["saturated_ndcg"] is True
    assert rows[0]["tie_like_seed"] is True


def test_normalize_and_summarize_curation_decisions() -> None:
    seed_rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.8},
            {"batch_index": 2, "doi": "10.1000/2", "status": "completed"},
            {"batch_index": 3, "doi": "10.1000/3", "status": "failed"},
        ]
    )

    decisions = normalize_curation_decisions(
        seed_rows,
        {"10.1000/1": "keep", "10.1000/2": "review", "10.1000/3": "exclude"},
    )
    summary = summarize_curation_decisions(
        seed_rows,
        decisions,
        comparison_context_used=False,
    )

    assert summary["total_seeds"] == 3
    assert summary["keep_count"] == 1
    assert summary["review_count"] == 1
    assert summary["exclude_count"] == 1
    assert summary["failed_seed_count"] == 1
    assert summary["usable_completed_seed_count"] == 1


def test_filter_curation_rows_respects_decision_and_flag_filters() -> None:
    seed_rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "precision_at_k": 1.0},
            {"batch_index": 2, "doi": "10.1000/2", "status": "failed"},
        ]
    )
    decisions = normalize_curation_decisions(seed_rows, None)

    filtered = filter_curation_rows(
        seed_rows,
        decisions,
        decision_filter="review",
        only_failed=False,
        only_saturated=True,
        only_tie_like=False,
        doi_filter="",
    )

    assert len(filtered) == 1
    assert filtered[0]["doi"] == "10.1000/1"
    assert filtered[0]["decision"] == "review"


def test_build_seed_decision_rows_includes_flags_and_current_decision() -> None:
    seed_rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.8},
        ]
    )
    decisions = {"10.1000/1": "keep"}

    rows = build_seed_decision_rows(seed_rows, decisions)

    assert rows[0]["decision"] == "keep"
    assert "quality_flags" in rows[0]
    assert rows[0]["quality_flags"]["failed_seed"] is False


def test_export_benchmark_curation_writes_expected_files(tmp_path: Path) -> None:
    seed_rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.8},
            {"batch_index": 2, "doi": "10.1000/2", "status": "completed", "precision_at_k": 1.0},
            {"batch_index": 3, "doi": "10.1000/3", "status": "failed"},
        ]
    )
    decisions = {
        "10.1000/1": "keep",
        "10.1000/2": "review",
        "10.1000/3": "exclude",
    }
    request = build_benchmark_curation_export_request(
        curation_id="curation_001",
        reviewer="Alice",
        notes="smoke seed-set cleanup",
        export_only_kept_to_csv=True,
        include_review_seeds_csv=True,
        include_markdown_summary=True,
    )

    result = export_benchmark_curation(
        base_dir=tmp_path / "runs" / "benchmark_curations",
        request=request,
        context_metadata={
            "primary_batch": {"batch_id": "batch_009", "batch_dir": "runs/batches/batch_009"},
            "secondary_batch": {"batch_id": "batch_010"},
            "selected_comparison_metric": "ndcg_at_k",
            "source_benchmark_preset": {"benchmark_preset_id": "benchmark_smoke_001"},
        },
        seed_rows=seed_rows,
        decisions=decisions,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    curated_csv = result.curated_seeds_csv_path.read_text(encoding="utf-8")
    review_csv = result.review_seeds_csv_path.read_text(encoding="utf-8") if result.review_seeds_csv_path else ""

    assert result.curation_dir == tmp_path / "runs" / "benchmark_curations" / "curation_001"
    assert result.seed_decisions_path.exists()
    assert result.curated_seeds_csv_path.exists()
    assert result.report_path is not None and result.report_path.exists()
    assert result.review_seeds_csv_path is not None and result.review_seeds_csv_path.exists()
    assert manifest["counts"]["keep_count"] == 1
    assert "10.1000/1" in curated_csv
    assert "10.1000/2" in review_csv


def test_export_benchmark_curation_refuses_overwrite(tmp_path: Path) -> None:
    seed_rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.8},
        ]
    )
    request = build_benchmark_curation_export_request(
        curation_id="curation_002",
        reviewer="",
        notes="",
        export_only_kept_to_csv=True,
        include_review_seeds_csv=True,
        include_markdown_summary=False,
    )
    context = {"primary_batch": {"batch_id": "batch_009"}}

    export_benchmark_curation(
        base_dir=tmp_path / "runs" / "benchmark_curations",
        request=request,
        context_metadata=context,
        seed_rows=seed_rows,
        decisions=None,
    )

    with pytest.raises(BenchmarkCurationError):
        export_benchmark_curation(
            base_dir=tmp_path / "runs" / "benchmark_curations",
            request=request,
            context_metadata=context,
            seed_rows=seed_rows,
            decisions=None,
        )


def test_build_seed_curation_rows_handles_missing_comparison_context() -> None:
    rows = build_seed_curation_rows(
        [
            {"batch_index": 1, "doi": "10.1000/1", "status": "completed", "ndcg_at_k": 0.8},
        ],
        comparison_rows_by_doi=None,
        selected_metric=None,
    )

    assert rows[0]["improvement_delta"] is None
    assert rows[0]["tie_like_seed"] is False
