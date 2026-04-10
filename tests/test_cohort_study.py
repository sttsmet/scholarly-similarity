from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.cohort_analysis import CohortPairwiseAnalysisRow
from src.ui.cohort_study import (
    CohortStudyError,
    build_candidate_decision_rows,
    build_cohort_leaderboard_export_rows,
    build_cohort_study_context_key,
    build_cohort_study_export_request,
    build_cohort_study_table_rows,
    export_cohort_study,
    filter_cohort_study_rows,
    normalize_cohort_study_decisions,
    suggest_cohort_study_decision,
    summarize_cohort_study_decisions,
)
from src.ui.experiment_matrix import build_experiment_batch_row


def _pairwise_row(
    tmp_path: Path,
    *,
    batch_id: str,
    pairwise_status: str = "usable",
    common_completed_seed_count: int = 4,
    improvement_delta_mean: float | None = 0.05,
    wins: int = 3,
    losses: int = 1,
    ties: int = 0,
    tie_rate: float | None = 0.0,
    guardrail_verdict: str | None = "pass",
) -> CohortPairwiseAnalysisRow:
    return CohortPairwiseAnalysisRow(
        reference_batch_id="batch_ref",
        candidate_batch_id=batch_id,
        candidate_batch_dir=tmp_path / "runs" / "batches" / batch_id,
        candidate_status="completed",
        candidate_completed_seed_count=4,
        candidate_failed_seed_count=0,
        candidate_theory_config=f"configs/{batch_id}.yaml",
        candidate_launch_source_type="launch_profile",
        accepted_baseline_id="baseline_001",
        benchmark_preset_id="benchmark_smoke_001",
        eval_preset_id="eval_micro_001",
        launch_profile_id="launch_profile_001",
        source_curation_id="curation_001",
        cohort_key="cohort_a",
        cohort_summary="data/benchmarks/seeds.csv | refs=10 | related=10 | hardneg=10 | top_k=10 | label=silver",
        selected_metric="ndcg_at_k",
        status_mode="common completed only",
        common_doi_count=max(common_completed_seed_count, 0),
        common_completed_seed_count=common_completed_seed_count,
        paired_seed_count=common_completed_seed_count if pairwise_status == "usable" else 0,
        reference_mean=0.5,
        reference_median=0.5,
        candidate_mean=0.55 if improvement_delta_mean is not None else None,
        candidate_median=0.55 if improvement_delta_mean is not None else None,
        raw_delta_mean=improvement_delta_mean,
        raw_delta_median=improvement_delta_mean,
        improvement_delta_mean=improvement_delta_mean,
        improvement_delta_median=improvement_delta_mean,
        wins=wins,
        losses=losses,
        ties=ties,
        tie_rate=tie_rate,
        pairwise_status=pairwise_status,
        available_metrics=("ndcg_at_k",),
        guardrail_verdict=guardrail_verdict,
        guardrail_reasons=("reason",) if guardrail_verdict else (),
        reference_run_context_summary={"accepted_baseline_id": "baseline_001"},
        candidate_run_context_summary={"launch_profile_id": "launch_profile_001"},
        summary_payload={"selected_metric": "ndcg_at_k"},
    )


def _reference_row(tmp_path: Path) -> object:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ref"
    batch_dir.mkdir(parents=True, exist_ok=True)
    return build_experiment_batch_row(
        batch_dir=batch_dir,
        manifest_payload={
            "batch_id": "batch_ref",
            "created_at": "2026-04-01T10:00:00Z",
            "status": "completed",
            "seed_count": 4,
            "completed_seed_count": 4,
            "failed_seed_count": 0,
            "theory_config": "configs/reference.yaml",
            "seeds_csv": "data/benchmarks/seeds.csv",
            "options": {
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={"metric_aggregates": {"ndcg_at_k": {"mean": 0.5}}},
        run_context_payload={"accepted_baseline_id": "baseline_001"},
        manifest_mtime=0.0,
    )


def test_suggest_cohort_study_decision_shortlist_case(tmp_path: Path) -> None:
    decision, reasons = suggest_cohort_study_decision(_pairwise_row(tmp_path, batch_id="batch_good"))

    assert decision == "shortlist"
    assert any("positive" in reason.lower() for reason in reasons)


def test_suggest_cohort_study_decision_review_case(tmp_path: Path) -> None:
    decision, reasons = suggest_cohort_study_decision(
        _pairwise_row(
            tmp_path,
            batch_id="batch_review",
            common_completed_seed_count=2,
            improvement_delta_mean=0.0,
            wins=1,
            losses=1,
            ties=2,
            tie_rate=1.0,
            guardrail_verdict="weak",
        )
    )

    assert decision == "review"
    assert any("weak" in reason.lower() or "neutral" in reason.lower() for reason in reasons)


def test_suggest_cohort_study_decision_drop_case_for_unusable_row(tmp_path: Path) -> None:
    decision, reasons = suggest_cohort_study_decision(
        _pairwise_row(
            tmp_path,
            batch_id="batch_drop",
            pairwise_status="unusable",
            common_completed_seed_count=0,
            improvement_delta_mean=None,
            wins=0,
            losses=0,
            ties=0,
            tie_rate=None,
            guardrail_verdict="fail",
        )
    )

    assert decision == "drop"
    assert any("unusable" in reason.lower() or "fail" in reason.lower() for reason in reasons)


def test_normalize_summarize_and_filter_cohort_study_decisions(tmp_path: Path) -> None:
    rows = [
        _pairwise_row(tmp_path, batch_id="batch_good"),
        _pairwise_row(
            tmp_path,
            batch_id="batch_review",
            common_completed_seed_count=2,
            improvement_delta_mean=0.0,
            wins=1,
            losses=1,
            ties=2,
            tie_rate=1.0,
            guardrail_verdict="weak",
        ),
        _pairwise_row(
            tmp_path,
            batch_id="batch_drop",
            pairwise_status="unusable",
            common_completed_seed_count=0,
            improvement_delta_mean=None,
            wins=0,
            losses=0,
            ties=0,
            tie_rate=None,
            guardrail_verdict="fail",
        ),
    ]

    decisions = normalize_cohort_study_decisions(rows, {"batch_review": "shortlist"})
    summary = summarize_cohort_study_decisions(
        rows,
        decisions,
        reference_batch_id="batch_ref",
        selected_metric="ndcg_at_k",
    )
    filtered = filter_cohort_study_rows(
        rows,
        decisions,
        decision_filter="shortlist",
        only_usable=False,
        only_unusable=False,
        search_text="batch",
    )

    assert decisions["batch_good"] == "shortlist"
    assert decisions["batch_review"] == "shortlist"
    assert decisions["batch_drop"] == "drop"
    assert summary["shortlist_count"] == 2
    assert summary["drop_count"] == 1
    assert summary["usable_candidate_rows"] == 2
    assert len(filtered) == 2


def test_build_export_rows_and_context_key_are_stable(tmp_path: Path) -> None:
    rows = [
        _pairwise_row(tmp_path, batch_id="batch_good"),
        _pairwise_row(
            tmp_path,
            batch_id="batch_drop",
            pairwise_status="unusable",
            common_completed_seed_count=0,
            improvement_delta_mean=None,
            wins=0,
            losses=0,
            ties=0,
            tie_rate=None,
            guardrail_verdict="fail",
        ),
    ]
    decisions = normalize_cohort_study_decisions(rows, None)

    leaderboard_rows = build_cohort_leaderboard_export_rows(rows)
    decision_rows = build_candidate_decision_rows(rows, decisions)
    table_rows = build_cohort_study_table_rows(rows, decisions)
    context_key = build_cohort_study_context_key(
        cohort_key="cohort_a",
        reference_batch_id="batch_ref",
        selected_metric="ndcg_at_k",
        candidate_batch_ids=["batch_drop", "batch_good"],
    )

    assert leaderboard_rows[0]["candidate_batch_id"] == "batch_good"
    assert decision_rows[1]["decision"] == "drop"
    assert table_rows[0]["decision"] == "shortlist"
    assert "batch_good" in context_key


def test_export_cohort_study_writes_expected_files(tmp_path: Path) -> None:
    rows = [
        _pairwise_row(tmp_path, batch_id="batch_good"),
        _pairwise_row(
            tmp_path,
            batch_id="batch_review",
            common_completed_seed_count=2,
            improvement_delta_mean=0.0,
            wins=1,
            losses=1,
            ties=2,
            tie_rate=1.0,
            guardrail_verdict="weak",
        ),
        _pairwise_row(
            tmp_path,
            batch_id="batch_drop",
            pairwise_status="unusable",
            common_completed_seed_count=0,
            improvement_delta_mean=None,
            wins=0,
            losses=0,
            ties=0,
            tie_rate=None,
            guardrail_verdict="fail",
        ),
    ]
    decisions = {
        "batch_good": "shortlist",
        "batch_review": "review",
        "batch_drop": "drop",
    }
    request = build_cohort_study_export_request(
        study_id="study_001",
        reviewer="Alice",
        notes="smoke cohort study",
        include_markdown_summary=True,
        include_shortlist_csv=True,
    )

    result = export_cohort_study(
        base_dir=tmp_path / "runs" / "cohort_studies",
        request=request,
        cohort_key="cohort_a",
        cohort_summary="cohort summary",
        reference_row=_reference_row(tmp_path),
        selected_metric="ndcg_at_k",
        pairwise_rows=rows,
        decisions=decisions,
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    shortlist_csv = result.shortlist_csv_path.read_text(encoding="utf-8") if result.shortlist_csv_path else ""

    assert result.study_dir == tmp_path / "runs" / "cohort_studies" / "study_001"
    assert result.leaderboard_path.exists()
    assert result.decisions_path.exists()
    assert result.report_path is not None and result.report_path.exists()
    assert result.shortlist_csv_path is not None and result.shortlist_csv_path.exists()
    assert manifest["shortlist_count"] == 1
    assert "batch_good" in shortlist_csv


def test_export_cohort_study_refuses_overwrite(tmp_path: Path) -> None:
    rows = [_pairwise_row(tmp_path, batch_id="batch_good")]
    request = build_cohort_study_export_request(
        study_id="study_002",
        reviewer="",
        notes="",
        include_markdown_summary=False,
        include_shortlist_csv=False,
    )
    reference_row = _reference_row(tmp_path)

    export_cohort_study(
        base_dir=tmp_path / "runs" / "cohort_studies",
        request=request,
        cohort_key="cohort_a",
        cohort_summary="cohort summary",
        reference_row=reference_row,
        selected_metric="ndcg_at_k",
        pairwise_rows=rows,
        decisions=None,
    )

    with pytest.raises(CohortStudyError):
        export_cohort_study(
            base_dir=tmp_path / "runs" / "cohort_studies",
            request=request,
            cohort_key="cohort_a",
            cohort_summary="cohort summary",
            reference_row=reference_row,
            selected_metric="ndcg_at_k",
            pairwise_rows=rows,
            decisions=None,
        )
