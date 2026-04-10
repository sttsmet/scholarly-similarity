from __future__ import annotations

from pathlib import Path

from src.ui.cohort_analysis import (
    build_pairwise_analysis_detail,
    build_pairwise_analysis_rows,
    build_pairwise_analysis_table_rows,
    choose_default_cohort_analysis_metric,
    choose_default_reference_batch_id,
    filter_pairwise_analysis_rows,
    find_pairwise_analysis_row,
    pairwise_metric_availability_counts,
    sort_pairwise_analysis_rows,
)
from src.ui.experiment_matrix import build_experiment_batch_row


def _experiment_row(
    tmp_path: Path,
    *,
    batch_id: str,
    created_at: str,
    status: str = "completed",
    theory_config: str = "configs/theory_v001.yaml",
    run_context_payload: dict[str, object] | None = None,
) -> object:
    batch_dir = tmp_path / "runs" / "batches" / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    return build_experiment_batch_row(
        batch_dir=batch_dir,
        manifest_payload={
            "batch_id": batch_id,
            "created_at": created_at,
            "status": status,
            "seed_count": 3,
            "completed_seed_count": 3 if status == "completed" else 0,
            "failed_seed_count": 0,
            "theory_config": theory_config,
            "seeds_csv": "data/benchmarks/seeds.csv",
            "options": {
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={"metric_aggregates": {"ndcg_at_k": {"mean": 0.8}}},
        run_context_payload=run_context_payload,
        manifest_mtime=0.0,
    )


def _reference_seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": 0.40,
            "precision_at_k": 0.40,
            "recall_at_k": 0.50,
            "brier_score": 0.40,
        },
        {
            "batch_index": 2,
            "doi": "10.1000/b",
            "status": "completed",
            "ndcg_at_k": 0.50,
            "precision_at_k": 0.50,
            "recall_at_k": 0.60,
            "brier_score": 0.30,
        },
        {
            "batch_index": 3,
            "doi": "10.1000/c",
            "status": "completed",
            "ndcg_at_k": 0.60,
            "precision_at_k": 0.60,
            "recall_at_k": 0.70,
            "brier_score": 0.20,
        },
    ]


def _better_seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": 0.45,
            "precision_at_k": 0.45,
            "recall_at_k": 0.55,
            "brier_score": 0.35,
        },
        {
            "batch_index": 2,
            "doi": "10.1000/b",
            "status": "completed",
            "ndcg_at_k": 0.55,
            "precision_at_k": 0.55,
            "recall_at_k": 0.65,
            "brier_score": 0.25,
        },
        {
            "batch_index": 3,
            "doi": "10.1000/c",
            "status": "completed",
            "ndcg_at_k": 0.65,
            "precision_at_k": 0.65,
            "recall_at_k": 0.75,
            "brier_score": 0.15,
        },
    ]


def _worse_seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": 0.30,
            "precision_at_k": 0.35,
            "recall_at_k": 0.45,
            "brier_score": 0.45,
        },
        {
            "batch_index": 2,
            "doi": "10.1000/b",
            "status": "completed",
            "ndcg_at_k": 0.40,
            "precision_at_k": 0.45,
            "recall_at_k": 0.55,
            "brier_score": 0.35,
        },
        {
            "batch_index": 3,
            "doi": "10.1000/c",
            "status": "completed",
            "ndcg_at_k": 0.50,
            "precision_at_k": 0.55,
            "recall_at_k": 0.65,
            "brier_score": 0.25,
        },
    ]


def _missing_metric_seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": None,
            "precision_at_k": 0.45,
        },
        {
            "batch_index": 2,
            "doi": "10.1000/b",
            "status": "completed",
            "ndcg_at_k": None,
            "precision_at_k": 0.55,
        },
    ]


def test_choose_default_reference_batch_prefers_current_primary_when_present(tmp_path: Path) -> None:
    older = _experiment_row(tmp_path, batch_id="batch_005", created_at="2026-04-01T10:00:00Z")
    newer = _experiment_row(tmp_path, batch_id="batch_009", created_at="2026-04-01T12:00:00Z")

    selected = choose_default_reference_batch_id(
        [older, newer],
        current_primary_batch_id="batch_005",
    )

    assert selected == "batch_005"


def test_choose_default_reference_batch_falls_back_to_newest_completed(tmp_path: Path) -> None:
    running = _experiment_row(
        tmp_path,
        batch_id="batch_004",
        created_at="2026-04-01T09:00:00Z",
        status="running",
    )
    completed_old = _experiment_row(tmp_path, batch_id="batch_005", created_at="2026-04-01T10:00:00Z")
    completed_new = _experiment_row(tmp_path, batch_id="batch_006", created_at="2026-04-01T11:00:00Z")

    selected = choose_default_reference_batch_id([running, completed_old, completed_new])

    assert selected == "batch_006"


def test_pairwise_metric_availability_and_default_metric_selection(tmp_path: Path) -> None:
    reference = _reference_seed_rows()
    candidate_seed_rows_by_id = {
        "batch_better": _better_seed_rows(),
        "batch_missing_ndcg": _missing_metric_seed_rows(),
    }

    metric_counts = pairwise_metric_availability_counts(
        reference_seed_rows=reference,
        candidate_seed_rows_by_id=candidate_seed_rows_by_id,
    )
    selected_metric = choose_default_cohort_analysis_metric(
        metric_counts,
        candidate_count=2,
    )

    assert metric_counts["ndcg_at_k"] == 1
    assert metric_counts["precision_at_k"] == 2
    assert selected_metric == "precision_at_k"


def test_build_pairwise_rows_skips_reference_and_sorts_by_improvement(tmp_path: Path) -> None:
    reference_row = _experiment_row(
        tmp_path,
        batch_id="batch_ref",
        created_at="2026-04-01T10:00:00Z",
        run_context_payload={"accepted_baseline_id": "baseline_001"},
    )
    better_row = _experiment_row(
        tmp_path,
        batch_id="batch_better",
        created_at="2026-04-01T11:00:00Z",
        theory_config="configs/theory_better.yaml",
        run_context_payload={"launch_profile_id": "launch_better"},
    )
    worse_row = _experiment_row(
        tmp_path,
        batch_id="batch_worse",
        created_at="2026-04-01T12:00:00Z",
        theory_config="configs/theory_worse.yaml",
    )

    rows = build_pairwise_analysis_rows(
        reference_row=reference_row,
        reference_seed_rows=_reference_seed_rows(),
        candidate_rows=[reference_row, better_row, worse_row],
        candidate_seed_rows_by_id={
            "batch_ref": _reference_seed_rows(),
            "batch_better": _better_seed_rows(),
            "batch_worse": _worse_seed_rows(),
        },
        selected_metric="ndcg_at_k",
    )
    sorted_rows = sort_pairwise_analysis_rows(rows)

    assert [row.candidate_batch_id for row in rows] == ["batch_better", "batch_worse"]
    assert [row.candidate_batch_id for row in sorted_rows] == ["batch_better", "batch_worse"]
    assert sorted_rows[0].improvement_delta_mean is not None
    assert sorted_rows[0].improvement_delta_mean > 0
    assert sorted_rows[0].guardrail_verdict == "pass"


def test_build_pairwise_row_keeps_unusable_candidate_honest(tmp_path: Path) -> None:
    reference_row = _experiment_row(tmp_path, batch_id="batch_ref", created_at="2026-04-01T10:00:00Z")
    candidate_row = _experiment_row(tmp_path, batch_id="batch_candidate", created_at="2026-04-01T11:00:00Z")

    rows = build_pairwise_analysis_rows(
        reference_row=reference_row,
        reference_seed_rows=_reference_seed_rows(),
        candidate_rows=[candidate_row],
        candidate_seed_rows_by_id={"batch_candidate": _missing_metric_seed_rows()},
        selected_metric="ndcg_at_k",
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.paired_seed_count == 0
    assert row.pairwise_status == "unusable"
    assert row.guardrail_verdict == "fail"
    assert "The selected metric is unavailable for paired comparison." in row.guardrail_reasons


def test_filter_find_and_detail_helpers_work_with_old_batches_without_run_context(tmp_path: Path) -> None:
    reference_row = _experiment_row(tmp_path, batch_id="batch_ref", created_at="2026-04-01T10:00:00Z")
    candidate_row = _experiment_row(
        tmp_path,
        batch_id="batch_candidate",
        created_at="2026-04-01T11:00:00Z",
        theory_config="configs/theory_candidate.yaml",
        run_context_payload=None,
    )

    rows = build_pairwise_analysis_rows(
        reference_row=reference_row,
        reference_seed_rows=_reference_seed_rows(),
        candidate_rows=[candidate_row],
        candidate_seed_rows_by_id={"batch_candidate": _better_seed_rows()},
        selected_metric="ndcg_at_k",
    )
    filtered = filter_pairwise_analysis_rows(rows, search_text="theory_candidate")
    found = find_pairwise_analysis_row(rows, "batch_candidate")
    detail = build_pairwise_analysis_detail(found)
    table_rows = build_pairwise_analysis_table_rows(filtered)

    assert filtered == rows
    assert found is not None
    assert detail["identity"]["reference_batch_id"] == "batch_ref"
    assert detail["candidate_run_context_summary"] is None
    assert table_rows[0]["candidate_batch_id"] == "batch_candidate"
    assert table_rows[0]["guardrail_verdict"] == "pass"
