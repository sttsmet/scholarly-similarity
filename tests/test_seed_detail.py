from __future__ import annotations

from src.ui.seed_detail import (
    build_seed_detail_sections,
    choose_default_seed_doi,
    find_seed_row_by_doi,
    select_seed_rows_for_subset,
)


def _seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "run_dir": "runs/seed_one",
            "experiment_id": "batch_005",
            "reused_existing_run": True,
            "precision_at_k": 0.9,
            "recall_at_k": 0.5,
            "metrics": {
                "precision_at_k": 0.9,
                "recall_at_k": 0.5,
                "dcg_at_k": 8.1,
                "mean_conf_by_label": {"0": 0.7},
            },
            "corpus_manifest_json": "runs/seed_one/manifest.json",
            "evaluation_summary_json": "runs/seed_one/evaluation_summary.json",
        },
        {
            "batch_index": 2,
            "doi": "10.1103/PhysRevA.64.052312",
            "status": "failed",
            "run_dir": "runs/seed_two",
            "experiment_id": "batch_005",
            "failed_stage": "build-local-corpus",
            "error_type": "OpenAlexNotFoundError",
            "error_message": "OpenAlex work not found",
            "metrics": None,
        },
        {
            "batch_index": 3,
            "doi": "10.1038/nphys1133",
            "status": "completed",
            "run_dir": "runs/seed_three",
            "experiment_id": "batch_005",
            "ndcg_at_k": 1.0,
            "metrics": {"ndcg_at_k": 1.0},
        },
    ]


def _worst_cases() -> dict[str, object]:
    return {
        "best_seeds": [
            {"batch_index": 3, "doi": "10.1038/nphys1133"},
            {"batch_index": 1, "doi": "10.1038/nphys1170"},
        ],
        "worst_seeds": [
            {"batch_index": 1, "doi": "10.1038/nphys1170"},
        ],
    }


def test_select_seed_rows_for_subset_handles_status_and_ranked_groups() -> None:
    rows = _seed_rows()

    completed_rows = select_seed_rows_for_subset(rows, subset="completed")
    failed_rows = select_seed_rows_for_subset(rows, subset="failed")
    best_rows = select_seed_rows_for_subset(rows, subset="best", worst_cases=_worst_cases())
    worst_rows = select_seed_rows_for_subset(rows, subset="worst", worst_cases=_worst_cases())

    assert [row["batch_index"] for row in completed_rows] == [1, 3]
    assert [row["batch_index"] for row in failed_rows] == [2]
    assert [row["batch_index"] for row in best_rows] == [3, 1]
    assert [row["batch_index"] for row in worst_rows] == [1]


def test_choose_default_seed_doi_prefers_current_selection_then_completed_seed() -> None:
    rows = _seed_rows()

    assert choose_default_seed_doi(rows, preferred_doi="10.1103/PhysRevA.64.052312") == "10.1103/PhysRevA.64.052312"
    assert choose_default_seed_doi(rows, preferred_batch_index=3) == "10.1038/nphys1133"
    assert choose_default_seed_doi(rows, preferred_doi="missing") == "10.1038/nphys1170"
    assert choose_default_seed_doi([rows[1]]) == "10.1103/PhysRevA.64.052312"


def test_find_seed_row_by_doi_returns_matching_row() -> None:
    row = find_seed_row_by_doi(_seed_rows(), "10.1038/nphys1133")

    assert row is not None
    assert row["batch_index"] == 3


def test_build_seed_detail_sections_groups_metrics_failure_and_artifacts() -> None:
    completed_sections = build_seed_detail_sections(_seed_rows()[0])
    failed_sections = build_seed_detail_sections(_seed_rows()[1])

    assert completed_sections.identity["doi"] == "10.1038/nphys1170"
    assert completed_sections.metrics["precision_at_k"] == 0.9
    assert completed_sections.metrics["dcg_at_k"] == 8.1
    assert completed_sections.extra_metrics == {"mean_conf_by_label": {"0": 0.7}}
    assert completed_sections.artifact_paths["corpus_manifest_json"] == "runs/seed_one/manifest.json"

    assert failed_sections.failure == {
        "failed_stage": "build-local-corpus",
        "error_type": "OpenAlexNotFoundError",
        "error_message": "OpenAlex work not found",
    }
    assert failed_sections.metrics == {}
    assert failed_sections.artifact_paths == {}
