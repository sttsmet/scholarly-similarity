from __future__ import annotations

from src.ui.diagnostics import (
    available_numeric_metrics,
    best_and_worst_rows,
    choose_default_primary_metric,
    choose_default_scatter_metrics,
    choose_ranking_metric,
    filter_diagnostic_rows,
    scatter_points,
)


def _seed_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "ndcg_at_k": 0.95,
            "precision_at_k": 0.9,
            "expected_calibration_error": 0.05,
            "run_dir": "runs/seed_one",
            "experiment_id": "batch_005",
        },
        {
            "batch_index": 2,
            "doi": "10.1038/nphys1133",
            "status": "completed",
            "ndcg_at_k": 0.75,
            "precision_at_k": 0.8,
            "expected_calibration_error": None,
            "run_dir": "runs/seed_two",
            "experiment_id": "batch_005",
        },
        {
            "batch_index": 3,
            "doi": "10.1103/PhysRevA.64.052312",
            "status": "failed",
            "ndcg_at_k": None,
            "precision_at_k": None,
            "expected_calibration_error": None,
            "run_dir": "runs/seed_three",
            "experiment_id": "batch_005",
        },
    ]


def test_filter_diagnostic_rows_defaults_to_completed_only() -> None:
    filtered_rows = filter_diagnostic_rows(_seed_rows())

    assert [row["batch_index"] for row in filtered_rows] == [1, 2]


def test_available_numeric_metrics_ignores_missing_values() -> None:
    metrics = available_numeric_metrics(_seed_rows())

    assert metrics == ["precision_at_k", "ndcg_at_k", "expected_calibration_error"]


def test_choose_default_metric_preferences() -> None:
    available_metrics = ["precision_at_k", "ndcg_at_k", "expected_calibration_error"]

    assert choose_default_primary_metric(available_metrics) == "ndcg_at_k"
    assert choose_default_scatter_metrics(available_metrics) == ("ndcg_at_k", "expected_calibration_error")


def test_choose_ranking_metric_falls_back_when_aggregate_metric_missing() -> None:
    available_metrics = ["precision_at_k", "recall_at_k"]

    assert choose_ranking_metric("brier_score", available_metrics) == "precision_at_k"
    assert choose_ranking_metric(None, []) is None


def test_best_and_worst_rows_exclude_failed_and_missing_values() -> None:
    best_rows, worst_rows = best_and_worst_rows(
        _seed_rows(),
        ranking_metric="ndcg_at_k",
        limit=1,
    )

    assert [row["batch_index"] for row in best_rows] == [1]
    assert [row["batch_index"] for row in worst_rows] == [2]


def test_scatter_points_require_both_metrics() -> None:
    points = scatter_points(
        _seed_rows(),
        x_metric="ndcg_at_k",
        y_metric="expected_calibration_error",
    )

    assert len(points) == 1
    assert points[0]["batch_index"] == 1
