from __future__ import annotations

import math
from typing import Any


DIAGNOSTIC_METRICS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
)


def filter_diagnostic_rows(
    rows: list[dict[str, Any]],
    *,
    status_mode: str = "completed only",
) -> list[dict[str, Any]]:
    if status_mode == "all":
        return [dict(row) for row in rows]
    return [dict(row) for row in rows if str(row.get("status", "")).strip().lower() == "completed"]


def available_numeric_metrics(rows: list[dict[str, Any]]) -> list[str]:
    return [
        metric_name
        for metric_name in DIAGNOSTIC_METRICS
        if any(_numeric_value(row.get(metric_name)) is not None for row in rows)
    ]


def choose_default_primary_metric(available_metrics: list[str]) -> str | None:
    if not available_metrics:
        return None
    if "ndcg_at_k" in available_metrics:
        return "ndcg_at_k"
    return available_metrics[0]


def choose_default_scatter_metrics(available_metrics: list[str]) -> tuple[str | None, str | None]:
    if not available_metrics:
        return None, None

    x_metric = "ndcg_at_k" if "ndcg_at_k" in available_metrics else available_metrics[0]
    y_preferences = (
        "expected_calibration_error",
        "precision_at_k",
        "recall_at_k",
        "brier_score",
        "ndcg_at_k",
    )
    for metric_name in y_preferences:
        if metric_name in available_metrics and metric_name != x_metric:
            return x_metric, metric_name
    return x_metric, None


def choose_ranking_metric(
    aggregate_ranking_metric: str | None,
    available_metrics: list[str],
) -> str | None:
    if aggregate_ranking_metric in available_metrics:
        return aggregate_ranking_metric
    for metric_name in ("ndcg_at_k", "precision_at_k"):
        if metric_name in available_metrics:
            return metric_name
    return available_metrics[0] if available_metrics else None


def metric_values(rows: list[dict[str, Any]], metric_name: str) -> list[float]:
    return [
        numeric_value
        for numeric_value in (_numeric_value(row.get(metric_name)) for row in rows)
        if numeric_value is not None
    ]


def scatter_points(
    rows: list[dict[str, Any]],
    *,
    x_metric: str,
    y_metric: str,
) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for row in rows:
        x_value = _numeric_value(row.get(x_metric))
        y_value = _numeric_value(row.get(y_metric))
        if x_value is None or y_value is None:
            continue
        points.append(
            {
                "batch_index": row.get("batch_index"),
                "doi": row.get("doi"),
                "status": row.get("status"),
                "run_dir": row.get("run_dir"),
                "experiment_id": row.get("experiment_id"),
                x_metric: x_value,
                y_metric: y_value,
            }
        )
    return points


def best_and_worst_rows(
    rows: list[dict[str, Any]],
    *,
    ranking_metric: str,
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if limit <= 0:
        return [], []

    completed_rows = [
        dict(row)
        for row in rows
        if str(row.get("status", "")).strip().lower() == "completed"
        and _numeric_value(row.get(ranking_metric)) is not None
    ]
    best_rows = sorted(
        completed_rows,
        key=lambda row: (
            -float(row[ranking_metric]),
            _batch_index_value(row),
            str(row.get("doi") or ""),
        ),
    )[:limit]
    worst_rows = sorted(
        completed_rows,
        key=lambda row: (
            float(row[ranking_metric]),
            _batch_index_value(row),
            str(row.get("doi") or ""),
        ),
    )[:limit]
    return best_rows, worst_rows


def _batch_index_value(row: dict[str, Any]) -> int:
    try:
        return int(row.get("batch_index"))
    except (TypeError, ValueError):
        return 0


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        numeric_value = float(value)
        if math.isnan(numeric_value) or math.isinf(numeric_value):
            return None
        return numeric_value
    return None
