from __future__ import annotations

from dataclasses import dataclass
from typing import Any


KNOWN_SEED_METRICS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
)
SEED_DETAIL_ARTIFACT_FIELDS = (
    "corpus_manifest_json",
    "silver_labels_csv",
    "experiment_dir",
    "experiment_manifest_json",
    "evaluation_summary_json",
    "evaluation_cases_json",
)
SEED_DETAIL_FAILURE_FIELDS = (
    "failed_stage",
    "error_type",
    "error_message",
)
SEED_DETAIL_SUBSETS = (
    "all",
    "completed",
    "failed",
    "best",
    "worst",
)


@dataclass(frozen=True, slots=True)
class SeedDetailSections:
    identity: dict[str, Any]
    metrics: dict[str, Any]
    extra_metrics: dict[str, Any]
    failure: dict[str, Any]
    artifact_paths: dict[str, str]


def select_seed_rows_for_subset(
    rows: list[dict[str, Any]],
    *,
    subset: str,
    worst_cases: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if subset == "completed":
        return [dict(row) for row in rows if _status_value(row) == "completed"]
    if subset == "failed":
        return [dict(row) for row in rows if _status_value(row) == "failed"]
    if subset == "best":
        return _ranked_subset_rows(rows, worst_cases, key="best_seeds")
    if subset == "worst":
        return _ranked_subset_rows(rows, worst_cases, key="worst_seeds")
    return [dict(row) for row in rows]


def choose_default_seed_doi(
    rows: list[dict[str, Any]],
    *,
    preferred_doi: str | None = None,
    preferred_batch_index: int | None = None,
) -> str | None:
    if preferred_doi:
        matched_row = find_seed_row_by_doi(rows, preferred_doi)
        if matched_row is not None:
            return str(matched_row.get("doi"))

    if preferred_batch_index is not None:
        for row in rows:
            if _batch_index_value(row) == preferred_batch_index:
                return _seed_doi(row)

    for row in rows:
        if _status_value(row) == "completed":
            return _seed_doi(row)

    return _seed_doi(rows[0]) if rows else None


def find_seed_row_by_doi(rows: list[dict[str, Any]], doi: str | None) -> dict[str, Any] | None:
    normalized_doi = _normalized_text(doi)
    if normalized_doi is None:
        return None
    for row in rows:
        if _normalized_text(row.get("doi")) == normalized_doi:
            return dict(row)
    return None


def build_seed_detail_sections(row: dict[str, Any]) -> SeedDetailSections:
    identity = {
        "doi": row.get("doi"),
        "status": row.get("status"),
        "batch_index": row.get("batch_index"),
        "run_dir": row.get("run_dir"),
        "experiment_id": row.get("experiment_id"),
        "reused_existing_run": row.get("reused_existing_run"),
    }

    metrics: dict[str, Any] = {
        metric_name: row.get(metric_name)
        for metric_name in KNOWN_SEED_METRICS
        if row.get(metric_name) is not None
    }
    extra_metrics: dict[str, Any] = {}
    raw_metrics = row.get("metrics")
    if isinstance(raw_metrics, dict):
        for metric_name, metric_value in raw_metrics.items():
            if metric_value is None:
                continue
            if metric_name not in metrics and _is_numeric(metric_value):
                metrics[metric_name] = metric_value
            elif metric_name not in metrics:
                extra_metrics[metric_name] = metric_value

    failure = {
        field_name: row.get(field_name)
        for field_name in SEED_DETAIL_FAILURE_FIELDS
        if row.get(field_name) not in (None, "")
    }
    artifact_paths = {
        field_name: str(row.get(field_name))
        for field_name in SEED_DETAIL_ARTIFACT_FIELDS
        if row.get(field_name) not in (None, "")
    }
    return SeedDetailSections(
        identity=identity,
        metrics=metrics,
        extra_metrics=extra_metrics,
        failure=failure,
        artifact_paths=artifact_paths,
    )


def _ranked_subset_rows(
    rows: list[dict[str, Any]],
    worst_cases: dict[str, Any] | None,
    *,
    key: str,
) -> list[dict[str, Any]]:
    ranked_rows = worst_cases.get(key, []) if isinstance(worst_cases, dict) else []
    if not isinstance(ranked_rows, list):
        return []
    row_map = {
        _batch_index_value(row): dict(row)
        for row in rows
    }
    selected_rows: list[dict[str, Any]] = []
    for ranked_row in ranked_rows:
        if not isinstance(ranked_row, dict):
            continue
        batch_index = _batch_index_value(ranked_row)
        row = row_map.get(batch_index)
        if row is not None:
            selected_rows.append(row)
    return selected_rows


def _batch_index_value(row: dict[str, Any]) -> int:
    try:
        return int(row.get("batch_index"))
    except (TypeError, ValueError):
        return -1


def _seed_doi(row: dict[str, Any]) -> str | None:
    doi = _normalized_text(row.get("doi"))
    return doi


def _normalized_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _status_value(row: dict[str, Any]) -> str:
    return str(row.get("status", "")).strip().lower()


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
