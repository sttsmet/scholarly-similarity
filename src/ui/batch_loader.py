from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from src.config import REPO_ROOT
from src.eval.benchmark import BatchAggregateEvalResult, SeedBatchManifest


REQUIRED_BATCH_FILENAMES = (
    "batch_manifest.json",
    "aggregate_summary.json",
    "seed_table.jsonl",
    "worst_cases.json",
)
KNOWN_METRIC_FIELDS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
)
OPTIONAL_SEED_FIELDS = (
    "run_id",
    "run_dir",
    "experiment_id",
    "theory_config",
    "seed_openalex_id",
    "evaluation_mode",
    "evidence_tier",
    "metric_scope",
    "benchmark_labels_path",
    "benchmark_labels_snapshot_path",
    "benchmark_dataset_id",
    "benchmark_labels_sha256",
    "benchmark_labels_row_count",
    "benchmark_schema_version",
    "candidate_count",
    "judged_count",
    "evaluation_summary_json",
    "evaluation_cases_json",
    "failed_stage",
    "error_type",
    "error_message",
    "started_at",
    "completed_at",
    "duration_seconds",
    "reused_existing_run",
    "corpus_manifest_json",
    "silver_labels_csv",
    "experiment_dir",
    "experiment_manifest_json",
)


class BatchLoadError(ValueError):
    """Raised when a batch directory cannot be loaded for UI display."""


@dataclass(frozen=True, slots=True)
class BatchUiBundle:
    """Normalized in-memory bundle used by the Streamlit reader UI."""

    batch_dir: Path
    manifest: SeedBatchManifest
    aggregate_summary: BatchAggregateEvalResult
    worst_cases: dict[str, Any]
    seed_table_rows: list[dict[str, Any]]
    seed_run_rows: list[dict[str, Any]]
    seed_rows_by_batch_index: dict[int, dict[str, Any]]
    seed_runs_by_batch_index: dict[int, dict[str, Any]]


def discover_batch_dirs(root: str | Path | None = None) -> list[Path]:
    """Return available local batch directories under the standard runs tree."""

    base_dir = Path(root) if root is not None else (REPO_ROOT / "runs" / "batches")
    if not base_dir.exists() or not base_dir.is_dir():
        return []
    return sorted((path for path in base_dir.iterdir() if path.is_dir()), key=lambda path: path.name)


def load_batch_bundle(batch_dir: str | Path) -> BatchUiBundle:
    """Load an existing batch directory as a read-only UI bundle."""

    resolved_batch_dir = _resolve_batch_dir(batch_dir)
    if not resolved_batch_dir.exists():
        raise BatchLoadError(f"Batch directory does not exist: {resolved_batch_dir}")
    if not resolved_batch_dir.is_dir():
        raise BatchLoadError(f"Batch path is not a directory: {resolved_batch_dir}")

    required_paths = {name: resolved_batch_dir / name for name in REQUIRED_BATCH_FILENAMES}
    missing_paths = [path.name for path in required_paths.values() if not path.exists()]
    if missing_paths:
        missing_list = ", ".join(missing_paths)
        raise BatchLoadError(f"Batch directory is missing required artifact file(s): {missing_list}")

    manifest = _load_manifest(required_paths["batch_manifest.json"])
    aggregate_summary = _load_aggregate_summary(required_paths["aggregate_summary.json"])
    worst_cases = _load_worst_cases(required_paths["worst_cases.json"])

    seed_run_path = resolved_batch_dir / "seed_runs.jsonl"
    seed_run_rows = _load_jsonl_records(seed_run_path) if seed_run_path.exists() else []
    seed_runs_by_batch_index = {
        _require_batch_index(row, source_name=seed_run_path.name): row
        for row in seed_run_rows
    }

    seed_table_rows = [
        _normalize_seed_row(
            row,
            seed_run_rows_by_index=seed_runs_by_batch_index,
            source_name=required_paths["seed_table.jsonl"].name,
        )
        for row in _load_jsonl_records(required_paths["seed_table.jsonl"])
    ]
    seed_rows_by_batch_index = {
        int(row["batch_index"]): row
        for row in seed_table_rows
    }

    return BatchUiBundle(
        batch_dir=resolved_batch_dir,
        manifest=manifest,
        aggregate_summary=aggregate_summary,
        worst_cases=worst_cases,
        seed_table_rows=seed_table_rows,
        seed_run_rows=seed_run_rows,
        seed_rows_by_batch_index=seed_rows_by_batch_index,
        seed_runs_by_batch_index=seed_runs_by_batch_index,
    )


def _resolve_batch_dir(batch_dir: str | Path) -> Path:
    raw_value = str(batch_dir).strip()
    if not raw_value:
        raise BatchLoadError("Batch directory path must not be empty.")
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def _load_manifest(path: Path) -> SeedBatchManifest:
    payload = _load_json_object(path)
    try:
        return SeedBatchManifest.model_validate(payload)
    except ValidationError as exc:
        raise BatchLoadError(f"Invalid {path.name}: {exc}") from exc


def _load_aggregate_summary(path: Path) -> BatchAggregateEvalResult:
    payload = _load_json_object(path)
    try:
        return BatchAggregateEvalResult.model_validate(payload)
    except ValidationError as exc:
        raise BatchLoadError(f"Invalid {path.name}: {exc}") from exc


def _load_worst_cases(path: Path) -> dict[str, Any]:
    payload = _load_json_object(path)
    for key in ("best_seeds", "worst_seeds", "failed_seeds"):
        value = payload.get(key)
        if not isinstance(value, list):
            raise BatchLoadError(f"Invalid {path.name}: '{key}' must be a JSON array.")
    return payload


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise BatchLoadError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise BatchLoadError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise BatchLoadError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise BatchLoadError(f"Could not read {path.name}: {exc}") from exc

    records: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(raw_lines, start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BatchLoadError(
                f"Malformed JSONL in {path.name} at line {line_number}: {exc.msg}"
            ) from exc
        if not isinstance(payload, dict):
            raise BatchLoadError(f"Invalid {path.name} line {line_number}: expected a JSON object.")
        records.append(payload)
    return records


def _normalize_seed_row(
    row: dict[str, Any],
    *,
    seed_run_rows_by_index: dict[int, dict[str, Any]],
    source_name: str,
) -> dict[str, Any]:
    batch_index = _require_batch_index(row, source_name=source_name)
    normalized = dict(row)
    normalized["batch_index"] = batch_index
    normalized["doi"] = _optional_str(row.get("doi")) or f"seed_{batch_index}"
    normalized["status"] = _optional_str(row.get("status")) or "unknown"

    for metric_name in KNOWN_METRIC_FIELDS:
        normalized.setdefault(metric_name, None)
    for field_name in OPTIONAL_SEED_FIELDS:
        normalized.setdefault(field_name, None)

    seed_run_row = seed_run_rows_by_index.get(batch_index)
    if seed_run_row is not None:
        for field_name in OPTIONAL_SEED_FIELDS:
            if normalized.get(field_name) in (None, ""):
                normalized[field_name] = seed_run_row.get(field_name)
        if normalized.get("metrics") is None:
            normalized["metrics"] = seed_run_row.get("metrics")

    metrics = normalized.get("metrics")
    if metrics is None:
        metrics = {
            metric_name: normalized.get(metric_name)
            for metric_name in KNOWN_METRIC_FIELDS
            if normalized.get(metric_name) is not None
        }
    normalized["metrics"] = metrics
    return normalized


def _require_batch_index(row: dict[str, Any], *, source_name: str) -> int:
    if "batch_index" not in row:
        raise BatchLoadError(f"Invalid {source_name}: row is missing 'batch_index'.")
    try:
        return int(row["batch_index"])
    except (TypeError, ValueError) as exc:
        raise BatchLoadError(f"Invalid {source_name}: 'batch_index' must be an integer.") from exc


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
