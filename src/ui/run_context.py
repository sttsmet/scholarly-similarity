from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT


RUN_CONTEXT_FILENAME = "run_context.json"


class RunContextError(ValueError):
    """Raised when a UI run-context sidecar cannot be built or read safely."""


def build_run_context_payload_from_request(
    request: Any,
    *,
    launch_source_type: str,
    accepted_baseline: dict[str, Any] | None = None,
    benchmark_preset: dict[str, Any] | None = None,
    evaluation_preset: dict[str, Any] | None = None,
    launch_profile: dict[str, Any] | None = None,
    candidate_lineage: dict[str, Any] | None = None,
    study_source: dict[str, Any] | None = None,
    reviewer: str | None = None,
    notes: str | None = None,
    batch_status: str | None = None,
    error_message: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    batch_id = _optional_str(getattr(request, "batch_id", None))
    if batch_id is None:
        raise RunContextError("Run context requires a batch_id.")

    normalized_launch_source = _optional_str(launch_source_type)
    if normalized_launch_source is None:
        raise RunContextError("Run context requires a launch_source_type.")

    payload: dict[str, Any] = {
        "batch_id": batch_id,
        "created_at": created_at or _utc_timestamp(),
        "launch_source_type": normalized_launch_source,
        "initial_doi_context": _optional_str(getattr(request, "initial_doi_context", None)),
        "theory_config_path": _serialize_path(getattr(request, "theory_config_path", None)),
        "seeds_csv": _serialize_path(getattr(request, "seeds_csv_path", None)),
        "max_references": getattr(request, "max_references", None),
        "max_related": getattr(request, "max_related", None),
        "max_hard_negatives": getattr(request, "max_hard_negatives", None),
        "top_k": getattr(request, "top_k", None),
        "label_source": _optional_str(getattr(request, "label_source", None)),
        "evaluation_mode": _optional_str(getattr(request, "evaluation_mode", None)),
        "metric_scope": _optional_str(getattr(request, "metric_scope", None)),
        "benchmark_labels_path": _serialize_path(getattr(request, "benchmark_labels_path", None)),
        "benchmark_dataset_id": _optional_str(getattr(request, "benchmark_dataset_id", None)),
        "benchmark_labels_sha256": _optional_str(getattr(request, "benchmark_labels_sha256", None)),
        "refresh": bool(getattr(request, "refresh", False)),
        "accepted_baseline_id": None,
        "accepted_baseline_dir": None,
        "accepted_theory_snapshot": None,
        "benchmark_preset_id": None,
        "benchmark_preset_path": None,
        "eval_preset_id": None,
        "eval_preset_path": None,
        "launch_profile_id": None,
        "launch_profile_path": None,
        "source_type": None,
        "source_curation_id": None,
        "source_curation_dir": None,
        "source_study_id": None,
        "source_study_dir": None,
        "source_reference_batch_id": None,
        "source_candidate_batch_id": None,
        "source_candidate_decision": None,
        "source_suggested_decision": None,
        "source_selected_metric": None,
        "candidate_id": None,
        "packet_id": None,
        "comparison_id": None,
        "candidate_run_dir": None,
        "reviewer": _optional_str(reviewer),
        "notes": _optional_str(notes),
        "batch_status": _optional_str(batch_status),
        "error_message": _optional_str(error_message),
    }

    _merge_fields(
        payload,
        accepted_baseline,
        (
            "accepted_baseline_id",
            "accepted_baseline_dir",
            "accepted_theory_snapshot",
        ),
    )
    _merge_fields(
        payload,
        benchmark_preset,
        (
            "benchmark_preset_id",
            "benchmark_preset_path",
            "source_type",
            "source_curation_id",
            "source_curation_dir",
        ),
    )
    _merge_fields(
        payload,
        evaluation_preset,
        (
            "eval_preset_id",
            "eval_preset_path",
        ),
    )
    _merge_fields(
        payload,
        launch_profile,
        (
            "launch_profile_id",
            "launch_profile_path",
        ),
    )
    _merge_fields(
        payload,
        candidate_lineage,
        (
            "candidate_id",
            "packet_id",
            "comparison_id",
            "candidate_run_dir",
        ),
    )
    _merge_fields(
        payload,
        study_source,
        (
            "source_type",
            "source_study_id",
            "source_study_dir",
            "source_reference_batch_id",
            "source_candidate_batch_id",
            "source_candidate_decision",
            "source_suggested_decision",
            "source_selected_metric",
        ),
    )
    return payload


def write_run_context(batch_dir: str | Path, payload: dict[str, Any]) -> Path:
    resolved_dir = _resolve_existing_dir(batch_dir)
    path = resolved_dir / RUN_CONTEXT_FILENAME
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_run_context_if_present(batch_dir: str | Path) -> tuple[dict[str, Any] | None, str | None]:
    text = _optional_str(batch_dir)
    if text is None:
        return None, None
    resolved_dir = Path(text).expanduser()
    if not resolved_dir.is_absolute():
        resolved_dir = (REPO_ROOT / resolved_dir).resolve()
    path = resolved_dir / RUN_CONTEXT_FILENAME
    if not path.exists():
        return None, None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        return None, f"Could not read run_context.json: {exc}"
    except json.JSONDecodeError as exc:
        return None, f"Malformed run_context.json at line {exc.lineno}, column {exc.colno}: {exc.msg}"
    if not isinstance(payload, dict):
        return None, "Invalid run_context.json: expected a JSON object."
    return payload, None


def build_run_context_summary(payload: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "launch_source_type",
        "accepted_baseline_id",
        "benchmark_preset_id",
        "eval_preset_id",
        "launch_profile_id",
        "source_type",
        "source_curation_id",
        "source_curation_dir",
        "source_study_id",
        "source_study_dir",
        "source_reference_batch_id",
        "source_candidate_batch_id",
        "source_candidate_decision",
        "source_suggested_decision",
        "source_selected_metric",
        "candidate_id",
        "packet_id",
        "comparison_id",
        "candidate_run_dir",
    )
    return {
        key: _serialize_path(value) if key.endswith("_dir") or key.endswith("_path") else value
        for key in keys
        if (value := payload.get(key)) not in (None, "")
    }


def _merge_fields(
    payload: dict[str, Any],
    source: dict[str, Any] | None,
    keys: tuple[str, ...],
) -> None:
    if not isinstance(source, dict):
        return
    for key in keys:
        value = source.get(key)
        if key.endswith("_dir") or key.endswith("_path") or key in {
            "accepted_theory_snapshot",
        }:
            payload[key] = _serialize_path(value)
        else:
            payload[key] = _optional_str(value) if isinstance(value, str) else value


def _resolve_existing_dir(value: str | Path) -> Path:
    text = _optional_str(value)
    if text is None:
        raise RunContextError("Batch directory is required for run_context.json.")
    candidate = Path(text.replace("\\", "/")).expanduser()
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    if not candidate.exists():
        raise RunContextError(f"Batch directory does not exist: {candidate}")
    if not candidate.is_dir():
        raise RunContextError(f"Batch directory is not a directory: {candidate}")
    return candidate


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: Any) -> str | None:
    text = _optional_str(value)
    if text is None:
        return None
    path = Path(text.replace("\\", "/")).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
