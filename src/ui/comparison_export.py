from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.comparison import (
    ComparisonMetricSummary,
    DIAGNOSTIC_METRIC_LABELS,
    comparison_metric_summary_payload,
)


DECISION_STATUS_OPTIONS = (
    "accept_candidate",
    "reject_candidate",
    "needs_review",
)


class ComparisonExportError(ValueError):
    """Raised when a comparison export request cannot be completed."""


@dataclass(frozen=True, slots=True)
class ComparisonSaveRequest:
    comparison_id: str
    reviewer: str | None
    decision_status: str
    notes: str | None


@dataclass(frozen=True, slots=True)
class ComparisonSaveResult:
    comparison_id: str
    comparison_dir: Path
    manifest_path: Path
    paired_seed_table_path: Path
    decision_record_path: Path


def build_comparison_save_request(
    *,
    comparison_id: str,
    reviewer: str,
    decision_status: str,
    notes: str,
) -> ComparisonSaveRequest:
    normalized_comparison_id = comparison_id.strip()
    if not normalized_comparison_id:
        raise ComparisonExportError("Comparison ID is required.")
    if normalized_comparison_id in {".", ".."} or Path(normalized_comparison_id).name != normalized_comparison_id:
        raise ComparisonExportError("Comparison ID must be a single directory name.")
    if decision_status not in DECISION_STATUS_OPTIONS:
        raise ComparisonExportError("Decision status is invalid.")

    normalized_reviewer = reviewer.strip() or None
    normalized_notes = notes.strip() or None
    return ComparisonSaveRequest(
        comparison_id=normalized_comparison_id,
        reviewer=normalized_reviewer,
        decision_status=decision_status,
        notes=normalized_notes,
    )


def save_comparison_artifacts(
    *,
    base_dir: str | Path,
    request: ComparisonSaveRequest,
    primary_bundle: Any,
    secondary_bundle: Any,
    selected_metric: str,
    status_mode: str,
    common_doi_count: int,
    common_completed_seed_count: int,
    compatibility_warning_list: list[str],
    paired_rows: list[dict[str, Any]],
    summary: ComparisonMetricSummary,
    study_source_context: dict[str, Any] | None = None,
) -> ComparisonSaveResult:
    if primary_bundle is None:
        raise ComparisonExportError("A primary batch must be loaded before saving a comparison.")
    if secondary_bundle is None:
        raise ComparisonExportError("A secondary batch must be loaded before saving a comparison.")
    if not str(selected_metric).strip():
        raise ComparisonExportError("A comparison metric must be selected before saving.")
    if common_doi_count <= 0:
        raise ComparisonExportError("At least one overlapping DOI is required before saving.")
    if not paired_rows:
        raise ComparisonExportError("No paired comparison rows are available for the current selection.")

    comparison_root = Path(base_dir)
    comparison_root.mkdir(parents=True, exist_ok=True)
    comparison_dir = comparison_root / request.comparison_id
    if comparison_dir.exists():
        raise ComparisonExportError(f"Comparison directory already exists: {comparison_dir}")
    comparison_dir.mkdir(parents=False, exist_ok=False)

    created_at = _utc_timestamp()
    manifest_path = comparison_dir / "comparison_manifest.json"
    paired_seed_table_path = comparison_dir / "paired_seed_table.jsonl"
    decision_record_path = comparison_dir / "decision_record.json"
    output_paths = {
        "comparison_manifest_json": _serialize_path(manifest_path),
        "paired_seed_table_jsonl": _serialize_path(paired_seed_table_path),
        "decision_record_json": _serialize_path(decision_record_path),
    }

    serialized_rows = serialize_paired_seed_rows(paired_rows)
    manifest_payload = build_comparison_manifest_payload(
        comparison_id=request.comparison_id,
        comparison_dir=comparison_dir,
        created_at=created_at,
        reviewer=request.reviewer,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric=selected_metric,
        status_mode=status_mode,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        compatibility_warning_list=compatibility_warning_list,
        summary=summary,
        output_paths=output_paths,
        paired_seed_count=len(serialized_rows),
        study_source_context=study_source_context,
    )
    decision_payload = build_decision_record_payload(
        comparison_id=request.comparison_id,
        created_at=created_at,
        reviewer=request.reviewer,
        decision_status=request.decision_status,
        notes=request.notes,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric=selected_metric,
        status_mode=status_mode,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        summary=summary,
        paired_seed_count=len(serialized_rows),
        study_source_context=study_source_context,
    )

    _write_json(manifest_path, manifest_payload)
    _write_jsonl(paired_seed_table_path, serialized_rows)
    _write_json(decision_record_path, decision_payload)
    return ComparisonSaveResult(
        comparison_id=request.comparison_id,
        comparison_dir=comparison_dir,
        manifest_path=manifest_path,
        paired_seed_table_path=paired_seed_table_path,
        decision_record_path=decision_record_path,
    )


def build_comparison_manifest_payload(
    *,
    comparison_id: str,
    comparison_dir: str | Path,
    created_at: str,
    reviewer: str | None,
    primary_bundle: Any,
    secondary_bundle: Any,
    selected_metric: str,
    status_mode: str,
    common_doi_count: int,
    common_completed_seed_count: int,
    compatibility_warning_list: list[str],
    summary: ComparisonMetricSummary,
    output_paths: dict[str, str],
    paired_seed_count: int,
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_metadata = _comparison_evidence_metadata(primary_bundle, secondary_bundle)
    payload = {
        "comparison_id": comparison_id,
        "comparison_dir": _serialize_path(comparison_dir),
        "created_at": created_at,
        "reviewer": reviewer,
        "primary_batch": dict(evidence_metadata["primary_batch"]),
        "secondary_batch": dict(evidence_metadata["secondary_batch"]),
        "selected_comparison_metric": selected_metric,
        "selected_comparison_metric_display_name": _comparison_metric_display_name(selected_metric),
        "selected_comparison_metric_category": _comparison_metric_category(selected_metric),
        "comparison_status_mode": status_mode,
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_seed_count,
        "paired_seed_count": paired_seed_count,
        "compatibility_warnings": list(compatibility_warning_list),
        "comparison_eligible": evidence_metadata["comparison_eligible"],
        "comparison_eligibility_reasons": list(evidence_metadata["comparison_eligibility_reasons"]),
        "evaluation_mode": evidence_metadata["evaluation_mode"],
        "evidence_tier": evidence_metadata["evidence_tier"],
        "metric_scope": evidence_metadata["metric_scope"],
        "benchmark_dataset_id": evidence_metadata["benchmark_dataset_id"],
        "benchmark_labels_sha256": evidence_metadata["benchmark_labels_sha256"],
        "benchmark_maturity_tier": evidence_metadata["benchmark_maturity_tier"],
        "promotion_ready": evidence_metadata["promotion_ready"],
        "promotion_ineligibility_reasons": list(
            evidence_metadata["promotion_ineligibility_reasons"]
        ),
        "selected_metric_summary": comparison_metric_summary_payload(summary),
        "movement_diagnostics": comparison_metric_summary_payload(summary).get("movement_diagnostics"),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
        "output_paths": dict(output_paths),
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload.update(dict(study_source_context))
    return payload


def build_decision_record_payload(
    *,
    comparison_id: str,
    created_at: str,
    reviewer: str | None,
    decision_status: str,
    notes: str | None,
    primary_bundle: Any,
    secondary_bundle: Any,
    selected_metric: str,
    status_mode: str,
    common_doi_count: int,
    common_completed_seed_count: int,
    summary: ComparisonMetricSummary,
    paired_seed_count: int,
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_metadata = _comparison_evidence_metadata(primary_bundle, secondary_bundle)
    payload = {
        "comparison_id": comparison_id,
        "created_at": created_at,
        "reviewer": reviewer,
        "decision_status": decision_status,
        "notes": notes,
        "selected_comparison_metric": selected_metric,
        "selected_comparison_metric_display_name": _comparison_metric_display_name(selected_metric),
        "selected_comparison_metric_category": _comparison_metric_category(selected_metric),
        "comparison_status_mode": status_mode,
        "primary_batch_id": getattr(primary_bundle.manifest, "batch_id", None),
        "secondary_batch_id": getattr(secondary_bundle.manifest, "batch_id", None),
        "primary_theory_config": getattr(primary_bundle.manifest, "theory_config", None),
        "secondary_theory_config": getattr(secondary_bundle.manifest, "theory_config", None),
        "evaluation_mode": evidence_metadata["evaluation_mode"],
        "evidence_tier": evidence_metadata["evidence_tier"],
        "metric_scope": evidence_metadata["metric_scope"],
        "benchmark_dataset_id": evidence_metadata["benchmark_dataset_id"],
        "benchmark_labels_sha256": evidence_metadata["benchmark_labels_sha256"],
        "benchmark_maturity_tier": evidence_metadata["benchmark_maturity_tier"],
        "promotion_ready": evidence_metadata["promotion_ready"],
        "promotion_ineligibility_reasons": list(
            evidence_metadata["promotion_ineligibility_reasons"]
        ),
        "comparison_eligible": evidence_metadata["comparison_eligible"],
        "comparison_eligibility_reasons": list(evidence_metadata["comparison_eligibility_reasons"]),
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_seed_count,
        "paired_seed_count": paired_seed_count,
        "wins": summary.wins,
        "losses": summary.losses,
        "ties": summary.ties,
        "selected_metric_summary": comparison_metric_summary_payload(summary),
        "movement_diagnostics": comparison_metric_summary_payload(summary).get("movement_diagnostics"),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload.update(dict(study_source_context))
    return payload


def serialize_paired_seed_rows(paired_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized_rows: list[dict[str, Any]] = []
    for row in paired_rows:
        serialized_row: dict[str, Any] = {}
        for key, value in row.items():
            if key.endswith("_run_dir"):
                serialized_row[key] = _serialize_path(value)
            else:
                serialized_row[key] = value
        serialized_rows.append(serialized_row)
    return serialized_rows

def _comparison_evidence_metadata(primary_bundle: Any, secondary_bundle: Any) -> dict[str, Any]:
    primary_batch = _batch_evidence_payload(primary_bundle)
    secondary_batch = _batch_evidence_payload(secondary_bundle)
    reasons: list[str] = []

    primary_mode = _optional_str(primary_batch.get("evaluation_mode"))
    secondary_mode = _optional_str(secondary_batch.get("evaluation_mode"))
    if primary_mode is not None and secondary_mode is not None and primary_mode != secondary_mode:
        reasons.append("primary and secondary batches use different evaluation_mode values")

    primary_dataset_id = _optional_str(primary_batch.get("benchmark_dataset_id"))
    secondary_dataset_id = _optional_str(secondary_batch.get("benchmark_dataset_id"))
    if primary_dataset_id != secondary_dataset_id:
        reasons.append("primary and secondary batches use different benchmark_dataset_id values")

    primary_labels_sha256 = _optional_str(primary_batch.get("benchmark_labels_sha256"))
    secondary_labels_sha256 = _optional_str(secondary_batch.get("benchmark_labels_sha256"))
    if primary_labels_sha256 != secondary_labels_sha256:
        reasons.append("primary and secondary batches use different benchmark_labels_sha256 values")

    primary_maturity_tier = _optional_str(primary_batch.get("benchmark_maturity_tier"))
    secondary_maturity_tier = _optional_str(secondary_batch.get("benchmark_maturity_tier"))
    if primary_maturity_tier != secondary_maturity_tier:
        reasons.append("primary and secondary batches use different benchmark_maturity_tier values")

    primary_promotion_ready = _optional_bool(primary_batch.get("promotion_ready"))
    secondary_promotion_ready = _optional_bool(secondary_batch.get("promotion_ready"))
    if primary_promotion_ready != secondary_promotion_ready:
        reasons.append("primary and secondary batches use different promotion_ready values")

    return {
        "primary_batch": primary_batch,
        "secondary_batch": secondary_batch,
        "evaluation_mode": _shared_value(primary_mode, secondary_mode),
        "evidence_tier": _shared_value(
            _optional_str(primary_batch.get("evidence_tier")),
            _optional_str(secondary_batch.get("evidence_tier")),
        ),
        "metric_scope": _shared_value(
            _optional_str(primary_batch.get("metric_scope")),
            _optional_str(secondary_batch.get("metric_scope")),
        ),
        "benchmark_dataset_id": _shared_value(primary_dataset_id, secondary_dataset_id),
        "benchmark_labels_sha256": _shared_value(primary_labels_sha256, secondary_labels_sha256),
        "benchmark_maturity_tier": _shared_value(primary_maturity_tier, secondary_maturity_tier),
        "promotion_ready": _shared_bool(primary_promotion_ready, secondary_promotion_ready),
        "promotion_ineligibility_reasons": list(
            dict.fromkeys(
                _normalize_reason_list(primary_batch.get("promotion_ineligibility_reasons"))
                + _normalize_reason_list(secondary_batch.get("promotion_ineligibility_reasons"))
            )
        ),
        "comparison_eligible": not reasons,
        "comparison_eligibility_reasons": reasons,
    }


def _batch_evidence_payload(bundle: Any) -> dict[str, Any]:
    manifest = getattr(bundle, "manifest", None)
    options = getattr(manifest, "options", None)
    evaluation_mode = _optional_str(getattr(options, "evaluation_mode", None))
    return {
        "batch_id": getattr(manifest, "batch_id", None),
        "batch_dir": _serialize_path(getattr(bundle, "batch_dir", None)),
        "theory_config": getattr(manifest, "theory_config", None),
        "evaluation_mode": evaluation_mode,
        "evidence_tier": _optional_str(getattr(options, "evidence_tier", None)) or evaluation_mode,
        "metric_scope": getattr(options, "metric_scope", None),
        "benchmark_dataset_id": getattr(options, "benchmark_dataset_id", None),
        "benchmark_labels_sha256": getattr(options, "benchmark_labels_sha256", None),
        "benchmark_maturity_tier": getattr(options, "benchmark_maturity_tier", None),
        "promotion_ready": getattr(options, "promotion_ready", None),
        "promotion_ineligibility_reasons": list(
            getattr(options, "promotion_ineligibility_reasons", []) or []
        ),
        "benchmark_labels_path": _serialize_path(getattr(options, "benchmark_labels_path", None)),
        "benchmark_labels_snapshot_path": _serialize_path(
            getattr(options, "benchmark_labels_snapshot_path", None)
        ),
    }


def _comparison_metric_display_name(metric_name: str) -> str:
    return DIAGNOSTIC_METRIC_LABELS.get(metric_name, metric_name)


def _comparison_metric_category(metric_name: str) -> str:
    return "confidence_diagnostic" if metric_name in DIAGNOSTIC_METRIC_LABELS else "headline_ranking"


def _shared_value(primary_value: str | None, secondary_value: str | None) -> str | None:
    if primary_value is None or secondary_value is None:
        return None
    if primary_value != secondary_value:
        return None
    return primary_value


def _shared_bool(primary_value: bool | None, secondary_value: bool | None) -> bool | None:
    if primary_value is None or secondary_value is None:
        return None
    if primary_value != secondary_value:
        return None
    return primary_value


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _normalize_reason_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(
        dict.fromkeys(
            reason
            for reason in (_optional_str(item) for item in value)
            if reason is not None
        )
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    contents = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    path.write_text((contents + "\n") if contents else "", encoding="utf-8")


def _serialize_path(value: Any) -> str | None:
    if value in (None, ""):
        return None
    path = Path(str(value))
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
