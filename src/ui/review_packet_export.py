from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any

import yaml

from src.config import REPO_ROOT
from src.ui.comparison import (
    ComparisonMetricSummary,
    MOVEMENT_ROW_FIELDS,
    comparison_metric_summary_payload,
    movement_diagnostics_payload,
)
from src.ui.study_provenance import (
    build_evidence_summary_study_source_block,
    build_review_packet_study_source_fields,
    load_saved_study_source_from_comparison_dir,
)


ALLOWED_REVISION_PREFIXES = (
    "sim_weights",
    "confidence_factors",
    "confidence_parameters",
    "sim_parameters",
    "explanation",
)


class ReviewPacketExportError(ValueError):
    """Raised when review packet export cannot be completed."""


@dataclass(frozen=True, slots=True)
class ReviewPacketExportRequest:
    packet_id: str
    comparison_id: str
    reviewer: str | None
    selected_metric: str
    max_regressions: int
    max_improvements: int


@dataclass(frozen=True, slots=True)
class ReviewPacketExportResult:
    packet_id: str
    comparison_id: str
    packet_dir: Path
    manifest_path: Path
    evidence_summary_path: Path
    regressions_path: Path
    improvements_path: Path
    allowed_revision_paths_path: Path
    baseline_snapshot_path: Path
    candidate_template_path: Path


def build_review_packet_export_request(
    *,
    packet_id: str,
    comparison_id: str,
    reviewer: str,
    selected_metric: str,
    max_regressions: int,
    max_improvements: int,
) -> ReviewPacketExportRequest:
    normalized_packet_id = _normalize_directory_name(packet_id, label="Packet ID")
    normalized_comparison_id = _normalize_directory_name(comparison_id, label="Comparison ID")
    normalized_metric = selected_metric.strip()
    if not normalized_metric:
        raise ReviewPacketExportError("Selected metric is required.")

    regressions_value = int(max_regressions)
    improvements_value = int(max_improvements)
    if regressions_value <= 0:
        raise ReviewPacketExportError("Max regressions must be a positive integer.")
    if improvements_value <= 0:
        raise ReviewPacketExportError("Max improvements must be a positive integer.")

    normalized_reviewer = reviewer.strip() or None
    return ReviewPacketExportRequest(
        packet_id=normalized_packet_id,
        comparison_id=normalized_comparison_id,
        reviewer=normalized_reviewer,
        selected_metric=normalized_metric,
        max_regressions=regressions_value,
        max_improvements=improvements_value,
    )


def flatten_allowed_scalar_paths(
    theory_payload: dict[str, Any],
    *,
    allowed_prefixes: tuple[str, ...] = ALLOWED_REVISION_PREFIXES,
) -> list[str]:
    flattened_paths: list[str] = []
    for prefix in allowed_prefixes:
        prefix_value = theory_payload.get(prefix)
        if isinstance(prefix_value, dict):
            _flatten_scalar_children(prefix, prefix_value, flattened_paths)
    return sorted(flattened_paths)


def select_top_packet_rows(
    paired_rows: list[dict[str, Any]],
    *,
    max_regressions: int,
    max_improvements: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sorted_rows = sorted(
        paired_rows,
        key=lambda row: (float(row.get("improvement_delta", 0.0)), str(row.get("doi", ""))),
    )
    regressions = sorted_rows[:max_regressions]
    improvements = list(
        reversed(
            sorted(
                paired_rows,
                key=lambda row: (float(row.get("improvement_delta", 0.0)), str(row.get("doi", ""))),
            )[-max_improvements:]
        )
    )
    return regressions, improvements


def build_review_packet_manifest_payload(
    *,
    packet_id: str,
    packet_dir: str | Path,
    created_at: str,
    comparison_id: str,
    reviewer: str | None,
    primary_bundle: Any,
    secondary_bundle: Any,
    selected_metric: str,
    common_doi_count: int,
    common_completed_seed_count: int,
    summary: ComparisonMetricSummary,
    output_paths: dict[str, str | None],
    source_comparison_paths: dict[str, str | None],
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_metadata = _comparison_evidence_metadata(primary_bundle, secondary_bundle)
    payload = {
        "packet_id": packet_id,
        "packet_dir": _serialize_path(packet_dir),
        "created_at": created_at,
        "comparison_id": comparison_id,
        "reviewer": reviewer,
        "primary_batch": dict(evidence_metadata["primary_batch"]),
        "secondary_batch": dict(evidence_metadata["secondary_batch"]),
        "selected_packet_metric": selected_metric,
        "evaluation_mode": evidence_metadata["evaluation_mode"],
        "evidence_tier": evidence_metadata["evidence_tier"],
        "metric_scope": evidence_metadata["metric_scope"],
        "benchmark_dataset_id": evidence_metadata["benchmark_dataset_id"],
        "benchmark_labels_sha256": evidence_metadata["benchmark_labels_sha256"],
        "benchmark_maturity_tier": evidence_metadata["benchmark_maturity_tier"],
        "promotion_ready": evidence_metadata["promotion_ready"],
        "promotion_eligible": evidence_metadata["promotion_eligible"],
        "promotion_eligibility_reasons": list(evidence_metadata["promotion_eligibility_reasons"]),
        "promotion_ineligibility_reasons": list(
            evidence_metadata["promotion_eligibility_reasons"]
        ),
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_seed_count,
        "wins": summary.wins,
        "losses": summary.losses,
        "ties": summary.ties,
        "movement_diagnostics": movement_diagnostics_payload(summary.movement_diagnostics),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
        "output_paths": dict(output_paths),
        "source_comparison_paths": dict(source_comparison_paths),
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload.update(dict(study_source_context))
    return payload


def build_evidence_summary_payload(
    *,
    selected_metric: str,
    compatibility_warning_list: list[str],
    summary: ComparisonMetricSummary,
    regressions: list[dict[str, Any]],
    improvements: list[dict[str, Any]],
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evidence_metadata = _comparison_evidence_metadata_from_rows(
        compatibility_warning_list=compatibility_warning_list,
        regressions=regressions,
        improvements=improvements,
    )
    payload = {
        "selected_metric": selected_metric,
        "compatibility_warnings": list(compatibility_warning_list),
        "evaluation_mode": evidence_metadata["evaluation_mode"],
        "evidence_tier": evidence_metadata["evidence_tier"],
        "metric_scope": evidence_metadata["metric_scope"],
        "benchmark_dataset_id": evidence_metadata["benchmark_dataset_id"],
        "benchmark_labels_sha256": evidence_metadata["benchmark_labels_sha256"],
        "benchmark_maturity_tier": evidence_metadata["benchmark_maturity_tier"],
        "promotion_ready": evidence_metadata["promotion_ready"],
        "promotion_eligible": evidence_metadata["promotion_eligible"],
        "promotion_eligibility_reasons": list(evidence_metadata["promotion_eligibility_reasons"]),
        "promotion_ineligibility_reasons": list(
            evidence_metadata["promotion_eligibility_reasons"]
        ),
        "primary_mean": summary.primary_mean,
        "primary_median": summary.primary_median,
        "secondary_mean": summary.secondary_mean,
        "secondary_median": summary.secondary_median,
        "raw_delta_mean": summary.raw_delta_mean,
        "raw_delta_median": summary.raw_delta_median,
        "improvement_delta_mean": summary.improvement_delta_mean,
        "improvement_delta_median": summary.improvement_delta_median,
        "wins": summary.wins,
        "losses": summary.losses,
        "ties": summary.ties,
        "movement_diagnostics": movement_diagnostics_payload(summary.movement_diagnostics),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
        "selected_metric_summary": comparison_metric_summary_payload(summary),
        "best_regressions_count_included": len(regressions),
        "best_improvements_count_included": len(improvements),
        "top_regression_dois": [str(row.get("doi")) for row in regressions if row.get("doi")],
        "top_improvement_dois": [str(row.get("doi")) for row in improvements if row.get("doi")],
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload["study_source"] = dict(study_source_context)
    return payload


def build_candidate_reply_template_text(
    *,
    packet_id: str,
    comparison_id: str,
    baseline_theory_config: str,
) -> str:
    template_payload = {
        "packet_id": packet_id,
        "comparison_id": comparison_id,
        "baseline_theory_config": baseline_theory_config,
        "proposed_changes": [],
        "rationale": "",
        "notes": "TEMPLATE ONLY - not an actual generator reply",
    }
    return (
        "# TEMPLATE ONLY - not an actual generator reply\n"
        + yaml.safe_dump(template_payload, sort_keys=False, allow_unicode=False)
    )


def serialize_packet_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    serialized_rows: list[dict[str, Any]] = []
    for row in rows:
        serialized_row = {
            "doi": row.get("doi"),
            "primary_metric_value": row.get("primary_metric_value"),
            "secondary_metric_value": row.get("secondary_metric_value"),
            "raw_delta": row.get("raw_delta"),
            "improvement_delta": row.get("improvement_delta"),
            "primary_run_dir": _serialize_path(row.get("primary_run_dir")),
            "primary_experiment_id": row.get("primary_experiment_id"),
            "secondary_run_dir": _serialize_path(row.get("secondary_run_dir")),
            "secondary_experiment_id": row.get("secondary_experiment_id"),
            "evaluation_mode": row.get("secondary_evaluation_mode") or row.get("primary_evaluation_mode"),
            "evidence_tier": row.get("secondary_evidence_tier") or row.get("primary_evidence_tier"),
            "metric_scope": row.get("secondary_metric_scope") or row.get("primary_metric_scope"),
            "benchmark_dataset_id": row.get("secondary_benchmark_dataset_id") or row.get("primary_benchmark_dataset_id"),
            "benchmark_labels_sha256": row.get("secondary_benchmark_labels_sha256") or row.get("primary_benchmark_labels_sha256"),
            "benchmark_maturity_tier": row.get("secondary_benchmark_maturity_tier") or row.get("primary_benchmark_maturity_tier"),
            "promotion_ready": row.get("secondary_promotion_ready")
            if row.get("secondary_promotion_ready") is not None
            else row.get("primary_promotion_ready"),
        }
        for field_name in MOVEMENT_ROW_FIELDS:
            if field_name in row:
                serialized_row[field_name] = row.get(field_name)
        serialized_rows.append(serialized_row)
    return serialized_rows


def save_review_packet_artifacts(
    *,
    base_dir: str | Path,
    request: ReviewPacketExportRequest,
    primary_bundle: Any,
    secondary_bundle: Any,
    compatibility_warning_list: list[str],
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_rows: list[dict[str, Any]],
    summary: ComparisonMetricSummary,
) -> ReviewPacketExportResult:
    if primary_bundle is None:
        raise ReviewPacketExportError("A primary batch must be loaded before exporting a review packet.")
    if secondary_bundle is None:
        raise ReviewPacketExportError("A secondary batch must be loaded before exporting a review packet.")
    candidate_evidence_errors = candidate_packet_evidence_errors(
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        paired_rows=paired_rows,
    )
    if candidate_evidence_errors:
        raise ReviewPacketExportError(candidate_evidence_errors[0])
    if common_doi_count <= 0:
        raise ReviewPacketExportError("At least one overlapping DOI is required before exporting a review packet.")
    if not paired_rows:
        raise ReviewPacketExportError(
            "candidate_run_incomplete: no candidate-specific paired comparison rows are available for the selected packet metric."
        )

    theory_config_value = getattr(primary_bundle.manifest, "theory_config", None)
    if not theory_config_value:
        raise ReviewPacketExportError("The primary batch does not expose a theory config path.")

    resolved_theory_path = _resolve_path(theory_config_value)
    if not resolved_theory_path.exists():
        raise ReviewPacketExportError(f"Primary theory config does not exist: {resolved_theory_path}")
    if not resolved_theory_path.is_file():
        raise ReviewPacketExportError(f"Primary theory config is not a file: {resolved_theory_path}")

    theory_text = resolved_theory_path.read_text(encoding="utf-8")
    theory_payload = yaml.safe_load(theory_text)
    if not isinstance(theory_payload, dict):
        raise ReviewPacketExportError("Primary theory config must be a YAML mapping.")
    allowed_revision_paths = flatten_allowed_scalar_paths(theory_payload)

    comparison_dir = Path(base_dir) / request.comparison_id
    packet_dir = comparison_dir / "review_packets" / request.packet_id
    if packet_dir.exists():
        raise ReviewPacketExportError(f"Review packet directory already exists: {packet_dir}")
    packet_dir.mkdir(parents=True, exist_ok=False)

    created_at = _utc_timestamp()
    manifest_path = packet_dir / "review_packet_manifest.json"
    evidence_summary_path = packet_dir / "evidence_summary.json"
    regressions_path = packet_dir / "top_regressions.jsonl"
    improvements_path = packet_dir / "top_improvements.jsonl"
    allowed_revision_paths_path = packet_dir / "allowed_revision_paths.json"
    baseline_snapshot_path = packet_dir / "baseline_theory_snapshot.yaml"
    candidate_template_path = packet_dir / "candidate_reply_TEMPLATE.yaml"

    regressions, improvements = select_top_packet_rows(
        paired_rows,
        max_regressions=request.max_regressions,
        max_improvements=request.max_improvements,
    )
    serialized_regressions = serialize_packet_rows(regressions)
    serialized_improvements = serialize_packet_rows(improvements)
    output_paths = {
        "review_packet_manifest_json": _serialize_path(manifest_path),
        "evidence_summary_json": _serialize_path(evidence_summary_path),
        "top_regressions_jsonl": _serialize_path(regressions_path),
        "top_improvements_jsonl": _serialize_path(improvements_path),
        "allowed_revision_paths_json": _serialize_path(allowed_revision_paths_path),
        "baseline_theory_snapshot_yaml": _serialize_path(baseline_snapshot_path),
        "candidate_reply_template_yaml": _serialize_path(candidate_template_path),
    }
    source_comparison_paths = _resolve_source_comparison_paths(comparison_dir)
    saved_study_source = load_saved_study_source_from_comparison_dir(comparison_dir)
    manifest_payload = build_review_packet_manifest_payload(
        packet_id=request.packet_id,
        packet_dir=packet_dir,
        created_at=created_at,
        comparison_id=request.comparison_id,
        reviewer=request.reviewer,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric=request.selected_metric,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        summary=summary,
        output_paths=output_paths,
        source_comparison_paths=source_comparison_paths,
        study_source_context=build_review_packet_study_source_fields(saved_study_source),
    )
    evidence_payload = build_evidence_summary_payload(
        selected_metric=request.selected_metric,
        compatibility_warning_list=compatibility_warning_list,
        summary=summary,
        regressions=serialized_regressions,
        improvements=serialized_improvements,
        study_source_context=build_evidence_summary_study_source_block(saved_study_source),
    )
    allowed_paths_payload = {
        "baseline_theory_source": _serialize_path(resolved_theory_path),
        "allowed_prefixes": list(ALLOWED_REVISION_PREFIXES),
        "allowed_scalar_paths": allowed_revision_paths,
    }

    _write_json(manifest_path, manifest_payload)
    _write_json(evidence_summary_path, evidence_payload)
    _write_jsonl(regressions_path, serialized_regressions)
    _write_jsonl(improvements_path, serialized_improvements)
    _write_json(allowed_revision_paths_path, allowed_paths_payload)
    copyfile(resolved_theory_path, baseline_snapshot_path)
    candidate_template_path.write_text(
        build_candidate_reply_template_text(
            packet_id=request.packet_id,
            comparison_id=request.comparison_id,
            baseline_theory_config=baseline_snapshot_path.name,
        ),
        encoding="utf-8",
    )
    return ReviewPacketExportResult(
        packet_id=request.packet_id,
        comparison_id=request.comparison_id,
        packet_dir=packet_dir,
        manifest_path=manifest_path,
        evidence_summary_path=evidence_summary_path,
        regressions_path=regressions_path,
        improvements_path=improvements_path,
        allowed_revision_paths_path=allowed_revision_paths_path,
        baseline_snapshot_path=baseline_snapshot_path,
        candidate_template_path=candidate_template_path,
    )


def candidate_packet_evidence_errors(
    *,
    primary_bundle: Any,
    secondary_bundle: Any,
    paired_rows: list[dict[str, Any]],
) -> list[str]:
    errors: list[str] = []
    secondary_manifest = getattr(secondary_bundle, "manifest", None)
    secondary_status = _optional_str(getattr(secondary_manifest, "status", None))
    if secondary_status is not None and secondary_status != "completed":
        errors.append(
            "candidate_run_incomplete: secondary batch status is not completed."
        )

    completed_seed_count = getattr(secondary_manifest, "completed_seed_count", None)
    if completed_seed_count is not None:
        try:
            if int(completed_seed_count) <= 0:
                errors.append(
                    "candidate_run_incomplete: secondary batch completed_seed_count is zero."
                )
        except (TypeError, ValueError):
            errors.append(
                "candidate_run_incomplete: secondary batch completed_seed_count is invalid."
            )

    if not paired_rows:
        errors.append(
            "candidate_run_incomplete: no candidate-specific paired comparison rows are available."
        )
        return list(dict.fromkeys(errors))

    expected_primary_id = _optional_str(getattr(getattr(primary_bundle, "manifest", None), "batch_id", None))
    expected_secondary_id = _optional_str(getattr(secondary_manifest, "batch_id", None))

    for row in paired_rows:
        row_primary_id = _optional_str(row.get("primary_experiment_id"))
        row_secondary_id = _optional_str(row.get("secondary_experiment_id"))
        if (
            expected_primary_id is not None
            and row_primary_id is not None
            and row_primary_id != expected_primary_id
        ):
            errors.append(
                "candidate_run_incomplete: paired rows do not reference the loaded primary batch."
            )
            break
        if (
            expected_secondary_id is not None
            and row_secondary_id is not None
            and row_secondary_id != expected_secondary_id
        ):
            errors.append(
                "candidate_run_incomplete: paired rows do not reference the loaded secondary batch."
            )
            break
        if row_primary_id is not None and row_secondary_id is not None and row_primary_id == row_secondary_id:
            errors.append(
                "candidate_run_incomplete: paired rows reuse placeholder baseline evidence instead of candidate-specific evidence."
            )
            break

    return list(dict.fromkeys(errors))


def _flatten_scalar_children(prefix: str, value: dict[str, Any], flattened_paths: list[str]) -> None:
    for child_key, child_value in value.items():
        child_path = f"{prefix}.{child_key}"
        if isinstance(child_value, dict):
            _flatten_scalar_children(child_path, child_value, flattened_paths)
        elif _is_scalar(child_value):
            flattened_paths.append(child_path)


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ReviewPacketExportError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise ReviewPacketExportError(f"{label} must be a single directory name.")
    return normalized


def _resolve_source_comparison_paths(comparison_dir: Path) -> dict[str, str | None]:
    manifest_path = comparison_dir / "comparison_manifest.json"
    decision_record_path = comparison_dir / "decision_record.json"
    return {
        "comparison_dir": _serialize_path(comparison_dir),
        "comparison_manifest_json": _serialize_path(manifest_path) if manifest_path.exists() else None,
        "decision_record_json": _serialize_path(decision_record_path) if decision_record_path.exists() else None,
    }


def _resolve_path(value: str | Path) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


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


def _comparison_evidence_metadata(primary_bundle: Any, secondary_bundle: Any) -> dict[str, Any]:
    primary_batch = _batch_evidence_payload(primary_bundle)
    secondary_batch = _batch_evidence_payload(secondary_bundle)
    reasons = _comparison_evidence_reasons(primary_batch, secondary_batch)
    return {
        "primary_batch": primary_batch,
        "secondary_batch": secondary_batch,
        "evaluation_mode": _shared_value(
            _optional_str(primary_batch.get("evaluation_mode")),
            _optional_str(secondary_batch.get("evaluation_mode")),
        ),
        "evidence_tier": _shared_value(
            _optional_str(primary_batch.get("evidence_tier")),
            _optional_str(secondary_batch.get("evidence_tier")),
        ),
        "metric_scope": _shared_value(
            _optional_str(primary_batch.get("metric_scope")),
            _optional_str(secondary_batch.get("metric_scope")),
        ),
        "benchmark_dataset_id": _shared_value(
            _optional_str(primary_batch.get("benchmark_dataset_id")),
            _optional_str(secondary_batch.get("benchmark_dataset_id")),
        ),
        "benchmark_labels_sha256": _shared_value(
            _optional_str(primary_batch.get("benchmark_labels_sha256")),
            _optional_str(secondary_batch.get("benchmark_labels_sha256")),
        ),
        "benchmark_maturity_tier": _shared_value(
            _optional_str(primary_batch.get("benchmark_maturity_tier")),
            _optional_str(secondary_batch.get("benchmark_maturity_tier")),
        ),
        "promotion_ready": _shared_bool(
            _optional_bool(primary_batch.get("promotion_ready")),
            _optional_bool(secondary_batch.get("promotion_ready")),
        ),
        "promotion_eligible": not reasons,
        "promotion_eligibility_reasons": reasons,
    }


def _comparison_evidence_metadata_from_rows(
    *,
    compatibility_warning_list: list[str],
    regressions: list[dict[str, Any]],
    improvements: list[dict[str, Any]],
) -> dict[str, Any]:
    sample_row = regressions[0] if regressions else (improvements[0] if improvements else {})
    reasons = [
        warning
        for warning in compatibility_warning_list
        if "evaluation_mode" in warning or "benchmark_" in warning.lower() or "cross-mode" in warning.lower()
    ]
    return {
        "evaluation_mode": sample_row.get("evaluation_mode"),
        "evidence_tier": sample_row.get("evidence_tier"),
        "metric_scope": sample_row.get("metric_scope"),
        "benchmark_dataset_id": sample_row.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": sample_row.get("benchmark_labels_sha256"),
        "benchmark_maturity_tier": sample_row.get("benchmark_maturity_tier"),
        "promotion_ready": sample_row.get("promotion_ready"),
        "promotion_eligible": not reasons,
        "promotion_eligibility_reasons": reasons,
    }


def _comparison_evidence_reasons(
    primary_batch: dict[str, Any],
    secondary_batch: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    for field_name in ("evaluation_mode", "benchmark_dataset_id", "benchmark_labels_sha256"):
        if _optional_str(primary_batch.get(field_name)) != _optional_str(secondary_batch.get(field_name)):
            reasons.append(f"primary and secondary batches differ on {field_name}")
    primary_promotion_ready = _optional_bool(primary_batch.get("promotion_ready"))
    secondary_promotion_ready = _optional_bool(secondary_batch.get("promotion_ready"))
    if primary_promotion_ready is not True or secondary_promotion_ready is not True:
        reasons.append("primary and secondary batches are not both marked promotion_ready")
    reasons.extend(_normalize_reason_list(primary_batch.get("promotion_ineligibility_reasons")))
    reasons.extend(_normalize_reason_list(secondary_batch.get("promotion_ineligibility_reasons")))
    return reasons


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
        "benchmark_labels_snapshot_path": _serialize_path(
            getattr(options, "benchmark_labels_snapshot_path", None)
        ),
    }


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
