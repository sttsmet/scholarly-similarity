from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.comparison import (
    ComparisonMetricSummary,
    comparison_metric_summary_payload,
)
from src.ui.comparison_export import DECISION_STATUS_OPTIONS, serialize_paired_seed_rows
from src.ui.decision_guardrails import (
    DecisionGuardrailAssessment,
    build_guardrail_artifact_fields,
)
from src.ui.study_provenance import extract_study_source_fields


class ReevalOutcomeExportError(ValueError):
    """Raised when a re-evaluation outcome cannot be saved."""


@dataclass(frozen=True, slots=True)
class ReevalOutcomeSaveRequest:
    candidate_run_dir: Path
    outcome_id: str
    reviewer: str | None
    decision_status: str
    notes: str | None
    selected_metric: str
    override_used: bool = False
    override_reason: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateRunContext:
    candidate_run_dir: Path
    manifest_path: Path
    batch_run_result_path: Path
    manifest: dict[str, Any]
    batch_run_result: dict[str, Any]
    comparison_id: str | None
    packet_id: str | None
    candidate_id: str | None


@dataclass(frozen=True, slots=True)
class ReevalOutcomeSaveResult:
    outcome_id: str
    outcome_dir: Path
    manifest_path: Path
    paired_seed_table_path: Path
    decision_record_path: Path


def build_reeval_outcome_save_request(
    *,
    candidate_run_dir: str | Path,
    outcome_id: str,
    reviewer: str,
    decision_status: str,
    notes: str,
    selected_metric: str,
) -> ReevalOutcomeSaveRequest:
    resolved_candidate_run_dir = _resolve_path(candidate_run_dir)
    if not str(selected_metric).strip():
        raise ReevalOutcomeExportError("Selected metric is required.")
    normalized_outcome_id = _normalize_directory_name(outcome_id, label="Outcome ID")
    if decision_status not in DECISION_STATUS_OPTIONS:
        raise ReevalOutcomeExportError("Decision status is invalid.")
    return ReevalOutcomeSaveRequest(
        candidate_run_dir=resolved_candidate_run_dir,
        outcome_id=normalized_outcome_id,
        reviewer=_optional_str(reviewer),
        decision_status=decision_status,
        notes=_optional_str(notes),
        selected_metric=str(selected_metric).strip(),
    )


def load_candidate_run_context(candidate_run_dir: str | Path) -> CandidateRunContext:
    resolved_dir = _resolve_path(candidate_run_dir)
    if not resolved_dir.exists():
        raise ReevalOutcomeExportError(f"Candidate run directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise ReevalOutcomeExportError(f"Candidate run path is not a directory: {resolved_dir}")

    manifest_path = resolved_dir / "candidate_apply_manifest.json"
    batch_run_result_path = resolved_dir / "batch_run_result.json"
    if not manifest_path.exists():
        raise ReevalOutcomeExportError(f"Candidate run is missing candidate_apply_manifest.json: {manifest_path}")
    if not batch_run_result_path.exists():
        raise ReevalOutcomeExportError(f"Candidate run is missing batch_run_result.json: {batch_run_result_path}")

    manifest = _load_json_object(manifest_path)
    batch_run_result = _load_json_object(batch_run_result_path)
    return CandidateRunContext(
        candidate_run_dir=resolved_dir,
        manifest_path=manifest_path,
        batch_run_result_path=batch_run_result_path,
        manifest=manifest,
        batch_run_result=batch_run_result,
        comparison_id=_optional_str(manifest.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")),
        candidate_id=_optional_str(manifest.get("candidate_id")),
    )


def candidate_run_compatibility_errors(
    *,
    candidate_run: CandidateRunContext,
    primary_bundle: Any,
    secondary_bundle: Any,
) -> list[str]:
    errors: list[str] = []
    if primary_bundle is None:
        errors.append("A primary/reference batch must be loaded before saving a re-eval outcome.")
    if secondary_bundle is None:
        errors.append("A secondary/candidate batch must be loaded before saving a re-eval outcome.")
    if errors:
        return errors

    manifest = candidate_run.manifest
    primary_source = manifest.get("source_primary_batch")
    if isinstance(primary_source, dict):
        source_batch_id = _optional_str(primary_source.get("batch_id"))
        if source_batch_id is not None and source_batch_id != _optional_str(getattr(primary_bundle.manifest, "batch_id", None)):
            errors.append(
                "Candidate run source primary batch does not match the currently loaded primary batch."
            )
        source_batch_dir = _optional_str(primary_source.get("batch_dir"))
        if source_batch_dir is not None and _normalize_path_string(source_batch_dir) != _normalize_path_string(getattr(primary_bundle, "batch_dir", None)):
            errors.append(
                "Candidate run source primary batch directory does not match the currently loaded primary batch."
            )
        source_theory = _optional_str(primary_source.get("theory_config"))
        current_theory = _optional_str(getattr(primary_bundle.manifest, "theory_config", None))
        if source_theory is not None and current_theory is not None and _normalize_path_string(source_theory) != _normalize_path_string(current_theory):
            errors.append(
                "Candidate run source primary theory config does not match the currently loaded primary batch."
            )

    output_batch_id = _optional_str(manifest.get("output_batch_id"))
    if output_batch_id is not None and output_batch_id != _optional_str(getattr(secondary_bundle.manifest, "batch_id", None)):
        errors.append(
            "Candidate run output batch does not match the currently loaded secondary batch."
        )
    output_batch_dir = _optional_str(manifest.get("output_batch_dir"))
    if output_batch_dir is not None and _normalize_path_string(output_batch_dir) != _normalize_path_string(getattr(secondary_bundle, "batch_dir", None)):
        errors.append(
            "Candidate run output batch directory does not match the currently loaded secondary batch."
        )

    batch_result_id = _optional_str(candidate_run.batch_run_result.get("batch_id"))
    if batch_result_id is not None and batch_result_id != _optional_str(getattr(secondary_bundle.manifest, "batch_id", None)):
        errors.append(
            "Candidate run batch_run_result.json does not match the currently loaded secondary batch."
        )

    batch_result_status = _optional_str(candidate_run.batch_run_result.get("status"))
    if batch_result_status is not None and batch_result_status != "completed":
        errors.append(
            "candidate_run_incomplete: candidate batch_run_result.json status is not completed."
        )

    completed_seed_count = candidate_run.batch_run_result.get("completed_seed_count")
    if completed_seed_count is not None:
        try:
            if int(completed_seed_count) <= 0:
                errors.append(
                    "candidate_run_incomplete: candidate batch_run_result.json completed_seed_count is zero."
                )
        except (TypeError, ValueError):
            errors.append(
                "candidate_run_incomplete: candidate batch_run_result.json completed_seed_count is invalid."
            )

    return list(dict.fromkeys(errors))


def save_reeval_outcome_artifacts(
    *,
    request: ReevalOutcomeSaveRequest,
    candidate_run: CandidateRunContext,
    primary_bundle: Any,
    secondary_bundle: Any,
    common_doi_count: int,
    common_completed_seed_count: int,
    paired_rows: list[dict[str, Any]],
    summary: ComparisonMetricSummary,
    guardrail_assessment: DecisionGuardrailAssessment | None = None,
) -> ReevalOutcomeSaveResult:
    compatibility_errors = candidate_run_compatibility_errors(
        candidate_run=candidate_run,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
    )
    if compatibility_errors:
        raise ReevalOutcomeExportError(compatibility_errors[0])
    if common_doi_count <= 0:
        raise ReevalOutcomeExportError("At least one overlapping DOI is required before saving a re-eval outcome.")
    if not paired_rows:
        raise ReevalOutcomeExportError("No paired comparison rows are available for the selected decision metric.")

    outcomes_dir = request.candidate_run_dir / "outcomes"
    outcomes_dir.mkdir(parents=True, exist_ok=True)
    outcome_dir = outcomes_dir / request.outcome_id
    if outcome_dir.exists():
        raise ReevalOutcomeExportError(f"Outcome directory already exists: {outcome_dir}")
    outcome_dir.mkdir(parents=False, exist_ok=False)

    created_at = _utc_timestamp()
    manifest_path = outcome_dir / "reeval_outcome_manifest.json"
    paired_seed_table_path = outcome_dir / "reeval_paired_seed_table.jsonl"
    decision_record_path = outcome_dir / "reeval_decision_record.json"
    output_paths = {
        "reeval_outcome_manifest_json": _serialize_path(manifest_path),
        "reeval_paired_seed_table_jsonl": _serialize_path(paired_seed_table_path),
        "reeval_decision_record_json": _serialize_path(decision_record_path),
    }

    serialized_rows = serialize_paired_seed_rows(paired_rows)
    manifest_payload = build_reeval_outcome_manifest_payload(
        request=request,
        outcome_dir=outcome_dir,
        created_at=created_at,
        candidate_run=candidate_run,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        summary=summary,
        guardrail_assessment=guardrail_assessment,
        output_paths=output_paths,
        paired_seed_count=len(serialized_rows),
    )
    decision_payload = build_reeval_decision_record_payload(
        request=request,
        created_at=created_at,
        candidate_run=candidate_run,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=common_doi_count,
        common_completed_seed_count=common_completed_seed_count,
        summary=summary,
        guardrail_assessment=guardrail_assessment,
        paired_seed_count=len(serialized_rows),
    )

    _write_json(manifest_path, manifest_payload)
    _write_jsonl(paired_seed_table_path, serialized_rows)
    _write_json(decision_record_path, decision_payload)
    return ReevalOutcomeSaveResult(
        outcome_id=request.outcome_id,
        outcome_dir=outcome_dir,
        manifest_path=manifest_path,
        paired_seed_table_path=paired_seed_table_path,
        decision_record_path=decision_record_path,
    )


def build_reeval_outcome_manifest_payload(
    *,
    request: ReevalOutcomeSaveRequest,
    outcome_dir: str | Path,
    created_at: str,
    candidate_run: CandidateRunContext,
    primary_bundle: Any,
    secondary_bundle: Any,
    common_doi_count: int,
    common_completed_seed_count: int,
    summary: ComparisonMetricSummary,
    guardrail_assessment: DecisionGuardrailAssessment | None,
    output_paths: dict[str, str],
    paired_seed_count: int,
) -> dict[str, Any]:
    study_source_context = extract_study_source_fields(candidate_run.manifest)
    evidence_metadata = _reeval_evidence_metadata(
        candidate_run=candidate_run,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        guardrail_assessment=guardrail_assessment,
    )
    payload = {
        "outcome_id": request.outcome_id,
        "outcome_dir": _serialize_path(outcome_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "decision_status": request.decision_status,
        "notes": request.notes,
        "selected_metric": request.selected_metric,
        "candidate_id": candidate_run.candidate_id,
        "candidate_run_dir": _serialize_path(candidate_run.candidate_run_dir),
        "packet_id": candidate_run.packet_id,
        "comparison_id": candidate_run.comparison_id,
        "primary_batch": dict(evidence_metadata["primary_batch"]),
        "secondary_batch": dict(evidence_metadata["secondary_batch"]),
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
        "candidate_theory_snapshot_path": _candidate_theory_snapshot_path(candidate_run.manifest),
        "candidate_reply_yaml_path": _candidate_reply_yaml_path(candidate_run.manifest),
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_seed_count,
        "paired_seed_count": paired_seed_count,
        "selected_metric_summary": comparison_metric_summary_payload(summary),
        "movement_diagnostics": comparison_metric_summary_payload(summary).get("movement_diagnostics"),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
        "output_paths": dict(output_paths),
    }
    if study_source_context:
        payload.update(study_source_context)
    payload.update(
        build_guardrail_artifact_fields(
            guardrail_assessment,
            override_used=request.override_used,
            override_reason=request.override_reason,
        )
    )
    return payload


def build_reeval_decision_record_payload(
    *,
    request: ReevalOutcomeSaveRequest,
    created_at: str,
    candidate_run: CandidateRunContext,
    primary_bundle: Any,
    secondary_bundle: Any,
    common_doi_count: int,
    common_completed_seed_count: int,
    summary: ComparisonMetricSummary,
    guardrail_assessment: DecisionGuardrailAssessment | None,
    paired_seed_count: int,
) -> dict[str, Any]:
    study_source_context = extract_study_source_fields(candidate_run.manifest)
    evidence_metadata = _reeval_evidence_metadata(
        candidate_run=candidate_run,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        guardrail_assessment=guardrail_assessment,
    )
    payload = {
        "outcome_id": request.outcome_id,
        "created_at": created_at,
        "reviewer": request.reviewer,
        "decision_status": request.decision_status,
        "notes": request.notes,
        "selected_metric": request.selected_metric,
        "candidate_id": candidate_run.candidate_id,
        "packet_id": candidate_run.packet_id,
        "comparison_id": candidate_run.comparison_id,
        "primary_batch_id": getattr(primary_bundle.manifest, "batch_id", None),
        "secondary_batch_id": getattr(secondary_bundle.manifest, "batch_id", None),
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
        "common_doi_count": common_doi_count,
        "common_completed_seed_count": common_completed_seed_count,
        "paired_seed_count": paired_seed_count,
        "wins": summary.wins,
        "losses": summary.losses,
        "ties": summary.ties,
        "candidate_theory_snapshot_path": _candidate_theory_snapshot_path(candidate_run.manifest),
        "candidate_reply_yaml_path": _candidate_reply_yaml_path(candidate_run.manifest),
        "selected_metric_summary": comparison_metric_summary_payload(summary),
        "movement_diagnostics": comparison_metric_summary_payload(summary).get("movement_diagnostics"),
        "movement_diagnostic_note": (
            summary.movement_diagnostics.movement_diagnostic_note
            if summary.movement_diagnostics is not None
            else None
        ),
    }
    if study_source_context:
        payload.update(study_source_context)
    payload.update(
        build_guardrail_artifact_fields(
            guardrail_assessment,
            override_used=request.override_used,
            override_reason=request.override_reason,
        )
    )
    return payload


def _candidate_theory_snapshot_path(manifest: dict[str, Any]) -> str | None:
    path = _optional_str(manifest.get("candidate_theory_snapshot_path"))
    if path is not None:
        return path
    output_paths = manifest.get("output_paths")
    if isinstance(output_paths, dict):
        return _optional_str(output_paths.get("candidate_theory_snapshot_yaml"))
    return None


def _candidate_reply_yaml_path(manifest: dict[str, Any]) -> str | None:
    preferred = _optional_str(manifest.get("copied_reply_yaml"))
    if preferred is not None:
        return preferred
    fallback = _optional_str(manifest.get("reply_yaml_path"))
    if fallback is not None:
        return fallback
    output_paths = manifest.get("output_paths")
    if isinstance(output_paths, dict):
        return _optional_str(output_paths.get("candidate_reply_yaml"))
    return None

def _reeval_evidence_metadata(
    *,
    candidate_run: CandidateRunContext,
    primary_bundle: Any,
    secondary_bundle: Any,
    guardrail_assessment: DecisionGuardrailAssessment | None,
) -> dict[str, Any]:
    primary_batch = _batch_evidence_payload(primary_bundle)
    secondary_batch = _batch_evidence_payload(secondary_bundle)
    return {
        "primary_batch": primary_batch,
        "secondary_batch": secondary_batch,
        "evaluation_mode": (
            guardrail_assessment.evaluation_mode
            if guardrail_assessment is not None and guardrail_assessment.evaluation_mode is not None
            else _shared_value(
                _optional_str(primary_batch.get("evaluation_mode")),
                _optional_str(secondary_batch.get("evaluation_mode")),
            )
        ),
        "evidence_tier": _shared_value(
            _optional_str(primary_batch.get("evidence_tier")),
            _optional_str(secondary_batch.get("evidence_tier")),
        ),
        "metric_scope": (
            guardrail_assessment.metric_scope
            if guardrail_assessment is not None and guardrail_assessment.metric_scope is not None
            else _shared_value(
                _optional_str(primary_batch.get("metric_scope")),
                _optional_str(secondary_batch.get("metric_scope")),
            )
        ),
        "benchmark_dataset_id": (
            guardrail_assessment.benchmark_dataset_id
            if guardrail_assessment is not None and guardrail_assessment.benchmark_dataset_id is not None
            else _shared_value(
                _optional_str(primary_batch.get("benchmark_dataset_id")),
                _optional_str(secondary_batch.get("benchmark_dataset_id")),
            )
        ),
        "benchmark_labels_sha256": (
            guardrail_assessment.benchmark_labels_sha256
            if guardrail_assessment is not None and guardrail_assessment.benchmark_labels_sha256 is not None
            else _shared_value(
                _optional_str(primary_batch.get("benchmark_labels_sha256")),
                _optional_str(secondary_batch.get("benchmark_labels_sha256")),
            )
        ),
        "benchmark_maturity_tier": (
            guardrail_assessment.benchmark_maturity_tier
            if guardrail_assessment is not None and guardrail_assessment.benchmark_maturity_tier is not None
            else _shared_value(
                _optional_str(primary_batch.get("benchmark_maturity_tier")),
                _optional_str(secondary_batch.get("benchmark_maturity_tier")),
            )
        ),
        "promotion_ready": (
            guardrail_assessment.promotion_ready
            if guardrail_assessment is not None
            else _shared_bool(
                _optional_bool(primary_batch.get("promotion_ready")),
                _optional_bool(secondary_batch.get("promotion_ready")),
            )
        ),
        "promotion_ineligibility_reasons": (
            list(guardrail_assessment.promotion_ineligibility_reasons)
            if guardrail_assessment is not None
            else list(
                dict.fromkeys(
                    _normalize_reason_list(primary_batch.get("promotion_ineligibility_reasons"))
                    + _normalize_reason_list(
                        secondary_batch.get("promotion_ineligibility_reasons")
                    )
                )
            )
        ),
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


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ReevalOutcomeExportError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise ReevalOutcomeExportError(f"{label} must be a single directory name.")
    return normalized


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ReevalOutcomeExportError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ReevalOutcomeExportError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ReevalOutcomeExportError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _resolve_path(value: str | Path) -> Path:
    raw_value = str(value).strip()
    if not raw_value:
        raise ReevalOutcomeExportError("Candidate Run Directory is required.")
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def _normalize_path_string(value: str | Path | None) -> str | None:
    if value in (None, ""):
        return None
    return str(_resolve_path(value))


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


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    contents = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    path.write_text((contents + "\n") if contents else "", encoding="utf-8")


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
