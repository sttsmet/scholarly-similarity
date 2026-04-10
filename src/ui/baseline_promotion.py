from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any

from src.config import REPO_ROOT
from src.ui.decision_guardrails import (
    DecisionGuardrailAssessment,
    build_guardrail_artifact_fields,
    requires_explicit_promotion_override,
)
from src.ui.reeval_outcome_export import CandidateRunContext, load_candidate_run_context
from src.ui.run_context import load_run_context_if_present
from src.ui.study_provenance import extract_study_source_fields


class BaselinePromotionError(ValueError):
    """Raised when an accepted baseline promotion cannot be completed."""


@dataclass(frozen=True, slots=True)
class BaselinePromotionRequest:
    candidate_run_dir: Path
    outcome_dir: Path
    baseline_id: str
    reviewer: str | None
    notes: str | None
    override_used: bool = False
    override_reason: str | None = None


@dataclass(frozen=True, slots=True)
class OutcomeContext:
    outcome_dir: Path
    manifest_path: Path
    decision_record_path: Path
    manifest: dict[str, Any]
    decision_record: dict[str, Any]
    outcome_id: str | None
    decision_status: str | None
    comparison_id: str | None
    packet_id: str | None
    candidate_id: str | None


@dataclass(frozen=True, slots=True)
class BaselinePromotionResult:
    baseline_id: str
    baseline_dir: Path
    manifest_path: Path
    accepted_theory_snapshot_path: Path
    candidate_reply_path: Path
    applied_changes_path: Path
    promotion_record_path: Path


def build_baseline_promotion_request(
    *,
    candidate_run_dir: str | Path,
    outcome_dir: str | Path,
    baseline_id: str,
    reviewer: str,
    notes: str,
    override_used: bool = False,
    override_reason: str = "",
) -> BaselinePromotionRequest:
    return BaselinePromotionRequest(
        candidate_run_dir=_resolve_path(candidate_run_dir, label="Candidate Run Directory"),
        outcome_dir=_resolve_path(outcome_dir, label="Outcome Directory"),
        baseline_id=_normalize_directory_name(baseline_id, label="Baseline ID"),
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        override_used=bool(override_used),
        override_reason=_optional_str(override_reason),
    )


def load_outcome_context(outcome_dir: str | Path) -> OutcomeContext:
    resolved_dir = _resolve_path(outcome_dir, label="Outcome Directory")
    if not resolved_dir.exists():
        raise BaselinePromotionError(f"Outcome directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise BaselinePromotionError(f"Outcome path is not a directory: {resolved_dir}")

    manifest_path = resolved_dir / "reeval_outcome_manifest.json"
    decision_record_path = resolved_dir / "reeval_decision_record.json"
    if not manifest_path.exists():
        raise BaselinePromotionError(f"Outcome is missing reeval_outcome_manifest.json: {manifest_path}")
    if not decision_record_path.exists():
        raise BaselinePromotionError(f"Outcome is missing reeval_decision_record.json: {decision_record_path}")

    manifest = _load_json_object(manifest_path)
    decision_record = _load_json_object(decision_record_path)
    return OutcomeContext(
        outcome_dir=resolved_dir,
        manifest_path=manifest_path,
        decision_record_path=decision_record_path,
        manifest=manifest,
        decision_record=decision_record,
        outcome_id=_optional_str(manifest.get("outcome_id")) or _optional_str(decision_record.get("outcome_id")),
        decision_status=_optional_str(decision_record.get("decision_status")) or _optional_str(manifest.get("decision_status")),
        comparison_id=_optional_str(manifest.get("comparison_id")) or _optional_str(decision_record.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")) or _optional_str(decision_record.get("packet_id")),
        candidate_id=_optional_str(manifest.get("candidate_id")) or _optional_str(decision_record.get("candidate_id")),
    )


def promotion_compatibility_errors(
    *,
    candidate_run: CandidateRunContext,
    outcome: OutcomeContext,
    primary_bundle: Any | None = None,
    secondary_bundle: Any | None = None,
) -> list[str]:
    errors: list[str] = []

    for label in ("comparison_id", "packet_id", "candidate_id"):
        candidate_value = getattr(candidate_run, label)
        outcome_value = getattr(outcome, label)
        if candidate_value is not None and outcome_value is not None and candidate_value != outcome_value:
            errors.append(
                f"Candidate run and outcome {label} do not match: '{candidate_value}' vs '{outcome_value}'."
            )

    manifest_candidate_run_dir = _optional_str(outcome.manifest.get("candidate_run_dir"))
    if (
        manifest_candidate_run_dir is not None
        and _normalize_path_string(manifest_candidate_run_dir) != _normalize_path_string(candidate_run.candidate_run_dir)
    ):
        errors.append("Outcome candidate_run_dir does not match the selected candidate run directory.")

    manifest_status = _optional_str(outcome.manifest.get("decision_status"))
    decision_status = _optional_str(outcome.decision_record.get("decision_status"))
    if manifest_status is not None and decision_status is not None and manifest_status != decision_status:
        errors.append("Outcome manifest and decision record disagree on decision_status.")

    _compare_batch_metadata(
        errors,
        candidate_run.manifest.get("source_primary_batch"),
        outcome.manifest.get("primary_batch"),
        label="primary batch",
    )
    _compare_batch_metadata(
        errors,
        {
            "batch_id": candidate_run.manifest.get("output_batch_id"),
            "batch_dir": candidate_run.manifest.get("output_batch_dir"),
        },
        outcome.manifest.get("secondary_batch"),
        label="secondary batch",
    )

    if primary_bundle is not None:
        _compare_bundle_metadata(
            errors,
            outcome.manifest.get("primary_batch"),
            primary_bundle,
            label="loaded primary batch",
        )
    if secondary_bundle is not None:
        _compare_bundle_metadata(
            errors,
            outcome.manifest.get("secondary_batch"),
            secondary_bundle,
            label="loaded secondary batch",
        )

    return list(dict.fromkeys(errors))


def _build_promotion_evidence_policy(
    *,
    outcome: OutcomeContext,
    primary_bundle: Any | None = None,
    secondary_bundle: Any | None = None,
) -> dict[str, Any]:
    primary_metadata = _load_batch_evidence_metadata(
        outcome.manifest.get("primary_batch"),
        bundle=primary_bundle,
    )
    secondary_metadata = _load_batch_evidence_metadata(
        outcome.manifest.get("secondary_batch"),
        bundle=secondary_bundle,
    )

    reasons: list[str] = []
    primary_mode = _optional_str(primary_metadata.get("evaluation_mode"))
    secondary_mode = _optional_str(secondary_metadata.get("evaluation_mode"))
    if primary_mode != "independent_benchmark" or secondary_mode != "independent_benchmark":
        reasons.append(
            "Promotion evidence requires both primary and secondary batches to use independent_benchmark mode."
        )

    primary_dataset_id = _optional_str(primary_metadata.get("benchmark_dataset_id"))
    secondary_dataset_id = _optional_str(secondary_metadata.get("benchmark_dataset_id"))
    if primary_dataset_id is None or secondary_dataset_id is None:
        reasons.append("Benchmark dataset id is missing from the compared batch evidence.")
    elif primary_dataset_id != secondary_dataset_id:
        reasons.append("Primary and secondary batches do not share the same benchmark_dataset_id.")

    primary_labels_sha256 = _optional_str(primary_metadata.get("benchmark_labels_sha256"))
    secondary_labels_sha256 = _optional_str(secondary_metadata.get("benchmark_labels_sha256"))
    if primary_labels_sha256 is None or secondary_labels_sha256 is None:
        reasons.append("Benchmark labels SHA256 is missing from the compared batch evidence.")
    elif primary_labels_sha256 != secondary_labels_sha256:
        reasons.append("Primary and secondary batches do not share the same benchmark_labels_sha256.")

    primary_maturity_tier = _optional_str(primary_metadata.get("benchmark_maturity_tier"))
    secondary_maturity_tier = _optional_str(secondary_metadata.get("benchmark_maturity_tier"))
    if primary_maturity_tier is None or secondary_maturity_tier is None:
        reasons.append("Benchmark maturity tier is missing from the compared batch evidence.")

    primary_promotion_ready = _optional_bool(primary_metadata.get("promotion_ready"))
    secondary_promotion_ready = _optional_bool(secondary_metadata.get("promotion_ready"))
    if primary_promotion_ready is not True or secondary_promotion_ready is not True:
        reasons.append("Compared benchmark evidence is not marked promotion_ready.")

    reasons.extend(_normalized_reason_list(primary_metadata.get("promotion_ineligibility_reasons")))
    reasons.extend(_normalized_reason_list(secondary_metadata.get("promotion_ineligibility_reasons")))

    shared_metric_scope = _shared_value(
        _optional_str(primary_metadata.get("metric_scope")),
        _optional_str(secondary_metadata.get("metric_scope")),
    )
    return {
        "promotion_eligible": not reasons,
        "ineligibility_reasons": reasons,
        "evaluation_mode": _shared_value(primary_mode, secondary_mode),
        "metric_scope": shared_metric_scope,
        "benchmark_dataset_id": _shared_value(primary_dataset_id, secondary_dataset_id),
        "benchmark_labels_sha256": _shared_value(primary_labels_sha256, secondary_labels_sha256),
        "benchmark_maturity_tier": _shared_value(primary_maturity_tier, secondary_maturity_tier),
        "promotion_ready": (
            True if primary_promotion_ready is True and secondary_promotion_ready is True and not reasons else False
        ),
        "primary_batch_evidence": primary_metadata,
        "secondary_batch_evidence": secondary_metadata,
    }


def _load_batch_evidence_metadata(saved_payload: Any, *, bundle: Any | None = None) -> dict[str, Any]:
    batch_dir = _optional_str(_batch_value(saved_payload, "batch_dir"))
    manifest_payload: dict[str, Any] = {}
    if bundle is not None:
        manifest = getattr(bundle, "manifest", None)
        options = getattr(manifest, "options", None)
        manifest_payload = {
            "batch_id": getattr(manifest, "batch_id", None),
            "batch_dir": _normalize_path_string(getattr(bundle, "batch_dir", None)),
            "evaluation_mode": getattr(options, "evaluation_mode", None),
            "evidence_tier": getattr(options, "evidence_tier", None),
            "metric_scope": getattr(options, "metric_scope", None),
            "benchmark_labels_path": getattr(options, "benchmark_labels_path", None),
            "benchmark_labels_snapshot_path": getattr(options, "benchmark_labels_snapshot_path", None),
            "benchmark_dataset_id": getattr(options, "benchmark_dataset_id", None),
            "benchmark_labels_sha256": getattr(options, "benchmark_labels_sha256", None),
            "benchmark_maturity_tier": getattr(options, "benchmark_maturity_tier", None),
            "promotion_ready": getattr(options, "promotion_ready", None),
            "promotion_ineligibility_reasons": getattr(
                options, "promotion_ineligibility_reasons", None
            ),
        }
    elif batch_dir is not None:
        manifest_payload = _load_batch_manifest_evidence(batch_dir)
        manifest_payload.setdefault("batch_dir", _normalize_path_string(batch_dir))

    run_context_payload, _ = load_run_context_if_present(batch_dir) if batch_dir is not None else (None, None)
    run_context_payload = run_context_payload if isinstance(run_context_payload, dict) else {}

    return {
        "batch_id": _optional_str(manifest_payload.get("batch_id")) or _optional_str(_batch_value(saved_payload, "batch_id")),
        "batch_dir": _optional_str(manifest_payload.get("batch_dir")) or batch_dir,
        "evaluation_mode": _optional_str(manifest_payload.get("evaluation_mode")) or _optional_str(run_context_payload.get("evaluation_mode")),
        "evidence_tier": _optional_str(manifest_payload.get("evidence_tier")) or _optional_str(run_context_payload.get("evidence_tier")),
        "metric_scope": _optional_str(manifest_payload.get("metric_scope")) or _optional_str(run_context_payload.get("metric_scope")),
        "benchmark_labels_path": _optional_str(manifest_payload.get("benchmark_labels_snapshot_path"))
        or _optional_str(run_context_payload.get("benchmark_labels_snapshot_path"))
        or _optional_str(manifest_payload.get("benchmark_labels_path"))
        or _optional_str(run_context_payload.get("benchmark_labels_path")),
        "benchmark_labels_snapshot_path": _optional_str(manifest_payload.get("benchmark_labels_snapshot_path"))
        or _optional_str(run_context_payload.get("benchmark_labels_snapshot_path")),
        "benchmark_dataset_id": _optional_str(manifest_payload.get("benchmark_dataset_id")) or _optional_str(run_context_payload.get("benchmark_dataset_id")),
        "benchmark_labels_sha256": _optional_str(manifest_payload.get("benchmark_labels_sha256")) or _optional_str(run_context_payload.get("benchmark_labels_sha256")),
        "benchmark_maturity_tier": _optional_str(manifest_payload.get("benchmark_maturity_tier"))
        or _optional_str(run_context_payload.get("benchmark_maturity_tier")),
        "promotion_ready": _optional_bool(manifest_payload.get("promotion_ready"))
        if _optional_bool(manifest_payload.get("promotion_ready")) is not None
        else _optional_bool(run_context_payload.get("promotion_ready")),
        "promotion_ineligibility_reasons": _normalized_reason_list(
            manifest_payload.get("promotion_ineligibility_reasons")
        )
        or _normalized_reason_list(run_context_payload.get("promotion_ineligibility_reasons")),
    }


def _load_batch_manifest_evidence(batch_dir: str | Path) -> dict[str, Any]:
    resolved_batch_dir = _resolve_path(batch_dir, label="Batch Directory")
    manifest_path = resolved_batch_dir / "batch_manifest.json"
    if not manifest_path.exists():
        return {}
    payload = _load_json_object(manifest_path)
    options = payload.get("options")
    if not isinstance(options, dict):
        return {}
    return {
        "batch_id": payload.get("batch_id"),
        "evaluation_mode": options.get("evaluation_mode"),
        "evidence_tier": options.get("evidence_tier"),
        "metric_scope": options.get("metric_scope"),
        "benchmark_labels_path": options.get("benchmark_labels_path"),
        "benchmark_labels_snapshot_path": options.get("benchmark_labels_snapshot_path"),
        "benchmark_dataset_id": options.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": options.get("benchmark_labels_sha256"),
        "benchmark_maturity_tier": options.get("benchmark_maturity_tier"),
        "promotion_ready": options.get("promotion_ready"),
        "promotion_ineligibility_reasons": options.get("promotion_ineligibility_reasons"),
    }


def save_accepted_baseline_artifacts(
    *,
    base_dir: str | Path,
    request: BaselinePromotionRequest,
    candidate_run: CandidateRunContext,
    outcome: OutcomeContext,
    guardrail_assessment: DecisionGuardrailAssessment | None = None,
    primary_bundle: Any | None = None,
    secondary_bundle: Any | None = None,
) -> BaselinePromotionResult:
    compatibility_errors = promotion_compatibility_errors(
        candidate_run=candidate_run,
        outcome=outcome,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
    )
    if compatibility_errors:
        raise BaselinePromotionError(compatibility_errors[0])

    evidence_policy = _build_promotion_evidence_policy(
        outcome=outcome,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
    )
    if outcome.decision_status != "accept_candidate":
        raise BaselinePromotionError("Only outcomes with decision_status 'accept_candidate' can be promoted.")
    override_required = (
        (guardrail_assessment is not None and requires_explicit_promotion_override(guardrail_assessment))
        or not bool(evidence_policy.get("promotion_eligible"))
    )
    if override_required:
        if not request.override_used:
            raise BaselinePromotionError(
                "This outcome did not pass the evidence-mode guardrails. Explicit override acknowledgment is required."
            )
        if request.override_reason is None:
            raise BaselinePromotionError(
                "An override rationale is required when promoting a weak or failed outcome."
            )

    candidate_theory_source = _require_existing_file(
        _candidate_theory_snapshot_path(candidate_run.manifest),
        label="Candidate theory snapshot",
    )
    candidate_reply_source = _require_existing_file(
        _candidate_reply_yaml_path(candidate_run.manifest),
        label="Candidate reply YAML",
    )
    applied_changes_source = _require_existing_file(
        _applied_changes_path(candidate_run.candidate_run_dir, candidate_run.manifest),
        label="Applied changes artifact",
    )

    baseline_root = Path(base_dir)
    baseline_root.mkdir(parents=True, exist_ok=True)
    baseline_dir = baseline_root / request.baseline_id
    if baseline_dir.exists():
        raise BaselinePromotionError(f"Accepted baseline directory already exists: {baseline_dir}")
    baseline_dir.mkdir(parents=False, exist_ok=False)

    manifest_path = baseline_dir / "accepted_baseline_manifest.json"
    accepted_theory_snapshot_path = baseline_dir / "accepted_theory_snapshot.yaml"
    candidate_reply_path = baseline_dir / "candidate_reply.yaml"
    applied_changes_path = baseline_dir / "applied_changes.jsonl"
    promotion_record_path = baseline_dir / "promotion_record.json"
    created_at = _utc_timestamp()

    copyfile(candidate_theory_source, accepted_theory_snapshot_path)
    copyfile(candidate_reply_source, candidate_reply_path)
    copyfile(applied_changes_source, applied_changes_path)

    output_paths = {
        "accepted_baseline_manifest_json": _serialize_path(manifest_path),
        "accepted_theory_snapshot_yaml": _serialize_path(accepted_theory_snapshot_path),
        "candidate_reply_yaml": _serialize_path(candidate_reply_path),
        "applied_changes_jsonl": _serialize_path(applied_changes_path),
        "promotion_record_json": _serialize_path(promotion_record_path),
    }

    manifest_payload = build_accepted_baseline_manifest_payload(
        request=request,
        baseline_dir=baseline_dir,
        created_at=created_at,
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=accepted_theory_snapshot_path,
        candidate_reply_path=candidate_reply_path,
        applied_changes_path=applied_changes_path,
        guardrail_assessment=guardrail_assessment,
        evidence_policy=evidence_policy,
        output_paths=output_paths,
    )
    promotion_record_payload = build_promotion_record_payload(
        request=request,
        created_at=created_at,
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=accepted_theory_snapshot_path,
        candidate_reply_path=candidate_reply_path,
        guardrail_assessment=guardrail_assessment,
        evidence_policy=evidence_policy,
    )

    _write_json(manifest_path, manifest_payload)
    _write_json(promotion_record_path, promotion_record_payload)
    return BaselinePromotionResult(
        baseline_id=request.baseline_id,
        baseline_dir=baseline_dir,
        manifest_path=manifest_path,
        accepted_theory_snapshot_path=accepted_theory_snapshot_path,
        candidate_reply_path=candidate_reply_path,
        applied_changes_path=applied_changes_path,
        promotion_record_path=promotion_record_path,
    )


def build_accepted_baseline_manifest_payload(
    *,
    request: BaselinePromotionRequest,
    baseline_dir: str | Path,
    created_at: str,
    candidate_run: CandidateRunContext,
    outcome: OutcomeContext,
    accepted_theory_snapshot_path: Path,
    candidate_reply_path: Path,
    applied_changes_path: Path,
    guardrail_assessment: DecisionGuardrailAssessment | None,
    output_paths: dict[str, str],
    evidence_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_manifest = outcome.manifest
    study_source_context = extract_study_source_fields(
        outcome.manifest,
        outcome.decision_record,
        candidate_run.manifest,
    )
    normalized_evidence_policy = evidence_policy or _default_evidence_policy()
    payload = {
        "baseline_id": request.baseline_id,
        "baseline_dir": _serialize_path(baseline_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "source_lineage": {
            "comparison_id": outcome.comparison_id,
            "packet_id": outcome.packet_id,
            "candidate_id": outcome.candidate_id,
            "outcome_id": outcome.outcome_id,
            "candidate_run_dir": _serialize_path(candidate_run.candidate_run_dir),
            "outcome_dir": _serialize_path(outcome.outcome_dir),
        },
        "decision_status": outcome.decision_status,
        "selected_metric": _selected_metric(outcome),
        "source_primary_batch": _batch_payload(outcome_manifest.get("primary_batch")),
        "source_secondary_batch": _batch_payload(outcome_manifest.get("secondary_batch")),
        "evaluation_mode": normalized_evidence_policy.get("evaluation_mode"),
        "evidence_tier": normalized_evidence_policy.get("evaluation_mode"),
        "metric_scope": normalized_evidence_policy.get("metric_scope"),
        "benchmark_dataset_id": normalized_evidence_policy.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": normalized_evidence_policy.get("benchmark_labels_sha256"),
        "benchmark_maturity_tier": normalized_evidence_policy.get("benchmark_maturity_tier"),
        "promotion_ready": bool(normalized_evidence_policy.get("promotion_ready", False)),
        "promotion_eligible": bool(normalized_evidence_policy.get("promotion_eligible", False)),
        "promotion_ineligibility_reasons": list(
            normalized_evidence_policy.get("ineligibility_reasons", [])
        ),
        "primary_batch_evidence": dict(normalized_evidence_policy.get("primary_batch_evidence", {})),
        "secondary_batch_evidence": dict(normalized_evidence_policy.get("secondary_batch_evidence", {})),
        "accepted_theory_snapshot_path": _serialize_path(accepted_theory_snapshot_path),
        "candidate_reply_yaml_path": _serialize_path(candidate_reply_path),
        "applied_changes_path": _serialize_path(applied_changes_path),
        "outcome_summary": {
            "common_doi_count": outcome_manifest.get("common_doi_count"),
            "common_completed_seed_count": outcome_manifest.get("common_completed_seed_count"),
            **_selected_metric_summary(outcome),
        },
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
    payload["benchmark_maturity_tier"] = normalized_evidence_policy.get("benchmark_maturity_tier")
    payload["promotion_ready"] = bool(normalized_evidence_policy.get("promotion_ready", False))
    payload["promotion_eligible"] = bool(normalized_evidence_policy.get("promotion_eligible", False))
    payload["promotion_ineligibility_reasons"] = list(
        normalized_evidence_policy.get("ineligibility_reasons", [])
    )
    return payload


def build_promotion_record_payload(
    *,
    request: BaselinePromotionRequest,
    created_at: str,
    candidate_run: CandidateRunContext,
    outcome: OutcomeContext,
    accepted_theory_snapshot_path: Path,
    candidate_reply_path: Path,
    guardrail_assessment: DecisionGuardrailAssessment | None,
    evidence_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    outcome_manifest = outcome.manifest
    summary = _selected_metric_summary(outcome)
    study_source_context = extract_study_source_fields(
        outcome.manifest,
        outcome.decision_record,
        candidate_run.manifest,
    )
    normalized_evidence_policy = evidence_policy or _default_evidence_policy()
    payload = {
        "baseline_id": request.baseline_id,
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "decision_status": outcome.decision_status,
        "comparison_id": outcome.comparison_id,
        "packet_id": outcome.packet_id,
        "candidate_id": outcome.candidate_id,
        "outcome_id": outcome.outcome_id,
        "selected_metric": _selected_metric(outcome),
        "source_primary_batch_id": _batch_value(outcome_manifest.get("primary_batch"), "batch_id"),
        "source_secondary_batch_id": _batch_value(outcome_manifest.get("secondary_batch"), "batch_id"),
        "evaluation_mode": normalized_evidence_policy.get("evaluation_mode"),
        "evidence_tier": normalized_evidence_policy.get("evaluation_mode"),
        "metric_scope": normalized_evidence_policy.get("metric_scope"),
        "benchmark_dataset_id": normalized_evidence_policy.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": normalized_evidence_policy.get("benchmark_labels_sha256"),
        "benchmark_maturity_tier": normalized_evidence_policy.get("benchmark_maturity_tier"),
        "promotion_ready": bool(normalized_evidence_policy.get("promotion_ready", False)),
        "promotion_eligible": bool(normalized_evidence_policy.get("promotion_eligible", False)),
        "promotion_ineligibility_reasons": list(
            normalized_evidence_policy.get("ineligibility_reasons", [])
        ),
        "accepted_theory_snapshot_path": _serialize_path(accepted_theory_snapshot_path),
        "candidate_reply_yaml_path": _serialize_path(candidate_reply_path),
        "selected_metric_summary": summary,
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
    payload["benchmark_maturity_tier"] = normalized_evidence_policy.get("benchmark_maturity_tier")
    payload["promotion_ready"] = bool(normalized_evidence_policy.get("promotion_ready", False))
    payload["promotion_eligible"] = bool(normalized_evidence_policy.get("promotion_eligible", False))
    payload["promotion_ineligibility_reasons"] = list(
        normalized_evidence_policy.get("ineligibility_reasons", [])
    )
    return payload


def _compare_batch_metadata(
    errors: list[str],
    candidate_payload: Any,
    outcome_payload: Any,
    *,
    label: str,
) -> None:
    if not isinstance(candidate_payload, dict) or not isinstance(outcome_payload, dict):
        return
    for field_name in ("batch_id", "batch_dir"):
        candidate_value = _optional_str(candidate_payload.get(field_name))
        outcome_value = _optional_str(outcome_payload.get(field_name))
        if candidate_value is None or outcome_value is None:
            continue
        if field_name.endswith("_dir"):
            if _normalize_path_string(candidate_value) != _normalize_path_string(outcome_value):
                errors.append(f"Candidate run and outcome {label} {field_name} do not match.")
        elif candidate_value != outcome_value:
            errors.append(f"Candidate run and outcome {label} {field_name} do not match.")


def _compare_bundle_metadata(
    errors: list[str],
    saved_payload: Any,
    bundle: Any,
    *,
    label: str,
) -> None:
    if not isinstance(saved_payload, dict):
        return
    saved_batch_id = _optional_str(saved_payload.get("batch_id"))
    current_batch_id = _optional_str(getattr(bundle.manifest, "batch_id", None))
    if saved_batch_id is not None and current_batch_id is not None and saved_batch_id != current_batch_id:
        errors.append(f"Saved artifact lineage does not match the current {label}.")

    saved_batch_dir = _optional_str(saved_payload.get("batch_dir"))
    current_batch_dir = _normalize_path_string(getattr(bundle, "batch_dir", None))
    if saved_batch_dir is not None and current_batch_dir is not None and _normalize_path_string(saved_batch_dir) != current_batch_dir:
        errors.append(f"Saved artifact batch directory does not match the current {label}.")


def _batch_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"batch_id": None, "batch_dir": None, "theory_config": None}
    return {
        "batch_id": value.get("batch_id"),
        "batch_dir": value.get("batch_dir"),
        "theory_config": value.get("theory_config"),
    }


def _batch_value(payload: Any, field_name: str) -> Any:
    if not isinstance(payload, dict):
        return None
    return payload.get(field_name)


def _selected_metric(outcome: OutcomeContext) -> str | None:
    return _optional_str(outcome.manifest.get("selected_metric")) or _optional_str(outcome.decision_record.get("selected_metric"))


def _selected_metric_summary(outcome: OutcomeContext) -> dict[str, Any]:
    summary = outcome.manifest.get("selected_metric_summary")
    if not isinstance(summary, dict):
        summary = outcome.decision_record.get("selected_metric_summary")
    if not isinstance(summary, dict):
        return {
            "primary_mean": None,
            "primary_median": None,
            "secondary_mean": None,
            "secondary_median": None,
            "raw_delta_mean": None,
            "raw_delta_median": None,
            "improvement_delta_mean": None,
            "improvement_delta_median": None,
            "wins": None,
            "losses": None,
            "ties": None,
        }
    return {
        "primary_mean": summary.get("primary_mean"),
        "primary_median": summary.get("primary_median"),
        "secondary_mean": summary.get("secondary_mean"),
        "secondary_median": summary.get("secondary_median"),
        "raw_delta_mean": summary.get("raw_delta_mean"),
        "raw_delta_median": summary.get("raw_delta_median"),
        "improvement_delta_mean": summary.get("improvement_delta_mean"),
        "improvement_delta_median": summary.get("improvement_delta_median"),
        "wins": summary.get("wins"),
        "losses": summary.get("losses"),
        "ties": summary.get("ties"),
    }


def _candidate_theory_snapshot_path(manifest: dict[str, Any]) -> str | Path | None:
    direct = _optional_str(manifest.get("candidate_theory_snapshot_path"))
    if direct is not None:
        return direct
    output_paths = manifest.get("output_paths")
    if isinstance(output_paths, dict):
        return _optional_str(output_paths.get("candidate_theory_snapshot_yaml"))
    return None


def _candidate_reply_yaml_path(manifest: dict[str, Any]) -> str | Path | None:
    preferred = _optional_str(manifest.get("copied_reply_yaml"))
    if preferred is not None:
        return preferred
    output_paths = manifest.get("output_paths")
    if isinstance(output_paths, dict):
        reply_path = _optional_str(output_paths.get("candidate_reply_yaml"))
        if reply_path is not None:
            return reply_path
    return _optional_str(manifest.get("reply_yaml_path"))


def _applied_changes_path(candidate_run_dir: Path, manifest: dict[str, Any]) -> str | Path:
    output_paths = manifest.get("output_paths")
    if isinstance(output_paths, dict):
        output_value = _optional_str(output_paths.get("applied_changes_jsonl"))
        if output_value is not None:
            return output_value
    return candidate_run_dir / "applied_changes.jsonl"


def _require_existing_file(value: str | Path | None, *, label: str) -> Path:
    if value in (None, ""):
        raise BaselinePromotionError(f"{label} is required.")
    path = _resolve_path(value, label=label)
    if not path.exists():
        raise BaselinePromotionError(f"{label} does not exist: {path}")
    if not path.is_file():
        raise BaselinePromotionError(f"{label} is not a file: {path}")
    return path


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise BaselinePromotionError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise BaselinePromotionError(f"{label} must be a single directory name.")
    return normalized


def _default_evidence_policy() -> dict[str, Any]:
    return {
        "promotion_eligible": False,
        "ineligibility_reasons": [],
        "evaluation_mode": None,
        "metric_scope": None,
        "benchmark_dataset_id": None,
        "benchmark_labels_sha256": None,
        "benchmark_maturity_tier": None,
        "promotion_ready": False,
        "primary_batch_evidence": {},
        "secondary_batch_evidence": {},
    }


def _shared_value(primary: str | None, secondary: str | None) -> str | None:
    if primary is None or secondary is None:
        return None
    if primary != secondary:
        return None
    return primary


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise BaselinePromotionError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise BaselinePromotionError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise BaselinePromotionError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _resolve_path(value: str | Path, *, label: str) -> Path:
    raw_value = str(value).strip()
    if not raw_value:
        raise BaselinePromotionError(f"{label} is required.")
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def _normalize_path_string(value: str | Path | None) -> str | None:
    if value in (None, ""):
        return None
    return str(_resolve_path(value, label="Path"))


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _normalized_reason_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(
        dict.fromkeys(
            reason
            for reason in (_optional_str(item) for item in value)
            if reason is not None
        )
    )


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


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
