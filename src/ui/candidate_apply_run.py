from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any, Callable
from uuid import uuid4

import yaml

from src.agents.reply_parser import parse_structured_reply
from src.agents.revision_validator import (
    GeneratorReplyModel,
    apply_generator_changes,
    validate_applied_candidate_theory,
    validate_generator_reply_payload,
)
from src.config import REPO_ROOT, TheoryConfig, load_runtime_config
from src.eval.benchmark import BATCH_DIRNAME
from src.ui.batch_loader import BatchUiBundle
from src.ui.batch_runner import BatchRunOutcome, BatchRunRequest, BatchRunSummary, run_batch_request
from src.ui.reply_preview import ReviewPacketBundle, preview_candidate_reply
from src.ui.study_provenance import extract_study_source_fields


class CandidateApplyRunError(ValueError):
    """Raised when a candidate reply cannot be materialized and evaluated."""


@dataclass(frozen=True, slots=True)
class CandidateApplyRunRequest:
    candidate_id: str
    output_batch_id: str
    reviewer: str | None
    notes: str | None
    candidate_revision_id: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateApplyRunResult:
    candidate_id: str
    candidate_revision_id: str | None
    comparison_id: str
    packet_id: str
    candidate_dir: Path
    manifest_path: Path
    copied_reply_path: Path
    candidate_theory_snapshot_path: Path
    applied_changes_path: Path
    batch_run_request_path: Path
    batch_run_result_path: Path
    output_batch_id: str
    output_batch_dir: Path
    output_batch_request: BatchRunRequest
    status: str
    selected_metric: str | None
    batch_summary: BatchRunSummary | None
    loaded_secondary_bundle: BatchUiBundle | None
    partial_secondary_bundle: BatchUiBundle | None
    error_message: str | None


def build_candidate_apply_run_request(
    *,
    candidate_id: str,
    output_batch_id: str,
    reviewer: str,
    notes: str,
    candidate_revision_id: str = "",
) -> CandidateApplyRunRequest:
    return CandidateApplyRunRequest(
        candidate_id=_normalize_directory_name(candidate_id, label="Candidate ID"),
        output_batch_id=_normalize_directory_name(output_batch_id, label="Output Batch ID"),
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        candidate_revision_id=_optional_str(candidate_revision_id),
    )


def packet_primary_compatibility_errors(
    packet_bundle: ReviewPacketBundle,
    primary_bundle: BatchUiBundle | None,
) -> list[str]:
    if primary_bundle is None:
        return ["Load the primary/reference batch before applying a candidate reply."]

    errors: list[str] = []
    packet_primary = packet_bundle.manifest.get("primary_batch")
    if not isinstance(packet_primary, dict):
        return errors

    packet_batch_id = _optional_str(packet_primary.get("batch_id"))
    primary_batch_id = _optional_str(getattr(primary_bundle.manifest, "batch_id", None))
    if packet_batch_id is not None and primary_batch_id is not None and packet_batch_id != primary_batch_id:
        errors.append(
            "Review packet primary batch mismatch: "
            f"packet has '{packet_batch_id}', loaded primary batch is '{primary_batch_id}'."
        )

    packet_batch_dir = _optional_str(packet_primary.get("batch_dir"))
    if packet_batch_dir is not None and _normalize_path_string(packet_batch_dir) != _normalize_path_string(primary_bundle.batch_dir):
        errors.append(
            "Review packet primary batch directory does not match the currently loaded primary batch."
        )

    packet_theory_config = _optional_str(packet_primary.get("theory_config"))
    primary_theory_config = _optional_str(getattr(primary_bundle.manifest, "theory_config", None))
    if (
        packet_theory_config is not None
        and primary_theory_config is not None
        and _normalize_path_string(packet_theory_config) != _normalize_path_string(primary_theory_config)
    ):
        errors.append(
            "Review packet primary theory config does not match the currently loaded primary batch."
        )
    return errors


def derive_parity_batch_request(
    *,
    primary_bundle: BatchUiBundle | None,
    candidate_theory_snapshot_path: Path,
    output_batch_id: str,
    runtime_loader: Callable[[], Any] = load_runtime_config,
) -> BatchRunRequest:
    if primary_bundle is None:
        raise CandidateApplyRunError("A primary/reference batch must be loaded before running a candidate batch.")

    seeds_csv_value = _optional_str(getattr(primary_bundle.manifest, "seeds_csv", None))
    if seeds_csv_value is None:
        raise CandidateApplyRunError("The primary batch does not expose a seeds CSV path for parity re-run.")

    seeds_csv_path = _resolve_existing_file(seeds_csv_value, label="Primary batch seeds CSV")

    options = getattr(primary_bundle.manifest, "options", None)
    if options is None:
        raise CandidateApplyRunError("The primary batch does not expose effective batch options for parity re-run.")

    max_references = _required_int_option(options, "max_references")
    max_related = _required_int_option(options, "max_related")
    max_hard_negatives = _required_int_option(options, "max_hard_negatives")
    top_k = _required_int_option(options, "top_k")
    label_source = _optional_str(getattr(options, "label_source", None))
    if label_source is None:
        raise CandidateApplyRunError("The primary batch does not expose label_source for parity re-run.")
    evaluation_mode = _optional_str(getattr(options, "evaluation_mode", None))
    metric_scope = _optional_str(getattr(options, "metric_scope", None))
    benchmark_labels_path = _effective_benchmark_labels_path(options)
    benchmark_dataset_id = _optional_str(getattr(options, "benchmark_dataset_id", None))
    benchmark_labels_sha256 = _optional_str(getattr(options, "benchmark_labels_sha256", None))

    runtime = runtime_loader()
    runs_dir = _optional_str(getattr(runtime, "runs_dir", None))
    if runs_dir is None:
        raise CandidateApplyRunError("Runtime config does not expose a runs_dir for candidate batch execution.")

    fresh_batch_id = _fresh_candidate_batch_id(output_batch_id)
    batch_dir = REPO_ROOT / runs_dir / BATCH_DIRNAME / fresh_batch_id
    if batch_dir.exists():
        raise CandidateApplyRunError(f"Fresh output batch directory already exists: {batch_dir}")

    return BatchRunRequest(
        initial_doi_context="candidate apply/run",
        theory_config_path=candidate_theory_snapshot_path,
        seeds_csv_path=seeds_csv_path,
        batch_id=fresh_batch_id,
        batch_dir=batch_dir,
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
        top_k=top_k,
        label_source=label_source,
        evaluation_mode=evaluation_mode or "silver_provenance_regression",
        benchmark_labels_path=benchmark_labels_path,
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=benchmark_labels_sha256,
        metric_scope=metric_scope or "local_corpus_ranking",
        refresh=bool(getattr(options, "refresh", False)),
    )


def validate_candidate_reply_for_apply(
    *,
    packet_bundle: ReviewPacketBundle,
    reply_path: str | Path,
    candidate_revision_id: str | None = None,
) -> tuple[GeneratorReplyModel, Path]:
    preview_result = preview_candidate_reply(
        packet_bundle=packet_bundle,
        reply_path=reply_path,
        candidate_revision_id=candidate_revision_id,
    )
    if preview_result.state == "template_only":
        raise CandidateApplyRunError(preview_result.errors[0] if preview_result.errors else "Template-only replies cannot be applied.")
    if preview_result.state != "valid":
        message = preview_result.errors[0] if preview_result.errors else "Candidate reply preview is not valid."
        raise CandidateApplyRunError(message)

    reply_text = preview_result.reply_path.read_text(encoding="utf-8")
    payload = parse_structured_reply(reply_text)
    validated_reply = validate_generator_reply_payload(
        payload=payload,
        theory_payload=packet_bundle.baseline_theory_payload,
        candidate_revision_id=candidate_revision_id,
    )
    return validated_reply, preview_result.reply_path


def build_applied_changes_rows(
    *,
    packet_bundle: ReviewPacketBundle,
    validated_reply: GeneratorReplyModel,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for change in validated_reply.changes:
        baseline_leaf = packet_bundle.baseline_scalar_leaves.get(change.path)
        baseline_value = baseline_leaf.value if baseline_leaf is not None else None
        numeric_delta = None
        if baseline_leaf is not None:
            numeric_delta = float(change.value) - float(baseline_leaf.value)
        rows.append(
            {
                "path": change.path,
                "baseline_value": baseline_value,
                "proposed_value": change.value,
                "numeric_delta": numeric_delta,
                "status": "applied",
            }
        )
    return rows


def build_batch_run_request_payload(
    request: BatchRunRequest,
    *,
    requested_output_batch_id: str | None = None,
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "requested_output_batch_id": requested_output_batch_id,
        "seeds_csv": _serialize_path(request.seeds_csv_path),
        "theory_config": _serialize_path(request.theory_config_path),
        "batch_id": request.batch_id,
        "batch_dir": _serialize_path(request.batch_dir),
        "max_references": request.max_references,
        "max_related": request.max_related,
        "max_hard_negatives": request.max_hard_negatives,
        "top_k": request.top_k,
        "label_source": request.label_source,
        "evaluation_mode": request.evaluation_mode,
        "metric_scope": request.metric_scope,
        "benchmark_labels_path": _serialize_path(request.benchmark_labels_path),
        "benchmark_dataset_id": request.benchmark_dataset_id,
        "benchmark_labels_sha256": request.benchmark_labels_sha256,
        "evidence_tier": _evidence_tier(request.evaluation_mode),
        "promotion_eligible": _promotion_eligible_from_request(request),
        "refresh": request.refresh,
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload.update(dict(study_source_context))
    return payload


def build_batch_run_result_payload(
    *,
    request: BatchRunRequest,
    outcome: BatchRunOutcome | None = None,
    error: Exception | None = None,
) -> dict[str, Any]:
    summary = outcome.summary if outcome is not None else None
    output_paths = summary.output_paths if summary is not None and summary.output_paths is not None else {}
    return {
        "batch_id": request.batch_id,
        "batch_dir": _serialize_path(request.batch_dir),
        "status": "completed" if outcome is not None and outcome.success else "failed",
        "seed_count": summary.seed_count if summary is not None else None,
        "completed_seed_count": summary.completed_seed_count if summary is not None else None,
        "failed_seed_count": summary.failed_seed_count if summary is not None else None,
        "aggregate_summary_path": output_paths.get("aggregate_summary_json"),
        "evaluation_mode": _optional_str(getattr(request, "evaluation_mode", None)),
        "metric_scope": _optional_str(getattr(request, "metric_scope", None)),
        "benchmark_labels_path": _serialize_path(getattr(request, "benchmark_labels_path", None)),
        "benchmark_dataset_id": _optional_str(getattr(request, "benchmark_dataset_id", None)),
        "benchmark_labels_sha256": _optional_str(getattr(request, "benchmark_labels_sha256", None)),
        "evidence_tier": _evidence_tier(getattr(request, "evaluation_mode", None)),
        "promotion_eligible": _promotion_eligible_from_request(request),
        "error_type": type(error).__name__ if error is not None else None,
        "error_message": str(error) if error is not None else (outcome.error_message if outcome is not None else None),
    }


def build_candidate_apply_manifest_payload(
    *,
    request: CandidateApplyRunRequest,
    candidate_dir: Path,
    created_at: str,
    packet_bundle: ReviewPacketBundle,
    source_reply_path: Path,
    copied_reply_path: Path,
    candidate_theory_snapshot_path: Path,
    output_batch_request: BatchRunRequest,
    output_paths: dict[str, str],
    status: str,
    primary_bundle: BatchUiBundle,
    selected_metric: str | None,
    error: Exception | None = None,
    study_source_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "candidate_id": request.candidate_id,
        "candidate_revision_id": request.candidate_revision_id,
        "candidate_dir": _serialize_path(candidate_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "packet_id": packet_bundle.manifest.get("packet_id"),
        "comparison_id": packet_bundle.manifest.get("comparison_id"),
        "reply_yaml_path": _serialize_path(source_reply_path),
        "copied_reply_yaml": _serialize_path(copied_reply_path),
        "baseline_theory_snapshot_source": _serialize_path(packet_bundle.baseline_snapshot_path),
        "candidate_theory_snapshot_path": _serialize_path(candidate_theory_snapshot_path),
        "requested_output_batch_id": request.output_batch_id,
        "output_batch_id": output_batch_request.batch_id,
        "output_batch_dir": _serialize_path(output_batch_request.batch_dir),
        "status": status,
        "selected_metric_context": selected_metric,
        "evaluation_mode": output_batch_request.evaluation_mode,
        "metric_scope": output_batch_request.metric_scope,
        "benchmark_labels_path": _serialize_path(output_batch_request.benchmark_labels_path),
        "benchmark_dataset_id": output_batch_request.benchmark_dataset_id,
        "benchmark_labels_sha256": output_batch_request.benchmark_labels_sha256,
        "evidence_tier": _evidence_tier(output_batch_request.evaluation_mode),
        "promotion_eligible": _promotion_eligible_from_request(output_batch_request),
        "source_primary_batch": {
            "batch_id": getattr(primary_bundle.manifest, "batch_id", None),
            "batch_dir": _serialize_path(primary_bundle.batch_dir),
            "theory_config": _serialize_path(getattr(primary_bundle.manifest, "theory_config", None)),
            "seeds_csv": _serialize_path(getattr(primary_bundle.manifest, "seeds_csv", None)),
            "options": _batch_options_payload(getattr(primary_bundle.manifest, "options", None)),
        },
        "output_paths": dict(output_paths),
        "error_type": type(error).__name__ if error is not None else None,
        "error_message": str(error) if error is not None else None,
    }
    if isinstance(study_source_context, dict) and study_source_context:
        payload.update(dict(study_source_context))
    return payload


def run_candidate_apply_and_batch(
    *,
    request: CandidateApplyRunRequest,
    packet_bundle: ReviewPacketBundle,
    reply_path: str | Path,
    primary_bundle: BatchUiBundle | None,
    previous_secondary_bundle: BatchUiBundle | None = None,
    selected_metric: str | None = None,
    batch_runner: Callable[..., BatchRunOutcome] = run_batch_request,
) -> CandidateApplyRunResult:
    compatibility_errors = packet_primary_compatibility_errors(packet_bundle, primary_bundle)
    if compatibility_errors:
        raise CandidateApplyRunError(compatibility_errors[0])
    if primary_bundle is None:
        raise CandidateApplyRunError("A primary/reference batch must be loaded before applying a candidate reply.")

    comparison_id = _required_manifest_text(packet_bundle.manifest, "comparison_id")
    packet_id = _required_manifest_text(packet_bundle.manifest, "packet_id")
    resolved_reply_path = _resolve_packet_reply_path(
        packet_bundle=packet_bundle,
        reply_path=reply_path,
    )

    candidate_runs_dir = packet_bundle.packet_dir / "candidate_runs"
    candidate_dir = candidate_runs_dir / request.candidate_id
    if candidate_dir.exists():
        raise CandidateApplyRunError(f"Candidate directory already exists: {candidate_dir}")

    candidate_theory_snapshot_path = candidate_dir / "candidate_theory_snapshot.yaml"
    batch_request = derive_parity_batch_request(
        primary_bundle=primary_bundle,
        candidate_theory_snapshot_path=candidate_theory_snapshot_path,
        output_batch_id=request.output_batch_id,
    )

    created_at = _utc_timestamp()
    manifest_path = candidate_dir / "candidate_apply_manifest.json"
    copied_reply_path = candidate_dir / "candidate_reply.yaml"
    applied_changes_path = candidate_dir / "applied_changes.jsonl"
    batch_run_request_path = candidate_dir / "batch_run_request.json"
    batch_run_result_path = candidate_dir / "batch_run_result.json"
    output_paths = {
        "candidate_apply_manifest_json": _serialize_path(manifest_path),
        "candidate_reply_yaml": _serialize_path(copied_reply_path),
        "candidate_theory_snapshot_yaml": _serialize_path(candidate_theory_snapshot_path),
        "applied_changes_jsonl": _serialize_path(applied_changes_path),
        "batch_run_request_json": _serialize_path(batch_run_request_path),
        "batch_run_result_json": _serialize_path(batch_run_result_path),
    }
    study_source_context = extract_study_source_fields(packet_bundle.manifest)

    candidate_dir.mkdir(parents=True, exist_ok=False)
    _write_json(
        manifest_path,
        build_candidate_apply_manifest_payload(
            request=request,
            candidate_dir=candidate_dir,
            created_at=created_at,
            packet_bundle=packet_bundle,
            source_reply_path=resolved_reply_path,
            copied_reply_path=copied_reply_path,
            candidate_theory_snapshot_path=candidate_theory_snapshot_path,
            output_batch_request=batch_request,
            output_paths=output_paths,
            status="running",
            primary_bundle=primary_bundle,
            selected_metric=selected_metric,
            study_source_context=study_source_context,
        ),
    )

    try:
        copyfile(resolved_reply_path, copied_reply_path)
        validated_reply = validate_generator_reply_payload(
            payload=parse_structured_reply(copied_reply_path.read_text(encoding="utf-8")),
            theory_payload=packet_bundle.baseline_theory_payload,
            candidate_revision_id=request.candidate_revision_id,
        )
        request = CandidateApplyRunRequest(
            candidate_id=request.candidate_id,
            output_batch_id=request.output_batch_id,
            reviewer=request.reviewer,
            notes=request.notes,
            candidate_revision_id=validated_reply.candidate_revision_id,
        )
        updated_payload = apply_generator_changes(
            theory_payload=packet_bundle.baseline_theory_payload,
            validated_reply=validated_reply,
        )
        validate_applied_candidate_theory(
            baseline_payload=packet_bundle.baseline_theory_payload,
            updated_payload=updated_payload,
            validated_reply=validated_reply,
        )
        validated_theory = TheoryConfig.model_validate(updated_payload)
        candidate_theory_snapshot_path.write_text(
            yaml.safe_dump(
                validated_theory.model_dump(mode="json", exclude_defaults=True),
                sort_keys=False,
                allow_unicode=False,
            ),
            encoding="utf-8",
        )
        _write_jsonl(
            applied_changes_path,
            build_applied_changes_rows(packet_bundle=packet_bundle, validated_reply=validated_reply),
        )
        _write_json(
            batch_run_request_path,
            build_batch_run_request_payload(
                batch_request,
                requested_output_batch_id=request.output_batch_id,
                study_source_context=study_source_context,
            ),
        )
    except Exception as exc:
        _write_json(batch_run_result_path, build_batch_run_result_payload(request=batch_request, error=exc))
        _write_json(
            manifest_path,
            build_candidate_apply_manifest_payload(
                request=request,
                candidate_dir=candidate_dir,
                created_at=created_at,
                packet_bundle=packet_bundle,
                source_reply_path=resolved_reply_path,
                copied_reply_path=copied_reply_path,
                candidate_theory_snapshot_path=candidate_theory_snapshot_path,
                output_batch_request=batch_request,
                output_paths=output_paths,
                status="failed",
                primary_bundle=primary_bundle,
                selected_metric=selected_metric,
                error=exc,
                study_source_context=study_source_context,
            ),
        )
        raise CandidateApplyRunError(str(exc)) from exc

    outcome = batch_runner(batch_request, previous_bundle=previous_secondary_bundle)
    result_error = RuntimeError(outcome.error_message) if outcome.error_message else None
    _write_json(
        batch_run_result_path,
        build_batch_run_result_payload(request=batch_request, outcome=outcome, error=result_error),
    )
    _write_json(
        manifest_path,
        build_candidate_apply_manifest_payload(
            request=request,
            candidate_dir=candidate_dir,
            created_at=created_at,
            packet_bundle=packet_bundle,
            source_reply_path=resolved_reply_path,
            copied_reply_path=copied_reply_path,
            candidate_theory_snapshot_path=candidate_theory_snapshot_path,
            output_batch_request=batch_request,
            output_paths=output_paths,
            status="completed" if outcome.success else "failed",
            primary_bundle=primary_bundle,
            selected_metric=selected_metric,
            error=result_error,
            study_source_context=study_source_context,
        ),
    )
    return CandidateApplyRunResult(
        candidate_id=request.candidate_id,
        candidate_revision_id=request.candidate_revision_id,
        comparison_id=comparison_id,
        packet_id=packet_id,
        candidate_dir=candidate_dir,
        manifest_path=manifest_path,
        copied_reply_path=copied_reply_path,
        candidate_theory_snapshot_path=candidate_theory_snapshot_path,
        applied_changes_path=applied_changes_path,
        batch_run_request_path=batch_run_request_path,
        batch_run_result_path=batch_run_result_path,
        output_batch_id=batch_request.batch_id,
        output_batch_dir=batch_request.batch_dir,
        output_batch_request=batch_request,
        status="completed" if outcome.success else "failed",
        selected_metric=selected_metric,
        batch_summary=outcome.summary,
        loaded_secondary_bundle=outcome.loaded_bundle if outcome.success else None,
        partial_secondary_bundle=outcome.partial_bundle,
        error_message=outcome.error_message,
    )


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise CandidateApplyRunError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise CandidateApplyRunError(f"{label} must be a single directory name.")
    return normalized


def _required_manifest_text(payload: dict[str, Any], field_name: str) -> str:
    text = _optional_str(payload.get(field_name))
    if text is None:
        raise CandidateApplyRunError(f"Review packet manifest is missing required field: {field_name}")
    return text


def _required_int_option(options: Any, field_name: str) -> int:
    value = getattr(options, field_name, None)
    if value is None:
        raise CandidateApplyRunError(f"The primary batch option '{field_name}' is required for parity re-run.")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CandidateApplyRunError(
            f"The primary batch option '{field_name}' must be an integer."
        ) from exc


def _resolve_existing_file(value: str | Path, *, label: str) -> Path:
    candidate = _resolve_path(value)
    if not candidate.exists():
        raise CandidateApplyRunError(f"{label} does not exist: {candidate}")
    if not candidate.is_file():
        raise CandidateApplyRunError(f"{label} is not a file: {candidate}")
    return candidate


def _resolve_packet_reply_path(
    *,
    packet_bundle: ReviewPacketBundle,
    reply_path: str | Path,
) -> Path:
    raw_value = str(reply_path).strip()
    if not raw_value:
        raise CandidateApplyRunError("Candidate reply path is required.")
    candidate = Path(raw_value.replace("\\", "/")).expanduser()
    if candidate.is_absolute():
        resolved = candidate
    else:
        relative_candidate = (packet_bundle.packet_dir / candidate).resolve()
        resolved = relative_candidate if relative_candidate.exists() else _resolve_path(candidate)
    if not resolved.exists():
        raise CandidateApplyRunError(f"Candidate reply does not exist: {resolved}")
    if not resolved.is_file():
        raise CandidateApplyRunError(f"Candidate reply is not a file: {resolved}")
    return resolved


def _resolve_path(value: str | Path) -> Path:
    candidate = Path(str(value).strip().replace("\\", "/")).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _normalize_path_string(value: str | Path) -> str:
    return str(_resolve_path(value))


def _batch_options_payload(options: Any) -> dict[str, Any] | None:
    if options is None:
        return None
    return {
        "max_references": getattr(options, "max_references", None),
        "max_related": getattr(options, "max_related", None),
        "max_hard_negatives": getattr(options, "max_hard_negatives", None),
        "top_k": getattr(options, "top_k", None),
        "label_source": getattr(options, "label_source", None),
        "evaluation_mode": getattr(options, "evaluation_mode", None),
        "metric_scope": getattr(options, "metric_scope", None),
        "benchmark_labels_path": _serialize_path(_effective_benchmark_labels_path(options)),
        "benchmark_dataset_id": getattr(options, "benchmark_dataset_id", None),
        "benchmark_labels_sha256": getattr(options, "benchmark_labels_sha256", None),
        "evidence_tier": getattr(options, "evidence_tier", None) or _evidence_tier(getattr(options, "evaluation_mode", None)),
        "refresh": getattr(options, "refresh", None),
    }


def _effective_benchmark_labels_path(options: Any) -> Path | None:
    snapshot_path = _optional_str(getattr(options, "benchmark_labels_snapshot_path", None))
    if snapshot_path is not None:
        return _resolve_existing_file(snapshot_path, label="Primary batch benchmark labels snapshot")
    labels_path = _optional_str(getattr(options, "benchmark_labels_path", None))
    if labels_path is None:
        return None
    return _resolve_existing_file(labels_path, label="Primary batch benchmark labels")


def _evidence_tier(evaluation_mode: object) -> str | None:
    return _optional_str(evaluation_mode)


def _promotion_eligible_from_request(request: BatchRunRequest) -> bool:
    return (
        _optional_str(getattr(request, "evaluation_mode", None)) == "independent_benchmark"
        and _optional_str(getattr(request, "benchmark_dataset_id", None)) is not None
        and _optional_str(getattr(request, "benchmark_labels_sha256", None)) is not None
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


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fresh_candidate_batch_id(requested_batch_id: str) -> str:
    base_id = _normalize_directory_name(requested_batch_id, label="Output Batch ID")
    suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = uuid4().hex[:8]
    return f"{base_id}__{suffix}__{token}"
