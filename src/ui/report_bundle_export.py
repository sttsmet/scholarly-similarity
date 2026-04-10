from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any

from src.config import REPO_ROOT
from src.ui.run_context import build_run_context_summary, load_run_context_if_present
from src.ui.study_provenance import extract_study_source_fields


EXPECTED_REPORT_ARTIFACT_TYPES = (
    "primary_batch",
    "secondary_batch",
    "comparison",
    "review_packet",
    "candidate_run",
    "reeval_outcome",
    "accepted_baseline",
    "benchmark_preset",
    "evaluation_preset",
    "launch_profile",
)
KNOWN_BATCH_METRICS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
)


class ReportBundleExportError(ValueError):
    """Raised when a report bundle cannot be created."""


@dataclass(frozen=True, slots=True)
class ReportBundleExportRequest:
    report_id: str
    reviewer: str | None
    notes: str | None
    include_raw_copied_artifacts: bool
    include_markdown_summary: bool


@dataclass(frozen=True, slots=True)
class ReportArtifactContext:
    artifact_type: str
    metadata: dict[str, Any]
    snapshot: dict[str, Any]
    copy_specs: tuple[tuple[str, Path], ...]
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReportBundleExportResult:
    report_id: str
    report_dir: Path
    manifest_path: Path
    context_snapshot_path: Path
    included_artifacts_path: Path
    summary_path: Path | None
    artifacts_dir: Path | None
    warnings: tuple[str, ...]


def build_report_export_request(
    *,
    report_id: str,
    reviewer: str,
    notes: str,
    include_raw_copied_artifacts: bool,
    include_markdown_summary: bool,
) -> ReportBundleExportRequest:
    normalized_report_id = _normalize_directory_name(report_id, label="Report ID")
    return ReportBundleExportRequest(
        report_id=normalized_report_id,
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        include_raw_copied_artifacts=bool(include_raw_copied_artifacts),
        include_markdown_summary=bool(include_markdown_summary),
    )


def build_report_manifest_payload(
    *,
    request: ReportBundleExportRequest,
    report_dir: str | Path,
    created_at: str,
    context: dict[str, ReportArtifactContext | None],
    output_paths: dict[str, str | None],
    warnings: list[str],
) -> dict[str, Any]:
    included_types = [artifact_type for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES if context.get(artifact_type) is not None]
    missing_types = [artifact_type for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES if context.get(artifact_type) is None]
    payload: dict[str, Any] = {
        "report_id": request.report_id,
        "report_dir": _serialize_path(report_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "included_artifact_types": included_types,
        "missing_artifact_types": missing_types,
        "warnings": list(warnings),
        "output_paths": dict(output_paths),
    }
    for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES:
        payload[artifact_type] = context.get(artifact_type).metadata if context.get(artifact_type) is not None else None
    return payload


def build_context_snapshot_payload(
    context: dict[str, ReportArtifactContext | None],
    *,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "warnings": list(warnings),
        **{
            artifact_type: context_entry.snapshot if context_entry is not None else None
            for artifact_type, context_entry in context.items()
        },
    }


def build_report_summary_markdown(
    *,
    report_id: str,
    reviewer: str | None,
    notes: str | None,
    created_at: str,
    context: dict[str, ReportArtifactContext | None],
    warnings: list[str],
) -> str:
    primary_batch = _metadata(context.get("primary_batch"))
    secondary_batch = _metadata(context.get("secondary_batch"))
    comparison = _metadata(context.get("comparison"))
    accepted_baseline = _metadata(context.get("accepted_baseline"))
    benchmark_preset = _metadata(context.get("benchmark_preset"))
    evaluation_preset = _metadata(context.get("evaluation_preset"))
    launch_profile = _metadata(context.get("launch_profile"))

    lines = [
        f"# Research Report Bundle: {report_id}",
        "",
        f"- Created at: `{created_at}`",
        f"- Reviewer: `{reviewer or 'n/a'}`",
        f"- Notes: `{notes or 'n/a'}`",
        "",
    ]
    if primary_batch:
        lines.extend(
            [
                "## Primary Batch",
                f"- Batch ID: `{primary_batch.get('batch_id') or 'n/a'}`",
                f"- Batch Dir: `{primary_batch.get('batch_dir') or 'n/a'}`",
                f"- Theory Config: `{primary_batch.get('theory_config') or 'n/a'}`",
                f"- Status: `{primary_batch.get('status') or 'n/a'}`",
                f"- Seeds: `{primary_batch.get('seed_count') if primary_batch.get('seed_count') is not None else 'n/a'}`",
            ]
        )
        primary_run_context = primary_batch.get("run_context") if isinstance(primary_batch.get("run_context"), dict) else {}
        if primary_run_context:
            lines.append(f"- Launch Source: `{primary_run_context.get('launch_source_type') or 'n/a'}`")
            if primary_run_context.get("accepted_baseline_id") is not None:
                lines.append(f"- Accepted Baseline: `{primary_run_context.get('accepted_baseline_id')}`")
            if primary_run_context.get("benchmark_preset_id") is not None:
                lines.append(f"- Benchmark Preset: `{primary_run_context.get('benchmark_preset_id')}`")
            if primary_run_context.get("eval_preset_id") is not None:
                lines.append(f"- Evaluation Preset: `{primary_run_context.get('eval_preset_id')}`")
            if primary_run_context.get("launch_profile_id") is not None:
                lines.append(f"- Launch Profile: `{primary_run_context.get('launch_profile_id')}`")
        lines.extend(_aggregate_metric_markdown(primary_batch.get("aggregate_metrics"), title="Primary Aggregate Metrics"))
    if secondary_batch:
        lines.extend(
            [
                "",
                "## Secondary Batch",
                f"- Batch ID: `{secondary_batch.get('batch_id') or 'n/a'}`",
                f"- Batch Dir: `{secondary_batch.get('batch_dir') or 'n/a'}`",
                f"- Theory Config: `{secondary_batch.get('theory_config') or 'n/a'}`",
                f"- Status: `{secondary_batch.get('status') or 'n/a'}`",
            ]
        )
        secondary_run_context = (
            secondary_batch.get("run_context") if isinstance(secondary_batch.get("run_context"), dict) else {}
        )
        if secondary_run_context:
            lines.append(f"- Launch Source: `{secondary_run_context.get('launch_source_type') or 'n/a'}`")
        lines.extend(_aggregate_metric_markdown(secondary_batch.get("aggregate_metrics"), title="Secondary Aggregate Metrics"))
    if comparison:
        lines.extend(
            [
                "",
                "## Comparison",
                f"- Comparison ID: `{comparison.get('comparison_id') or 'n/a'}`",
                f"- Selected Metric: `{comparison.get('selected_metric') or 'n/a'}`",
                f"- Decision Status: `{comparison.get('decision_status') or 'n/a'}`",
                f"- Common DOI Count: `{comparison.get('common_doi_count') if comparison.get('common_doi_count') is not None else 'n/a'}`",
                f"- Common Completed Seed Count: `{comparison.get('common_completed_seed_count') if comparison.get('common_completed_seed_count') is not None else 'n/a'}`",
            ]
        )
    if accepted_baseline:
        lines.extend(
            [
                "",
                "## Accepted Baseline",
                f"- Baseline ID: `{accepted_baseline.get('baseline_id') or 'n/a'}`",
                f"- Selected Metric: `{accepted_baseline.get('selected_metric') or 'n/a'}`",
                f"- Accepted Theory Snapshot: `{accepted_baseline.get('accepted_theory_snapshot_path') or 'n/a'}`",
            ]
        )
    if benchmark_preset or evaluation_preset or launch_profile:
        lines.extend(["", "## Selected Presets / Profile"])
        if benchmark_preset:
            lines.append(f"- Benchmark Preset: `{benchmark_preset.get('benchmark_preset_id') or benchmark_preset.get('preset_id') or 'n/a'}`")
        if evaluation_preset:
            lines.append(f"- Evaluation Preset: `{evaluation_preset.get('eval_preset_id') or evaluation_preset.get('preset_id') or 'n/a'}`")
        if launch_profile:
            lines.append(f"- Launch Profile: `{launch_profile.get('launch_profile_id') or launch_profile.get('preset_id') or 'n/a'}`")
    if warnings:
        lines.extend(["", "## Warnings"])
        for warning in warnings:
            lines.append(f"- {warning}")
    return "\n".join(lines) + "\n"


def export_report_bundle(
    *,
    base_dir: str | Path,
    request: ReportBundleExportRequest,
    context: dict[str, ReportArtifactContext | None],
) -> ReportBundleExportResult:
    if context.get("primary_batch") is None:
        raise ReportBundleExportError("A primary batch must be loaded before exporting a report bundle.")

    report_root = Path(base_dir)
    report_root.mkdir(parents=True, exist_ok=True)
    report_dir = report_root / request.report_id
    if report_dir.exists():
        raise ReportBundleExportError(f"Report directory already exists: {report_dir}")

    created_at = _utc_timestamp()
    warnings = _context_warnings(context)
    report_dir.mkdir(parents=False, exist_ok=False)
    manifest_path = report_dir / "report_manifest.json"
    context_snapshot_path = report_dir / "context_snapshot.json"
    included_artifacts_path = report_dir / "included_artifacts.json"
    summary_path = report_dir / "report_summary.md" if request.include_markdown_summary else None
    artifacts_dir = report_dir / "artifacts" if request.include_raw_copied_artifacts else None

    copied_listing: dict[str, list[dict[str, str | None]]] = {}
    missing_listing: dict[str, list[dict[str, str | None]]] = {}
    if artifacts_dir is not None:
        artifacts_dir.mkdir(parents=False, exist_ok=False)
        for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES:
            context_entry = context.get(artifact_type)
            if context_entry is None:
                continue
            for destination_name, source_path in context_entry.copy_specs:
                if source_path.exists() and source_path.is_file():
                    destination_path = artifacts_dir / destination_name
                    copyfile(source_path, destination_path)
                    copied_listing.setdefault(artifact_type, []).append(
                        {
                            "source": _serialize_path(source_path),
                            "destination": _serialize_path(destination_path),
                        }
                    )
                else:
                    missing_listing.setdefault(artifact_type, []).append(
                        {
                            "source": _serialize_path(source_path),
                            "destination": _serialize_path(artifacts_dir / destination_name),
                        }
                    )

    included_artifacts_payload = {
        "raw_artifact_copying_enabled": request.include_raw_copied_artifacts,
        "copied": copied_listing,
        "missing": missing_listing,
        "warnings": list(warnings),
    }
    output_paths = {
        "report_manifest_json": _serialize_path(manifest_path),
        "context_snapshot_json": _serialize_path(context_snapshot_path),
        "included_artifacts_json": _serialize_path(included_artifacts_path),
        "report_summary_md": _serialize_path(summary_path) if summary_path is not None else None,
        "artifacts_dir": _serialize_path(artifacts_dir) if artifacts_dir is not None else None,
    }
    manifest_payload = build_report_manifest_payload(
        request=request,
        report_dir=report_dir,
        created_at=created_at,
        context=context,
        output_paths=output_paths,
        warnings=warnings,
    )
    context_snapshot_payload = build_context_snapshot_payload(context, warnings=warnings)

    _write_json(manifest_path, manifest_payload)
    _write_json(context_snapshot_path, context_snapshot_payload)
    _write_json(included_artifacts_path, included_artifacts_payload)
    if summary_path is not None:
        summary_path.write_text(
            build_report_summary_markdown(
                report_id=request.report_id,
                reviewer=request.reviewer,
                notes=request.notes,
                created_at=created_at,
                context=context,
                warnings=warnings,
            ),
            encoding="utf-8",
        )

    return ReportBundleExportResult(
        report_id=request.report_id,
        report_dir=report_dir,
        manifest_path=manifest_path,
        context_snapshot_path=context_snapshot_path,
        included_artifacts_path=included_artifacts_path,
        summary_path=summary_path,
        artifacts_dir=artifacts_dir,
        warnings=tuple(warnings),
    )


def build_batch_report_context(
    *,
    role: str,
    bundle: Any,
) -> ReportArtifactContext:
    batch_dir = Path(getattr(bundle, "batch_dir", ""))
    manifest = getattr(bundle, "manifest", None)
    aggregate_summary = getattr(bundle, "aggregate_summary", None)
    if manifest is None:
        raise ReportBundleExportError(f"{role.title()} batch context is missing a manifest object.")

    warnings: list[str] = []
    run_context_payload, run_context_warning = load_run_context_if_present(batch_dir)
    if run_context_warning:
        warnings.append(run_context_warning)
    run_context_summary = (
        build_run_context_summary(run_context_payload)
        if isinstance(run_context_payload, dict)
        else None
    )

    metadata = {
        "batch_id": getattr(manifest, "batch_id", None),
        "batch_dir": _serialize_path(batch_dir),
        "theory_config": getattr(manifest, "theory_config", None),
        "seeds_csv": getattr(manifest, "seeds_csv", None),
        "status": getattr(manifest, "status", None),
        "seed_count": getattr(manifest, "seed_count", None),
        "completed_seed_count": getattr(manifest, "completed_seed_count", None),
        "failed_seed_count": getattr(manifest, "failed_seed_count", None),
        "ranking_metric": getattr(aggregate_summary, "ranking_metric", None),
        "aggregate_metrics": _aggregate_metrics_payload(aggregate_summary),
        "run_context": run_context_summary,
    }
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(batch_dir / "batch_manifest.json"),
        "aggregate_summary_path": _serialize_path(batch_dir / "aggregate_summary.json"),
        "seed_table_path": _serialize_path(batch_dir / "seed_table.jsonl"),
        "run_context_path": _serialize_path(batch_dir / "run_context.json"),
        "run_context_payload": run_context_payload,
    }
    return ReportArtifactContext(
        artifact_type=f"{role}_batch",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            (f"{role}_batch_manifest.json", batch_dir / "batch_manifest.json"),
            (f"{role}_aggregate_summary.json", batch_dir / "aggregate_summary.json"),
            (f"{role}_seed_table.jsonl", batch_dir / "seed_table.jsonl"),
            (f"{role}_run_context.json", batch_dir / "run_context.json"),
        ),
        warnings=tuple(warnings),
    )


def build_comparison_report_context(comparison_dir: str | Path) -> ReportArtifactContext:
    resolved_dir = _resolve_existing_dir(comparison_dir, label="Comparison directory")
    manifest_path = resolved_dir / "comparison_manifest.json"
    decision_record_path = resolved_dir / "decision_record.json"
    paired_seed_table_path = resolved_dir / "paired_seed_table.jsonl"

    warnings: list[str] = []
    manifest = _safe_load_json_object(manifest_path, warnings, label="comparison manifest")
    decision_record = _safe_load_json_object(
        decision_record_path,
        warnings,
        label="comparison decision record",
    )
    metadata = {
        "comparison_id": _first_nonempty(
            _dict_value(manifest, "comparison_id"),
            _dict_value(decision_record, "comparison_id"),
            resolved_dir.name,
        ),
        "comparison_dir": _serialize_path(resolved_dir),
        "selected_metric": _dict_value(manifest, "selected_comparison_metric"),
        "decision_status": _dict_value(decision_record, "decision_status"),
        "common_doi_count": _dict_value(manifest, "common_doi_count"),
        "common_completed_seed_count": _dict_value(manifest, "common_completed_seed_count"),
        "primary_batch_id": _dict_value(_dict_value(manifest, "primary_batch"), "batch_id"),
        "secondary_batch_id": _dict_value(_dict_value(manifest, "secondary_batch"), "batch_id"),
    }
    metadata.update(extract_study_source_fields(decision_record, manifest, require_active=True))
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(manifest_path),
        "decision_record_path": _serialize_path(decision_record_path),
        "paired_seed_table_path": _serialize_path(paired_seed_table_path),
    }
    return ReportArtifactContext(
        artifact_type="comparison",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            ("comparison_manifest.json", manifest_path),
            ("comparison_decision_record.json", decision_record_path),
            ("paired_seed_table.jsonl", paired_seed_table_path),
        ),
        warnings=tuple(warnings),
    )


def build_review_packet_report_context(packet_dir: str | Path) -> ReportArtifactContext:
    resolved_dir = _resolve_existing_dir(packet_dir, label="Review packet directory")
    manifest_path = resolved_dir / "review_packet_manifest.json"
    evidence_summary_path = resolved_dir / "evidence_summary.json"
    allowed_revision_paths_path = resolved_dir / "allowed_revision_paths.json"
    baseline_snapshot_path = resolved_dir / "baseline_theory_snapshot.yaml"

    warnings: list[str] = []
    manifest = _safe_load_json_object(manifest_path, warnings, label="review packet manifest")
    metadata = {
        "packet_id": _first_nonempty(_dict_value(manifest, "packet_id"), resolved_dir.name),
        "comparison_id": _dict_value(manifest, "comparison_id"),
        "reviewer": _dict_value(manifest, "reviewer"),
        "selected_metric": _dict_value(manifest, "selected_packet_metric"),
        "primary_batch_id": _dict_value(_dict_value(manifest, "primary_batch"), "batch_id"),
        "secondary_batch_id": _dict_value(_dict_value(manifest, "secondary_batch"), "batch_id"),
        "packet_dir": _serialize_path(resolved_dir),
    }
    metadata.update(extract_study_source_fields(manifest))
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(manifest_path),
        "evidence_summary_path": _serialize_path(evidence_summary_path),
        "allowed_revision_paths_path": _serialize_path(allowed_revision_paths_path),
        "baseline_theory_snapshot_path": _serialize_path(baseline_snapshot_path),
    }
    return ReportArtifactContext(
        artifact_type="review_packet",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            ("review_packet_manifest.json", manifest_path),
            ("evidence_summary.json", evidence_summary_path),
            ("allowed_revision_paths.json", allowed_revision_paths_path),
            ("baseline_theory_snapshot.yaml", baseline_snapshot_path),
        ),
        warnings=tuple(warnings),
    )


def build_candidate_run_report_context(candidate_run_dir: str | Path) -> ReportArtifactContext:
    resolved_dir = _resolve_existing_dir(candidate_run_dir, label="Candidate run directory")
    manifest_path = resolved_dir / "candidate_apply_manifest.json"
    reply_path = resolved_dir / "candidate_reply.yaml"
    candidate_theory_snapshot_path = resolved_dir / "candidate_theory_snapshot.yaml"
    applied_changes_path = resolved_dir / "applied_changes.jsonl"
    batch_run_request_path = resolved_dir / "batch_run_request.json"
    batch_run_result_path = resolved_dir / "batch_run_result.json"

    warnings: list[str] = []
    manifest = _safe_load_json_object(manifest_path, warnings, label="candidate apply manifest")
    batch_run_result = _safe_load_json_object(batch_run_result_path, warnings, label="candidate batch result")
    metadata = {
        "candidate_id": _first_nonempty(_dict_value(manifest, "candidate_id"), resolved_dir.name),
        "comparison_id": _dict_value(manifest, "comparison_id"),
        "packet_id": _dict_value(manifest, "packet_id"),
        "output_batch_id": _first_nonempty(
            _dict_value(manifest, "output_batch_id"),
            _dict_value(batch_run_result, "batch_id"),
        ),
        "status": _first_nonempty(_dict_value(manifest, "status"), _dict_value(batch_run_result, "status")),
        "reply_yaml_path": _serialize_path(reply_path),
        "candidate_theory_snapshot_path": _serialize_path(candidate_theory_snapshot_path),
        "candidate_run_dir": _serialize_path(resolved_dir),
    }
    metadata.update(extract_study_source_fields(manifest))
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(manifest_path),
        "applied_changes_path": _serialize_path(applied_changes_path),
        "batch_run_request_path": _serialize_path(batch_run_request_path),
        "batch_run_result_path": _serialize_path(batch_run_result_path),
    }
    return ReportArtifactContext(
        artifact_type="candidate_run",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            ("candidate_apply_manifest.json", manifest_path),
            ("candidate_reply.yaml", reply_path),
            ("candidate_theory_snapshot.yaml", candidate_theory_snapshot_path),
            ("applied_changes.jsonl", applied_changes_path),
            ("batch_run_request.json", batch_run_request_path),
            ("batch_run_result.json", batch_run_result_path),
        ),
        warnings=tuple(warnings),
    )


def build_reeval_outcome_report_context(outcome_dir: str | Path) -> ReportArtifactContext:
    resolved_dir = _resolve_existing_dir(outcome_dir, label="Re-eval outcome directory")
    manifest_path = resolved_dir / "reeval_outcome_manifest.json"
    decision_record_path = resolved_dir / "reeval_decision_record.json"
    paired_seed_table_path = resolved_dir / "reeval_paired_seed_table.jsonl"

    warnings: list[str] = []
    manifest = _safe_load_json_object(manifest_path, warnings, label="re-eval outcome manifest")
    decision_record = _safe_load_json_object(
        decision_record_path,
        warnings,
        label="re-eval decision record",
    )
    summary = _dict_value(manifest, "selected_metric_summary") or _dict_value(
        decision_record,
        "selected_metric_summary",
    )
    metadata = {
        "outcome_id": _first_nonempty(
            _dict_value(manifest, "outcome_id"),
            _dict_value(decision_record, "outcome_id"),
            resolved_dir.name,
        ),
        "decision_status": _first_nonempty(
            _dict_value(decision_record, "decision_status"),
            _dict_value(manifest, "decision_status"),
        ),
        "selected_metric": _first_nonempty(
            _dict_value(manifest, "selected_metric"),
            _dict_value(decision_record, "selected_metric"),
        ),
        "candidate_id": _first_nonempty(
            _dict_value(manifest, "candidate_id"),
            _dict_value(decision_record, "candidate_id"),
        ),
        "packet_id": _first_nonempty(
            _dict_value(manifest, "packet_id"),
            _dict_value(decision_record, "packet_id"),
        ),
        "comparison_id": _first_nonempty(
            _dict_value(manifest, "comparison_id"),
            _dict_value(decision_record, "comparison_id"),
        ),
        "common_doi_count": _dict_value(manifest, "common_doi_count"),
        "common_completed_seed_count": _dict_value(manifest, "common_completed_seed_count"),
        "wins": _dict_value(summary, "wins"),
        "losses": _dict_value(summary, "losses"),
        "ties": _dict_value(summary, "ties"),
        "outcome_dir": _serialize_path(resolved_dir),
    }
    metadata.update(extract_study_source_fields(decision_record, manifest))
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(manifest_path),
        "decision_record_path": _serialize_path(decision_record_path),
        "paired_seed_table_path": _serialize_path(paired_seed_table_path),
    }
    return ReportArtifactContext(
        artifact_type="reeval_outcome",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            ("reeval_outcome_manifest.json", manifest_path),
            ("reeval_decision_record.json", decision_record_path),
            ("reeval_paired_seed_table.jsonl", paired_seed_table_path),
        ),
        warnings=tuple(warnings),
    )


def build_accepted_baseline_report_context(baseline_dir: str | Path) -> ReportArtifactContext:
    resolved_dir = _resolve_existing_dir(baseline_dir, label="Accepted baseline directory")
    manifest_path = resolved_dir / "accepted_baseline_manifest.json"
    theory_snapshot_path = resolved_dir / "accepted_theory_snapshot.yaml"
    candidate_reply_path = resolved_dir / "candidate_reply.yaml"
    applied_changes_path = resolved_dir / "applied_changes.jsonl"
    promotion_record_path = resolved_dir / "promotion_record.json"

    warnings: list[str] = []
    manifest = _safe_load_json_object(manifest_path, warnings, label="accepted baseline manifest")
    promotion_record = _safe_load_json_object(
        promotion_record_path,
        warnings,
        label="accepted baseline promotion record",
    )
    source_lineage = _dict_value(manifest, "source_lineage")
    outcome_summary = _dict_value(manifest, "outcome_summary")
    metadata = {
        "baseline_id": _first_nonempty(_dict_value(manifest, "baseline_id"), resolved_dir.name),
        "decision_status": _dict_value(manifest, "decision_status"),
        "selected_metric": _dict_value(manifest, "selected_metric"),
        "comparison_id": _dict_value(source_lineage, "comparison_id"),
        "packet_id": _dict_value(source_lineage, "packet_id"),
        "candidate_id": _dict_value(source_lineage, "candidate_id"),
        "outcome_id": _dict_value(source_lineage, "outcome_id"),
        "source_primary_batch_id": _dict_value(
            _dict_value(manifest, "source_primary_batch"),
            "batch_id",
        ),
        "source_secondary_batch_id": _dict_value(
            _dict_value(manifest, "source_secondary_batch"),
            "batch_id",
        ),
        "common_doi_count": _dict_value(outcome_summary, "common_doi_count"),
        "common_completed_seed_count": _dict_value(outcome_summary, "common_completed_seed_count"),
        "wins": _dict_value(outcome_summary, "wins"),
        "losses": _dict_value(outcome_summary, "losses"),
        "ties": _dict_value(outcome_summary, "ties"),
        "accepted_theory_snapshot_path": _serialize_path(theory_snapshot_path),
        "baseline_dir": _serialize_path(resolved_dir),
        "reviewer": _first_nonempty(
            _dict_value(manifest, "reviewer"),
            _dict_value(promotion_record, "reviewer"),
        ),
        "notes": _first_nonempty(
            _dict_value(manifest, "notes"),
            _dict_value(promotion_record, "notes"),
        ),
    }
    metadata.update(extract_study_source_fields(promotion_record, manifest))
    snapshot = {
        **metadata,
        "manifest_path": _serialize_path(manifest_path),
        "candidate_reply_path": _serialize_path(candidate_reply_path),
        "applied_changes_path": _serialize_path(applied_changes_path),
        "promotion_record_path": _serialize_path(promotion_record_path),
    }
    return ReportArtifactContext(
        artifact_type="accepted_baseline",
        metadata=metadata,
        snapshot=snapshot,
        copy_specs=(
            ("accepted_baseline_manifest.json", manifest_path),
            ("accepted_theory_snapshot.yaml", theory_snapshot_path),
            ("candidate_reply.yaml", candidate_reply_path),
            ("applied_changes.jsonl", applied_changes_path),
            ("promotion_record.json", promotion_record_path),
        ),
        warnings=tuple(warnings),
    )


def build_preset_report_context(
    *,
    artifact_type: str,
    preset_id: str | None,
    preset_path: str | Path,
) -> ReportArtifactContext:
    if artifact_type not in {"benchmark_preset", "evaluation_preset", "launch_profile"}:
        raise ReportBundleExportError(f"Unsupported preset artifact type: {artifact_type}")

    resolved_path = _resolve_existing_file(preset_path, label=f"{artifact_type.replace('_', ' ')} file")
    warnings: list[str] = []
    payload = _safe_load_json_object(resolved_path, warnings, label=f"{artifact_type.replace('_', ' ')} JSON")
    normalized_id = _first_nonempty(
        payload.get("benchmark_preset_id") if artifact_type == "benchmark_preset" else None,
        payload.get("eval_preset_id") if artifact_type == "evaluation_preset" else None,
        payload.get("launch_profile_id") if artifact_type == "launch_profile" else None,
        preset_id,
        resolved_path.stem,
    )

    metadata: dict[str, Any] = {
        "preset_id": normalized_id,
        "preset_path": _serialize_path(resolved_path),
        "created_at": _dict_value(payload, "created_at"),
    }
    if artifact_type == "benchmark_preset":
        metadata.update(
            {
                "benchmark_preset_id": normalized_id,
                "seeds_csv": _dict_value(payload, "seeds_csv"),
                "description": _dict_value(payload, "description"),
                "tags": list(_dict_value(payload, "tags") or []),
            }
        )
        destination_name = "benchmark_preset.json"
    elif artifact_type == "evaluation_preset":
        metadata.update(
            {
                "eval_preset_id": normalized_id,
                "max_references": _dict_value(payload, "max_references"),
                "max_related": _dict_value(payload, "max_related"),
                "max_hard_negatives": _dict_value(payload, "max_hard_negatives"),
                "top_k": _dict_value(payload, "top_k"),
                "label_source": _dict_value(payload, "label_source"),
                "refresh": _dict_value(payload, "refresh"),
                "description": _dict_value(payload, "description"),
            }
        )
        destination_name = "eval_preset.json"
    else:
        metadata.update(
            {
                "launch_profile_id": normalized_id,
                "accepted_baseline_id": _dict_value(payload, "accepted_baseline_id"),
                "accepted_theory_snapshot": _dict_value(payload, "accepted_theory_snapshot"),
                "benchmark_preset_id": _dict_value(payload, "benchmark_preset_id"),
                "seeds_csv": _dict_value(payload, "seeds_csv"),
                "eval_preset_id": _dict_value(payload, "eval_preset_id"),
                "max_references": _dict_value(payload, "max_references"),
                "max_related": _dict_value(payload, "max_related"),
                "max_hard_negatives": _dict_value(payload, "max_hard_negatives"),
                "top_k": _dict_value(payload, "top_k"),
                "label_source": _dict_value(payload, "label_source"),
                "refresh": _dict_value(payload, "refresh"),
                "description": _dict_value(payload, "description"),
                "tags": list(_dict_value(payload, "tags") or []),
            }
        )
        destination_name = "launch_profile.json"

    return ReportArtifactContext(
        artifact_type=artifact_type,
        metadata=metadata,
        snapshot=dict(metadata),
        copy_specs=((destination_name, resolved_path),),
        warnings=tuple(warnings),
    )


def _aggregate_metrics_payload(aggregate_summary: Any) -> dict[str, dict[str, Any]]:
    metric_aggregates = getattr(aggregate_summary, "metric_aggregates", None)
    if not hasattr(metric_aggregates, "items"):
        return {}

    payload: dict[str, dict[str, Any]] = {}
    for metric_name, stats in sorted(metric_aggregates.items()):
        if hasattr(stats, "model_dump"):
            payload[metric_name] = stats.model_dump(mode="json")
        elif isinstance(stats, dict):
            payload[metric_name] = dict(stats)
    return payload


def _aggregate_metric_markdown(
    aggregate_metrics: Any,
    *,
    title: str,
) -> list[str]:
    if not isinstance(aggregate_metrics, dict) or not aggregate_metrics:
        return ["", f"## {title}", "- No aggregate metrics available."]

    lines = ["", f"## {title}"]
    for metric_name, values in aggregate_metrics.items():
        if not isinstance(values, dict):
            continue
        lines.append(
            f"- {metric_name}: mean={values.get('mean')}, median={values.get('median')}, count={values.get('count')}"
        )
    return lines


def _context_warnings(
    context: dict[str, ReportArtifactContext | None],
) -> list[str]:
    warnings: list[str] = []
    for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES:
        context_entry = context.get(artifact_type)
        if context_entry is None:
            continue
        for warning in context_entry.warnings:
            warnings.append(f"{artifact_type}: {warning}")
    return warnings


def _metadata(context_entry: ReportArtifactContext | None) -> dict[str, Any]:
    return dict(context_entry.metadata) if context_entry is not None else {}


def _safe_load_json_object(
    path: Path,
    warnings: list[str],
    *,
    label: str,
) -> dict[str, Any]:
    if not path.exists():
        warnings.append(f"Missing {label}: {path}")
        return {}
    try:
        return _load_json_object(path)
    except ReportBundleExportError as exc:
        warnings.append(str(exc))
        return {}


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ReportBundleExportError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ReportBundleExportError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ReportBundleExportError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _dict_value(payload: Any, field_name: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(field_name)
    return None


def _resolve_existing_dir(value: str | Path, *, label: str) -> Path:
    path = _resolve_path(value, label=label)
    if not path.exists():
        raise ReportBundleExportError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise ReportBundleExportError(f"{label} is not a directory: {path}")
    return path


def _resolve_existing_file(value: str | Path, *, label: str) -> Path:
    path = _resolve_path(value, label=label)
    if not path.exists():
        raise ReportBundleExportError(f"{label} does not exist: {path}")
    if not path.is_file():
        raise ReportBundleExportError(f"{label} is not a file: {path}")
    return path


def _resolve_path(value: str | Path, *, label: str) -> Path:
    text = _optional_str(value)
    if text is None:
        raise ReportBundleExportError(f"{label} is required.")
    candidate = Path(text.replace("\\", "/")).expanduser()
    if candidate.is_absolute():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise ReportBundleExportError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise ReportBundleExportError(f"{label} must be a single directory name.")
    return normalized


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


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


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
