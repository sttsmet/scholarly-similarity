from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.run_context import load_run_context_if_present


ARTIFACT_FAMILY_ORDER = (
    "batches",
    "comparisons",
    "review_packets",
    "candidate_runs",
    "reeval_outcomes",
    "accepted_baselines",
    "benchmark_presets",
    "evaluation_presets",
    "launch_profiles",
    "reports",
    "benchmark_audits",
    "benchmark_curations",
    "cohort_studies",
)


@dataclass(frozen=True, slots=True)
class WorkspaceSmokeResult:
    repo_root: Path
    artifact_family_counts: dict[str, int]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]


def run_workspace_smoke(repo_root: str | Path | None = None) -> WorkspaceSmokeResult:
    resolved_repo_root = _resolve_repo_root(repo_root)
    counts = {family: 0 for family in ARTIFACT_FAMILY_ORDER}
    warnings: list[str] = []
    errors: list[str] = []

    _scan_batches(resolved_repo_root, counts, warnings, errors)
    _scan_comparisons(resolved_repo_root, counts, warnings, errors)
    _scan_accepted_baselines(resolved_repo_root, counts, warnings, errors)
    _scan_presets(resolved_repo_root, counts, warnings, errors)
    _scan_reports(resolved_repo_root, counts, warnings, errors)
    _scan_benchmark_audits(resolved_repo_root, counts, warnings, errors)
    _scan_benchmark_curations(resolved_repo_root, counts, warnings, errors)
    _scan_cohort_studies(resolved_repo_root, counts, warnings, errors)

    return WorkspaceSmokeResult(
        repo_root=resolved_repo_root,
        artifact_family_counts=counts,
        warnings=tuple(dict.fromkeys(warnings)),
        errors=tuple(dict.fromkeys(errors)),
    )


def workspace_smoke_exit_code(result: WorkspaceSmokeResult) -> int:
    return 1 if result.errors else 0


def format_workspace_smoke_report(result: WorkspaceSmokeResult) -> str:
    lines = [
        "Scholarly Similarity v1 Smoke Validation",
        f"Repo root: {result.repo_root}",
        "",
        "Artifact family counts:",
    ]
    for family in ARTIFACT_FAMILY_ORDER:
        lines.append(f"- {family}: {result.artifact_family_counts.get(family, 0)}")

    lines.extend(["", f"Warnings ({len(result.warnings)}):"])
    lines.extend(f"- {message}" for message in result.warnings) if result.warnings else lines.append("- none")
    lines.extend(["", f"Errors ({len(result.errors)}):"])
    lines.extend(f"- {message}" for message in result.errors) if result.errors else lines.append("- none")
    status = "PASSED" if not result.errors else "FAILED"
    lines.extend(
        [
            "",
            f"Final summary: {status} with {len(result.errors)} hard error(s) and {len(result.warnings)} warning(s).",
        ]
    )
    return "\n".join(lines)


def _resolve_repo_root(repo_root: str | Path | None) -> Path:
    if repo_root is None:
        return REPO_ROOT
    candidate = Path(str(repo_root).strip()).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()


def _iter_artifact_dirs(path: Path, errors: list[str], *, label: str) -> list[Path]:
    if not path.exists():
        return []
    if not path.is_dir():
        errors.append(f"Artifact root for {label} exists but is not a directory: {path}")
        return []
    return sorted((child for child in path.iterdir() if child.is_dir()), key=lambda child: child.name.lower())


def _iter_json_files(path: Path, errors: list[str], *, label: str) -> list[Path]:
    if not path.exists():
        return []
    if not path.is_dir():
        errors.append(f"Artifact root for {label} exists but is not a directory: {path}")
        return []
    return sorted(path.glob("*.json"), key=lambda child: child.name.lower())


def _require_json_object(path: Path, issues: list[str], *, label: str) -> dict[str, Any] | None:
    if not path.exists():
        issues.append(f"{label} is missing required file: {path.name}")
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        issues.append(f"{label} could not read {path.name}: {exc}")
        return None
    except json.JSONDecodeError as exc:
        issues.append(f"{label} has malformed {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}")
        return None
    if not isinstance(payload, dict):
        issues.append(f"{label} has invalid {path.name}: expected a JSON object.")
        return None
    return payload


def _check_output_paths(
    payload: dict[str, Any],
    repo_root: Path,
    artifact_dir: Path,
    warnings: list[str],
    *,
    label: str,
) -> None:
    output_paths = payload.get("output_paths")
    if not isinstance(output_paths, dict):
        return
    for key, value in sorted(output_paths.items()):
        if value in (None, ""):
            continue
        resolved = _resolve_path_like(value, repo_root, artifact_dir=artifact_dir)
        if not resolved.exists():
            warnings.append(f"{label} references a missing output path for '{key}': {value}")


def _check_dir_field(
    payload: dict[str, Any],
    field_name: str,
    expected_dir: Path,
    repo_root: Path,
    issues: list[str],
    *,
    label: str,
) -> None:
    value = _optional_str(payload.get(field_name))
    if value is None:
        return
    if _resolve_path_like(value, repo_root, artifact_dir=expected_dir) != expected_dir.resolve():
        issues.append(f"{label} has {field_name}='{value}' but the artifact directory is '{expected_dir}'.")


def _check_consistent_dir_path(
    payload: dict[str, Any],
    field_name: str,
    expected_dir: Path,
    repo_root: Path,
    issues: list[str],
    *,
    label: str,
) -> None:
    value = _optional_str(payload.get(field_name))
    if value is None:
        return
    if _resolve_path_like(value, repo_root, artifact_dir=expected_dir.parent) != expected_dir.resolve():
        issues.append(f"{label} has {field_name}='{value}' but the expected directory is '{expected_dir}'.")


def _check_id_field(
    payload: dict[str, Any],
    field_name: str,
    expected_value: str,
    issues: list[str],
    *,
    label: str,
) -> None:
    value = _optional_str(payload.get(field_name))
    if value is not None and value != expected_value:
        issues.append(f"{label} has {field_name}='{value}' but the directory name is '{expected_value}'.")


def _check_parent_field(
    payload: dict[str, Any],
    field_name: str,
    expected_value: str,
    issues: list[str],
    *,
    label: str,
) -> None:
    value = _optional_str(payload.get(field_name))
    if value is not None and value != expected_value:
        issues.append(f"{label} has {field_name}='{value}' but the parent lineage expects '{expected_value}'.")


def _check_file_stem_id(
    payload: dict[str, Any],
    field_name: str,
    expected_value: str,
    warnings: list[str],
    *,
    label: str,
) -> None:
    value = _optional_str(payload.get(field_name))
    if value is not None and value != expected_value:
        warnings.append(f"{label} has {field_name}='{value}' but the filename stem is '{expected_value}'.")


def _check_consistent_text_fields(
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
    left_field_name: str,
    issues: list[str],
    *,
    label: str,
    other_field_name: str | None = None,
) -> None:
    right_field_name = other_field_name or left_field_name
    left_value = _optional_str(left_payload.get(left_field_name))
    right_value = _optional_str(right_payload.get(right_field_name))
    if left_value is not None and right_value is not None and left_value != right_value:
        issues.append(f"{label} disagrees on {left_field_name}: '{left_value}' vs '{right_value}'.")


def _check_consistent_path_fields(
    left_payload: dict[str, Any],
    right_payload: dict[str, Any],
    left_field_name: str,
    repo_root: Path,
    issues: list[str],
    *,
    label: str,
    other_field_name: str | None = None,
) -> None:
    right_field_name = other_field_name or left_field_name
    left_value = _optional_str(left_payload.get(left_field_name))
    right_value = _optional_str(right_payload.get(right_field_name))
    if left_value is None or right_value is None:
        return
    if _resolve_path_like(left_value, repo_root) != _resolve_path_like(right_value, repo_root):
        issues.append(f"{label} disagrees on {left_field_name}: '{left_value}' vs '{right_value}'.")


def _check_batch_payload_exists(
    payload: dict[str, Any],
    field_name: str,
    repo_root: Path,
    warnings: list[str],
    *,
    label: str,
) -> None:
    batch_payload = _dict_value(payload, field_name)
    if not isinstance(batch_payload, dict):
        return
    batch_dir = _optional_str(batch_payload.get("batch_dir"))
    if batch_dir is not None and not _resolve_path_like(batch_dir, repo_root).exists():
        warnings.append(f"{label} references a missing batch_dir in {field_name}: {batch_dir}")


def _check_path_field_exists(
    payload: dict[str, Any],
    field_name: str,
    repo_root: Path,
    warnings: list[str],
    *,
    label: str,
    artifact_dir: Path | None = None,
) -> None:
    _check_path_value_exists(payload.get(field_name), repo_root, warnings, label=f"{label} {field_name}", artifact_dir=artifact_dir)


def _check_path_value_exists(
    value: object,
    repo_root: Path,
    warnings: list[str],
    *,
    label: str,
    artifact_dir: Path | None = None,
) -> None:
    text = _optional_str(value)
    if text is not None and not _resolve_path_like(text, repo_root, artifact_dir=artifact_dir).exists():
        warnings.append(f"{label} references a missing path: {text}")


def _check_source_reference_alignment(
    payload: dict[str, Any],
    primary_batch_id: object,
    warnings: list[str],
    *,
    label: str,
) -> None:
    source_reference_batch_id = _optional_str(payload.get("source_reference_batch_id"))
    normalized_primary_batch_id = _optional_str(primary_batch_id)
    if (
        source_reference_batch_id is not None
        and normalized_primary_batch_id is not None
        and source_reference_batch_id != normalized_primary_batch_id
    ):
        warnings.append(
            f"{label} has source_reference_batch_id='{source_reference_batch_id}' but the saved primary batch is '{normalized_primary_batch_id}'."
        )


def _check_study_source_dir(
    payload: dict[str, Any],
    repo_root: Path,
    warnings: list[str],
    *,
    label: str,
) -> None:
    source_study_dir = _optional_str(payload.get("source_study_dir"))
    if source_study_dir is not None and not _resolve_path_like(source_study_dir, repo_root).exists():
        warnings.append(f"{label} references a missing source_study_dir: {source_study_dir}")


def _resolve_path_like(value: str | Path, repo_root: Path, *, artifact_dir: Path | None = None) -> Path:
    candidate = Path(str(value).strip()).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    if artifact_dir is not None and len(candidate.parts) == 1:
        return (artifact_dir / candidate).resolve()
    repo_candidate = (repo_root / candidate).resolve()
    if repo_candidate.exists():
        return repo_candidate
    if artifact_dir is not None:
        artifact_candidate = (artifact_dir / candidate).resolve()
        if artifact_candidate.exists():
            return artifact_candidate
    return repo_candidate


def _dict_value(payload: Any, field_name: str) -> Any:
    return payload.get(field_name) if isinstance(payload, dict) else None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _scan_batches(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    batches_root = repo_root / "runs" / "batches"
    for batch_dir in _iter_artifact_dirs(batches_root, errors, label="batches"):
        counts["batches"] += 1
        label = f"batch '{batch_dir.name}'"
        manifest = _require_json_object(batch_dir / "batch_manifest.json", errors, label=label)
        aggregate_summary = _require_json_object(batch_dir / "aggregate_summary.json", errors, label=label)
        if manifest is not None:
            _check_id_field(manifest, "batch_id", batch_dir.name, errors, label=label)
            _check_dir_field(manifest, "batch_dir", batch_dir, repo_root, errors, label=label)
            _check_output_paths(manifest, repo_root, batch_dir, warnings, label=label)
            _check_path_field_exists(manifest, "theory_config", repo_root, warnings, label=label)
            _check_path_field_exists(manifest, "seeds_csv", repo_root, warnings, label=label)
        if aggregate_summary is not None:
            if not (batch_dir / "seed_table.jsonl").exists():
                warnings.append(f"{label} is missing seed_table.jsonl.")
            if not (batch_dir / "worst_cases.json").exists():
                warnings.append(f"{label} is missing worst_cases.json.")

        run_context_payload, run_context_warning = load_run_context_if_present(batch_dir)
        if run_context_warning:
            warnings.append(f"{label}: {run_context_warning}")
        elif isinstance(run_context_payload, dict) and manifest is not None:
            _check_consistent_text_fields(
                manifest,
                run_context_payload,
                "batch_id",
                warnings,
                label=f"{label} run_context",
            )


def _scan_comparisons(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    comparisons_root = repo_root / "runs" / "comparisons"
    for comparison_dir in _iter_artifact_dirs(comparisons_root, errors, label="comparisons"):
        counts["comparisons"] += 1
        comparison_label = f"comparison '{comparison_dir.name}'"
        comparison_manifest = _require_json_object(
            comparison_dir / "comparison_manifest.json",
            errors,
            label=comparison_label,
        )
        decision_record = _require_json_object(
            comparison_dir / "decision_record.json",
            errors,
            label=comparison_label,
        )
        if comparison_manifest is not None:
            _check_id_field(comparison_manifest, "comparison_id", comparison_dir.name, errors, label=comparison_label)
            _check_dir_field(comparison_manifest, "comparison_dir", comparison_dir, repo_root, errors, label=comparison_label)
            _check_output_paths(comparison_manifest, repo_root, comparison_dir, warnings, label=comparison_label)
            _check_batch_payload_exists(comparison_manifest, "primary_batch", repo_root, warnings, label=comparison_label)
            _check_batch_payload_exists(comparison_manifest, "secondary_batch", repo_root, warnings, label=comparison_label)
            _check_source_reference_alignment(
                comparison_manifest,
                _dict_value(_dict_value(comparison_manifest, "primary_batch"), "batch_id"),
                warnings,
                label=comparison_label,
            )
            _check_study_source_dir(comparison_manifest, repo_root, warnings, label=comparison_label)
        if comparison_manifest is not None and decision_record is not None:
            _check_consistent_text_fields(
                comparison_manifest,
                decision_record,
                "comparison_id",
                errors,
                label=f"{comparison_label} decision record",
            )
        if not (comparison_dir / "paired_seed_table.jsonl").exists():
            warnings.append(f"{comparison_label} is missing paired_seed_table.jsonl.")

        review_packets_dir = comparison_dir / "review_packets"
        for packet_dir in _iter_artifact_dirs(review_packets_dir, errors, label=f"{comparison_label} review_packets"):
            counts["review_packets"] += 1
            packet_label = f"review packet '{packet_dir.name}' under comparison '{comparison_dir.name}'"
            packet_manifest = _require_json_object(
                packet_dir / "review_packet_manifest.json",
                errors,
                label=packet_label,
            )
            evidence_summary = _require_json_object(
                packet_dir / "evidence_summary.json",
                errors,
                label=packet_label,
            )
            _require_json_object(packet_dir / "allowed_revision_paths.json", errors, label=packet_label)
            if packet_manifest is not None:
                _check_id_field(packet_manifest, "packet_id", packet_dir.name, errors, label=packet_label)
                _check_dir_field(packet_manifest, "packet_dir", packet_dir, repo_root, errors, label=packet_label)
                _check_parent_field(packet_manifest, "comparison_id", comparison_dir.name, errors, label=packet_label)
                _check_output_paths(packet_manifest, repo_root, packet_dir, warnings, label=packet_label)
                _check_batch_payload_exists(packet_manifest, "primary_batch", repo_root, warnings, label=packet_label)
                _check_batch_payload_exists(packet_manifest, "secondary_batch", repo_root, warnings, label=packet_label)
                _check_source_reference_alignment(
                    packet_manifest,
                    _dict_value(_dict_value(packet_manifest, "primary_batch"), "batch_id"),
                    warnings,
                    label=packet_label,
                )
                _check_study_source_dir(packet_manifest, repo_root, warnings, label=packet_label)
            if evidence_summary is not None and packet_manifest is not None:
                _check_source_reference_alignment(
                    evidence_summary,
                    _dict_value(_dict_value(packet_manifest, "primary_batch"), "batch_id"),
                    warnings,
                    label=f"{packet_label} evidence summary",
                )
            if not (packet_dir / "baseline_theory_snapshot.yaml").exists():
                warnings.append(f"{packet_label} is missing baseline_theory_snapshot.yaml.")
            if not (packet_dir / "candidate_reply_TEMPLATE.yaml").exists():
                warnings.append(f"{packet_label} is missing candidate_reply_TEMPLATE.yaml.")

            _scan_candidate_runs(
                repo_root=repo_root,
                comparison_dir=comparison_dir,
                packet_dir=packet_dir,
                counts=counts,
                warnings=warnings,
                errors=errors,
            )


def _scan_candidate_runs(
    *,
    repo_root: Path,
    comparison_dir: Path,
    packet_dir: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    candidate_runs_dir = packet_dir / "candidate_runs"
    for candidate_run_dir in _iter_artifact_dirs(candidate_runs_dir, errors, label=f"packet '{packet_dir.name}' candidate_runs"):
        counts["candidate_runs"] += 1
        candidate_label = f"candidate run '{candidate_run_dir.name}' under packet '{packet_dir.name}'"
        candidate_manifest = _require_json_object(
            candidate_run_dir / "candidate_apply_manifest.json",
            errors,
            label=candidate_label,
        )
        batch_run_result = _require_json_object(
            candidate_run_dir / "batch_run_result.json",
            errors,
            label=candidate_label,
        )
        if candidate_manifest is not None:
            _check_id_field(candidate_manifest, "candidate_id", candidate_run_dir.name, errors, label=candidate_label)
            _check_dir_field(candidate_manifest, "candidate_dir", candidate_run_dir, repo_root, errors, label=candidate_label)
            _check_parent_field(candidate_manifest, "comparison_id", comparison_dir.name, errors, label=candidate_label)
            _check_parent_field(candidate_manifest, "packet_id", packet_dir.name, errors, label=candidate_label)
            _check_output_paths(candidate_manifest, repo_root, candidate_run_dir, warnings, label=candidate_label)
            _check_path_field_exists(
                candidate_manifest,
                "copied_reply_yaml",
                repo_root,
                warnings,
                label=candidate_label,
                artifact_dir=candidate_run_dir,
            )
            _check_path_field_exists(
                candidate_manifest,
                "candidate_theory_snapshot_path",
                repo_root,
                warnings,
                label=candidate_label,
                artifact_dir=candidate_run_dir,
            )
            _check_path_field_exists(candidate_manifest, "output_batch_dir", repo_root, warnings, label=candidate_label)
            _check_batch_payload_exists(candidate_manifest, "source_primary_batch", repo_root, warnings, label=candidate_label)
            _check_source_reference_alignment(
                candidate_manifest,
                _dict_value(_dict_value(candidate_manifest, "source_primary_batch"), "batch_id"),
                warnings,
                label=candidate_label,
            )
            _check_study_source_dir(candidate_manifest, repo_root, warnings, label=candidate_label)
        if candidate_manifest is not None and batch_run_result is not None:
            _check_consistent_text_fields(
                candidate_manifest,
                batch_run_result,
                "output_batch_id",
                errors,
                label=f"{candidate_label} batch result",
                other_field_name="batch_id",
            )
            _check_consistent_path_fields(
                candidate_manifest,
                batch_run_result,
                "output_batch_dir",
                repo_root,
                errors,
                label=f"{candidate_label} batch result",
                other_field_name="batch_dir",
            )
        if not (candidate_run_dir / "batch_run_request.json").exists():
            warnings.append(f"{candidate_label} is missing batch_run_request.json.")

        outcomes_dir = candidate_run_dir / "outcomes"
        for outcome_dir in _iter_artifact_dirs(outcomes_dir, errors, label=f"{candidate_label} outcomes"):
            counts["reeval_outcomes"] += 1
            outcome_label = f"re-eval outcome '{outcome_dir.name}' under candidate run '{candidate_run_dir.name}'"
            outcome_manifest = _require_json_object(
                outcome_dir / "reeval_outcome_manifest.json",
                errors,
                label=outcome_label,
            )
            outcome_decision_record = _require_json_object(
                outcome_dir / "reeval_decision_record.json",
                errors,
                label=outcome_label,
            )
            if outcome_manifest is not None:
                _check_id_field(outcome_manifest, "outcome_id", outcome_dir.name, errors, label=outcome_label)
                _check_dir_field(outcome_manifest, "outcome_dir", outcome_dir, repo_root, errors, label=outcome_label)
                _check_parent_field(outcome_manifest, "comparison_id", comparison_dir.name, errors, label=outcome_label)
                _check_parent_field(outcome_manifest, "packet_id", packet_dir.name, errors, label=outcome_label)
                _check_parent_field(outcome_manifest, "candidate_id", candidate_run_dir.name, errors, label=outcome_label)
                _check_consistent_dir_path(
                    outcome_manifest,
                    "candidate_run_dir",
                    candidate_run_dir,
                    repo_root,
                    errors,
                    label=outcome_label,
                )
                _check_output_paths(outcome_manifest, repo_root, outcome_dir, warnings, label=outcome_label)
                _check_batch_payload_exists(outcome_manifest, "primary_batch", repo_root, warnings, label=outcome_label)
                _check_batch_payload_exists(outcome_manifest, "secondary_batch", repo_root, warnings, label=outcome_label)
                _check_source_reference_alignment(
                    outcome_manifest,
                    _dict_value(_dict_value(outcome_manifest, "primary_batch"), "batch_id"),
                    warnings,
                    label=outcome_label,
                )
                _check_study_source_dir(outcome_manifest, repo_root, warnings, label=outcome_label)
            if outcome_manifest is not None and outcome_decision_record is not None:
                for field_name in ("outcome_id", "comparison_id", "packet_id", "candidate_id", "decision_status"):
                    _check_consistent_text_fields(
                        outcome_manifest,
                        outcome_decision_record,
                        field_name,
                        errors,
                        label=f"{outcome_label} decision record",
                    )
            if not (outcome_dir / "reeval_paired_seed_table.jsonl").exists():
                warnings.append(f"{outcome_label} is missing reeval_paired_seed_table.jsonl.")


def _scan_accepted_baselines(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    baselines_root = repo_root / "runs" / "accepted_baselines"
    for baseline_dir in _iter_artifact_dirs(baselines_root, errors, label="accepted_baselines"):
        counts["accepted_baselines"] += 1
        baseline_label = f"accepted baseline '{baseline_dir.name}'"
        baseline_manifest = _require_json_object(
            baseline_dir / "accepted_baseline_manifest.json",
            errors,
            label=baseline_label,
        )
        promotion_record = _require_json_object(
            baseline_dir / "promotion_record.json",
            errors,
            label=baseline_label,
        )
        if baseline_manifest is not None:
            _check_id_field(baseline_manifest, "baseline_id", baseline_dir.name, errors, label=baseline_label)
            _check_dir_field(baseline_manifest, "baseline_dir", baseline_dir, repo_root, errors, label=baseline_label)
            _check_output_paths(baseline_manifest, repo_root, baseline_dir, warnings, label=baseline_label)
            _check_path_field_exists(
                baseline_manifest,
                "accepted_theory_snapshot_path",
                repo_root,
                warnings,
                label=baseline_label,
                artifact_dir=baseline_dir,
            )
            _check_path_field_exists(
                baseline_manifest,
                "candidate_reply_yaml_path",
                repo_root,
                warnings,
                label=baseline_label,
                artifact_dir=baseline_dir,
            )
            _check_path_field_exists(
                baseline_manifest,
                "applied_changes_path",
                repo_root,
                warnings,
                label=baseline_label,
                artifact_dir=baseline_dir,
            )
            _check_batch_payload_exists(baseline_manifest, "source_primary_batch", repo_root, warnings, label=baseline_label)
            _check_batch_payload_exists(baseline_manifest, "source_secondary_batch", repo_root, warnings, label=baseline_label)
            _check_source_reference_alignment(
                baseline_manifest,
                _dict_value(_dict_value(baseline_manifest, "source_primary_batch"), "batch_id"),
                warnings,
                label=baseline_label,
            )
            _check_study_source_dir(baseline_manifest, repo_root, warnings, label=baseline_label)
            source_lineage = _dict_value(baseline_manifest, "source_lineage")
            if isinstance(source_lineage, dict):
                _check_path_value_exists(
                    source_lineage.get("candidate_run_dir"),
                    repo_root,
                    warnings,
                    label=f"{baseline_label} source_lineage.candidate_run_dir",
                )
                _check_path_value_exists(
                    source_lineage.get("outcome_dir"),
                    repo_root,
                    warnings,
                    label=f"{baseline_label} source_lineage.outcome_dir",
                )
        if baseline_manifest is not None and promotion_record is not None:
            for field_name in ("baseline_id", "comparison_id", "packet_id", "candidate_id", "outcome_id", "decision_status"):
                _check_consistent_text_fields(
                    baseline_manifest,
                    promotion_record,
                    field_name,
                    errors,
                    label=f"{baseline_label} promotion record",
                )


def _scan_presets(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    benchmark_presets_root = repo_root / "configs" / "presets" / "benchmarks"
    for preset_path in _iter_json_files(benchmark_presets_root, errors, label="benchmark_presets"):
        counts["benchmark_presets"] += 1
        preset_label = f"benchmark preset '{preset_path.name}'"
        payload = _require_json_object(preset_path, errors, label=preset_label)
        if payload is not None:
            _check_file_stem_id(payload, "benchmark_preset_id", preset_path.stem, warnings, label=preset_label)
            _check_path_field_exists(payload, "seeds_csv", repo_root, warnings, label=preset_label)

    evaluation_presets_root = repo_root / "configs" / "presets" / "evals"
    for preset_path in _iter_json_files(evaluation_presets_root, errors, label="evaluation_presets"):
        counts["evaluation_presets"] += 1
        payload = _require_json_object(preset_path, errors, label=f"evaluation preset '{preset_path.name}'")
        if payload is not None:
            _check_file_stem_id(payload, "eval_preset_id", preset_path.stem, warnings, label=f"evaluation preset '{preset_path.name}'")

    launch_profiles_root = repo_root / "configs" / "presets" / "launch_profiles"
    for profile_path in _iter_json_files(launch_profiles_root, errors, label="launch_profiles"):
        counts["launch_profiles"] += 1
        profile_label = f"launch profile '{profile_path.name}'"
        payload = _require_json_object(profile_path, errors, label=profile_label)
        if payload is not None:
            _check_file_stem_id(payload, "launch_profile_id", profile_path.stem, warnings, label=profile_label)
            _check_path_field_exists(payload, "accepted_baseline_dir", repo_root, warnings, label=profile_label)
            _check_path_field_exists(payload, "accepted_theory_snapshot", repo_root, warnings, label=profile_label)
            _check_path_field_exists(payload, "seeds_csv", repo_root, warnings, label=profile_label)


def _scan_reports(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    reports_root = repo_root / "runs" / "reports"
    for report_dir in _iter_artifact_dirs(reports_root, errors, label="reports"):
        counts["reports"] += 1
        report_label = f"report '{report_dir.name}'"
        report_manifest = _require_json_object(report_dir / "report_manifest.json", errors, label=report_label)
        _require_json_object(report_dir / "context_snapshot.json", errors, label=report_label)
        _require_json_object(report_dir / "included_artifacts.json", errors, label=report_label)
        if report_manifest is not None:
            _check_id_field(report_manifest, "report_id", report_dir.name, errors, label=report_label)
            _check_dir_field(report_manifest, "report_dir", report_dir, repo_root, errors, label=report_label)
            _check_output_paths(report_manifest, repo_root, report_dir, warnings, label=report_label)


def _scan_benchmark_audits(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    audits_root = repo_root / "runs" / "benchmark_audits"
    for audit_dir in _iter_artifact_dirs(audits_root, errors, label="benchmark_audits"):
        counts["benchmark_audits"] += 1
        audit_label = f"benchmark audit '{audit_dir.name}'"
        audit_manifest = _require_json_object(
            audit_dir / "benchmark_audit_manifest.json",
            errors,
            label=audit_label,
        )
        _require_json_object(audit_dir / "primary_batch_health.json", errors, label=audit_label)
        if audit_manifest is not None:
            _check_id_field(audit_manifest, "audit_id", audit_dir.name, errors, label=audit_label)
            _check_dir_field(audit_manifest, "audit_dir", audit_dir, repo_root, errors, label=audit_label)
            _check_output_paths(audit_manifest, repo_root, audit_dir, warnings, label=audit_label)


def _scan_benchmark_curations(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    curations_root = repo_root / "runs" / "benchmark_curations"
    for curation_dir in _iter_artifact_dirs(curations_root, errors, label="benchmark_curations"):
        counts["benchmark_curations"] += 1
        curation_label = f"benchmark curation '{curation_dir.name}'"
        curation_manifest = _require_json_object(
            curation_dir / "curation_manifest.json",
            errors,
            label=curation_label,
        )
        if curation_manifest is not None:
            _check_id_field(curation_manifest, "curation_id", curation_dir.name, errors, label=curation_label)
            _check_dir_field(curation_manifest, "curation_dir", curation_dir, repo_root, errors, label=curation_label)
            _check_output_paths(curation_manifest, repo_root, curation_dir, warnings, label=curation_label)
        if not (curation_dir / "seed_decisions.jsonl").exists():
            errors.append(f"{curation_label} is missing seed_decisions.jsonl.")
        if not (curation_dir / "curated_seeds.csv").exists():
            errors.append(f"{curation_label} is missing curated_seeds.csv.")


def _scan_cohort_studies(
    repo_root: Path,
    counts: dict[str, int],
    warnings: list[str],
    errors: list[str],
) -> None:
    studies_root = repo_root / "runs" / "cohort_studies"
    for study_dir in _iter_artifact_dirs(studies_root, errors, label="cohort_studies"):
        counts["cohort_studies"] += 1
        study_label = f"cohort study '{study_dir.name}'"
        study_manifest = _require_json_object(
            study_dir / "cohort_study_manifest.json",
            errors,
            label=study_label,
        )
        if study_manifest is not None:
            _check_id_field(study_manifest, "study_id", study_dir.name, errors, label=study_label)
            _check_dir_field(study_manifest, "study_dir", study_dir, repo_root, errors, label=study_label)
            _check_output_paths(study_manifest, repo_root, study_dir, warnings, label=study_label)
            _check_batch_payload_exists(study_manifest, "reference_batch", repo_root, warnings, label=study_label)
        if not (study_dir / "candidate_decisions.jsonl").exists():
            errors.append(f"{study_label} is missing candidate_decisions.jsonl.")
        if not (study_dir / "cohort_leaderboard.jsonl").exists():
            errors.append(f"{study_label} is missing cohort_leaderboard.jsonl.")
