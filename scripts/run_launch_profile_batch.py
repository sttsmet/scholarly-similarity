#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.eval.benchmark import run_seed_batch
from src.ui.batch_loader import load_batch_bundle
from src.ui.batch_runner import (
    SUPPORTED_LABEL_SOURCES,
    BatchRunValidationError,
    build_batch_run_request,
    run_batch_request,
)
from src.ui.launch_profile_registry import (
    LaunchProfileEntry,
    build_launch_profile_run_batch_values,
    find_launch_profile_entry,
    scan_launch_profiles,
)
from src.ui.preset_registry import (
    find_benchmark_preset_entry,
    find_evaluation_preset_entry,
    scan_benchmark_presets,
    scan_evaluation_presets,
)
from src.ui.run_context import RunContextError, build_run_context_payload_from_request, write_run_context


class LaunchProfileBatchRunError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a batch from an existing launch profile using the canonical backend path."
    )
    parser.add_argument(
        "--launch-profile-id",
        required=True,
        help="Existing launch profile id under configs/presets/launch_profiles/",
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="New batch id written under runs/batches/",
    )
    parser.add_argument(
        "--initial-doi-context",
        default="",
        help="Optional UI-parity context string. It does not change backend batch semantics.",
    )
    return parser.parse_args()


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_path_string(value: object) -> str | None:
    text = _optional_str(value)
    if text is None:
        return None
    candidate = Path(text.replace("\\", "/")).expanduser()
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    return str(candidate)


def _request_matches_eval_payload(request: Any, payload: dict[str, Any]) -> bool:
    required_fields = (
        "max_references",
        "max_related",
        "max_hard_negatives",
        "top_k",
        "label_source",
        "refresh",
    )
    if any(field_name not in payload for field_name in required_fields):
        return False
    return (
        getattr(request, "max_references", None) == payload.get("max_references")
        and getattr(request, "max_related", None) == payload.get("max_related")
        and getattr(request, "max_hard_negatives", None) == payload.get("max_hard_negatives")
        and getattr(request, "top_k", None) == payload.get("top_k")
        and _optional_str(getattr(request, "label_source", None)) == _optional_str(payload.get("label_source"))
        and bool(getattr(request, "refresh", False)) == bool(payload.get("refresh", False))
    )


def _build_accepted_baseline_context(
    entry: LaunchProfileEntry,
    request: Any,
) -> dict[str, Any] | None:
    accepted_theory_snapshot = _optional_str(entry.payload.get("accepted_theory_snapshot"))
    if accepted_theory_snapshot is None:
        return None
    if _normalized_path_string(accepted_theory_snapshot) != _normalized_path_string(
        getattr(request, "theory_config_path", None)
    ):
        return None
    return {
        "accepted_baseline_id": _optional_str(entry.payload.get("accepted_baseline_id")),
        "accepted_baseline_dir": _optional_str(entry.payload.get("accepted_baseline_dir")),
        "accepted_theory_snapshot": accepted_theory_snapshot,
    }


def _build_benchmark_preset_context(
    entry: LaunchProfileEntry,
    request: Any,
) -> dict[str, Any] | None:
    benchmark_preset_id = _optional_str(entry.payload.get("benchmark_preset_id"))
    if benchmark_preset_id is None:
        return None

    context: dict[str, Any] = {
        "benchmark_preset_id": benchmark_preset_id,
    }

    entries, _warnings = scan_benchmark_presets()
    preset_entry = find_benchmark_preset_entry(entries, benchmark_preset_id)
    if preset_entry is None:
        return context
    if _normalized_path_string(preset_entry.seeds_csv_path) != _normalized_path_string(
        getattr(request, "seeds_csv_path", None)
    ):
        return context

    context["benchmark_preset_path"] = preset_entry.preset_path
    for field_name in ("source_type", "source_curation_id", "source_curation_dir"):
        if field_name in preset_entry.payload:
            context[field_name] = preset_entry.payload.get(field_name)
    return context


def _build_evaluation_preset_context(
    entry: LaunchProfileEntry,
    request: Any,
) -> dict[str, Any] | None:
    eval_preset_id = _optional_str(entry.payload.get("eval_preset_id"))
    if eval_preset_id is None:
        return None

    context: dict[str, Any] = {
        "eval_preset_id": eval_preset_id,
    }

    entries, _warnings = scan_evaluation_presets()
    preset_entry = find_evaluation_preset_entry(entries, eval_preset_id)
    if preset_entry is None:
        return context
    if not _request_matches_eval_payload(request, preset_entry.payload):
        return context

    context["eval_preset_path"] = preset_entry.preset_path
    return context


def _build_launch_profile_context(entry: LaunchProfileEntry) -> dict[str, Any]:
    return {
        "launch_profile_id": entry.profile_id,
        "launch_profile_path": entry.profile_path,
    }


def _write_launch_profile_run_context(
    *,
    entry: LaunchProfileEntry,
    request: Any,
    batch_status: str,
    error_message: str | None,
) -> Path:
    payload = build_run_context_payload_from_request(
        request,
        launch_source_type="launch_profile",
        accepted_baseline=_build_accepted_baseline_context(entry, request),
        benchmark_preset=_build_benchmark_preset_context(entry, request),
        evaluation_preset=_build_evaluation_preset_context(entry, request),
        launch_profile=_build_launch_profile_context(entry),
        batch_status=batch_status,
        error_message=error_message,
    )
    return write_run_context(request.batch_dir, payload)


def main() -> None:
    args = parse_args()

    launch_profile_id = _optional_str(args.launch_profile_id)
    batch_id = _optional_str(args.batch_id)
    initial_doi_context = _optional_str(args.initial_doi_context) or ""

    if launch_profile_id is None:
        raise LaunchProfileBatchRunError("--launch-profile-id must not be empty")
    if batch_id is None:
        raise LaunchProfileBatchRunError("--batch-id must not be empty")

    entries, _registry_warnings = scan_launch_profiles()
    launch_profile_entry = find_launch_profile_entry(entries, launch_profile_id)
    if launch_profile_entry is None:
        raise LaunchProfileBatchRunError(f"Launch profile not found: {launch_profile_id}")

    resolved_values, selected_warnings = build_launch_profile_run_batch_values(
        launch_profile_entry,
        allowed_label_sources=SUPPORTED_LABEL_SOURCES,
        fallback_label_source=SUPPORTED_LABEL_SOURCES[0],
    )

    try:
        request = build_batch_run_request(
            initial_doi_context=initial_doi_context,
            theory_config_path=resolved_values.get("theory_config_path", ""),
            seeds_csv_path=resolved_values.get("seeds_csv_path", ""),
            batch_id=batch_id,
            max_references=resolved_values.get("max_references", ""),
            max_related=resolved_values.get("max_related", ""),
            max_hard_negatives=resolved_values.get("max_hard_negatives", ""),
            top_k=resolved_values.get("top_k", ""),
            label_source=resolved_values.get("label_source", ""),
            refresh=bool(resolved_values.get("refresh", False)),
        )
    except BatchRunValidationError as exc:
        warning_prefix = ""
        if selected_warnings:
            warning_prefix = "Launch profile warnings:\n" + "\n".join(selected_warnings) + "\n"
        raise LaunchProfileBatchRunError(
            f"{warning_prefix}Could not build batch request for launch profile '{launch_profile_id}':\n{exc}"
        ) from exc

    outcome = run_batch_request(
        request,
        run_batch_service=run_seed_batch,
        batch_loader=load_batch_bundle,
    )

    run_context_written = False
    if request.batch_dir.exists():
        batch_status = _optional_str(getattr(outcome.summary, "status", None)) or (
            "completed" if outcome.success else "failed"
        )
        try:
            _write_launch_profile_run_context(
                entry=launch_profile_entry,
                request=request,
                batch_status=batch_status,
                error_message=outcome.error_message,
            )
            run_context_written = True
        except RunContextError as exc:
            raise LaunchProfileBatchRunError(f"Failed to write run_context.json: {exc}") from exc

    if not outcome.success:
        raise LaunchProfileBatchRunError(
            outcome.error_message or "Backend batch run failed without an error message."
        )

    if outcome.summary is None or outcome.loaded_bundle is None:
        raise LaunchProfileBatchRunError(
            "Backend batch run reported success but did not return the expected batch summary."
        )

    print(f"launch_profile_id: {launch_profile_entry.profile_id}")
    print(f"batch_id: {outcome.summary.batch_id}")
    print(f"batch_dir: {outcome.summary.batch_dir}")
    print(f"run_context_written: {str(run_context_written).lower()}")
    print(f"warnings_count: {len(selected_warnings)}")


if __name__ == "__main__":
    try:
        main()
    except LaunchProfileBatchRunError as exc:
        raise SystemExit(f"ERROR: {exc}")
