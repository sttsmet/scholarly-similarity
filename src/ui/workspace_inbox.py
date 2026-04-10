from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.baseline_registry import scan_accepted_baselines
from src.ui.cohort_study_registry import build_cohort_study_candidate_rows, scan_cohort_studies
from src.ui.study_provenance import extract_study_source_fields


WORKSPACE_INBOX_QUEUE_TYPES = (
    "shortlisted_candidates",
    "review_packets_pending_candidate_work",
    "candidate_runs_pending_outcome",
    "accepted_outcomes_pending_promotion",
    "weak_guarded_outcomes",
    "recent_accepted_baselines",
)

WORKSPACE_INBOX_QUEUE_LABELS = {
    "shortlisted_candidates": "Shortlisted Candidates",
    "review_packets_pending_candidate_work": "Review Packets Pending Candidate Work",
    "candidate_runs_pending_outcome": "Candidate Runs Pending Outcome",
    "accepted_outcomes_pending_promotion": "Accepted Outcomes Pending Promotion",
    "weak_guarded_outcomes": "Weak / Guarded Outcomes",
    "recent_accepted_baselines": "Recent Accepted Baselines",
}


@dataclass(frozen=True, slots=True)
class WorkspaceInboxItem:
    queue_type: str
    item_id: str
    item_key: str
    created_at: str | None
    created_at_display: str
    created_at_source: str
    timestamp_sort_key: float
    status: str | None
    decision: str | None
    decision_status: str | None
    usable: bool | None
    artifact_dir: Path
    artifact_dir_exists: bool
    comparison_id: str | None
    packet_id: str | None
    candidate_id: str | None
    outcome_id: str | None
    baseline_id: str | None
    study_id: str | None
    selected_metric: str | None
    guardrail_verdict: str | None
    source_candidate_decision: str | None
    suggested_decision: str | None
    accepted_baseline_id: str | None
    launch_profile_id: str | None
    target_primary_batch_id: str | None
    target_primary_batch_dir: Path | None
    target_primary_batch_exists: bool
    target_secondary_batch_id: str | None
    target_secondary_batch_dir: Path | None
    target_secondary_batch_exists: bool
    summary: str
    raw_payload: dict[str, Any] | None


def scan_workspace_inbox(
    base_dir: str | Path | None = None,
) -> tuple[list[WorkspaceInboxItem], list[str]]:
    runs_dir = _runs_dir(base_dir)
    warnings: list[str] = []
    items: list[WorkspaceInboxItem] = []

    items.extend(_scan_shortlisted_candidates(runs_dir, warnings))

    accepted_baseline_entries, baseline_warnings = scan_accepted_baselines(runs_dir / "accepted_baselines")
    warnings.extend(baseline_warnings)
    promoted_outcome_ids = _promoted_outcome_ids(accepted_baseline_entries)
    items.extend(_scan_recent_accepted_baselines(accepted_baseline_entries))

    comparisons_dir = runs_dir / "comparisons"
    if comparisons_dir.exists() and comparisons_dir.is_dir():
        for comparison_dir in sorted(comparisons_dir.iterdir(), key=lambda path: path.name.lower()):
            if not comparison_dir.is_dir():
                continue
            review_packets_dir = comparison_dir / "review_packets"
            if not review_packets_dir.exists() or not review_packets_dir.is_dir():
                continue
            for packet_dir in sorted(review_packets_dir.iterdir(), key=lambda path: path.name.lower()):
                if not packet_dir.is_dir():
                    continue
                packet_manifest_path = packet_dir / "review_packet_manifest.json"
                if not packet_manifest_path.exists():
                    continue
                try:
                    packet_manifest = _load_json_object(packet_manifest_path)
                except ValueError as exc:
                    warnings.append(f"Skipped review packet '{packet_dir.name}': {exc}")
                    continue

                if not _has_candidate_apply_descendants(packet_dir):
                    items.append(_build_review_packet_pending_item(packet_dir, packet_manifest, packet_manifest_path))

                candidate_runs_dir = packet_dir / "candidate_runs"
                if not candidate_runs_dir.exists() or not candidate_runs_dir.is_dir():
                    continue
                for candidate_run_dir in sorted(candidate_runs_dir.iterdir(), key=lambda path: path.name.lower()):
                    if not candidate_run_dir.is_dir():
                        continue
                    candidate_manifest_path = candidate_run_dir / "candidate_apply_manifest.json"
                    if not candidate_manifest_path.exists():
                        continue
                    try:
                        candidate_manifest = _load_json_object(candidate_manifest_path)
                    except ValueError as exc:
                        warnings.append(f"Skipped candidate run '{candidate_run_dir.name}': {exc}")
                        continue

                    if not _has_outcome_descendants(candidate_run_dir):
                        items.append(
                            _build_candidate_run_pending_item(
                                candidate_run_dir,
                                candidate_manifest,
                                candidate_manifest_path,
                            )
                        )

                    outcomes_dir = candidate_run_dir / "outcomes"
                    if not outcomes_dir.exists() or not outcomes_dir.is_dir():
                        continue
                    for outcome_dir in sorted(outcomes_dir.iterdir(), key=lambda path: path.name.lower()):
                        if not outcome_dir.is_dir():
                            continue
                        outcome_manifest_path = outcome_dir / "reeval_outcome_manifest.json"
                        if not outcome_manifest_path.exists():
                            continue
                        try:
                            outcome_manifest = _load_json_object(outcome_manifest_path)
                        except ValueError as exc:
                            warnings.append(f"Skipped re-eval outcome '{outcome_dir.name}': {exc}")
                            continue

                        pending_promotion_item = _build_pending_promotion_item(
                            outcome_dir,
                            outcome_manifest,
                            outcome_manifest_path,
                            promoted_outcome_ids=promoted_outcome_ids,
                        )
                        if pending_promotion_item is not None:
                            items.append(pending_promotion_item)

                        weak_outcome_item = _build_weak_outcome_item(
                            outcome_dir,
                            outcome_manifest,
                            outcome_manifest_path,
                        )
                        if weak_outcome_item is not None:
                            items.append(weak_outcome_item)

    items.sort(
        key=lambda item: (
            WORKSPACE_INBOX_QUEUE_TYPES.index(item.queue_type),
            -item.timestamp_sort_key,
            item.item_id.lower(),
            str(item.artifact_dir).lower(),
        )
    )
    return items, warnings


def filter_workspace_inbox_items(
    items: list[WorkspaceInboxItem],
    *,
    queue_types: list[str] | None = None,
    search_text: str | None = None,
) -> list[WorkspaceInboxItem]:
    allowed_queue_types = {
        queue_type for queue_type in (queue_types or list(WORKSPACE_INBOX_QUEUE_TYPES))
        if queue_type in WORKSPACE_INBOX_QUEUE_TYPES
    }
    if not allowed_queue_types:
        allowed_queue_types = set(WORKSPACE_INBOX_QUEUE_TYPES)

    needle = _optional_str(search_text)
    lowered_needle = needle.lower() if needle is not None else None
    filtered: list[WorkspaceInboxItem] = []
    for item in items:
        if item.queue_type not in allowed_queue_types:
            continue
        if lowered_needle is not None and lowered_needle not in _item_search_blob(item):
            continue
        filtered.append(item)
    return filtered


def group_workspace_inbox_items(
    items: list[WorkspaceInboxItem],
    *,
    recent_limit: int | None = None,
) -> dict[str, list[WorkspaceInboxItem]]:
    grouped = {queue_type: [] for queue_type in WORKSPACE_INBOX_QUEUE_TYPES}
    for item in items:
        grouped.setdefault(item.queue_type, []).append(item)
    if recent_limit is not None and recent_limit > 0:
        return {queue_type: rows[:recent_limit] for queue_type, rows in grouped.items()}
    return grouped


def build_workspace_inbox_table_rows(
    items: list[WorkspaceInboxItem],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                "item_id": item.item_id,
                "created_at": item.created_at_display,
                "status": item.status or item.decision_status or item.decision or "n/a",
                "study_id": item.study_id,
                "comparison_id": item.comparison_id,
                "packet_id": item.packet_id,
                "candidate_id": item.candidate_id,
                "outcome_id": item.outcome_id,
                "baseline_id": item.baseline_id,
                "selected_metric": item.selected_metric,
                "guardrail_verdict": item.guardrail_verdict,
                "summary": item.summary,
                "artifact_dir": _display_path(item.artifact_dir),
            }
        )
    return rows


def choose_default_workspace_inbox_item_key(
    items: list[WorkspaceInboxItem],
    *,
    preferred_item_key: str | None = None,
) -> str | None:
    if preferred_item_key:
        for item in items:
            if item.item_key == preferred_item_key:
                return item.item_key
    if not items:
        return None
    return items[0].item_key


def find_workspace_inbox_item(
    items: list[WorkspaceInboxItem],
    item_key: str | None,
) -> WorkspaceInboxItem | None:
    needle = _optional_str(item_key)
    if needle is None:
        return None
    for item in items:
        if item.item_key == needle:
            return item
    return None


def build_workspace_inbox_detail(
    item: WorkspaceInboxItem,
) -> dict[str, Any]:
    return {
        "identity": {
            "queue_type": item.queue_type,
            "item_id": item.item_id,
            "created_at": item.created_at_display,
            "created_at_source": item.created_at_source,
            "artifact_dir": item.artifact_dir,
            "artifact_dir_exists": item.artifact_dir_exists,
            "summary": item.summary,
        },
        "lineage": {
            "study_id": item.study_id,
            "comparison_id": item.comparison_id,
            "packet_id": item.packet_id,
            "candidate_id": item.candidate_id,
            "outcome_id": item.outcome_id,
            "baseline_id": item.baseline_id,
            "accepted_baseline_id": item.accepted_baseline_id,
            "launch_profile_id": item.launch_profile_id,
        },
        "decision": {
            "status": item.status,
            "decision": item.decision,
            "decision_status": item.decision_status,
            "usable": item.usable,
            "selected_metric": item.selected_metric,
            "guardrail_verdict": item.guardrail_verdict,
            "source_candidate_decision": item.source_candidate_decision,
            "suggested_decision": item.suggested_decision,
        },
        "targets": {
            "primary_batch_id": item.target_primary_batch_id,
            "primary_batch_dir": item.target_primary_batch_dir,
            "primary_batch_exists": item.target_primary_batch_exists,
            "secondary_batch_id": item.target_secondary_batch_id,
            "secondary_batch_dir": item.target_secondary_batch_dir,
            "secondary_batch_exists": item.target_secondary_batch_exists,
        },
        "raw_payload": item.raw_payload,
    }


def _scan_shortlisted_candidates(
    runs_dir: Path,
    warnings: list[str],
) -> list[WorkspaceInboxItem]:
    study_entries, study_warnings = scan_cohort_studies(runs_dir / "cohort_studies")
    warnings.extend(study_warnings)

    items: list[WorkspaceInboxItem] = []
    for entry in study_entries:
        for candidate_row in build_cohort_study_candidate_rows(entry):
            if _optional_str(candidate_row.decision) != "shortlist":
                continue
            summary = (
                f"Study `{entry.study_id}` shortlisted `{candidate_row.candidate_batch_id}` "
                f"against reference `{entry.reference_batch_id or 'n/a'}`."
            )
            items.append(
                WorkspaceInboxItem(
                    queue_type="shortlisted_candidates",
                    item_id=candidate_row.candidate_batch_id,
                    item_key=f"shortlisted_candidates::{entry.study_id}::{candidate_row.candidate_batch_id}",
                    created_at=entry.created_at,
                    created_at_display=entry.created_at_display,
                    created_at_source=entry.created_at_source,
                    timestamp_sort_key=entry.timestamp_sort_key,
                    status=candidate_row.candidate_status,
                    decision=candidate_row.decision,
                    decision_status=None,
                    usable=candidate_row.usable,
                    artifact_dir=entry.study_dir,
                    artifact_dir_exists=entry.study_dir.exists(),
                    comparison_id=None,
                    packet_id=None,
                    candidate_id=None,
                    outcome_id=None,
                    baseline_id=None,
                    study_id=entry.study_id,
                    selected_metric=candidate_row.selected_metric or entry.selected_metric,
                    guardrail_verdict=candidate_row.guardrail_verdict,
                    source_candidate_decision=candidate_row.decision,
                    suggested_decision=candidate_row.suggested_decision,
                    accepted_baseline_id=candidate_row.accepted_baseline_id,
                    launch_profile_id=candidate_row.launch_profile_id,
                    target_primary_batch_id=entry.reference_batch_id,
                    target_primary_batch_dir=entry.reference_batch_dir,
                    target_primary_batch_exists=entry.reference_batch_exists,
                    target_secondary_batch_id=candidate_row.candidate_batch_id,
                    target_secondary_batch_dir=candidate_row.candidate_batch_dir,
                    target_secondary_batch_exists=candidate_row.candidate_batch_exists,
                    summary=summary,
                    raw_payload={
                        "study_manifest": entry.manifest,
                        "candidate_decision_row": candidate_row.raw_decision_row,
                        "candidate_leaderboard_row": candidate_row.raw_leaderboard_row,
                    },
                )
            )
    return items


def _scan_recent_accepted_baselines(
    entries: list[Any],
) -> list[WorkspaceInboxItem]:
    items: list[WorkspaceInboxItem] = []
    for entry in entries:
        created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
            _optional_str(entry.manifest.get("created_at")),
            entry.manifest_path.stat().st_mtime,
        )
        source_lineage = _dict_copy(entry.manifest.get("source_lineage"))
        study_source = extract_study_source_fields(entry.manifest, entry.promotion_record)
        summary = (
            f"Accepted baseline `{entry.baseline_id}` from outcome "
            f"`{_optional_str(source_lineage.get('outcome_id')) or 'n/a'}`."
        )
        items.append(
            WorkspaceInboxItem(
                queue_type="recent_accepted_baselines",
                item_id=entry.baseline_id,
                item_key=f"recent_accepted_baselines::{entry.baseline_id}",
                created_at=created_at,
                created_at_display=created_at_display,
                created_at_source=created_at_source,
                timestamp_sort_key=timestamp_sort_key,
                status=_optional_str(entry.manifest.get("decision_status")),
                decision=None,
                decision_status=_optional_str(entry.manifest.get("decision_status")),
                usable=None,
                artifact_dir=entry.baseline_dir,
                artifact_dir_exists=entry.baseline_dir.exists(),
                comparison_id=_optional_str(source_lineage.get("comparison_id")),
                packet_id=_optional_str(source_lineage.get("packet_id")),
                candidate_id=_optional_str(source_lineage.get("candidate_id")),
                outcome_id=_optional_str(source_lineage.get("outcome_id")),
                baseline_id=entry.baseline_id,
                study_id=_optional_str(study_source.get("source_study_id")),
                selected_metric=_optional_str(entry.manifest.get("selected_metric")),
                guardrail_verdict=_optional_str(entry.manifest.get("guardrail_verdict")),
                source_candidate_decision=_optional_str(study_source.get("source_candidate_decision")),
                suggested_decision=_optional_str(study_source.get("source_suggested_decision")),
                accepted_baseline_id=entry.baseline_id,
                launch_profile_id=None,
                target_primary_batch_id=None,
                target_primary_batch_dir=None,
                target_primary_batch_exists=False,
                target_secondary_batch_id=None,
                target_secondary_batch_dir=None,
                target_secondary_batch_exists=False,
                summary=summary,
                raw_payload=entry.manifest,
            )
        )
    return items


def _build_review_packet_pending_item(
    packet_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
) -> WorkspaceInboxItem:
    created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
        _optional_str(manifest.get("created_at")),
        manifest_path.stat().st_mtime,
    )
    study_source = extract_study_source_fields(manifest)
    primary_batch = _dict_copy(manifest.get("primary_batch"))
    primary_batch_dir = _resolve_optional_path(primary_batch.get("batch_dir"))
    summary = (
        f"Review packet `{_optional_str(manifest.get('packet_id')) or packet_dir.name}` "
        "has no saved candidate apply manifest detected yet."
    )
    return WorkspaceInboxItem(
        queue_type="review_packets_pending_candidate_work",
        item_id=_optional_str(manifest.get("packet_id")) or packet_dir.name,
        item_key=(
            "review_packets_pending_candidate_work::"
            f"{_optional_str(manifest.get('comparison_id')) or packet_dir.parent.parent.name}::"
            f"{_optional_str(manifest.get('packet_id')) or packet_dir.name}"
        ),
        created_at=created_at,
        created_at_display=created_at_display,
        created_at_source=created_at_source,
        timestamp_sort_key=timestamp_sort_key,
        status=_optional_str(manifest.get("status")),
        decision=None,
        decision_status=None,
        usable=None,
        artifact_dir=packet_dir,
        artifact_dir_exists=packet_dir.exists(),
        comparison_id=_optional_str(manifest.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")) or packet_dir.name,
        candidate_id=None,
        outcome_id=None,
        baseline_id=None,
        study_id=_optional_str(study_source.get("source_study_id")),
        selected_metric=_optional_str(manifest.get("selected_packet_metric"))
        or _optional_str(study_source.get("source_selected_metric")),
        guardrail_verdict=None,
        source_candidate_decision=_optional_str(study_source.get("source_candidate_decision")),
        suggested_decision=_optional_str(study_source.get("source_suggested_decision")),
        accepted_baseline_id=None,
        launch_profile_id=None,
        target_primary_batch_id=_optional_str(primary_batch.get("batch_id")),
        target_primary_batch_dir=primary_batch_dir,
        target_primary_batch_exists=bool(primary_batch_dir is not None and primary_batch_dir.exists()),
        target_secondary_batch_id=None,
        target_secondary_batch_dir=None,
        target_secondary_batch_exists=False,
        summary=summary,
        raw_payload=manifest,
    )


def _build_candidate_run_pending_item(
    candidate_run_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
) -> WorkspaceInboxItem:
    created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
        _optional_str(manifest.get("created_at")),
        manifest_path.stat().st_mtime,
    )
    study_source = extract_study_source_fields(manifest)
    source_primary_batch = _dict_copy(manifest.get("source_primary_batch"))
    primary_batch_dir = _resolve_optional_path(source_primary_batch.get("batch_dir"))
    output_batch_dir = _resolve_optional_path(manifest.get("output_batch_dir"))
    summary = (
        f"Candidate run `{_optional_str(manifest.get('candidate_id')) or candidate_run_dir.name}` "
        "has no saved re-eval outcome manifest detected yet."
    )
    return WorkspaceInboxItem(
        queue_type="candidate_runs_pending_outcome",
        item_id=_optional_str(manifest.get("candidate_id")) or candidate_run_dir.name,
        item_key=(
            "candidate_runs_pending_outcome::"
            f"{_optional_str(manifest.get('comparison_id')) or candidate_run_dir.parents[2].name}::"
            f"{_optional_str(manifest.get('packet_id')) or candidate_run_dir.parent.parent.name}::"
            f"{_optional_str(manifest.get('candidate_id')) or candidate_run_dir.name}"
        ),
        created_at=created_at,
        created_at_display=created_at_display,
        created_at_source=created_at_source,
        timestamp_sort_key=timestamp_sort_key,
        status=_optional_str(manifest.get("status")),
        decision=None,
        decision_status=None,
        usable=None,
        artifact_dir=candidate_run_dir,
        artifact_dir_exists=candidate_run_dir.exists(),
        comparison_id=_optional_str(manifest.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")),
        candidate_id=_optional_str(manifest.get("candidate_id")) or candidate_run_dir.name,
        outcome_id=None,
        baseline_id=None,
        study_id=_optional_str(study_source.get("source_study_id")),
        selected_metric=_optional_str(manifest.get("selected_metric_context"))
        or _optional_str(study_source.get("source_selected_metric")),
        guardrail_verdict=None,
        source_candidate_decision=_optional_str(study_source.get("source_candidate_decision")),
        suggested_decision=_optional_str(study_source.get("source_suggested_decision")),
        accepted_baseline_id=None,
        launch_profile_id=None,
        target_primary_batch_id=_optional_str(source_primary_batch.get("batch_id")),
        target_primary_batch_dir=primary_batch_dir,
        target_primary_batch_exists=bool(primary_batch_dir is not None and primary_batch_dir.exists()),
        target_secondary_batch_id=_optional_str(manifest.get("output_batch_id")),
        target_secondary_batch_dir=output_batch_dir,
        target_secondary_batch_exists=bool(output_batch_dir is not None and output_batch_dir.exists()),
        summary=summary,
        raw_payload=manifest,
    )


def _build_pending_promotion_item(
    outcome_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
    *,
    promoted_outcome_ids: set[str],
) -> WorkspaceInboxItem | None:
    if _optional_str(manifest.get("decision_status")) != "accept_candidate":
        return None
    outcome_id = _optional_str(manifest.get("outcome_id"))
    if outcome_id is None or outcome_id in promoted_outcome_ids:
        return None

    created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
        _optional_str(manifest.get("created_at")),
        manifest_path.stat().st_mtime,
    )
    study_source = extract_study_source_fields(manifest)
    primary_batch = _dict_copy(manifest.get("primary_batch"))
    secondary_batch = _dict_copy(manifest.get("secondary_batch"))
    primary_batch_dir = _resolve_optional_path(primary_batch.get("batch_dir"))
    secondary_batch_dir = _resolve_optional_path(secondary_batch.get("batch_dir"))
    summary = f"Accepted outcome `{outcome_id}` has no linked accepted baseline artifact detected yet."
    return WorkspaceInboxItem(
        queue_type="accepted_outcomes_pending_promotion",
        item_id=outcome_id,
        item_key=f"accepted_outcomes_pending_promotion::{outcome_id}",
        created_at=created_at,
        created_at_display=created_at_display,
        created_at_source=created_at_source,
        timestamp_sort_key=timestamp_sort_key,
        status=_optional_str(manifest.get("status")),
        decision=None,
        decision_status=_optional_str(manifest.get("decision_status")),
        usable=None,
        artifact_dir=outcome_dir,
        artifact_dir_exists=outcome_dir.exists(),
        comparison_id=_optional_str(manifest.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")),
        candidate_id=_optional_str(manifest.get("candidate_id")),
        outcome_id=outcome_id,
        baseline_id=None,
        study_id=_optional_str(study_source.get("source_study_id")),
        selected_metric=_optional_str(manifest.get("selected_metric"))
        or _optional_str(study_source.get("source_selected_metric")),
        guardrail_verdict=_optional_str(manifest.get("guardrail_verdict")),
        source_candidate_decision=_optional_str(study_source.get("source_candidate_decision")),
        suggested_decision=_optional_str(study_source.get("source_suggested_decision")),
        accepted_baseline_id=None,
        launch_profile_id=None,
        target_primary_batch_id=_optional_str(primary_batch.get("batch_id")),
        target_primary_batch_dir=primary_batch_dir,
        target_primary_batch_exists=bool(primary_batch_dir is not None and primary_batch_dir.exists()),
        target_secondary_batch_id=_optional_str(secondary_batch.get("batch_id")),
        target_secondary_batch_dir=secondary_batch_dir,
        target_secondary_batch_exists=bool(secondary_batch_dir is not None and secondary_batch_dir.exists()),
        summary=summary,
        raw_payload=manifest,
    )


def _build_weak_outcome_item(
    outcome_dir: Path,
    manifest: dict[str, Any],
    manifest_path: Path,
) -> WorkspaceInboxItem | None:
    guardrail_verdict = _optional_str(manifest.get("guardrail_verdict"))
    if guardrail_verdict not in {"weak", "fail"}:
        return None

    created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
        _optional_str(manifest.get("created_at")),
        manifest_path.stat().st_mtime,
    )
    study_source = extract_study_source_fields(manifest)
    primary_batch = _dict_copy(manifest.get("primary_batch"))
    secondary_batch = _dict_copy(manifest.get("secondary_batch"))
    primary_batch_dir = _resolve_optional_path(primary_batch.get("batch_dir"))
    secondary_batch_dir = _resolve_optional_path(secondary_batch.get("batch_dir"))
    outcome_id = _optional_str(manifest.get("outcome_id")) or outcome_dir.name
    summary = f"Outcome `{outcome_id}` is guarded by evidence verdict `{guardrail_verdict}`."
    return WorkspaceInboxItem(
        queue_type="weak_guarded_outcomes",
        item_id=outcome_id,
        item_key=f"weak_guarded_outcomes::{outcome_id}",
        created_at=created_at,
        created_at_display=created_at_display,
        created_at_source=created_at_source,
        timestamp_sort_key=timestamp_sort_key,
        status=_optional_str(manifest.get("status")),
        decision=None,
        decision_status=_optional_str(manifest.get("decision_status")),
        usable=None,
        artifact_dir=outcome_dir,
        artifact_dir_exists=outcome_dir.exists(),
        comparison_id=_optional_str(manifest.get("comparison_id")),
        packet_id=_optional_str(manifest.get("packet_id")),
        candidate_id=_optional_str(manifest.get("candidate_id")),
        outcome_id=outcome_id,
        baseline_id=None,
        study_id=_optional_str(study_source.get("source_study_id")),
        selected_metric=_optional_str(manifest.get("selected_metric"))
        or _optional_str(study_source.get("source_selected_metric")),
        guardrail_verdict=guardrail_verdict,
        source_candidate_decision=_optional_str(study_source.get("source_candidate_decision")),
        suggested_decision=_optional_str(study_source.get("source_suggested_decision")),
        accepted_baseline_id=None,
        launch_profile_id=None,
        target_primary_batch_id=_optional_str(primary_batch.get("batch_id")),
        target_primary_batch_dir=primary_batch_dir,
        target_primary_batch_exists=bool(primary_batch_dir is not None and primary_batch_dir.exists()),
        target_secondary_batch_id=_optional_str(secondary_batch.get("batch_id")),
        target_secondary_batch_dir=secondary_batch_dir,
        target_secondary_batch_exists=bool(secondary_batch_dir is not None and secondary_batch_dir.exists()),
        summary=summary,
        raw_payload=manifest,
    )


def _runs_dir(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return REPO_ROOT / "runs"
    path = Path(str(base_dir).strip()).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (REPO_ROOT / path).resolve()


def _promoted_outcome_ids(entries: list[Any]) -> set[str]:
    outcome_ids: set[str] = set()
    for entry in entries:
        source_lineage = _dict_copy(entry.manifest.get("source_lineage"))
        outcome_id = _optional_str(source_lineage.get("outcome_id"))
        if outcome_id is not None:
            outcome_ids.add(outcome_id)
    return outcome_ids


def _has_candidate_apply_descendants(packet_dir: Path) -> bool:
    candidate_runs_dir = packet_dir / "candidate_runs"
    if not candidate_runs_dir.exists() or not candidate_runs_dir.is_dir():
        return False
    for candidate_dir in candidate_runs_dir.iterdir():
        if candidate_dir.is_dir() and (candidate_dir / "candidate_apply_manifest.json").exists():
            return True
    return False


def _has_outcome_descendants(candidate_run_dir: Path) -> bool:
    outcomes_dir = candidate_run_dir / "outcomes"
    if not outcomes_dir.exists() or not outcomes_dir.is_dir():
        return False
    for outcome_dir in outcomes_dir.iterdir():
        if outcome_dir.is_dir() and (outcome_dir / "reeval_outcome_manifest.json").exists():
            return True
    return False


def _timestamp_fields(created_at_text: str | None, mtime: float) -> tuple[str | None, str, str, float]:
    parsed_timestamp = _parse_timestamp(created_at_text)
    if parsed_timestamp is not None:
        return (
            created_at_text,
            created_at_text,
            "manifest created_at",
            parsed_timestamp.timestamp(),
        )
    timestamp = datetime.fromtimestamp(mtime, tz=timezone.utc)
    display = timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return (
        None,
        display,
        "manifest mtime",
        timestamp.timestamp(),
    )


def _parse_timestamp(value: str | None) -> datetime | None:
    text = _optional_str(value)
    if text is None:
        return None
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _item_search_blob(item: WorkspaceInboxItem) -> str:
    values = (
        item.queue_type,
        item.item_id,
        item.status,
        item.decision,
        item.decision_status,
        item.summary,
        item.study_id,
        item.comparison_id,
        item.packet_id,
        item.candidate_id,
        item.outcome_id,
        item.baseline_id,
        item.accepted_baseline_id,
        item.launch_profile_id,
        item.selected_metric,
        item.guardrail_verdict,
        _display_path(item.artifact_dir),
        _display_path(item.target_primary_batch_dir),
        _display_path(item.target_secondary_batch_dir),
    )
    return " ".join(value.lower() for value in values if isinstance(value, str))


def _resolve_optional_path(value: Any) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid {path.name}: expected a JSON object")
    return payload


def _dict_copy(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _display_path(value: str | Path | None) -> str | None:
    if value is None:
        return None
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
