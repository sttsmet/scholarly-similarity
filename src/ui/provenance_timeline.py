from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.run_context import build_run_context_summary, load_run_context_if_present


@dataclass(frozen=True, slots=True)
class TimelineEntry:
    entry_key: str
    artifact_type: str
    artifact_id: str
    created_at: str | None
    timestamp_display: str
    timestamp_source: str
    timestamp_sort_key: float
    status: str | None
    artifact_dir: Path
    manifest_path: Path
    lineage: dict[str, Any]
    summary: str
    manifest: dict[str, Any]


_MANIFEST_GLOBS: tuple[tuple[str, str], ...] = (
    ("batch", "runs/batches/*/batch_manifest.json"),
    ("comparison", "runs/comparisons/*/comparison_manifest.json"),
    ("review_packet", "runs/comparisons/*/review_packets/*/review_packet_manifest.json"),
    ("candidate_run", "runs/comparisons/*/review_packets/*/candidate_runs/*/candidate_apply_manifest.json"),
    (
        "reeval_outcome",
        "runs/comparisons/*/review_packets/*/candidate_runs/*/outcomes/*/reeval_outcome_manifest.json",
    ),
    ("accepted_baseline", "runs/accepted_baselines/*/accepted_baseline_manifest.json"),
)


def scan_provenance_timeline(
    base_dir: str | Path | None = None,
) -> tuple[list[TimelineEntry], list[str]]:
    repo_root = _repo_root(base_dir)
    if not repo_root.exists():
        return [], []

    entries: list[TimelineEntry] = []
    warnings: list[str] = []
    for artifact_type, pattern in _MANIFEST_GLOBS:
        for manifest_path in sorted(repo_root.glob(pattern), key=lambda path: str(path).lower()):
            try:
                manifest = _load_json_object(manifest_path)
            except ValueError as exc:
                warnings.append(f"Skipped `{_display_path(manifest_path)}`: {exc}")
                continue
            entries.append(_build_timeline_entry(artifact_type, manifest_path, manifest))

    entries.sort(
        key=lambda entry: (
            -entry.timestamp_sort_key,
            entry.artifact_type,
            entry.artifact_id.lower(),
            str(entry.artifact_dir).lower(),
        )
    )
    return entries, warnings


def build_timeline_rows(entries: list[TimelineEntry]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        rows.append(
            {
                "timestamp": entry.timestamp_display,
                "artifact_type": entry.artifact_type,
                "artifact_id": entry.artifact_id,
                "status": entry.status,
                "summary": entry.summary,
                "path": _display_path(entry.artifact_dir),
            }
        )
    return rows


def available_timeline_statuses(entries: list[TimelineEntry]) -> list[str]:
    return sorted({status for entry in entries if (status := _optional_str(entry.status)) is not None})


def filter_timeline_entries(
    entries: list[TimelineEntry],
    *,
    artifact_types: list[str] | None = None,
    statuses: list[str] | None = None,
    search_text: str | None = None,
) -> list[TimelineEntry]:
    selected_types = {value for value in artifact_types or [] if value}
    selected_statuses = {value for value in statuses or [] if value}
    search_terms = _optional_str(search_text)
    needle = search_terms.lower() if search_terms is not None else None

    filtered: list[TimelineEntry] = []
    for entry in entries:
        if selected_types and entry.artifact_type not in selected_types:
            continue
        if selected_statuses and _optional_str(entry.status) not in selected_statuses:
            continue
        if needle is not None and needle not in _search_blob(entry):
            continue
        filtered.append(entry)
    return filtered


def choose_default_timeline_entry_key(
    entries: list[TimelineEntry],
    *,
    preferred_entry_key: str | None = None,
) -> str | None:
    if preferred_entry_key:
        for entry in entries:
            if entry.entry_key == preferred_entry_key:
                return entry.entry_key
    if not entries:
        return None
    return entries[0].entry_key


def find_timeline_entry(
    entries: list[TimelineEntry],
    entry_key: str | None,
) -> TimelineEntry | None:
    if not entry_key:
        return None
    for entry in entries:
        if entry.entry_key == entry_key:
            return entry
    return None


def build_timeline_detail(entry: TimelineEntry) -> dict[str, Any]:
    manifest = entry.manifest
    run_context_payload: dict[str, Any] | None = None
    run_context_warning: str | None = None
    run_context_summary: dict[str, Any] | None = None
    if entry.artifact_type == "batch":
        run_context_payload, run_context_warning = load_run_context_if_present(entry.artifact_dir)
        run_context_summary = (
            build_run_context_summary(run_context_payload)
            if isinstance(run_context_payload, dict)
            else None
        )
    detail = {
        "identity": {
            "artifact_type": entry.artifact_type,
            "artifact_id": entry.artifact_id,
            "created_at": entry.created_at,
            "timestamp_display": entry.timestamp_display,
            "timestamp_source": entry.timestamp_source,
            "status": entry.status,
            "artifact_dir": entry.artifact_dir,
            "manifest_path": entry.manifest_path,
            "summary": entry.summary,
        },
        "lineage": dict(entry.lineage),
        "key_fields": {},
        "run_context_summary": run_context_summary,
        "run_context_payload": run_context_payload,
        "run_context_warning": run_context_warning,
        "raw_manifest": manifest,
    }

    if entry.artifact_type == "batch":
        detail["key_fields"] = {
            "batch_id": entry.artifact_id,
            "seeds_csv": manifest.get("seeds_csv"),
            "theory_config": manifest.get("theory_config"),
            "seed_count": manifest.get("seed_count"),
            "completed_seed_count": manifest.get("completed_seed_count"),
            "failed_seed_count": manifest.get("failed_seed_count"),
            "options": _dict_copy(manifest.get("options")),
        }
    elif entry.artifact_type == "comparison":
        detail["key_fields"] = {
            "comparison_id": entry.artifact_id,
            "primary_batch_id": _dict_value(manifest.get("primary_batch"), "batch_id"),
            "secondary_batch_id": _dict_value(manifest.get("secondary_batch"), "batch_id"),
            "selected_metric": manifest.get("selected_comparison_metric"),
            "common_doi_count": manifest.get("common_doi_count"),
            "common_completed_seed_count": manifest.get("common_completed_seed_count"),
        }
    elif entry.artifact_type == "review_packet":
        detail["key_fields"] = {
            "packet_id": entry.artifact_id,
            "comparison_id": manifest.get("comparison_id"),
            "reviewer": manifest.get("reviewer"),
            "selected_metric": manifest.get("selected_packet_metric"),
            "primary_batch_id": _dict_value(manifest.get("primary_batch"), "batch_id"),
            "secondary_batch_id": _dict_value(manifest.get("secondary_batch"), "batch_id"),
        }
    elif entry.artifact_type == "candidate_run":
        detail["key_fields"] = {
            "candidate_id": entry.artifact_id,
            "packet_id": manifest.get("packet_id"),
            "comparison_id": manifest.get("comparison_id"),
            "output_batch_id": manifest.get("output_batch_id"),
            "reply_yaml_path": _resolve_repo_path(
                manifest.get("copied_reply_yaml") or manifest.get("reply_yaml_path"),
                artifact_dir=entry.artifact_dir,
            ),
            "candidate_theory_snapshot_path": _resolve_repo_path(
                manifest.get("candidate_theory_snapshot_path"),
                artifact_dir=entry.artifact_dir,
            ),
            "status": manifest.get("status"),
        }
    elif entry.artifact_type == "reeval_outcome":
        summary = _dict_copy(manifest.get("selected_metric_summary"))
        detail["key_fields"] = {
            "outcome_id": entry.artifact_id,
            "decision_status": manifest.get("decision_status"),
            "selected_metric": manifest.get("selected_metric"),
            "candidate_id": manifest.get("candidate_id"),
            "wins": summary.get("wins"),
            "losses": summary.get("losses"),
            "ties": summary.get("ties"),
        }
    elif entry.artifact_type == "accepted_baseline":
        accepted_theory_snapshot_path = _resolve_repo_path(
            manifest.get("accepted_theory_snapshot_path"),
            artifact_dir=entry.artifact_dir,
        )
        fallback_snapshot_path = (entry.artifact_dir / "accepted_theory_snapshot.yaml").resolve()
        detail["key_fields"] = {
            "baseline_id": entry.artifact_id,
            "accepted_theory_snapshot_path": (
                accepted_theory_snapshot_path
                if accepted_theory_snapshot_path is not None
                else fallback_snapshot_path if fallback_snapshot_path.exists() else None
            ),
            "comparison_id": _dict_value(manifest.get("source_lineage"), "comparison_id"),
            "packet_id": _dict_value(manifest.get("source_lineage"), "packet_id"),
            "candidate_id": _dict_value(manifest.get("source_lineage"), "candidate_id"),
            "outcome_id": _dict_value(manifest.get("source_lineage"), "outcome_id"),
            "selected_metric": manifest.get("selected_metric"),
        }

    return detail


def _build_timeline_entry(
    artifact_type: str,
    manifest_path: Path,
    manifest: dict[str, Any],
) -> TimelineEntry:
    artifact_dir = manifest_path.parent
    artifact_id = _artifact_id_for(artifact_type, manifest, artifact_dir)
    timestamp_sort_key, created_at, timestamp_display, timestamp_source = _timestamp_fields(
        manifest_path,
        manifest.get("created_at"),
    )
    status = _status_for(artifact_type, manifest)
    lineage = _lineage_for(artifact_type, manifest, artifact_id)
    summary = _summary_for(artifact_type, manifest, artifact_id)

    return TimelineEntry(
        entry_key=f"{artifact_type}:{artifact_id}:{_display_path(artifact_dir)}",
        artifact_type=artifact_type,
        artifact_id=artifact_id,
        created_at=created_at,
        timestamp_display=timestamp_display,
        timestamp_source=timestamp_source,
        timestamp_sort_key=timestamp_sort_key,
        status=status,
        artifact_dir=artifact_dir,
        manifest_path=manifest_path,
        lineage=lineage,
        summary=summary,
        manifest=manifest,
    )


def _artifact_id_for(artifact_type: str, manifest: dict[str, Any], artifact_dir: Path) -> str:
    fields = {
        "batch": "batch_id",
        "comparison": "comparison_id",
        "review_packet": "packet_id",
        "candidate_run": "candidate_id",
        "reeval_outcome": "outcome_id",
        "accepted_baseline": "baseline_id",
    }
    field_name = fields[artifact_type]
    return _optional_str(manifest.get(field_name)) or artifact_dir.name


def _status_for(artifact_type: str, manifest: dict[str, Any]) -> str | None:
    if artifact_type in {"batch", "candidate_run"}:
        return _optional_str(manifest.get("status"))
    if artifact_type in {"reeval_outcome", "accepted_baseline"}:
        return _optional_str(manifest.get("decision_status"))
    return None


def _lineage_for(
    artifact_type: str,
    manifest: dict[str, Any],
    artifact_id: str,
) -> dict[str, Any]:
    if artifact_type == "batch":
        return {"batch_id": artifact_id}
    if artifact_type == "comparison":
        return {
            "comparison_id": artifact_id,
            "primary_batch_id": _dict_value(manifest.get("primary_batch"), "batch_id"),
            "secondary_batch_id": _dict_value(manifest.get("secondary_batch"), "batch_id"),
        }
    if artifact_type == "review_packet":
        return {
            "comparison_id": _optional_str(manifest.get("comparison_id")),
            "packet_id": artifact_id,
            "primary_batch_id": _dict_value(manifest.get("primary_batch"), "batch_id"),
            "secondary_batch_id": _dict_value(manifest.get("secondary_batch"), "batch_id"),
        }
    if artifact_type == "candidate_run":
        return {
            "comparison_id": _optional_str(manifest.get("comparison_id")),
            "packet_id": _optional_str(manifest.get("packet_id")),
            "candidate_id": artifact_id,
            "output_batch_id": _optional_str(manifest.get("output_batch_id")),
            "primary_batch_id": _dict_value(manifest.get("source_primary_batch"), "batch_id"),
        }
    if artifact_type == "reeval_outcome":
        return {
            "comparison_id": _optional_str(manifest.get("comparison_id")),
            "packet_id": _optional_str(manifest.get("packet_id")),
            "candidate_id": _optional_str(manifest.get("candidate_id")),
            "outcome_id": artifact_id,
            "primary_batch_id": _dict_value(manifest.get("primary_batch"), "batch_id"),
            "secondary_batch_id": _dict_value(manifest.get("secondary_batch"), "batch_id"),
        }
    source_lineage = _dict_copy(manifest.get("source_lineage"))
    return {
        "comparison_id": source_lineage.get("comparison_id"),
        "packet_id": source_lineage.get("packet_id"),
        "candidate_id": source_lineage.get("candidate_id"),
        "outcome_id": source_lineage.get("outcome_id"),
        "baseline_id": artifact_id,
        "primary_batch_id": _dict_value(manifest.get("source_primary_batch"), "batch_id"),
        "secondary_batch_id": _dict_value(manifest.get("source_secondary_batch"), "batch_id"),
    }


def _summary_for(artifact_type: str, manifest: dict[str, Any], artifact_id: str) -> str:
    if artifact_type == "batch":
        return (
            f"{artifact_id} | {manifest.get('status', 'unknown')} | "
            f"seeds={manifest.get('seed_count', 'n/a')}"
        )
    if artifact_type == "comparison":
        primary_batch_id = _dict_value(manifest.get("primary_batch"), "batch_id") or "n/a"
        secondary_batch_id = _dict_value(manifest.get("secondary_batch"), "batch_id") or "n/a"
        return f"{artifact_id} | {primary_batch_id} vs {secondary_batch_id}"
    if artifact_type == "review_packet":
        return (
            f"{artifact_id} | comparison={manifest.get('comparison_id', 'n/a')} | "
            f"metric={manifest.get('selected_packet_metric', 'n/a')}"
        )
    if artifact_type == "candidate_run":
        return (
            f"{artifact_id} | batch={manifest.get('output_batch_id', 'n/a')} | "
            f"status={manifest.get('status', 'unknown')}"
        )
    if artifact_type == "reeval_outcome":
        return (
            f"{artifact_id} | decision={manifest.get('decision_status', 'n/a')} | "
            f"candidate={manifest.get('candidate_id', 'n/a')}"
        )
    return (
        f"{artifact_id} | decision={manifest.get('decision_status', 'n/a')} | "
        f"candidate={_dict_value(manifest.get('source_lineage'), 'candidate_id') or 'n/a'}"
    )


def _timestamp_fields(
    manifest_path: Path,
    created_at_value: Any,
) -> tuple[float, str | None, str, str]:
    created_at = _optional_str(created_at_value)
    parsed = _parse_timestamp(created_at)
    if parsed is not None:
        iso_value = parsed.astimezone(timezone.utc).isoformat()
        return parsed.timestamp(), created_at, iso_value, "created_at"

    fallback = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=timezone.utc)
    fallback_display = f"{fallback.isoformat()} (manifest mtime)"
    return fallback.timestamp(), None, fallback_display, "manifest_mtime"


def _parse_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _search_blob(entry: TimelineEntry) -> str:
    values: list[str] = [
        entry.artifact_type,
        entry.artifact_id,
        entry.summary,
        _display_path(entry.artifact_dir),
        _display_path(entry.manifest_path),
        entry.status or "",
    ]
    for value in entry.lineage.values():
        if value not in (None, ""):
            values.append(str(value))
    return " ".join(values).lower()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read manifest: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"malformed JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("expected a JSON object")
    return payload


def _repo_root(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return REPO_ROOT
    candidate = Path(str(base_dir).strip()).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _resolve_repo_path(
    value: Any,
    *,
    artifact_dir: str | Path,
) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None

    text = text.replace("\\", "/")
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate

    repo_candidate = (REPO_ROOT / candidate).resolve()
    if repo_candidate.exists():
        return repo_candidate

    artifact_candidate = (Path(artifact_dir) / candidate).resolve()
    if artifact_candidate.exists():
        return artifact_candidate

    return repo_candidate


def _dict_copy(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _dict_value(value: Any, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _display_path(path: str | Path) -> str:
    candidate = Path(str(path).replace("\\", "/"))
    if not candidate.is_absolute():
        return str(candidate)
    try:
        return str(candidate.relative_to(REPO_ROOT))
    except ValueError:
        return str(candidate)
