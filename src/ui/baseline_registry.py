from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT


@dataclass(frozen=True, slots=True)
class AcceptedBaselineEntry:
    baseline_id: str
    baseline_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]
    promotion_record_path: Path | None
    promotion_record: dict[str, Any] | None
    accepted_theory_snapshot_path: Path | None


def scan_accepted_baselines(
    base_dir: str | Path | None = None,
) -> tuple[list[AcceptedBaselineEntry], list[str]]:
    registry_dir = _registry_dir(base_dir)
    if not registry_dir.exists():
        return [], []
    if not registry_dir.is_dir():
        return [], [f"Accepted baselines path is not a directory: {registry_dir}"]

    entries: list[AcceptedBaselineEntry] = []
    warnings: list[str] = []
    for child in sorted(registry_dir.iterdir(), key=lambda path: path.name.lower()):
        if not child.is_dir():
            continue

        manifest_path = child / "accepted_baseline_manifest.json"
        if not manifest_path.exists():
            warnings.append(f"Skipped '{child.name}': missing accepted_baseline_manifest.json.")
            continue

        try:
            manifest = _load_json_object(manifest_path)
        except ValueError as exc:
            warnings.append(f"Skipped '{child.name}': {exc}")
            continue

        promotion_record_path = child / "promotion_record.json"
        promotion_record: dict[str, Any] | None = None
        if promotion_record_path.exists():
            try:
                promotion_record = _load_json_object(promotion_record_path)
            except ValueError as exc:
                warnings.append(
                    f"Loaded baseline '{child.name}' without promotion_record.json details: {exc}"
                )

        baseline_id = _optional_str(manifest.get("baseline_id")) or child.name
        entries.append(
            AcceptedBaselineEntry(
                baseline_id=baseline_id,
                baseline_dir=child,
                manifest_path=manifest_path,
                manifest=manifest,
                promotion_record_path=promotion_record_path if promotion_record_path.exists() else None,
                promotion_record=promotion_record,
                accepted_theory_snapshot_path=extract_accepted_theory_snapshot_path(
                    manifest,
                    baseline_dir=child,
                ),
            )
        )

    return entries, warnings


def build_accepted_baseline_registry_rows(
    entries: list[AcceptedBaselineEntry],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        lineage = entry.manifest.get("source_lineage")
        primary_batch = entry.manifest.get("source_primary_batch")
        secondary_batch = entry.manifest.get("source_secondary_batch")
        rows.append(
            {
                "baseline_id": entry.baseline_id,
                "created_at": entry.manifest.get("created_at"),
                "reviewer": entry.manifest.get("reviewer"),
                "comparison_id": _dict_value(lineage, "comparison_id"),
                "packet_id": _dict_value(lineage, "packet_id"),
                "candidate_id": _dict_value(lineage, "candidate_id"),
                "outcome_id": _dict_value(lineage, "outcome_id"),
                "decision_status": entry.manifest.get("decision_status"),
                "selected_metric": entry.manifest.get("selected_metric"),
                "source_primary_batch_id": _dict_value(primary_batch, "batch_id"),
                "source_secondary_batch_id": _dict_value(secondary_batch, "batch_id"),
            }
        )
    return rows


def choose_default_accepted_baseline_id(
    entries: list[AcceptedBaselineEntry],
    *,
    preferred_baseline_id: str | None = None,
) -> str | None:
    if preferred_baseline_id:
        for entry in entries:
            if entry.baseline_id == preferred_baseline_id:
                return entry.baseline_id
    if not entries:
        return None
    return entries[0].baseline_id


def find_accepted_baseline_entry(
    entries: list[AcceptedBaselineEntry],
    baseline_id: str | None,
) -> AcceptedBaselineEntry | None:
    if not baseline_id:
        return None
    for entry in entries:
        if entry.baseline_id == baseline_id:
            return entry
    return None


def build_accepted_baseline_detail(
    entry: AcceptedBaselineEntry,
) -> dict[str, Any]:
    manifest = entry.manifest
    return {
        "identity": {
            "baseline_id": entry.baseline_id,
            "baseline_dir": entry.baseline_dir,
            "accepted_theory_snapshot_path": entry.accepted_theory_snapshot_path,
            "candidate_reply_yaml_path": _resolve_repo_path(
                manifest.get("candidate_reply_yaml_path"),
                baseline_dir=entry.baseline_dir,
            ),
            "applied_changes_path": _resolve_repo_path(
                manifest.get("applied_changes_path"),
                baseline_dir=entry.baseline_dir,
            ),
            "created_at": manifest.get("created_at"),
            "reviewer": manifest.get("reviewer"),
            "notes": manifest.get("notes"),
            "decision_status": manifest.get("decision_status"),
            "selected_metric": manifest.get("selected_metric"),
        },
        "source_lineage": _dict_copy(manifest.get("source_lineage")),
        "source_primary_batch": _dict_copy(manifest.get("source_primary_batch")),
        "source_secondary_batch": _dict_copy(manifest.get("source_secondary_batch")),
        "outcome_summary": _dict_copy(manifest.get("outcome_summary")),
        "raw_manifest": manifest,
        "promotion_record": entry.promotion_record,
    }


def extract_accepted_theory_snapshot_path(
    manifest: dict[str, Any],
    *,
    baseline_dir: str | Path,
) -> Path | None:
    preferred = _resolve_repo_path(
        manifest.get("accepted_theory_snapshot_path"),
        baseline_dir=baseline_dir,
    )
    if preferred is not None:
        return preferred
    fallback = Path(baseline_dir) / "accepted_theory_snapshot.yaml"
    return fallback


def _registry_dir(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return REPO_ROOT / "runs" / "accepted_baselines"
    path = Path(str(base_dir).strip()).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


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


def _resolve_repo_path(
    value: Any,
    *,
    baseline_dir: str | Path,
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
    baseline_candidate = (Path(baseline_dir) / candidate).resolve()
    if baseline_candidate.exists():
        return baseline_candidate
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
