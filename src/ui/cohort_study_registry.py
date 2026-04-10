from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT


COHORT_STUDY_REGISTRY_DECISION_FILTER_OPTIONS = (
    "all",
    "shortlist",
    "review",
    "drop",
)


@dataclass(frozen=True, slots=True)
class CohortStudyRegistryEntry:
    study_id: str
    study_dir: Path
    manifest_path: Path
    manifest: dict[str, Any]
    decisions_path: Path | None
    decision_rows: list[dict[str, Any]]
    leaderboard_path: Path | None
    leaderboard_rows: list[dict[str, Any]]
    report_path: Path | None
    report_markdown: str | None
    created_at: str | None
    created_at_display: str
    created_at_source: str
    timestamp_sort_key: float
    reviewer: str | None
    reference_batch_id: str | None
    reference_batch_dir: Path | None
    reference_batch_exists: bool
    selected_metric: str | None


@dataclass(frozen=True, slots=True)
class CohortStudyCandidateRow:
    candidate_batch_id: str
    decision: str | None
    suggested_decision: str | None
    usable: bool | None
    selected_metric: str | None
    improvement_delta_mean: float | None
    improvement_delta_median: float | None
    wins: int | None
    losses: int | None
    ties: int | None
    accepted_baseline_id: str | None
    benchmark_preset_id: str | None
    eval_preset_id: str | None
    launch_profile_id: str | None
    launch_source_type: str | None
    source_curation_id: str | None
    candidate_theory_config: str | None
    guardrail_verdict: str | None
    pairwise_status: str | None
    candidate_status: str | None
    candidate_batch_dir: Path | None
    candidate_batch_exists: bool
    raw_decision_row: dict[str, Any] | None
    raw_leaderboard_row: dict[str, Any] | None


def scan_cohort_studies(
    base_dir: str | Path | None = None,
) -> tuple[list[CohortStudyRegistryEntry], list[str]]:
    registry_dir = _registry_dir(base_dir)
    if not registry_dir.exists():
        return [], []
    if not registry_dir.is_dir():
        return [], [f"Cohort studies path is not a directory: {registry_dir}"]
    artifact_root = _artifact_root_from_registry_dir(registry_dir, base_dir=base_dir)

    entries: list[CohortStudyRegistryEntry] = []
    warnings: list[str] = []
    for child in sorted(registry_dir.iterdir(), key=lambda path: path.name.lower()):
        if not child.is_dir():
            continue
        manifest_path = child / "cohort_study_manifest.json"
        if not manifest_path.exists():
            warnings.append(f"Skipped '{child.name}': missing cohort_study_manifest.json.")
            continue
        try:
            manifest = _load_json_object(manifest_path)
        except ValueError as exc:
            warnings.append(f"Skipped '{child.name}': {exc}")
            continue

        decisions_path = _resolve_optional_path(
            _dict_value(_dict_value(manifest, "output_paths"), "candidate_decisions_jsonl"),
            artifact_dir=child,
            artifact_root=artifact_root,
            fallback_name="candidate_decisions.jsonl",
        )
        leaderboard_path = _resolve_optional_path(
            _dict_value(_dict_value(manifest, "output_paths"), "cohort_leaderboard_jsonl"),
            artifact_dir=child,
            artifact_root=artifact_root,
            fallback_name="cohort_leaderboard.jsonl",
        )
        report_path = _resolve_optional_path(
            _dict_value(_dict_value(manifest, "output_paths"), "cohort_study_report_md"),
            artifact_dir=child,
            artifact_root=artifact_root,
            fallback_name="cohort_study_report.md",
        )

        decision_rows: list[dict[str, Any]] = []
        leaderboard_rows: list[dict[str, Any]] = []
        report_markdown: str | None = None

        if decisions_path is not None and decisions_path.exists():
            try:
                decision_rows = _load_jsonl_records(decisions_path)
            except ValueError as exc:
                warnings.append(
                    f"Loaded study '{child.name}' without candidate_decisions.jsonl details: {exc}"
                )
        if leaderboard_path is not None and leaderboard_path.exists():
            try:
                leaderboard_rows = _load_jsonl_records(leaderboard_path)
            except ValueError as exc:
                warnings.append(
                    f"Loaded study '{child.name}' without cohort_leaderboard.jsonl details: {exc}"
                )
        if report_path is not None and report_path.exists():
            try:
                report_markdown = report_path.read_text(encoding="utf-8")
            except OSError as exc:
                warnings.append(
                    f"Loaded study '{child.name}' without cohort_study_report.md details: {exc}"
                )

        created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
            _optional_str(manifest.get("created_at")),
            manifest_path.stat().st_mtime,
        )
        reference_batch = _dict_copy(manifest.get("reference_batch"))
        reference_batch_id = _optional_str(reference_batch.get("batch_id"))
        reference_batch_dir = _resolve_batch_dir(
            reference_batch.get("batch_dir"),
            artifact_root=artifact_root,
            batch_id=reference_batch_id,
        )
        entries.append(
            CohortStudyRegistryEntry(
                study_id=_optional_str(manifest.get("study_id")) or child.name,
                study_dir=child,
                manifest_path=manifest_path,
                manifest=manifest,
                decisions_path=decisions_path if decisions_path is not None and decisions_path.exists() else None,
                decision_rows=decision_rows,
                leaderboard_path=leaderboard_path if leaderboard_path is not None and leaderboard_path.exists() else None,
                leaderboard_rows=leaderboard_rows,
                report_path=report_path if report_path is not None and report_path.exists() else None,
                report_markdown=report_markdown,
                created_at=created_at,
                created_at_display=created_at_display,
                created_at_source=created_at_source,
                timestamp_sort_key=timestamp_sort_key,
                reviewer=_optional_str(manifest.get("reviewer")),
                reference_batch_id=reference_batch_id,
                reference_batch_dir=reference_batch_dir,
                reference_batch_exists=bool(reference_batch_dir is not None and reference_batch_dir.exists()),
                selected_metric=_optional_str(manifest.get("selected_metric")),
            )
        )

    entries.sort(
        key=lambda entry: (
            -entry.timestamp_sort_key,
            entry.study_id.lower(),
            str(entry.study_dir).lower(),
        )
    )
    return entries, warnings


def build_cohort_study_registry_rows(
    entries: list[CohortStudyRegistryEntry],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in entries:
        rows.append(
            {
                "study_id": entry.study_id,
                "created_at": entry.created_at_display,
                "reviewer": entry.reviewer,
                "reference_batch_id": entry.reference_batch_id,
                "selected_metric": entry.selected_metric,
                "total_candidate_rows": entry.manifest.get("total_candidate_rows"),
                "shortlist_count": entry.manifest.get("shortlist_count"),
                "review_count": entry.manifest.get("review_count"),
                "drop_count": entry.manifest.get("drop_count"),
                "study_dir": _display_path(entry.study_dir),
            }
        )
    return rows


def choose_default_cohort_study_id(
    entries: list[CohortStudyRegistryEntry],
    *,
    preferred_study_id: str | None = None,
) -> str | None:
    if preferred_study_id:
        for entry in entries:
            if entry.study_id == preferred_study_id:
                return entry.study_id
    if not entries:
        return None
    return entries[0].study_id


def find_cohort_study_entry(
    entries: list[CohortStudyRegistryEntry],
    study_id: str | None,
) -> CohortStudyRegistryEntry | None:
    needle = _optional_str(study_id)
    if needle is None:
        return None
    for entry in entries:
        if entry.study_id == needle:
            return entry
    return None


def build_cohort_study_detail(
    entry: CohortStudyRegistryEntry,
) -> dict[str, Any]:
    manifest = entry.manifest
    return {
        "identity": {
            "study_id": entry.study_id,
            "study_dir": entry.study_dir,
            "created_at": entry.created_at_display,
            "created_at_source": entry.created_at_source,
            "reviewer": entry.reviewer,
            "notes": _optional_str(manifest.get("notes")),
            "cohort_summary": _optional_str(manifest.get("cohort_summary")),
            "selected_metric": entry.selected_metric,
        },
        "reference_batch": {
            **_dict_copy(manifest.get("reference_batch")),
            "batch_dir": entry.reference_batch_dir,
            "batch_exists": entry.reference_batch_exists,
        },
        "counts": {
            "total_candidate_rows": manifest.get("total_candidate_rows"),
            "usable_candidate_rows": manifest.get("usable_candidate_rows"),
            "unusable_candidate_rows": manifest.get("unusable_candidate_rows"),
            "shortlist_count": manifest.get("shortlist_count"),
            "review_count": manifest.get("review_count"),
            "drop_count": manifest.get("drop_count"),
        },
        "report_markdown": entry.report_markdown,
        "raw_manifest": manifest,
    }


def build_cohort_study_candidate_rows(
    entry: CohortStudyRegistryEntry,
) -> list[CohortStudyCandidateRow]:
    artifact_root = _artifact_root_from_study_dir(entry.study_dir)
    leaderboard_by_id = {
        _optional_str(row.get("candidate_batch_id")): row
        for row in entry.leaderboard_rows
        if _optional_str(row.get("candidate_batch_id")) is not None
    }
    decision_by_id = {
        _optional_str(row.get("candidate_batch_id")): row
        for row in entry.decision_rows
        if _optional_str(row.get("candidate_batch_id")) is not None
    }

    ordered_candidate_ids: list[str] = []
    for row in entry.decision_rows:
        candidate_batch_id = _optional_str(row.get("candidate_batch_id"))
        if candidate_batch_id is not None and candidate_batch_id not in ordered_candidate_ids:
            ordered_candidate_ids.append(candidate_batch_id)
    for row in entry.leaderboard_rows:
        candidate_batch_id = _optional_str(row.get("candidate_batch_id"))
        if candidate_batch_id is not None and candidate_batch_id not in ordered_candidate_ids:
            ordered_candidate_ids.append(candidate_batch_id)

    rows: list[CohortStudyCandidateRow] = []
    for candidate_batch_id in ordered_candidate_ids:
        decision_row = decision_by_id.get(candidate_batch_id)
        leaderboard_row = leaderboard_by_id.get(candidate_batch_id)
        candidate_batch_dir = _resolve_batch_dir(
            _dict_value(leaderboard_row, "candidate_batch_dir"),
            artifact_root=artifact_root,
            batch_id=candidate_batch_id,
        )
        pairwise_status = _optional_str(_dict_value(leaderboard_row, "pairwise_status")) or _optional_str(
            _dict_value(decision_row, "pairwise_status")
        )
        usable_value = _first_nonempty(
            _dict_value(decision_row, "usable"),
            _dict_value(leaderboard_row, "usable"),
        )
        usable = usable_value if isinstance(usable_value, bool) else (
            True if pairwise_status == "usable" else False if pairwise_status is not None else None
        )
        rows.append(
            CohortStudyCandidateRow(
                candidate_batch_id=candidate_batch_id,
                decision=_optional_str(_dict_value(decision_row, "decision")),
                suggested_decision=_optional_str(_dict_value(decision_row, "suggested_decision")),
                usable=usable,
                selected_metric=_first_nonempty_str(
                    _dict_value(decision_row, "selected_metric"),
                    _dict_value(leaderboard_row, "selected_metric"),
                    entry.selected_metric,
                ),
                improvement_delta_mean=_numeric_value(
                    _first_nonempty(
                        _dict_value(decision_row, "improvement_delta_mean"),
                        _dict_value(leaderboard_row, "improvement_delta_mean"),
                    )
                ),
                improvement_delta_median=_numeric_value(_dict_value(leaderboard_row, "improvement_delta_median")),
                wins=_int_or_none(
                    _first_nonempty(
                        _dict_value(decision_row, "wins"),
                        _dict_value(leaderboard_row, "wins"),
                    )
                ),
                losses=_int_or_none(
                    _first_nonempty(
                        _dict_value(decision_row, "losses"),
                        _dict_value(leaderboard_row, "losses"),
                    )
                ),
                ties=_int_or_none(
                    _first_nonempty(
                        _dict_value(decision_row, "ties"),
                        _dict_value(leaderboard_row, "ties"),
                    )
                ),
                accepted_baseline_id=_optional_str(_dict_value(leaderboard_row, "accepted_baseline_id")),
                benchmark_preset_id=_optional_str(_dict_value(leaderboard_row, "benchmark_preset_id")),
                eval_preset_id=_optional_str(_dict_value(leaderboard_row, "eval_preset_id")),
                launch_profile_id=_optional_str(_dict_value(leaderboard_row, "launch_profile_id")),
                launch_source_type=_optional_str(_dict_value(leaderboard_row, "launch_source_type")),
                source_curation_id=_optional_str(_dict_value(leaderboard_row, "source_curation_id")),
                candidate_theory_config=_optional_str(_dict_value(leaderboard_row, "candidate_theory_config")),
                guardrail_verdict=_first_nonempty_str(
                    _dict_value(decision_row, "guardrail_verdict"),
                    _dict_value(leaderboard_row, "guardrail_verdict"),
                ),
                pairwise_status=pairwise_status,
                candidate_status=_optional_str(_dict_value(leaderboard_row, "candidate_status")),
                candidate_batch_dir=candidate_batch_dir,
                candidate_batch_exists=bool(candidate_batch_dir is not None and candidate_batch_dir.exists()),
                raw_decision_row=dict(decision_row) if isinstance(decision_row, dict) else None,
                raw_leaderboard_row=dict(leaderboard_row) if isinstance(leaderboard_row, dict) else None,
            )
        )
    return rows


def build_cohort_study_candidate_table_rows(
    rows: list[CohortStudyCandidateRow],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_batch_id": row.candidate_batch_id,
            "decision": row.decision,
            "usable": row.usable,
            "selected_metric": row.selected_metric,
            "improvement_delta_mean": row.improvement_delta_mean,
            "wins": row.wins,
            "losses": row.losses,
            "ties": row.ties,
            "accepted_baseline_id": row.accepted_baseline_id,
            "launch_profile_id": row.launch_profile_id,
            "available_on_disk": "yes" if row.candidate_batch_exists else "no",
        }
        for row in rows
    ]


def filter_cohort_study_candidate_rows(
    rows: list[CohortStudyCandidateRow],
    *,
    decision_filter: str,
    usable_only: bool,
    search_text: str | None = None,
) -> list[CohortStudyCandidateRow]:
    needle = _optional_str(search_text)
    lowered_needle = needle.lower() if needle is not None else None
    filtered: list[CohortStudyCandidateRow] = []
    for row in rows:
        if decision_filter != "all" and _optional_str(row.decision) != decision_filter:
            continue
        if usable_only and row.usable is not True:
            continue
        if lowered_needle is not None and lowered_needle not in _candidate_search_blob(row):
            continue
        filtered.append(row)
    return filtered


def find_cohort_study_candidate_row(
    rows: list[CohortStudyCandidateRow],
    candidate_batch_id: str | None,
) -> CohortStudyCandidateRow | None:
    needle = _optional_str(candidate_batch_id)
    if needle is None:
        return None
    for row in rows:
        if row.candidate_batch_id == needle:
            return row
    return None


def build_cohort_study_candidate_detail(
    row: CohortStudyCandidateRow,
) -> dict[str, Any]:
    return {
        "identity": {
            "candidate_batch_id": row.candidate_batch_id,
            "decision": row.decision,
            "suggested_decision": row.suggested_decision,
            "usable": row.usable,
            "pairwise_status": row.pairwise_status,
            "candidate_batch_dir": row.candidate_batch_dir,
            "candidate_batch_exists": row.candidate_batch_exists,
        },
        "summary": {
            "selected_metric": row.selected_metric,
            "improvement_delta_mean": row.improvement_delta_mean,
            "improvement_delta_median": row.improvement_delta_median,
            "wins": row.wins,
            "losses": row.losses,
            "ties": row.ties,
            "guardrail_verdict": row.guardrail_verdict,
        },
        "provenance": {
            "accepted_baseline_id": row.accepted_baseline_id,
            "benchmark_preset_id": row.benchmark_preset_id,
            "eval_preset_id": row.eval_preset_id,
            "launch_profile_id": row.launch_profile_id,
            "launch_source_type": row.launch_source_type,
            "source_curation_id": row.source_curation_id,
            "candidate_theory_config": row.candidate_theory_config,
            "candidate_status": row.candidate_status,
        },
        "raw_decision_row": row.raw_decision_row,
        "raw_leaderboard_row": row.raw_leaderboard_row,
    }


def _registry_dir(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return REPO_ROOT / "runs" / "cohort_studies"
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


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ValueError(f"could not read {path.name}: {exc}") from exc
    records: list[dict[str, Any]] = []
    for index, raw_line in enumerate(raw_lines, start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"malformed JSONL in {path.name} at line {index}: {exc.msg}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"invalid {path.name} line {index}: expected a JSON object")
        records.append(payload)
    return records


def _resolve_optional_path(
    value: Any,
    *,
    artifact_dir: Path,
    artifact_root: Path,
    fallback_name: str,
) -> Path | None:
    preferred = _resolve_path(value, artifact_dir=artifact_dir, artifact_root=artifact_root)
    if preferred is not None:
        return preferred
    fallback = (artifact_dir / fallback_name).resolve()
    return fallback if fallback.exists() else None


def _resolve_path(value: Any, *, artifact_dir: Path, artifact_root: Path) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate
    artifact_root_candidate = (artifact_root / candidate).resolve()
    if artifact_root_candidate.exists():
        return artifact_root_candidate
    repo_candidate = (REPO_ROOT / candidate).resolve()
    if repo_candidate.exists():
        return repo_candidate
    artifact_candidate = (artifact_dir / candidate).resolve()
    if artifact_candidate.exists():
        return artifact_candidate
    return artifact_root_candidate


def _resolve_batch_dir(value: Any, *, artifact_root: Path, batch_id: str | None) -> Path | None:
    path = _resolve_path(value, artifact_dir=artifact_root, artifact_root=artifact_root)
    if path is not None:
        return path
    normalized_batch_id = _optional_str(batch_id)
    if normalized_batch_id is None:
        return None
    return (artifact_root / "runs" / "batches" / normalized_batch_id).resolve()


def _artifact_root_from_registry_dir(
    registry_dir: Path,
    *,
    base_dir: str | Path | None,
) -> Path:
    if base_dir is None:
        return REPO_ROOT
    if registry_dir.name == "cohort_studies" and registry_dir.parent.name == "runs":
        return registry_dir.parent.parent.resolve()
    return registry_dir.parent.resolve()


def _artifact_root_from_study_dir(study_dir: Path) -> Path:
    registry_dir = study_dir.parent
    if registry_dir.name == "cohort_studies" and registry_dir.parent.name == "runs":
        return registry_dir.parent.parent.resolve()
    return registry_dir.parent.resolve()


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


def _candidate_search_blob(row: CohortStudyCandidateRow) -> str:
    return " ".join(
        value.lower()
        for value in (
            row.candidate_batch_id,
            row.accepted_baseline_id,
            row.launch_profile_id,
            row.candidate_theory_config,
        )
        if isinstance(value, str) and value.strip()
    )


def _display_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _dict_copy(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _dict_value(value: Any, field_name: str) -> Any:
    if isinstance(value, dict):
        return value.get(field_name)
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_nonempty_str(*values: Any) -> str | None:
    for value in values:
        normalized = _optional_str(value)
        if normalized is not None:
            return normalized
    return None


def _numeric_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None
