from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from src.config import REPO_ROOT

EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION = "silver_provenance_regression"
EVALUATION_MODE_INDEPENDENT_BENCHMARK = "independent_benchmark"
EVALUATION_MODE_VALUES = (
    EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
    EVALUATION_MODE_INDEPENDENT_BENCHMARK,
)
DEFAULT_EVALUATION_MODE = EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION
DEFAULT_METRIC_SCOPE = "local_corpus_ranking"


class LaunchProfileRegistryError(ValueError):
    """Raised when a launch profile cannot be validated or saved."""


@dataclass(frozen=True, slots=True)
class LaunchProfileEntry:
    profile_id: str
    profile_path: Path
    payload: dict[str, Any]
    accepted_theory_snapshot_path: Path | None
    seeds_csv_path: Path | None


@dataclass(frozen=True, slots=True)
class LaunchProfileSaveRequest:
    profile_id: str
    accepted_baseline_id: str
    accepted_baseline_dir: Path
    accepted_theory_snapshot_path: Path
    benchmark_preset_id: str
    seeds_csv_path: Path
    eval_preset_id: str
    max_references: int
    max_related: int
    max_hard_negatives: int
    top_k: int
    label_source: str
    evaluation_mode: str
    metric_scope: str
    benchmark_labels_path: Path | None
    benchmark_dataset_id: str | None
    benchmark_labels_sha256: str | None
    refresh: bool
    description: str | None
    tags: list[str]


def launch_profiles_dir(base_dir: str | Path | None = None) -> Path:
    return _preset_dir(base_dir, default_relative=Path("configs") / "presets" / "launch_profiles")


def scan_launch_profiles(
    base_dir: str | Path | None = None,
) -> tuple[list[LaunchProfileEntry], list[str]]:
    registry_dir = launch_profiles_dir(base_dir)
    if not registry_dir.exists():
        return [], []
    if not registry_dir.is_dir():
        return [], [f"Launch profiles path is not a directory: {registry_dir}"]

    entries: list[LaunchProfileEntry] = []
    warnings: list[str] = []
    for profile_path in sorted(registry_dir.glob("*.json"), key=lambda path: path.name.lower()):
        try:
            payload = _load_json_object(profile_path)
        except ValueError as exc:
            warnings.append(f"Skipped launch profile '{profile_path.name}': {exc}")
            continue

        profile_id = _optional_str(payload.get("launch_profile_id")) or profile_path.stem
        entries.append(
            LaunchProfileEntry(
                profile_id=profile_id,
                profile_path=profile_path,
                payload=payload,
                accepted_theory_snapshot_path=_resolve_repo_path(payload.get("accepted_theory_snapshot")),
                seeds_csv_path=_resolve_repo_path(payload.get("seeds_csv")),
            )
        )

    return entries, warnings


def build_launch_profile_save_request(
    *,
    profile_id: str,
    accepted_baseline_id: str,
    accepted_baseline_dir: str | Path,
    accepted_theory_snapshot: str | Path,
    benchmark_preset_id: str,
    seeds_csv: str | Path,
    eval_preset_id: str,
    max_references: int,
    max_related: int,
    max_hard_negatives: int,
    top_k: int,
    label_source: str,
    evaluation_mode: str = DEFAULT_EVALUATION_MODE,
    benchmark_labels_path: str | Path | None = None,
    benchmark_dataset_id: str = "",
    benchmark_labels_sha256: str = "",
    refresh: bool,
    description: str,
    tags_text: str,
) -> LaunchProfileSaveRequest:
    normalized_profile_id = _normalize_filename_stem(profile_id, label="Launch Profile ID")
    normalized_baseline_id = _normalize_nonempty_string(
        accepted_baseline_id,
        label="Accepted baseline selection",
    )
    normalized_benchmark_id = _normalize_nonempty_string(
        benchmark_preset_id,
        label="Benchmark preset selection",
    )
    normalized_eval_id = _normalize_nonempty_string(
        eval_preset_id,
        label="Evaluation preset selection",
    )
    normalized_label_source = _normalize_nonempty_string(label_source, label="label_source")
    normalized_evaluation_mode = _normalize_evaluation_mode(evaluation_mode)

    baseline_dir = _resolve_existing_dir(
        accepted_baseline_dir,
        label="Accepted baseline directory",
    )
    accepted_theory_snapshot_path = _resolve_existing_file(
        accepted_theory_snapshot,
        label="Accepted theory snapshot",
    )
    seeds_csv_path = _resolve_existing_file(seeds_csv, label="Seeds CSV path")
    resolved_benchmark_labels_path = _resolve_optional_existing_file(
        benchmark_labels_path,
        label="Benchmark labels path",
    )
    normalized_benchmark_labels_sha256 = _optional_str(benchmark_labels_sha256)
    if resolved_benchmark_labels_path is not None and normalized_benchmark_labels_sha256 is None:
        normalized_benchmark_labels_sha256 = _sha256_file(resolved_benchmark_labels_path)

    return LaunchProfileSaveRequest(
        profile_id=normalized_profile_id,
        accepted_baseline_id=normalized_baseline_id,
        accepted_baseline_dir=baseline_dir,
        accepted_theory_snapshot_path=accepted_theory_snapshot_path,
        benchmark_preset_id=normalized_benchmark_id,
        seeds_csv_path=seeds_csv_path,
        eval_preset_id=normalized_eval_id,
        max_references=_nonnegative_int(max_references, label="max_references"),
        max_related=_nonnegative_int(max_related, label="max_related"),
        max_hard_negatives=_nonnegative_int(max_hard_negatives, label="max_hard_negatives"),
        top_k=_positive_int(top_k, label="top_k"),
        label_source=normalized_label_source,
        evaluation_mode=normalized_evaluation_mode,
        metric_scope=DEFAULT_METRIC_SCOPE,
        benchmark_labels_path=resolved_benchmark_labels_path,
        benchmark_dataset_id=_optional_str(benchmark_dataset_id),
        benchmark_labels_sha256=normalized_benchmark_labels_sha256,
        refresh=bool(refresh),
        description=_optional_str(description),
        tags=_parse_tags(tags_text),
    )


def save_launch_profile(
    request: LaunchProfileSaveRequest,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    registry_dir = launch_profiles_dir(base_dir)
    registry_dir.mkdir(parents=True, exist_ok=True)
    profile_path = registry_dir / f"{request.profile_id}.json"
    if profile_path.exists():
        raise LaunchProfileRegistryError(f"Launch profile already exists: {profile_path}")

    payload = {
        "launch_profile_id": request.profile_id,
        "created_at": _utc_timestamp(),
        "accepted_baseline_id": request.accepted_baseline_id,
        "accepted_baseline_dir": _serialize_path(request.accepted_baseline_dir),
        "accepted_theory_snapshot": _serialize_path(request.accepted_theory_snapshot_path),
        "benchmark_preset_id": request.benchmark_preset_id,
        "seeds_csv": _serialize_path(request.seeds_csv_path),
        "eval_preset_id": request.eval_preset_id,
        "max_references": request.max_references,
        "max_related": request.max_related,
        "max_hard_negatives": request.max_hard_negatives,
        "top_k": request.top_k,
        "label_source": request.label_source,
        "evaluation_mode": request.evaluation_mode,
        "metric_scope": request.metric_scope,
        "benchmark_labels_path": _serialize_optional_path(request.benchmark_labels_path),
        "benchmark_dataset_id": request.benchmark_dataset_id,
        "benchmark_labels_sha256": request.benchmark_labels_sha256,
        "refresh": request.refresh,
        "description": request.description,
        "tags": list(request.tags),
    }
    _write_json(profile_path, payload)
    return profile_path


def build_launch_profile_rows(
    entries: list[LaunchProfileEntry],
) -> list[dict[str, Any]]:
    return [
        {
            "launch_profile_id": entry.profile_id,
            "accepted_baseline_id": entry.payload.get("accepted_baseline_id"),
            "benchmark_preset_id": entry.payload.get("benchmark_preset_id"),
            "eval_preset_id": entry.payload.get("eval_preset_id"),
            "created_at": entry.payload.get("created_at"),
            "seeds_csv": entry.payload.get("seeds_csv"),
            "top_k": entry.payload.get("top_k"),
            "label_source": entry.payload.get("label_source"),
            "evaluation_mode": entry.payload.get("evaluation_mode", DEFAULT_EVALUATION_MODE),
            "benchmark_dataset_id": entry.payload.get("benchmark_dataset_id"),
        }
        for entry in entries
    ]


def choose_default_launch_profile_id(
    entries: list[LaunchProfileEntry],
    *,
    preferred_profile_id: str | None = None,
) -> str | None:
    if preferred_profile_id:
        for entry in entries:
            if entry.profile_id == preferred_profile_id:
                return entry.profile_id
    if not entries:
        return None
    return entries[0].profile_id


def find_launch_profile_entry(
    entries: list[LaunchProfileEntry],
    profile_id: str | None,
) -> LaunchProfileEntry | None:
    if not profile_id:
        return None
    for entry in entries:
        if entry.profile_id == profile_id:
            return entry
    return None


def build_launch_profile_detail(entry: LaunchProfileEntry) -> dict[str, Any]:
    return {
        "launch_profile_id": entry.profile_id,
        "profile_path": entry.profile_path,
        "created_at": entry.payload.get("created_at"),
        "accepted_baseline_id": entry.payload.get("accepted_baseline_id"),
        "accepted_baseline_dir": entry.payload.get("accepted_baseline_dir"),
        "accepted_theory_snapshot": entry.payload.get("accepted_theory_snapshot"),
        "accepted_theory_snapshot_path": entry.accepted_theory_snapshot_path,
        "benchmark_preset_id": entry.payload.get("benchmark_preset_id"),
        "seeds_csv": entry.payload.get("seeds_csv"),
        "seeds_csv_path": entry.seeds_csv_path,
        "eval_preset_id": entry.payload.get("eval_preset_id"),
        "max_references": entry.payload.get("max_references"),
        "max_related": entry.payload.get("max_related"),
        "max_hard_negatives": entry.payload.get("max_hard_negatives"),
        "top_k": entry.payload.get("top_k"),
        "label_source": entry.payload.get("label_source"),
        "evaluation_mode": entry.payload.get("evaluation_mode", DEFAULT_EVALUATION_MODE),
        "metric_scope": entry.payload.get("metric_scope", DEFAULT_METRIC_SCOPE),
        "benchmark_labels_path": entry.payload.get("benchmark_labels_path"),
        "benchmark_labels_resolved_path": _resolve_repo_path(entry.payload.get("benchmark_labels_path")),
        "benchmark_dataset_id": entry.payload.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": entry.payload.get("benchmark_labels_sha256"),
        "refresh": entry.payload.get("refresh"),
        "description": entry.payload.get("description"),
        "tags": entry.payload.get("tags") if isinstance(entry.payload.get("tags"), list) else [],
        "raw_payload": entry.payload,
    }


def build_launch_profile_run_batch_values(
    entry: LaunchProfileEntry,
    *,
    allowed_label_sources: Sequence[str],
    fallback_label_source: str,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []

    theory_config_path = _optional_str(entry.payload.get("accepted_theory_snapshot"))
    if theory_config_path is None:
        warnings.append(
            f"Launch profile '{entry.profile_id}' does not contain an accepted_theory_snapshot value."
        )
        return {}, warnings
    if entry.accepted_theory_snapshot_path is None or not entry.accepted_theory_snapshot_path.exists():
        warnings.append(
            f"Launch profile '{entry.profile_id}' points to an accepted theory snapshot that is missing on disk."
        )

    seeds_csv_path = _optional_str(entry.payload.get("seeds_csv"))
    if seeds_csv_path is None:
        warnings.append(f"Launch profile '{entry.profile_id}' does not contain a seeds_csv value.")
        return {}, warnings
    if entry.seeds_csv_path is None or not entry.seeds_csv_path.exists():
        warnings.append(
            f"Launch profile '{entry.profile_id}' points to a seeds CSV that is missing on disk."
        )

    evaluation_mode = _optional_str(entry.payload.get("evaluation_mode"))
    if evaluation_mode is None:
        evaluation_mode = DEFAULT_EVALUATION_MODE
    elif evaluation_mode not in EVALUATION_MODE_VALUES:
        warnings.append(
            f"Launch profile '{entry.profile_id}' uses unsupported evaluation_mode "
            f"'{evaluation_mode}'; using '{DEFAULT_EVALUATION_MODE}' instead."
        )
        evaluation_mode = DEFAULT_EVALUATION_MODE

    fallback_for_mode = (
        "benchmark"
        if evaluation_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK
        else fallback_label_source
    )
    label_source = _optional_str(entry.payload.get("label_source"))
    if label_source is None:
        label_source = fallback_for_mode
        warnings.append(
            f"Launch profile '{entry.profile_id}' is missing label_source; using '{label_source}'."
        )
    elif label_source not in allowed_label_sources:
        warnings.append(
            f"Launch profile '{entry.profile_id}' uses unsupported label_source '{label_source}'; "
            f"using '{fallback_for_mode}' instead."
        )
        label_source = fallback_for_mode

    benchmark_labels_path = _optional_str(entry.payload.get("benchmark_labels_path"))
    resolved_benchmark_labels_path = _resolve_repo_path(benchmark_labels_path)
    if benchmark_labels_path is not None and (
        resolved_benchmark_labels_path is None or not resolved_benchmark_labels_path.exists()
    ):
        warnings.append(
            f"Launch profile '{entry.profile_id}' points to benchmark labels that are missing on disk."
        )

    return (
        {
            "theory_config_path": theory_config_path,
            "seeds_csv_path": seeds_csv_path,
            "max_references": entry.payload.get("max_references"),
            "max_related": entry.payload.get("max_related"),
            "max_hard_negatives": entry.payload.get("max_hard_negatives"),
            "top_k": entry.payload.get("top_k"),
            "label_source": label_source,
            "evaluation_mode": evaluation_mode,
            "metric_scope": entry.payload.get("metric_scope", DEFAULT_METRIC_SCOPE),
            "benchmark_labels_path": benchmark_labels_path,
            "benchmark_dataset_id": _optional_str(entry.payload.get("benchmark_dataset_id")),
            "benchmark_labels_sha256": _optional_str(entry.payload.get("benchmark_labels_sha256")),
            "refresh": bool(entry.payload.get("refresh", False)),
        },
        warnings,
    )


def _preset_dir(base_dir: str | Path | None, *, default_relative: Path) -> Path:
    if base_dir is None:
        return REPO_ROOT / default_relative
    candidate = Path(str(base_dir).strip()).expanduser()
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


def _resolve_repo_path(value: Any) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    text = text.replace("\\", "/")
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _resolve_existing_file(value: str | Path, *, label: str) -> Path:
    path = _resolve_repo_path(value)
    if path is None:
        raise LaunchProfileRegistryError(f"{label} is required.")
    if not path.exists():
        raise LaunchProfileRegistryError(f"{label} does not exist: {path}")
    if not path.is_file():
        raise LaunchProfileRegistryError(f"{label} is not a file: {path}")
    return path


def _resolve_optional_existing_file(value: str | Path | None, *, label: str) -> Path | None:
    if _optional_str(value) is None:
        return None
    return _resolve_existing_file(value, label=label)


def _resolve_existing_dir(value: str | Path, *, label: str) -> Path:
    path = _resolve_repo_path(value)
    if path is None:
        raise LaunchProfileRegistryError(f"{label} is required.")
    if not path.exists():
        raise LaunchProfileRegistryError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise LaunchProfileRegistryError(f"{label} is not a directory: {path}")
    return path


def _normalize_filename_stem(value: str, *, label: str) -> str:
    normalized = _normalize_nonempty_string(value, label=label)
    invalid_chars = set('/\\:*?"<>|')
    if any(char in invalid_chars for char in normalized):
        raise LaunchProfileRegistryError(f"{label} contains invalid path characters.")
    return normalized


def _normalize_nonempty_string(value: object, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise LaunchProfileRegistryError(f"{label} is required.")
    return normalized


def _nonnegative_int(value: object, *, label: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise LaunchProfileRegistryError(f"{label} must be an integer.") from exc
    if parsed < 0:
        raise LaunchProfileRegistryError(f"{label} must be >= 0.")
    return parsed


def _positive_int(value: object, *, label: str) -> int:
    parsed = _nonnegative_int(value, label=label)
    if parsed < 1:
        raise LaunchProfileRegistryError(f"{label} must be >= 1.")
    return parsed


def _parse_tags(value: str) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for raw_tag in str(value).split(","):
        tag = raw_tag.strip()
        if not tag:
            continue
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        tags.append(tag)
    return tags


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _serialize_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def _serialize_optional_path(path: Path | None) -> str | None:
    if path is None:
        return None
    return _serialize_path(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_evaluation_mode(value: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        return DEFAULT_EVALUATION_MODE
    if normalized not in EVALUATION_MODE_VALUES:
        supported = ", ".join(EVALUATION_MODE_VALUES)
        raise LaunchProfileRegistryError(f"evaluation_mode must be one of: {supported}")
    return normalized


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
