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


class PresetRegistryError(ValueError):
    """Raised when a preset cannot be validated or saved."""


@dataclass(frozen=True, slots=True)
class BenchmarkPresetEntry:
    preset_id: str
    preset_path: Path
    payload: dict[str, Any]
    seeds_csv_path: Path | None


@dataclass(frozen=True, slots=True)
class EvaluationPresetEntry:
    preset_id: str
    preset_path: Path
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class BenchmarkPresetSaveRequest:
    preset_id: str
    seeds_csv_path: Path
    benchmark_labels_path: Path | None
    benchmark_dataset_id: str | None
    benchmark_labels_sha256: str | None
    benchmark_dataset_manifest_path: Path | None
    benchmark_schema_version: str | None
    benchmark_labels_row_count: int | None
    aspect_scope: str | None
    description: str | None
    tags: list[str]


@dataclass(frozen=True, slots=True)
class EvaluationPresetSaveRequest:
    preset_id: str
    max_references: int
    max_related: int
    max_hard_negatives: int
    top_k: int
    label_source: str
    evaluation_mode: str
    metric_scope: str
    refresh: bool
    description: str | None


def benchmark_presets_dir(base_dir: str | Path | None = None) -> Path:
    return _preset_dir(base_dir, default_relative=Path("configs") / "presets" / "benchmarks")


def evaluation_presets_dir(base_dir: str | Path | None = None) -> Path:
    return _preset_dir(base_dir, default_relative=Path("configs") / "presets" / "evals")


def scan_benchmark_presets(
    base_dir: str | Path | None = None,
) -> tuple[list[BenchmarkPresetEntry], list[str]]:
    registry_dir = benchmark_presets_dir(base_dir)
    if not registry_dir.exists():
        return [], []
    if not registry_dir.is_dir():
        return [], [f"Benchmark presets path is not a directory: {registry_dir}"]

    entries: list[BenchmarkPresetEntry] = []
    warnings: list[str] = []
    for preset_path in sorted(registry_dir.glob("*.json"), key=lambda path: path.name.lower()):
        try:
            payload = _load_json_object(preset_path)
        except ValueError as exc:
            warnings.append(f"Skipped benchmark preset '{preset_path.name}': {exc}")
            continue

        preset_id = _optional_str(payload.get("benchmark_preset_id")) or preset_path.stem
        entries.append(
            BenchmarkPresetEntry(
                preset_id=preset_id,
                preset_path=preset_path,
                payload=payload,
                seeds_csv_path=extract_benchmark_seeds_csv_path(payload),
            )
        )

    return entries, warnings


def scan_evaluation_presets(
    base_dir: str | Path | None = None,
) -> tuple[list[EvaluationPresetEntry], list[str]]:
    registry_dir = evaluation_presets_dir(base_dir)
    if not registry_dir.exists():
        return [], []
    if not registry_dir.is_dir():
        return [], [f"Evaluation presets path is not a directory: {registry_dir}"]

    entries: list[EvaluationPresetEntry] = []
    warnings: list[str] = []
    for preset_path in sorted(registry_dir.glob("*.json"), key=lambda path: path.name.lower()):
        try:
            payload = _load_json_object(preset_path)
        except ValueError as exc:
            warnings.append(f"Skipped evaluation preset '{preset_path.name}': {exc}")
            continue

        preset_id = _optional_str(payload.get("eval_preset_id")) or preset_path.stem
        entries.append(
            EvaluationPresetEntry(
                preset_id=preset_id,
                preset_path=preset_path,
                payload=payload,
            )
        )

    return entries, warnings


def build_benchmark_preset_save_request(
    *,
    preset_id: str,
    seeds_csv_path: str | Path,
    benchmark_labels_path: str | Path | None = None,
    benchmark_dataset_id: str = "",
    benchmark_labels_sha256: str = "",
    benchmark_dataset_manifest_path: str | Path | None = None,
    benchmark_schema_version: str = "",
    benchmark_labels_row_count: object | None = None,
    aspect_scope: str = "",
    description: str,
    tags_text: str,
) -> BenchmarkPresetSaveRequest:
    normalized_preset_id = _normalize_filename_stem(preset_id, label="Benchmark Preset ID")
    resolved_seeds_csv = _resolve_existing_file(seeds_csv_path, label="Seeds CSV path")
    resolved_benchmark_labels = _resolve_optional_existing_file(
        benchmark_labels_path,
        label="Benchmark labels path",
    )
    resolved_benchmark_dataset_manifest = _resolve_optional_existing_file(
        benchmark_dataset_manifest_path,
        label="Benchmark dataset manifest path",
    )
    normalized_benchmark_labels_sha256 = _optional_str(benchmark_labels_sha256)
    if resolved_benchmark_labels is not None and normalized_benchmark_labels_sha256 is None:
        normalized_benchmark_labels_sha256 = _sha256_file(resolved_benchmark_labels)
    normalized_labels_row_count: int | None = None
    if benchmark_labels_row_count not in (None, ""):
        normalized_labels_row_count = _nonnegative_int(
            benchmark_labels_row_count,
            label="benchmark_labels_row_count",
        )
    return BenchmarkPresetSaveRequest(
        preset_id=normalized_preset_id,
        seeds_csv_path=resolved_seeds_csv,
        benchmark_labels_path=resolved_benchmark_labels,
        benchmark_dataset_id=_optional_str(benchmark_dataset_id),
        benchmark_labels_sha256=normalized_benchmark_labels_sha256,
        benchmark_dataset_manifest_path=resolved_benchmark_dataset_manifest,
        benchmark_schema_version=_optional_str(benchmark_schema_version),
        benchmark_labels_row_count=normalized_labels_row_count,
        aspect_scope=_optional_str(aspect_scope),
        description=_optional_str(description),
        tags=_parse_tags(tags_text),
    )


def build_evaluation_preset_save_request(
    *,
    preset_id: str,
    max_references: int,
    max_related: int,
    max_hard_negatives: int,
    top_k: int,
    label_source: str,
    evaluation_mode: str = DEFAULT_EVALUATION_MODE,
    refresh: bool,
    description: str,
) -> EvaluationPresetSaveRequest:
    normalized_preset_id = _normalize_filename_stem(preset_id, label="Eval Preset ID")
    normalized_label_source = _optional_str(label_source)
    if normalized_label_source is None:
        raise PresetRegistryError("label_source is required.")
    normalized_evaluation_mode = _normalize_evaluation_mode(evaluation_mode)
    return EvaluationPresetSaveRequest(
        preset_id=normalized_preset_id,
        max_references=_nonnegative_int(max_references, label="max_references"),
        max_related=_nonnegative_int(max_related, label="max_related"),
        max_hard_negatives=_nonnegative_int(max_hard_negatives, label="max_hard_negatives"),
        top_k=_positive_int(top_k, label="top_k"),
        label_source=normalized_label_source,
        evaluation_mode=normalized_evaluation_mode,
        metric_scope=DEFAULT_METRIC_SCOPE,
        refresh=bool(refresh),
        description=_optional_str(description),
    )


def save_benchmark_preset(
    request: BenchmarkPresetSaveRequest,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    registry_dir = benchmark_presets_dir(base_dir)
    registry_dir.mkdir(parents=True, exist_ok=True)
    preset_path = registry_dir / f"{request.preset_id}.json"
    if preset_path.exists():
        raise PresetRegistryError(f"Benchmark preset already exists: {preset_path}")

    payload = {
        "benchmark_preset_id": request.preset_id,
        "created_at": _utc_timestamp(),
        "seeds_csv": _serialize_path(request.seeds_csv_path),
        "benchmark_labels_path": _serialize_path(request.benchmark_labels_path),
        "benchmark_dataset_id": request.benchmark_dataset_id,
        "benchmark_labels_sha256": request.benchmark_labels_sha256,
        "benchmark_dataset_manifest_path": _serialize_path(request.benchmark_dataset_manifest_path),
        "benchmark_schema_version": request.benchmark_schema_version,
        "benchmark_labels_row_count": request.benchmark_labels_row_count,
        "aspect_scope": request.aspect_scope,
        "description": request.description,
        "tags": list(request.tags),
    }
    _write_json(preset_path, payload)
    return preset_path


def save_evaluation_preset(
    request: EvaluationPresetSaveRequest,
    *,
    base_dir: str | Path | None = None,
) -> Path:
    registry_dir = evaluation_presets_dir(base_dir)
    registry_dir.mkdir(parents=True, exist_ok=True)
    preset_path = registry_dir / f"{request.preset_id}.json"
    if preset_path.exists():
        raise PresetRegistryError(f"Evaluation preset already exists: {preset_path}")

    payload = {
        "eval_preset_id": request.preset_id,
        "created_at": _utc_timestamp(),
        "max_references": request.max_references,
        "max_related": request.max_related,
        "max_hard_negatives": request.max_hard_negatives,
        "top_k": request.top_k,
        "label_source": request.label_source,
        "evaluation_mode": request.evaluation_mode,
        "metric_scope": request.metric_scope,
        "refresh": request.refresh,
        "description": request.description,
    }
    _write_json(preset_path, payload)
    return preset_path


def build_benchmark_preset_rows(
    entries: list[BenchmarkPresetEntry],
) -> list[dict[str, Any]]:
    return [
        {
            "benchmark_preset_id": entry.preset_id,
            "seeds_csv": entry.payload.get("seeds_csv"),
            "benchmark_labels_path": entry.payload.get("benchmark_labels_path"),
            "benchmark_dataset_id": entry.payload.get("benchmark_dataset_id"),
            "benchmark_dataset_manifest_path": entry.payload.get("benchmark_dataset_manifest_path"),
            "benchmark_schema_version": entry.payload.get("benchmark_schema_version"),
            "benchmark_labels_row_count": entry.payload.get("benchmark_labels_row_count"),
            "aspect_scope": entry.payload.get("aspect_scope"),
            "created_at": entry.payload.get("created_at"),
            "description": entry.payload.get("description"),
        }
        for entry in entries
    ]


def build_evaluation_preset_rows(
    entries: list[EvaluationPresetEntry],
) -> list[dict[str, Any]]:
    return [
        {
            "eval_preset_id": entry.preset_id,
            "max_references": entry.payload.get("max_references"),
            "max_related": entry.payload.get("max_related"),
            "max_hard_negatives": entry.payload.get("max_hard_negatives"),
            "top_k": entry.payload.get("top_k"),
            "label_source": entry.payload.get("label_source"),
            "evaluation_mode": entry.payload.get("evaluation_mode", DEFAULT_EVALUATION_MODE),
            "metric_scope": entry.payload.get("metric_scope", DEFAULT_METRIC_SCOPE),
            "refresh": entry.payload.get("refresh"),
            "created_at": entry.payload.get("created_at"),
        }
        for entry in entries
    ]


def choose_default_benchmark_preset_id(
    entries: list[BenchmarkPresetEntry],
    *,
    preferred_preset_id: str | None = None,
) -> str | None:
    return _choose_default_preset_id([entry.preset_id for entry in entries], preferred_preset_id)


def choose_default_evaluation_preset_id(
    entries: list[EvaluationPresetEntry],
    *,
    preferred_preset_id: str | None = None,
) -> str | None:
    return _choose_default_preset_id([entry.preset_id for entry in entries], preferred_preset_id)


def find_benchmark_preset_entry(
    entries: list[BenchmarkPresetEntry],
    preset_id: str | None,
) -> BenchmarkPresetEntry | None:
    return _find_preset_entry(entries, preset_id)


def find_evaluation_preset_entry(
    entries: list[EvaluationPresetEntry],
    preset_id: str | None,
) -> EvaluationPresetEntry | None:
    return _find_preset_entry(entries, preset_id)


def build_benchmark_preset_detail(
    entry: BenchmarkPresetEntry,
) -> dict[str, Any]:
    return {
        "benchmark_preset_id": entry.preset_id,
        "preset_path": entry.preset_path,
        "seeds_csv": entry.payload.get("seeds_csv"),
        "seeds_csv_path": entry.seeds_csv_path,
        "benchmark_labels_path": entry.payload.get("benchmark_labels_path"),
        "benchmark_labels_resolved_path": _resolve_repo_path(entry.payload.get("benchmark_labels_path")),
        "benchmark_dataset_id": entry.payload.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": entry.payload.get("benchmark_labels_sha256"),
        "benchmark_dataset_manifest_path": entry.payload.get("benchmark_dataset_manifest_path"),
        "benchmark_dataset_manifest_resolved_path": _resolve_repo_path(
            entry.payload.get("benchmark_dataset_manifest_path")
        ),
        "benchmark_schema_version": entry.payload.get("benchmark_schema_version"),
        "benchmark_labels_row_count": entry.payload.get("benchmark_labels_row_count"),
        "aspect_scope": entry.payload.get("aspect_scope"),
        "created_at": entry.payload.get("created_at"),
        "description": entry.payload.get("description"),
        "tags": entry.payload.get("tags") if isinstance(entry.payload.get("tags"), list) else [],
        "raw_payload": entry.payload,
    }


def build_evaluation_preset_detail(
    entry: EvaluationPresetEntry,
) -> dict[str, Any]:
    return {
        "eval_preset_id": entry.preset_id,
        "preset_path": entry.preset_path,
        "max_references": entry.payload.get("max_references"),
        "max_related": entry.payload.get("max_related"),
        "max_hard_negatives": entry.payload.get("max_hard_negatives"),
        "top_k": entry.payload.get("top_k"),
        "label_source": entry.payload.get("label_source"),
        "evaluation_mode": entry.payload.get("evaluation_mode", DEFAULT_EVALUATION_MODE),
        "metric_scope": entry.payload.get("metric_scope", DEFAULT_METRIC_SCOPE),
        "refresh": entry.payload.get("refresh"),
        "created_at": entry.payload.get("created_at"),
        "description": entry.payload.get("description"),
        "raw_payload": entry.payload,
    }


def extract_benchmark_seeds_csv_path(payload: dict[str, Any]) -> Path | None:
    return _resolve_repo_path(payload.get("seeds_csv"))


def build_benchmark_run_batch_values(
    entry: BenchmarkPresetEntry,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    seeds_csv = _optional_str(entry.payload.get("seeds_csv"))
    if seeds_csv is None:
        warnings.append(f"Benchmark preset '{entry.preset_id}' does not contain a seeds_csv value.")
        return {}, warnings
    if entry.seeds_csv_path is None or not entry.seeds_csv_path.exists():
        warnings.append(
            f"Benchmark preset '{entry.preset_id}' points to a seeds CSV that is missing on disk."
        )
    benchmark_labels_path = _optional_str(entry.payload.get("benchmark_labels_path"))
    resolved_benchmark_labels_path = _resolve_repo_path(benchmark_labels_path)
    if benchmark_labels_path is not None and (
        resolved_benchmark_labels_path is None or not resolved_benchmark_labels_path.exists()
    ):
        warnings.append(
            f"Benchmark preset '{entry.preset_id}' points to benchmark labels that are missing on disk."
        )
    benchmark_manifest_path = _optional_str(entry.payload.get("benchmark_dataset_manifest_path"))
    resolved_benchmark_manifest_path = _resolve_repo_path(benchmark_manifest_path)
    if benchmark_manifest_path is not None and (
        resolved_benchmark_manifest_path is None or not resolved_benchmark_manifest_path.exists()
    ):
        warnings.append(
            f"Benchmark preset '{entry.preset_id}' points to a benchmark dataset manifest that is missing on disk."
        )
    return {
        "seeds_csv_path": seeds_csv,
        "benchmark_labels_path": benchmark_labels_path,
        "benchmark_dataset_id": _optional_str(entry.payload.get("benchmark_dataset_id")),
        "benchmark_labels_sha256": _optional_str(entry.payload.get("benchmark_labels_sha256")),
    }, warnings


def build_evaluation_run_batch_values(
    entry: EvaluationPresetEntry,
    *,
    allowed_label_sources: Sequence[str],
    fallback_label_source: str,
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    evaluation_mode = _optional_str(entry.payload.get("evaluation_mode"))
    if evaluation_mode is None:
        evaluation_mode = DEFAULT_EVALUATION_MODE
    elif evaluation_mode not in EVALUATION_MODE_VALUES:
        warnings.append(
            f"Evaluation preset '{entry.preset_id}' uses unsupported evaluation_mode "
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
            f"Evaluation preset '{entry.preset_id}' is missing label_source; using '{label_source}'."
        )
    elif label_source not in allowed_label_sources:
        warnings.append(
            f"Evaluation preset '{entry.preset_id}' uses unsupported label_source '{label_source}'; "
            f"using '{fallback_for_mode}' instead."
        )
        label_source = fallback_for_mode

    return (
        {
            "max_references": entry.payload.get("max_references"),
            "max_related": entry.payload.get("max_related"),
            "max_hard_negatives": entry.payload.get("max_hard_negatives"),
            "top_k": entry.payload.get("top_k"),
            "label_source": label_source,
            "evaluation_mode": evaluation_mode,
            "metric_scope": entry.payload.get("metric_scope", DEFAULT_METRIC_SCOPE),
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


def _resolve_existing_file(value: str | Path, *, label: str) -> Path:
    resolved = _resolve_repo_path(value)
    if resolved is None:
        raise PresetRegistryError(f"{label} is required.")
    if not resolved.exists():
        raise PresetRegistryError(f"{label} does not exist: {resolved}")
    if not resolved.is_file():
        raise PresetRegistryError(f"{label} is not a file: {resolved}")
    return resolved


def _resolve_optional_existing_file(value: str | Path | None, *, label: str) -> Path | None:
    if _optional_str(value) is None:
        return None
    return _resolve_existing_file(value, label=label)


def _resolve_repo_path(value: Any) -> Path | None:
    text = _optional_str(value)
    if text is None:
        return None
    text = text.replace("\\", "/")
    path = Path(text).expanduser()
    if path.is_absolute():
        return path
    repo_candidate = (REPO_ROOT / path).resolve()
    if repo_candidate.exists():
        return repo_candidate
    return repo_candidate


def _serialize_path(value: str | Path | None) -> str | None:
    if value is None:
        return None
    path = Path(value)
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"malformed JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("expected a JSON object")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize_filename_stem(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise PresetRegistryError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized or normalized.endswith(".json"):
        raise PresetRegistryError(f"{label} must be a single preset id without path separators.")
    return normalized


def _parse_tags(value: str) -> list[str]:
    if not value.strip():
        return []
    seen: set[str] = set()
    tags: list[str] = []
    for raw_tag in value.split(","):
        tag = raw_tag.strip()
        if not tag or tag in seen:
            continue
        tags.append(tag)
        seen.add(tag)
    return tags


def _nonnegative_int(value: Any, *, label: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise PresetRegistryError(f"{label} must be non-negative.")
    return normalized


def _positive_int(value: Any, *, label: str) -> int:
    normalized = int(value)
    if normalized <= 0:
        raise PresetRegistryError(f"{label} must be positive.")
    return normalized


def _choose_default_preset_id(
    preset_ids: list[str],
    preferred_preset_id: str | None,
) -> str | None:
    if preferred_preset_id in preset_ids:
        return preferred_preset_id
    if not preset_ids:
        return None
    return preset_ids[0]


def _find_preset_entry(entries: list[Any], preset_id: str | None) -> Any | None:
    if not preset_id:
        return None
    for entry in entries:
        if entry.preset_id == preset_id:
            return entry
    return None


def _normalize_evaluation_mode(value: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        return DEFAULT_EVALUATION_MODE
    if normalized not in EVALUATION_MODE_VALUES:
        supported = ", ".join(EVALUATION_MODE_VALUES)
        raise PresetRegistryError(f"evaluation_mode must be one of: {supported}")
    return normalized


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
