from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.preset_registry import benchmark_presets_dir


class CuratedBenchmarkPresetError(ValueError):
    """Raised when a curated benchmark preset cannot be created."""


@dataclass(frozen=True, slots=True)
class CurationBundleContext:
    curation_id: str
    curation_dir: Path
    manifest_path: Path
    curated_seeds_csv_path: Path
    review_seeds_csv_path: Path | None
    manifest: dict[str, Any]
    curated_seed_count: int
    review_seed_count: int


@dataclass(frozen=True, slots=True)
class CuratedBenchmarkPresetSaveRequest:
    preset_id: str
    curation_dir: Path
    description: str | None
    tags: list[str]


def build_curated_benchmark_preset_save_request(
    *,
    preset_id: str,
    curation_dir: str | Path,
    description: str,
    tags_text: str,
) -> CuratedBenchmarkPresetSaveRequest:
    return CuratedBenchmarkPresetSaveRequest(
        preset_id=_normalize_filename_stem(preset_id, label="Benchmark Preset ID"),
        curation_dir=_resolve_existing_dir(curation_dir, label="Curation Directory"),
        description=_optional_str(description),
        tags=_parse_tags(tags_text),
    )


def load_curation_bundle_context(
    curation_dir: str | Path,
) -> CurationBundleContext:
    resolved_dir = _resolve_existing_dir(curation_dir, label="Curation Directory")
    manifest_path = resolved_dir / "curation_manifest.json"
    curated_seeds_csv_path = resolved_dir / "curated_seeds.csv"
    review_seeds_csv_path = resolved_dir / "review_seeds.csv"

    if not manifest_path.exists():
        raise CuratedBenchmarkPresetError(
            f"Curation bundle is missing curation_manifest.json: {resolved_dir}"
        )
    manifest = _load_json_object(manifest_path)
    if not curated_seeds_csv_path.exists() or not curated_seeds_csv_path.is_file():
        raise CuratedBenchmarkPresetError(
            f"Curation bundle is missing curated_seeds.csv: {resolved_dir}"
        )

    curation_id = _optional_str(manifest.get("curation_id")) or resolved_dir.name
    curated_seed_count = _count_csv_rows(curated_seeds_csv_path)
    review_seed_count = _count_csv_rows(review_seeds_csv_path) if review_seeds_csv_path.exists() else 0
    return CurationBundleContext(
        curation_id=curation_id,
        curation_dir=resolved_dir,
        manifest_path=manifest_path,
        curated_seeds_csv_path=curated_seeds_csv_path,
        review_seeds_csv_path=review_seeds_csv_path if review_seeds_csv_path.exists() else None,
        manifest=manifest,
        curated_seed_count=curated_seed_count,
        review_seed_count=review_seed_count,
    )


def build_benchmark_preset_payload_from_curation(
    *,
    request: CuratedBenchmarkPresetSaveRequest,
    curation: CurationBundleContext,
    created_at: str | None = None,
) -> dict[str, Any]:
    manifest_primary_batch = curation.manifest.get("primary_batch") if isinstance(curation.manifest.get("primary_batch"), dict) else {}
    manifest_secondary_batch = curation.manifest.get("secondary_batch") if isinstance(curation.manifest.get("secondary_batch"), dict) else {}
    source_benchmark_preset = (
        curation.manifest.get("source_benchmark_preset")
        if isinstance(curation.manifest.get("source_benchmark_preset"), dict)
        else {}
    )
    counts = curation.manifest.get("counts") if isinstance(curation.manifest.get("counts"), dict) else {}
    return {
        "benchmark_preset_id": request.preset_id,
        "created_at": created_at or _utc_timestamp(),
        "seeds_csv": _serialize_path(curation.curated_seeds_csv_path),
        "description": request.description,
        "tags": list(request.tags),
        "source_type": "benchmark_curation",
        "source_curation_id": curation.curation_id,
        "source_curation_dir": _serialize_path(curation.curation_dir),
        "source_curation_manifest": _serialize_path(curation.manifest_path),
        "curated_seed_count": curation.curated_seed_count,
        "review_seed_count": curation.review_seed_count,
        "source_primary_batch_id": _optional_str(manifest_primary_batch.get("batch_id")),
        "source_secondary_batch_id": _optional_str(manifest_secondary_batch.get("batch_id")),
        "source_benchmark_preset_id": _optional_str(source_benchmark_preset.get("benchmark_preset_id")),
        "source_selected_comparison_metric": _optional_str(curation.manifest.get("selected_comparison_metric")),
        "source_keep_count": counts.get("keep_count"),
        "source_review_count": counts.get("review_count"),
        "source_exclude_count": counts.get("exclude_count"),
    }


def save_curated_benchmark_preset(
    request: CuratedBenchmarkPresetSaveRequest,
    *,
    curation: CurationBundleContext,
    base_dir: str | Path | None = None,
) -> Path:
    registry_dir = benchmark_presets_dir(base_dir)
    registry_dir.mkdir(parents=True, exist_ok=True)
    preset_path = registry_dir / f"{request.preset_id}.json"
    if preset_path.exists():
        raise CuratedBenchmarkPresetError(f"Benchmark preset already exists: {preset_path}")

    payload = build_benchmark_preset_payload_from_curation(
        request=request,
        curation=curation,
    )
    _write_json(preset_path, payload)
    return preset_path


def _resolve_existing_dir(value: str | Path, *, label: str) -> Path:
    text = _optional_str(value)
    if text is None:
        raise CuratedBenchmarkPresetError(f"{label} is required.")
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists():
        raise CuratedBenchmarkPresetError(f"{label} does not exist: {path}")
    if not path.is_dir():
        raise CuratedBenchmarkPresetError(f"{label} is not a directory: {path}")
    return path


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise CuratedBenchmarkPresetError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise CuratedBenchmarkPresetError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise CuratedBenchmarkPresetError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def _normalize_filename_stem(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise CuratedBenchmarkPresetError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized or normalized.endswith(".json"):
        raise CuratedBenchmarkPresetError(f"{label} must be a single preset id without path separators.")
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
        seen.add(tag)
        tags.append(tag)
    return tags


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: str | Path | None) -> str | None:
    if value in (None, ""):
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
