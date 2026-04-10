from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.curated_benchmark_preset import (
    CuratedBenchmarkPresetError,
    build_benchmark_preset_payload_from_curation,
    build_curated_benchmark_preset_save_request,
    load_curation_bundle_context,
    save_curated_benchmark_preset,
)
from src.ui.preset_registry import build_benchmark_run_batch_values, scan_benchmark_presets


def _write_curation_bundle(base_dir: Path, *, with_review_csv: bool = True) -> Path:
    curation_dir = base_dir / "curation_001"
    curation_dir.mkdir(parents=True)
    (curation_dir / "curated_seeds.csv").write_text(
        "doi\n10.1000/1\n10.1000/2\n",
        encoding="utf-8",
    )
    if with_review_csv:
        (curation_dir / "review_seeds.csv").write_text(
            "doi,reason_summary\n10.1000/3,review\n",
            encoding="utf-8",
        )
    (curation_dir / "curation_manifest.json").write_text(
        json.dumps(
            {
                "curation_id": "curation_001",
                "primary_batch": {"batch_id": "batch_009"},
                "secondary_batch": {"batch_id": "batch_010"},
                "selected_comparison_metric": "ndcg_at_k",
                "source_benchmark_preset": {"benchmark_preset_id": "benchmark_smoke_001"},
                "counts": {
                    "total_seeds": 3,
                    "keep_count": 2,
                    "review_count": 1,
                    "exclude_count": 0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return curation_dir


def test_load_curation_bundle_context_validates_good_bundle(tmp_path: Path) -> None:
    curation_dir = _write_curation_bundle(tmp_path)

    context = load_curation_bundle_context(curation_dir)

    assert context.curation_id == "curation_001"
    assert context.curated_seed_count == 2
    assert context.review_seed_count == 1
    assert context.curated_seeds_csv_path.name == "curated_seeds.csv"


def test_load_curation_bundle_context_rejects_missing_curated_csv(tmp_path: Path) -> None:
    curation_dir = _write_curation_bundle(tmp_path)
    (curation_dir / "curated_seeds.csv").unlink()

    with pytest.raises(CuratedBenchmarkPresetError):
        load_curation_bundle_context(curation_dir)


def test_build_benchmark_preset_payload_from_curation_preserves_standard_fields(tmp_path: Path) -> None:
    curation_dir = _write_curation_bundle(tmp_path)
    context = load_curation_bundle_context(curation_dir)
    request = build_curated_benchmark_preset_save_request(
        preset_id="benchmark_curated_001",
        curation_dir=curation_dir,
        description="curated smoke benchmark 001",
        tags_text="curated,smoke",
    )

    payload = build_benchmark_preset_payload_from_curation(
        request=request,
        curation=context,
        created_at="2026-04-01T12:00:00Z",
    )

    assert payload["benchmark_preset_id"] == "benchmark_curated_001"
    assert payload["created_at"] == "2026-04-01T12:00:00Z"
    assert payload["seeds_csv"].endswith("curated_seeds.csv")
    assert payload["description"] == "curated smoke benchmark 001"
    assert payload["tags"] == ["curated", "smoke"]
    assert payload["source_type"] == "benchmark_curation"
    assert payload["source_curation_id"] == "curation_001"
    assert payload["curated_seed_count"] == 2
    assert payload["review_seed_count"] == 1
    assert payload["source_benchmark_preset_id"] == "benchmark_smoke_001"


def test_save_curated_benchmark_preset_refuses_overwrite(tmp_path: Path) -> None:
    curation_dir = _write_curation_bundle(tmp_path / "runs" / "benchmark_curations")
    context = load_curation_bundle_context(curation_dir)
    request = build_curated_benchmark_preset_save_request(
        preset_id="benchmark_curated_002",
        curation_dir=curation_dir,
        description="",
        tags_text="",
    )

    save_curated_benchmark_preset(
        request,
        curation=context,
        base_dir=tmp_path / "configs" / "presets" / "benchmarks",
    )

    with pytest.raises(CuratedBenchmarkPresetError):
        save_curated_benchmark_preset(
            request,
            curation=context,
            base_dir=tmp_path / "configs" / "presets" / "benchmarks",
        )


def test_saved_curated_benchmark_preset_remains_registry_compatible(tmp_path: Path) -> None:
    curation_dir = _write_curation_bundle(tmp_path / "runs" / "benchmark_curations", with_review_csv=False)
    context = load_curation_bundle_context(curation_dir)
    request = build_curated_benchmark_preset_save_request(
        preset_id="benchmark_curated_003",
        curation_dir=curation_dir,
        description="curated smoke benchmark",
        tags_text="curated",
    )

    preset_path = save_curated_benchmark_preset(
        request,
        curation=context,
        base_dir=tmp_path / "configs" / "presets" / "benchmarks",
    )

    entries, warnings = scan_benchmark_presets(tmp_path / "configs" / "presets" / "benchmarks")
    assert warnings == []
    assert preset_path.exists()
    assert len(entries) == 1
    values, prefill_warnings = build_benchmark_run_batch_values(entries[0])
    assert prefill_warnings == []
    assert values["seeds_csv_path"].endswith("curated_seeds.csv")
