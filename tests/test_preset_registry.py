from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.preset_registry import (
    PresetRegistryError,
    benchmark_presets_dir,
    build_benchmark_preset_detail,
    build_benchmark_preset_rows,
    build_benchmark_preset_save_request,
    build_benchmark_run_batch_values,
    build_evaluation_preset_detail,
    build_evaluation_preset_rows,
    build_evaluation_preset_save_request,
    build_evaluation_run_batch_values,
    choose_default_benchmark_preset_id,
    choose_default_evaluation_preset_id,
    evaluation_presets_dir,
    find_benchmark_preset_entry,
    find_evaluation_preset_entry,
    save_benchmark_preset,
    save_evaluation_preset,
    scan_benchmark_presets,
    scan_evaluation_presets,
)


def test_save_and_scan_benchmark_presets(tmp_path: Path) -> None:
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_benchmark_preset_save_request(
        preset_id="benchmark_smoke_001",
        seeds_csv_path=seeds_csv,
        description="smoke benchmark",
        tags_text="smoke, micro, smoke",
    )
    preset_path = save_benchmark_preset(
        request,
        base_dir=tmp_path / "configs" / "presets" / "benchmarks",
    )

    entries, warnings = scan_benchmark_presets(tmp_path / "configs" / "presets" / "benchmarks")

    assert preset_path == tmp_path / "configs" / "presets" / "benchmarks" / "benchmark_smoke_001.json"
    assert warnings == []
    assert [entry.preset_id for entry in entries] == ["benchmark_smoke_001"]
    detail = build_benchmark_preset_detail(entries[0])
    assert detail["description"] == "smoke benchmark"
    assert detail["tags"] == ["smoke", "micro"]
    assert detail["seeds_csv_path"] == seeds_csv
    rows = build_benchmark_preset_rows(entries)
    assert rows[0]["benchmark_preset_id"] == "benchmark_smoke_001"


def test_save_and_scan_evaluation_presets(tmp_path: Path) -> None:
    request = build_evaluation_preset_save_request(
        preset_id="eval_micro_001",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        evaluation_mode="silver_provenance_regression",
        refresh=False,
        description="micro eval",
    )
    preset_path = save_evaluation_preset(
        request,
        base_dir=tmp_path / "configs" / "presets" / "evals",
    )

    entries, warnings = scan_evaluation_presets(tmp_path / "configs" / "presets" / "evals")

    assert preset_path == tmp_path / "configs" / "presets" / "evals" / "eval_micro_001.json"
    assert warnings == []
    assert [entry.preset_id for entry in entries] == ["eval_micro_001"]
    detail = build_evaluation_preset_detail(entries[0])
    assert detail["max_references"] == 10
    assert detail["label_source"] == "silver"
    assert detail["evaluation_mode"] == "silver_provenance_regression"
    rows = build_evaluation_preset_rows(entries)
    assert rows[0]["eval_preset_id"] == "eval_micro_001"


def test_save_preset_refuses_overwrite(tmp_path: Path) -> None:
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    request = build_benchmark_preset_save_request(
        preset_id="benchmark_smoke_001",
        seeds_csv_path=seeds_csv,
        description="",
        tags_text="",
    )
    save_benchmark_preset(request, base_dir=tmp_path / "configs" / "presets" / "benchmarks")

    with pytest.raises(PresetRegistryError):
        save_benchmark_preset(request, base_dir=tmp_path / "configs" / "presets" / "benchmarks")


def test_scan_presets_skips_malformed_json(tmp_path: Path) -> None:
    registry_dir = tmp_path / "configs" / "presets" / "benchmarks"
    registry_dir.mkdir(parents=True)
    (registry_dir / "broken.json").write_text("{not json", encoding="utf-8")

    entries, warnings = scan_benchmark_presets(registry_dir)

    assert entries == []
    assert warnings
    assert "broken.json" in warnings[0]


def test_choose_default_and_find_preset_entries(tmp_path: Path) -> None:
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    benchmark_request = build_benchmark_preset_save_request(
        preset_id="benchmark_smoke_001",
        seeds_csv_path=seeds_csv,
        description="",
        tags_text="",
    )
    save_benchmark_preset(benchmark_request, base_dir=tmp_path / "configs" / "presets" / "benchmarks")
    benchmark_entries, _ = scan_benchmark_presets(tmp_path / "configs" / "presets" / "benchmarks")

    eval_request = build_evaluation_preset_save_request(
        preset_id="eval_micro_001",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        evaluation_mode="silver_provenance_regression",
        refresh=False,
        description="",
    )
    save_evaluation_preset(eval_request, base_dir=tmp_path / "configs" / "presets" / "evals")
    eval_entries, _ = scan_evaluation_presets(tmp_path / "configs" / "presets" / "evals")

    assert choose_default_benchmark_preset_id(benchmark_entries, preferred_preset_id="benchmark_smoke_001") == "benchmark_smoke_001"
    assert choose_default_evaluation_preset_id(eval_entries, preferred_preset_id="eval_micro_001") == "eval_micro_001"
    assert find_benchmark_preset_entry(benchmark_entries, "benchmark_smoke_001") is not None
    assert find_evaluation_preset_entry(eval_entries, "eval_micro_001") is not None


def test_build_run_batch_values_extracts_benchmark_and_eval_payloads(tmp_path: Path) -> None:
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    benchmark_request = build_benchmark_preset_save_request(
        preset_id="benchmark_smoke_001",
        seeds_csv_path=seeds_csv,
        description="",
        tags_text="smoke",
    )
    save_benchmark_preset(benchmark_request, base_dir=tmp_path / "configs" / "presets" / "benchmarks")
    benchmark_entries, _ = scan_benchmark_presets(tmp_path / "configs" / "presets" / "benchmarks")

    eval_registry_dir = tmp_path / "configs" / "presets" / "evals"
    eval_registry_dir.mkdir(parents=True)
    (eval_registry_dir / "legacy_eval.json").write_text(
        json.dumps(
            {
                "eval_preset_id": "legacy_eval",
                "created_at": "2026-03-29T12:00:00Z",
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "legacy_label",
                "refresh": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    eval_entries, _ = scan_evaluation_presets(eval_registry_dir)

    benchmark_values, benchmark_warnings = build_benchmark_run_batch_values(benchmark_entries[0])
    eval_values, eval_warnings = build_evaluation_run_batch_values(
        eval_entries[0],
        allowed_label_sources=["silver"],
        fallback_label_source="silver",
    )

    assert benchmark_values["seeds_csv_path"].endswith("seeds.csv")
    assert benchmark_warnings == []
    assert eval_values["label_source"] == "silver"
    assert eval_values["evaluation_mode"] == "silver_provenance_regression"
    assert eval_values["metric_scope"] == "local_corpus_ranking"
    assert eval_warnings


def test_save_and_scan_benchmark_presets_with_external_labels_metadata(tmp_path: Path) -> None:
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    labels_csv = tmp_path / "data" / "benchmarks" / "benchmark_labels.csv"
    manifest_json = tmp_path / "data" / "benchmarks" / "benchmark_dataset_manifest.json"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    labels_csv.write_text(
        "seed_openalex_id,candidate_openalex_id,label\n"
        "https://openalex.org/WSEED,https://openalex.org/WCAND1,2\n",
        encoding="utf-8",
    )
    manifest_json.write_text(
        json.dumps(
            {
                "benchmark_dataset_id": "benchmark_dataset_001",
                "benchmark_schema_version": "benchmark_labels.v1",
                "benchmark_labels_row_count": 1,
                "aspect_scope": "lineage",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    request = build_benchmark_preset_save_request(
        preset_id="benchmark_independent_001",
        seeds_csv_path=seeds_csv,
        benchmark_labels_path=labels_csv,
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_dataset_manifest_path=manifest_json,
        benchmark_schema_version="benchmark_labels.v1",
        benchmark_labels_row_count=1,
        aspect_scope="lineage",
        description="independent benchmark",
        tags_text="independent",
    )
    save_benchmark_preset(
        request,
        base_dir=tmp_path / "configs" / "presets" / "benchmarks",
    )
    entries, warnings = scan_benchmark_presets(tmp_path / "configs" / "presets" / "benchmarks")

    assert warnings == []
    detail = build_benchmark_preset_detail(entries[0])
    values, value_warnings = build_benchmark_run_batch_values(entries[0])

    assert detail["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert detail["benchmark_labels_resolved_path"] == labels_csv
    assert detail["benchmark_dataset_manifest_resolved_path"] == manifest_json
    assert detail["benchmark_schema_version"] == "benchmark_labels.v1"
    assert detail["benchmark_labels_row_count"] == 1
    assert detail["aspect_scope"] == "lineage"
    assert values["benchmark_labels_path"].endswith("benchmark_labels.csv")
    assert values["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert values["benchmark_labels_sha256"] is not None
    assert value_warnings == []


def test_default_preset_directories_use_configs_presets_layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import src.ui.preset_registry as preset_registry

    monkeypatch.setattr(preset_registry, "REPO_ROOT", tmp_path)

    assert benchmark_presets_dir() == tmp_path / "configs" / "presets" / "benchmarks"
    assert evaluation_presets_dir() == tmp_path / "configs" / "presets" / "evals"


def test_scan_benchmark_preset_accepts_backslash_encoded_unix_absolute_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import src.ui.preset_registry as preset_registry

    monkeypatch.setattr(preset_registry, "REPO_ROOT", tmp_path)

    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds_quantum_characterization_v3_resolved.csv"
    seeds_csv.parent.mkdir(parents=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    registry_dir = tmp_path / "configs" / "presets" / "benchmarks"
    registry_dir.mkdir(parents=True)
    preset_path = registry_dir / "benchmark_preset_quantum_characterization_v3_resolved.json"
    backslash_absolute = str(seeds_csv).replace("/", "\\")
    preset_path.write_text(
        json.dumps(
            {
                "benchmark_preset_id": "benchmark_preset_quantum_characterization_v3_resolved",
                "seeds_csv": backslash_absolute,
                "created_at": "2026-04-03T12:45:00Z",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    entries, warnings = scan_benchmark_presets(registry_dir)

    assert warnings == []
    assert len(entries) == 1
    assert entries[0].seeds_csv_path == seeds_csv

    values, value_warnings = build_benchmark_run_batch_values(entries[0])
    assert value_warnings == []
    assert values["seeds_csv_path"] == backslash_absolute
