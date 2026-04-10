from __future__ import annotations

import json
from pathlib import Path

import pytest

import src.ui.launch_profile_registry as launch_profile_registry
from src.ui.launch_profile_registry import (
    LaunchProfileRegistryError,
    build_launch_profile_detail,
    build_launch_profile_rows,
    build_launch_profile_run_batch_values,
    build_launch_profile_save_request,
    choose_default_launch_profile_id,
    find_launch_profile_entry,
    launch_profiles_dir,
    save_launch_profile,
    scan_launch_profiles,
)


def test_save_and_scan_launch_profiles(tmp_path: Path) -> None:
    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    accepted_theory_snapshot.parent.mkdir(parents=True)
    accepted_theory_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True, exist_ok=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_launch_profile_save_request(
        profile_id="launch_smoke_baseline_001",
        accepted_baseline_id="accepted_baseline_001",
        accepted_baseline_dir=accepted_baseline_dir,
        accepted_theory_snapshot=accepted_theory_snapshot,
        benchmark_preset_id="benchmark_smoke_001",
        seeds_csv=seeds_csv,
        eval_preset_id="eval_micro_001",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        refresh=False,
        description="smoke launch",
        tags_text="smoke, accepted, smoke",
    )
    profile_path = save_launch_profile(
        request,
        base_dir=tmp_path / "configs" / "presets" / "launch_profiles",
    )

    entries, warnings = scan_launch_profiles(tmp_path / "configs" / "presets" / "launch_profiles")

    assert warnings == []
    assert profile_path == (
        tmp_path / "configs" / "presets" / "launch_profiles" / "launch_smoke_baseline_001.json"
    )
    assert [entry.profile_id for entry in entries] == ["launch_smoke_baseline_001"]
    detail = build_launch_profile_detail(entries[0])
    assert detail["accepted_baseline_id"] == "accepted_baseline_001"
    assert detail["benchmark_preset_id"] == "benchmark_smoke_001"
    assert detail["eval_preset_id"] == "eval_micro_001"
    assert detail["tags"] == ["smoke", "accepted"]
    assert detail["accepted_theory_snapshot_path"] == accepted_theory_snapshot
    rows = build_launch_profile_rows(entries)
    assert rows[0]["launch_profile_id"] == "launch_smoke_baseline_001"


def test_save_launch_profile_refuses_overwrite(tmp_path: Path) -> None:
    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    accepted_theory_snapshot.parent.mkdir(parents=True)
    accepted_theory_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True, exist_ok=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_launch_profile_save_request(
        profile_id="launch_smoke_baseline_001",
        accepted_baseline_id="accepted_baseline_001",
        accepted_baseline_dir=accepted_baseline_dir,
        accepted_theory_snapshot=accepted_theory_snapshot,
        benchmark_preset_id="benchmark_smoke_001",
        seeds_csv=seeds_csv,
        eval_preset_id="eval_micro_001",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        refresh=False,
        description="",
        tags_text="",
    )

    save_launch_profile(request, base_dir=tmp_path / "configs" / "presets" / "launch_profiles")

    with pytest.raises(LaunchProfileRegistryError):
        save_launch_profile(request, base_dir=tmp_path / "configs" / "presets" / "launch_profiles")


def test_build_launch_profile_save_request_validates_selected_context(tmp_path: Path) -> None:
    with pytest.raises(LaunchProfileRegistryError) as exc_info:
        build_launch_profile_save_request(
            profile_id="",
            accepted_baseline_id="",
            accepted_baseline_dir=tmp_path / "missing_baseline",
            accepted_theory_snapshot=tmp_path / "missing_theory.yaml",
            benchmark_preset_id="",
            seeds_csv=tmp_path / "missing_seeds.csv",
            eval_preset_id="",
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
            top_k=0,
            label_source="",
            refresh=False,
            description="",
            tags_text="",
        )

    assert "Launch Profile ID is required." in str(exc_info.value)


def test_build_launch_profile_save_request_requires_existing_selected_artifacts(tmp_path: Path) -> None:
    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    accepted_baseline_dir.mkdir(parents=True)

    with pytest.raises(LaunchProfileRegistryError) as exc_info:
        build_launch_profile_save_request(
            profile_id="launch_smoke_baseline_001",
            accepted_baseline_id="accepted_baseline_001",
            accepted_baseline_dir=accepted_baseline_dir,
            accepted_theory_snapshot=accepted_baseline_dir / "accepted_theory_snapshot.yaml",
            benchmark_preset_id="benchmark_smoke_001",
            seeds_csv=tmp_path / "data" / "benchmarks" / "seeds.csv",
            eval_preset_id="eval_micro_001",
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
            top_k=10,
            label_source="silver",
            refresh=False,
            description="",
            tags_text="",
        )

    assert "Accepted theory snapshot does not exist" in str(exc_info.value)


def test_scan_launch_profiles_skips_malformed_json(tmp_path: Path) -> None:
    registry_dir = tmp_path / "configs" / "presets" / "launch_profiles"
    registry_dir.mkdir(parents=True)
    (registry_dir / "broken.json").write_text("{not json", encoding="utf-8")

    entries, warnings = scan_launch_profiles(registry_dir)

    assert entries == []
    assert warnings
    assert "broken.json" in warnings[0]


def test_build_launch_profile_run_batch_values_extracts_prefill_values_and_warns_for_legacy_label(
    tmp_path: Path,
) -> None:
    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    accepted_theory_snapshot.parent.mkdir(parents=True)
    accepted_theory_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")
    profile_path = tmp_path / "configs" / "presets" / "launch_profiles" / "legacy_profile.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        json.dumps(
            {
                "launch_profile_id": "legacy_profile",
                "created_at": "2026-03-30T12:00:00Z",
                "accepted_baseline_id": "accepted_baseline_001",
                "accepted_baseline_dir": str(accepted_baseline_dir),
                "accepted_theory_snapshot": str(accepted_theory_snapshot),
                "benchmark_preset_id": "benchmark_smoke_001",
                "seeds_csv": "data/benchmarks/missing.csv",
                "eval_preset_id": "eval_micro_001",
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

    entries, warnings = scan_launch_profiles(profile_path.parent)
    values, profile_warnings = build_launch_profile_run_batch_values(
        entries[0],
        allowed_label_sources=["silver"],
        fallback_label_source="silver",
    )

    assert warnings == []
    assert values["theory_config_path"] == str(accepted_theory_snapshot)
    assert values["label_source"] == "silver"
    assert values["evaluation_mode"] == "silver_provenance_regression"
    assert profile_warnings


def test_choose_default_find_entry_and_default_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(launch_profile_registry, "REPO_ROOT", tmp_path)

    assert launch_profiles_dir() == tmp_path / "configs" / "presets" / "launch_profiles"

    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    accepted_theory_snapshot.parent.mkdir(parents=True)
    accepted_theory_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")
    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_csv.parent.mkdir(parents=True, exist_ok=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_launch_profile_save_request(
        profile_id="launch_smoke_baseline_001",
        accepted_baseline_id="accepted_baseline_001",
        accepted_baseline_dir=accepted_baseline_dir,
        accepted_theory_snapshot=accepted_theory_snapshot,
        benchmark_preset_id="benchmark_smoke_001",
        seeds_csv=seeds_csv,
        eval_preset_id="eval_micro_001",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        refresh=False,
        description="",
        tags_text="",
    )
    save_launch_profile(request)
    entries, _ = scan_launch_profiles()

    assert choose_default_launch_profile_id(
        entries,
        preferred_profile_id="launch_smoke_baseline_001",
    ) == "launch_smoke_baseline_001"
    assert find_launch_profile_entry(entries, "launch_smoke_baseline_001") is not None


def test_scan_launch_profile_accepts_backslash_relative_paths_on_linux(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(launch_profile_registry, "REPO_ROOT", tmp_path)

    accepted_baseline_dir = tmp_path / "runs" / "accepted_baselines" / "baseline_001"
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    accepted_theory_snapshot.parent.mkdir(parents=True)
    accepted_theory_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")

    seeds_csv = tmp_path / "data" / "benchmarks" / "seeds_quantum_characterization_v3_resolved.csv"
    seeds_csv.parent.mkdir(parents=True, exist_ok=True)
    seeds_csv.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    profile_path = tmp_path / "configs" / "presets" / "launch_profiles" / "launch_quantum_characterization_v3_resolved.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text(
        json.dumps(
            {
                "launch_profile_id": "launch_quantum_characterization_v3_resolved",
                "created_at": "2026-04-03T12:45:00Z",
                "accepted_baseline_id": "baseline_001",
                "accepted_baseline_dir": "runs\\accepted_baselines\\baseline_001",
                "accepted_theory_snapshot": "runs\\accepted_baselines\\baseline_001\\accepted_theory_snapshot.yaml",
                "benchmark_preset_id": "benchmark_preset_quantum_characterization_v3_resolved",
                "seeds_csv": "data\\benchmarks\\seeds_quantum_characterization_v3_resolved.csv",
                "eval_preset_id": "eval_preset_002_boundary_harder",
                "max_references": 10,
                "max_related": 20,
                "max_hard_negatives": 20,
                "top_k": 10,
                "label_source": "silver",
                "refresh": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    entries, warnings = scan_launch_profiles(profile_path.parent)

    assert warnings == []
    assert len(entries) == 1
    assert entries[0].accepted_theory_snapshot_path == accepted_theory_snapshot
    assert entries[0].seeds_csv_path == seeds_csv

    values, profile_warnings = build_launch_profile_run_batch_values(
        entries[0],
        allowed_label_sources=["silver"],
        fallback_label_source="silver",
    )
    assert profile_warnings == []
    assert values["theory_config_path"] == "runs\\accepted_baselines\\baseline_001\\accepted_theory_snapshot.yaml"
    assert values["seeds_csv_path"] == "data\\benchmarks\\seeds_quantum_characterization_v3_resolved.csv"
