from __future__ import annotations

import json
from pathlib import Path

from src.ui.baseline_registry import (
    build_accepted_baseline_detail,
    build_accepted_baseline_registry_rows,
    choose_default_accepted_baseline_id,
    extract_accepted_theory_snapshot_path,
    find_accepted_baseline_entry,
    scan_accepted_baselines,
)


def _write_baseline_dir(
    base_dir: Path,
    *,
    baseline_id: str,
    created_at: str = "2026-03-29T12:00:00Z",
) -> Path:
    baseline_dir = base_dir / baseline_id
    baseline_dir.mkdir(parents=True)
    (baseline_dir / "accepted_theory_snapshot.yaml").write_text(
        "version: theory_v001\nsim_weights:\n  temporal: 0.2\n",
        encoding="utf-8",
    )
    (baseline_dir / "candidate_reply.yaml").write_text(
        "summary: accepted\nchanges:\n  - path: sim_weights.temporal\n    value: 0.2\n",
        encoding="utf-8",
    )
    (baseline_dir / "applied_changes.jsonl").write_text(
        json.dumps({"path": "sim_weights.temporal", "status": "applied"}) + "\n",
        encoding="utf-8",
    )
    (baseline_dir / "accepted_baseline_manifest.json").write_text(
        json.dumps(
            {
                "baseline_id": baseline_id,
                "created_at": created_at,
                "reviewer": "Alice",
                "notes": "Accepted after re-eval.",
                "decision_status": "accept_candidate",
                "selected_metric": "ndcg_at_k",
                "source_lineage": {
                    "comparison_id": "comparison_001",
                    "packet_id": "packet_001",
                    "candidate_id": "candidate_001",
                    "outcome_id": "outcome_accept_001",
                    "candidate_run_dir": "runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001",
                    "outcome_dir": "runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001/outcomes/outcome_accept_001",
                },
                "source_primary_batch": {
                    "batch_id": "batch_005",
                    "batch_dir": "runs/batches/batch_005",
                    "theory_config": "configs/theory_v001.yaml",
                },
                "source_secondary_batch": {
                    "batch_id": "batch_007",
                    "batch_dir": "runs/batches/batch_007",
                    "theory_config": "runs/accepted_baselines/accepted_baseline_001/accepted_theory_snapshot.yaml",
                },
                "accepted_theory_snapshot_path": str(baseline_dir / "accepted_theory_snapshot.yaml"),
                "candidate_reply_yaml_path": str(baseline_dir / "candidate_reply.yaml"),
                "applied_changes_path": str(baseline_dir / "applied_changes.jsonl"),
                "outcome_summary": {
                    "common_doi_count": 5,
                    "common_completed_seed_count": 4,
                    "primary_mean": 0.7,
                    "primary_median": 0.7,
                    "secondary_mean": 0.8,
                    "secondary_median": 0.8,
                    "raw_delta_mean": 0.1,
                    "raw_delta_median": 0.1,
                    "improvement_delta_mean": 0.1,
                    "improvement_delta_median": 0.1,
                    "wins": 3,
                    "losses": 1,
                    "ties": 0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (baseline_dir / "promotion_record.json").write_text(
        json.dumps(
            {
                "baseline_id": baseline_id,
                "reviewer": "Alice",
                "decision_status": "accept_candidate",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return baseline_dir


def test_scan_accepted_baselines_loads_multiple_valid_entries(tmp_path: Path) -> None:
    registry_dir = tmp_path / "runs" / "accepted_baselines"
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_001")
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_002")

    entries, warnings = scan_accepted_baselines(registry_dir)

    assert [entry.baseline_id for entry in entries] == [
        "accepted_baseline_001",
        "accepted_baseline_002",
    ]
    assert warnings == []
    rows = build_accepted_baseline_registry_rows(entries)
    assert rows[0]["comparison_id"] == "comparison_001"
    assert rows[0]["source_primary_batch_id"] == "batch_005"


def test_scan_accepted_baselines_skips_malformed_entries_without_crashing(tmp_path: Path) -> None:
    registry_dir = tmp_path / "runs" / "accepted_baselines"
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_001")
    bad_dir = registry_dir / "broken_baseline"
    bad_dir.mkdir(parents=True)
    (bad_dir / "accepted_baseline_manifest.json").write_text("{not json", encoding="utf-8")

    entries, warnings = scan_accepted_baselines(registry_dir)

    assert [entry.baseline_id for entry in entries] == ["accepted_baseline_001"]
    assert warnings
    assert "broken_baseline" in warnings[0]


def test_choose_default_accepted_baseline_id_prefers_existing_selection(tmp_path: Path) -> None:
    registry_dir = tmp_path / "runs" / "accepted_baselines"
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_001")
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_002")
    entries, _ = scan_accepted_baselines(registry_dir)

    assert (
        choose_default_accepted_baseline_id(
            entries,
            preferred_baseline_id="accepted_baseline_002",
        )
        == "accepted_baseline_002"
    )
    assert choose_default_accepted_baseline_id([], preferred_baseline_id="missing") is None


def test_extract_accepted_theory_snapshot_path_falls_back_to_default_filename(tmp_path: Path) -> None:
    baseline_dir = tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    baseline_dir.mkdir(parents=True)

    path = extract_accepted_theory_snapshot_path({}, baseline_dir=baseline_dir)

    assert path == baseline_dir / "accepted_theory_snapshot.yaml"


def test_build_accepted_baseline_detail_exposes_manifest_and_provenance(tmp_path: Path) -> None:
    registry_dir = tmp_path / "runs" / "accepted_baselines"
    _write_baseline_dir(registry_dir, baseline_id="accepted_baseline_001")
    entries, _ = scan_accepted_baselines(registry_dir)
    entry = find_accepted_baseline_entry(entries, "accepted_baseline_001")

    assert entry is not None
    detail = build_accepted_baseline_detail(entry)

    assert detail["identity"]["baseline_id"] == "accepted_baseline_001"
    assert detail["source_lineage"]["candidate_id"] == "candidate_001"
    assert detail["outcome_summary"]["wins"] == 3
    assert detail["promotion_record"]["decision_status"] == "accept_candidate"
