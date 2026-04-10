from __future__ import annotations

import json
from datetime import datetime, timezone
from os import utime
from pathlib import Path

from src.ui.provenance_timeline import (
    build_timeline_detail,
    choose_default_timeline_entry_key,
    filter_timeline_entries,
    find_timeline_entry,
    scan_provenance_timeline,
)


def test_scan_provenance_timeline_loads_multiple_artifact_types_newest_first(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "runs" / "batches" / "batch_005" / "batch_manifest.json",
        {
            "batch_id": "batch_005",
            "created_at": "2026-03-29T10:00:00+00:00",
            "status": "completed",
            "seed_count": 6,
        },
    )
    _write_json(
        tmp_path / "runs" / "comparisons" / "comparison_001" / "comparison_manifest.json",
        {
            "comparison_id": "comparison_001",
            "created_at": "2026-03-29T11:00:00+00:00",
            "primary_batch": {"batch_id": "batch_005"},
            "secondary_batch": {"batch_id": "batch_006"},
        },
    )
    _write_json(
        tmp_path
        / "runs"
        / "comparisons"
        / "comparison_001"
        / "review_packets"
        / "packet_001"
        / "review_packet_manifest.json",
        {
            "packet_id": "packet_001",
            "comparison_id": "comparison_001",
            "created_at": "2026-03-29T10:30:00+00:00",
            "selected_packet_metric": "ndcg_at_k",
        },
    )

    entries, warnings = scan_provenance_timeline(tmp_path)

    assert warnings == []
    assert [entry.artifact_type for entry in entries] == [
        "comparison",
        "review_packet",
        "batch",
    ]
    assert [entry.artifact_id for entry in entries] == [
        "comparison_001",
        "packet_001",
        "batch_005",
    ]


def test_scan_provenance_timeline_falls_back_to_manifest_mtime(tmp_path: Path) -> None:
    manifest_path = (
        tmp_path
        / "runs"
        / "accepted_baselines"
        / "accepted_baseline_001"
        / "accepted_baseline_manifest.json"
    )
    accepted_snapshot = manifest_path.parent / "accepted_theory_snapshot.yaml"
    accepted_snapshot.parent.mkdir(parents=True, exist_ok=True)
    accepted_snapshot.write_text("sim_weights:\n  direct: 0.5\n", encoding="utf-8")
    _write_json(
        manifest_path,
        {
            "baseline_id": "accepted_baseline_001",
            "decision_status": "accept_candidate",
            "accepted_theory_snapshot_path": "accepted_theory_snapshot.yaml",
            "source_lineage": {"candidate_id": "candidate_001"},
        },
    )
    fallback_timestamp = datetime(2026, 3, 29, 12, 15, tzinfo=timezone.utc).timestamp()
    utime(manifest_path, (fallback_timestamp, fallback_timestamp))

    entries, warnings = scan_provenance_timeline(tmp_path)

    assert warnings == []
    assert len(entries) == 1
    entry = entries[0]
    assert entry.timestamp_source == "manifest_mtime"
    assert "manifest mtime" in entry.timestamp_display

    detail = build_timeline_detail(entry)
    assert detail["key_fields"]["accepted_theory_snapshot_path"] == accepted_snapshot.resolve()


def test_filter_timeline_entries_supports_type_status_and_search(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "runs" / "batches" / "batch_005" / "batch_manifest.json",
        {
            "batch_id": "batch_005",
            "created_at": "2026-03-29T10:00:00+00:00",
            "status": "completed",
        },
    )
    _write_json(
        tmp_path
        / "runs"
        / "comparisons"
        / "comparison_001"
        / "review_packets"
        / "packet_001"
        / "candidate_runs"
        / "candidate_001"
        / "outcomes"
        / "outcome_001"
        / "reeval_outcome_manifest.json",
        {
            "outcome_id": "outcome_001",
            "created_at": "2026-03-29T11:00:00+00:00",
            "decision_status": "accept_candidate",
            "candidate_id": "candidate_001",
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
        },
    )

    entries, _ = scan_provenance_timeline(tmp_path)
    filtered = filter_timeline_entries(
        entries,
        artifact_types=["reeval_outcome"],
        statuses=["accept_candidate"],
        search_text="candidate_001",
    )

    assert len(filtered) == 1
    assert filtered[0].artifact_type == "reeval_outcome"
    assert filtered[0].artifact_id == "outcome_001"


def test_scan_provenance_timeline_skips_malformed_manifests(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "runs" / "batches" / "batch_005" / "batch_manifest.json",
        {
            "batch_id": "batch_005",
            "created_at": "2026-03-29T10:00:00+00:00",
            "status": "completed",
        },
    )
    malformed_path = tmp_path / "runs" / "comparisons" / "broken" / "comparison_manifest.json"
    malformed_path.parent.mkdir(parents=True, exist_ok=True)
    malformed_path.write_text("{not json", encoding="utf-8")

    entries, warnings = scan_provenance_timeline(tmp_path)

    assert len(entries) == 1
    assert entries[0].artifact_type == "batch"
    assert len(warnings) == 1
    assert "comparison_manifest.json" in warnings[0]


def test_choose_default_and_find_timeline_entry_use_filtered_entries(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "runs" / "batches" / "batch_005" / "batch_manifest.json",
        {
            "batch_id": "batch_005",
            "created_at": "2026-03-29T10:00:00+00:00",
            "status": "completed",
        },
    )
    _write_json(
        tmp_path / "runs" / "batches" / "batch_006" / "batch_manifest.json",
        {
            "batch_id": "batch_006",
            "created_at": "2026-03-29T11:00:00+00:00",
            "status": "failed",
        },
    )

    entries, _ = scan_provenance_timeline(tmp_path)
    default_key = choose_default_timeline_entry_key(entries)
    selected = find_timeline_entry(entries, default_key)

    assert default_key is not None
    assert selected is not None
    assert selected.artifact_id == "batch_006"


def test_build_timeline_detail_includes_batch_run_context_when_present(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_010"
    _write_json(
        batch_dir / "batch_manifest.json",
        {
            "batch_id": "batch_010",
            "created_at": "2026-03-29T10:00:00+00:00",
            "status": "completed",
        },
    )
    (batch_dir / "run_context.json").write_text(
        json.dumps(
            {
                "batch_id": "batch_010",
                "launch_source_type": "launch_profile",
                "accepted_baseline_id": "baseline_001",
                "benchmark_preset_id": "benchmark_curated_001",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    entries, _ = scan_provenance_timeline(tmp_path)
    detail = build_timeline_detail(entries[0])

    assert detail["run_context_summary"] == {
        "launch_source_type": "launch_profile",
        "accepted_baseline_id": "baseline_001",
        "benchmark_preset_id": "benchmark_curated_001",
    }
    assert detail["run_context_warning"] is None


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
