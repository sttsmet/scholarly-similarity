from __future__ import annotations

import json
from pathlib import Path

from src.ui.cohort_study_registry import (
    build_cohort_study_candidate_detail,
    build_cohort_study_candidate_rows,
    build_cohort_study_candidate_table_rows,
    build_cohort_study_detail,
    build_cohort_study_registry_rows,
    choose_default_cohort_study_id,
    filter_cohort_study_candidate_rows,
    find_cohort_study_candidate_row,
    find_cohort_study_entry,
    scan_cohort_studies,
)


def _write_study_dir(
    base_dir: Path,
    *,
    study_id: str,
    include_decisions: bool = True,
    create_reference_batch: bool = True,
    create_candidate_batch: bool = True,
) -> Path:
    study_dir = base_dir / study_id
    study_dir.mkdir(parents=True)
    if create_reference_batch:
        (base_dir.parent / "batches" / "batch_ref").mkdir(parents=True, exist_ok=True)
    if create_candidate_batch:
        (base_dir.parent / "batches" / "batch_candidate").mkdir(parents=True, exist_ok=True)
    manifest = {
        "study_id": study_id,
        "created_at": "2026-04-01T12:00:00Z",
        "reviewer": "Alice",
        "notes": "smoke cohort study",
        "cohort_key": "cohort_a",
        "cohort_summary": "data/benchmarks/seeds.csv | refs=10 | related=10 | hardneg=10 | top_k=10 | label=silver",
        "reference_batch": {
            "batch_id": "batch_ref",
            "batch_dir": "runs/batches/batch_ref",
            "status": "completed",
            "theory_config": "configs/reference.yaml",
        },
        "selected_metric": "ndcg_at_k",
        "total_candidate_rows": 1,
        "usable_candidate_rows": 1,
        "unusable_candidate_rows": 0,
        "shortlist_count": 1,
        "review_count": 0,
        "drop_count": 0,
        "output_paths": {
            "cohort_leaderboard_jsonl": str(study_dir / "cohort_leaderboard.jsonl"),
            "candidate_decisions_jsonl": str(study_dir / "candidate_decisions.jsonl"),
            "cohort_study_report_md": str(study_dir / "cohort_study_report.md"),
        },
    }
    (study_dir / "cohort_study_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    (study_dir / "cohort_leaderboard.jsonl").write_text(
        json.dumps(
            {
                "reference_batch_id": "batch_ref",
                "candidate_batch_id": "batch_candidate",
                "candidate_batch_dir": "runs/batches/batch_candidate",
                "candidate_status": "completed",
                "common_doi_count": 4,
                "common_completed_seed_count": 4,
                "selected_metric": "ndcg_at_k",
                "reference_mean": 0.5,
                "reference_median": 0.5,
                "candidate_mean": 0.6,
                "candidate_median": 0.6,
                "improvement_delta_mean": 0.1,
                "improvement_delta_median": 0.1,
                "wins": 3,
                "losses": 1,
                "ties": 0,
                "tie_rate": 0.0,
                "usable": True,
                "pairwise_status": "usable",
                "guardrail_verdict": "pass",
                "accepted_baseline_id": "baseline_001",
                "benchmark_preset_id": "benchmark_smoke_001",
                "eval_preset_id": "eval_micro_001",
                "launch_profile_id": "launch_profile_001",
                "launch_source_type": "launch_profile",
                "source_curation_id": "curation_001",
                "candidate_theory_config": "configs/candidate.yaml",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    if include_decisions:
        (study_dir / "candidate_decisions.jsonl").write_text(
            json.dumps(
                {
                    "candidate_batch_id": "batch_candidate",
                    "decision": "shortlist",
                    "suggested_decision": "shortlist",
                    "usable": True,
                    "pairwise_status": "usable",
                    "reasons": ["positive"],
                    "selected_metric": "ndcg_at_k",
                    "improvement_delta_mean": 0.1,
                    "wins": 3,
                    "losses": 1,
                    "ties": 0,
                    "guardrail_verdict": "pass",
                }
            )
            + "\n",
            encoding="utf-8",
        )
    (study_dir / "cohort_study_report.md").write_text(
        "# Cohort Study\n\nShortlisted candidate.\n",
        encoding="utf-8",
    )
    return study_dir


def test_scan_cohort_studies_loads_registry_rows_and_detail(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(base_dir, study_id="study_001")
    _write_study_dir(base_dir, study_id="study_002")

    entries, warnings = scan_cohort_studies(base_dir)

    assert [entry.study_id for entry in entries] == ["study_001", "study_002"]
    assert warnings == []
    rows = build_cohort_study_registry_rows(entries)
    detail = build_cohort_study_detail(entries[0])

    assert rows[0]["reference_batch_id"] == "batch_ref"
    assert detail["identity"]["study_id"] == "study_001"
    assert detail["reference_batch"]["batch_exists"] is True
    assert "Shortlisted candidate" in detail["report_markdown"]


def test_scan_cohort_studies_skips_malformed_manifest_without_crashing(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(base_dir, study_id="study_001")
    broken_dir = base_dir / "broken_study"
    broken_dir.mkdir(parents=True)
    (broken_dir / "cohort_study_manifest.json").write_text("{bad json", encoding="utf-8")

    entries, warnings = scan_cohort_studies(base_dir)

    assert [entry.study_id for entry in entries] == ["study_001"]
    assert warnings
    assert "broken_study" in warnings[0]


def test_candidate_rows_merge_decisions_and_leaderboard_and_filter(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(base_dir, study_id="study_001")
    entries, _ = scan_cohort_studies(base_dir)
    candidate_rows = build_cohort_study_candidate_rows(entries[0])

    assert candidate_rows[0].decision == "shortlist"
    assert candidate_rows[0].candidate_batch_exists is True
    filtered = filter_cohort_study_candidate_rows(
        candidate_rows,
        decision_filter="shortlist",
        usable_only=True,
        search_text="baseline_001",
    )
    table_rows = build_cohort_study_candidate_table_rows(filtered)
    detail = build_cohort_study_candidate_detail(candidate_rows[0])

    assert len(filtered) == 1
    assert table_rows[0]["candidate_batch_id"] == "batch_candidate"
    assert detail["summary"]["guardrail_verdict"] == "pass"


def test_candidate_rows_degrade_gracefully_without_decisions_file(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(base_dir, study_id="study_001", include_decisions=False)
    entries, warnings = scan_cohort_studies(base_dir)
    candidate_rows = build_cohort_study_candidate_rows(entries[0])

    assert warnings == []
    assert candidate_rows[0].decision is None
    assert candidate_rows[0].selected_metric == "ndcg_at_k"
    assert candidate_rows[0].usable is True


def test_missing_reference_and_candidate_batches_are_marked_honestly(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(
        base_dir,
        study_id="study_001",
        create_reference_batch=False,
        create_candidate_batch=False,
    )
    entries, _ = scan_cohort_studies(base_dir)
    entry = entries[0]
    candidate_rows = build_cohort_study_candidate_rows(entry)

    assert entry.reference_batch_exists is False
    assert candidate_rows[0].candidate_batch_exists is False


def test_choose_default_and_find_helpers_work(tmp_path: Path) -> None:
    base_dir = tmp_path / "runs" / "cohort_studies"
    _write_study_dir(base_dir, study_id="study_001")
    _write_study_dir(base_dir, study_id="study_002")
    entries, _ = scan_cohort_studies(base_dir)

    assert choose_default_cohort_study_id(entries, preferred_study_id="study_002") == "study_002"
    entry = find_cohort_study_entry(entries, "study_001")
    assert entry is not None
    candidate_rows = build_cohort_study_candidate_rows(entry)
    found_row = find_cohort_study_candidate_row(candidate_rows, "batch_candidate")
    assert found_row is not None
