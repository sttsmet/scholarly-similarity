from __future__ import annotations

import json
from pathlib import Path

from src.ui.workspace_inbox import filter_workspace_inbox_items, scan_workspace_inbox


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _write_batch_dir(base_runs: Path, batch_id: str) -> Path:
    batch_dir = base_runs / "batches" / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    return batch_dir


def _write_study(
    base_runs: Path,
    *,
    study_id: str = "study_001",
    candidate_batch_id: str = "batch_010",
    decision: str = "shortlist",
    created_at: str = "2026-03-30T09:00:00Z",
) -> Path:
    study_dir = base_runs / "cohort_studies" / study_id
    reference_batch_dir = _write_batch_dir(base_runs, "batch_005")
    candidate_batch_dir = _write_batch_dir(base_runs, candidate_batch_id)
    _write_json(
        study_dir / "cohort_study_manifest.json",
        {
            "study_id": study_id,
            "created_at": created_at,
            "selected_metric": "ndcg_at_k",
            "reference_batch": {
                "batch_id": "batch_005",
                "batch_dir": f"runs/batches/batch_005",
            },
        },
    )
    _write_jsonl(
        study_dir / "candidate_decisions.jsonl",
        [
            {
                "candidate_batch_id": candidate_batch_id,
                "decision": decision,
                "suggested_decision": "shortlist",
                "usable": True,
                "selected_metric": "ndcg_at_k",
                "wins": 4,
                "losses": 1,
                "ties": 0,
            }
        ],
    )
    _write_jsonl(
        study_dir / "cohort_leaderboard.jsonl",
        [
            {
                "candidate_batch_id": candidate_batch_id,
                "candidate_batch_dir": f"runs/batches/{candidate_batch_id}",
                "selected_metric": "ndcg_at_k",
                "improvement_delta_mean": 0.12,
                "wins": 4,
                "losses": 1,
                "ties": 0,
                "candidate_status": "completed",
                "accepted_baseline_id": "baseline_001",
                "launch_profile_id": "launch_smoke_001",
            }
        ],
    )
    assert reference_batch_dir.exists()
    assert candidate_batch_dir.exists()
    return study_dir


def _write_review_packet(
    base_runs: Path,
    *,
    comparison_id: str = "comparison_001",
    packet_id: str = "packet_001",
) -> Path:
    packet_dir = base_runs / "comparisons" / comparison_id / "review_packets" / packet_id
    _write_json(
        packet_dir / "review_packet_manifest.json",
        {
            "packet_id": packet_id,
            "comparison_id": comparison_id,
            "created_at": "2026-03-30T10:00:00Z",
            "selected_packet_metric": "ndcg_at_k",
            "primary_batch": {
                "batch_id": "batch_005",
                "batch_dir": "runs/batches/batch_005",
            },
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_candidate_decision": "shortlist",
            "source_selected_metric": "ndcg_at_k",
        },
    )
    return packet_dir


def _write_candidate_run(
    packet_dir: Path,
    *,
    candidate_id: str = "candidate_001",
    output_batch_id: str = "batch_011",
) -> Path:
    candidate_run_dir = packet_dir / "candidate_runs" / candidate_id
    _write_json(
        candidate_run_dir / "candidate_apply_manifest.json",
        {
            "candidate_id": candidate_id,
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
            "created_at": "2026-03-30T11:00:00Z",
            "status": "completed",
            "output_batch_id": output_batch_id,
            "output_batch_dir": f"runs/batches/{output_batch_id}",
            "source_primary_batch": {
                "batch_id": "batch_005",
                "batch_dir": "runs/batches/batch_005",
            },
            "selected_metric_context": "ndcg_at_k",
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_candidate_decision": "shortlist",
            "source_selected_metric": "ndcg_at_k",
        },
    )
    return candidate_run_dir


def _write_outcome(
    candidate_run_dir: Path,
    *,
    outcome_id: str = "outcome_001",
    decision_status: str = "accept_candidate",
    guardrail_verdict: str | None = None,
) -> Path:
    outcome_dir = candidate_run_dir / "outcomes" / outcome_id
    payload = {
        "outcome_id": outcome_id,
        "created_at": "2026-03-30T12:00:00Z",
        "decision_status": decision_status,
        "comparison_id": "comparison_001",
        "packet_id": "packet_001",
        "candidate_id": "candidate_001",
        "candidate_run_dir": str(candidate_run_dir),
        "selected_metric": "ndcg_at_k",
        "primary_batch": {
            "batch_id": "batch_005",
            "batch_dir": "runs/batches/batch_005",
        },
        "secondary_batch": {
            "batch_id": "batch_011",
            "batch_dir": "runs/batches/batch_011",
        },
        "source_type": "cohort_study",
        "source_study_id": "study_001",
        "source_candidate_decision": "shortlist",
        "source_selected_metric": "ndcg_at_k",
    }
    if guardrail_verdict is not None:
        payload["guardrail_verdict"] = guardrail_verdict
    _write_json(outcome_dir / "reeval_outcome_manifest.json", payload)
    return outcome_dir


def _write_baseline(base_runs: Path, *, baseline_id: str, outcome_id: str, created_at: str) -> Path:
    baseline_dir = base_runs / "accepted_baselines" / baseline_id
    _write_json(
        baseline_dir / "accepted_baseline_manifest.json",
        {
            "baseline_id": baseline_id,
            "created_at": created_at,
            "decision_status": "accept_candidate",
            "selected_metric": "ndcg_at_k",
            "source_lineage": {
                "comparison_id": "comparison_001",
                "packet_id": "packet_001",
                "candidate_id": "candidate_001",
                "outcome_id": outcome_id,
            },
        },
    )
    return baseline_dir


def test_scan_workspace_inbox_loads_shortlisted_candidates(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    _write_study(base_runs)

    items, warnings = scan_workspace_inbox(base_runs)

    shortlist_items = [item for item in items if item.queue_type == "shortlisted_candidates"]
    assert warnings == []
    assert len(shortlist_items) == 1
    assert shortlist_items[0].study_id == "study_001"
    assert shortlist_items[0].target_primary_batch_id == "batch_005"
    assert shortlist_items[0].target_secondary_batch_id == "batch_010"


def test_scan_workspace_inbox_detects_packets_pending_candidate_work(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    _write_review_packet(base_runs)

    items, _ = scan_workspace_inbox(base_runs)

    packet_items = [item for item in items if item.queue_type == "review_packets_pending_candidate_work"]
    assert len(packet_items) == 1
    assert packet_items[0].packet_id == "packet_001"


def test_scan_workspace_inbox_keeps_packets_pending_without_candidate_apply_manifest(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    packet_dir = _write_review_packet(base_runs)
    (packet_dir / "candidate_runs" / "candidate_partial").mkdir(parents=True, exist_ok=True)

    items, _ = scan_workspace_inbox(base_runs)

    packet_items = [item for item in items if item.queue_type == "review_packets_pending_candidate_work"]
    assert len(packet_items) == 1
    assert "candidate apply manifest" in packet_items[0].summary


def test_scan_workspace_inbox_detects_candidate_runs_pending_outcomes(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    packet_dir = _write_review_packet(base_runs)
    _write_candidate_run(packet_dir)

    items, _ = scan_workspace_inbox(base_runs)

    candidate_items = [item for item in items if item.queue_type == "candidate_runs_pending_outcome"]
    assert len(candidate_items) == 1
    assert candidate_items[0].candidate_id == "candidate_001"


def test_scan_workspace_inbox_detects_pending_promotion_and_hides_promoted_outcomes(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    packet_dir = _write_review_packet(base_runs)
    candidate_run_dir = _write_candidate_run(packet_dir)
    _write_outcome(candidate_run_dir, outcome_id="outcome_pending")
    _write_outcome(candidate_run_dir, outcome_id="outcome_promoted")
    _write_baseline(
        base_runs,
        baseline_id="baseline_001",
        outcome_id="outcome_promoted",
        created_at="2026-03-30T13:00:00Z",
    )

    items, _ = scan_workspace_inbox(base_runs)

    promotion_items = [item for item in items if item.queue_type == "accepted_outcomes_pending_promotion"]
    assert [item.outcome_id for item in promotion_items] == ["outcome_pending"]


def test_scan_workspace_inbox_loads_weak_guarded_outcomes(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    packet_dir = _write_review_packet(base_runs)
    candidate_run_dir = _write_candidate_run(packet_dir)
    _write_outcome(candidate_run_dir, outcome_id="outcome_weak", guardrail_verdict="weak")

    items, _ = scan_workspace_inbox(base_runs)

    weak_items = [item for item in items if item.queue_type == "weak_guarded_outcomes"]
    assert len(weak_items) == 1
    assert weak_items[0].guardrail_verdict == "weak"


def test_scan_workspace_inbox_loads_recent_baselines_newest_first(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    _write_baseline(
        base_runs,
        baseline_id="baseline_old",
        outcome_id="outcome_001",
        created_at="2026-03-30T10:00:00Z",
    )
    _write_baseline(
        base_runs,
        baseline_id="baseline_new",
        outcome_id="outcome_002",
        created_at="2026-03-30T14:00:00Z",
    )

    items, _ = scan_workspace_inbox(base_runs)

    baseline_items = [item for item in items if item.queue_type == "recent_accepted_baselines"]
    assert [item.baseline_id for item in baseline_items] == ["baseline_new", "baseline_old"]


def test_scan_workspace_inbox_loads_shortlisted_candidates_newest_first(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    _write_study(
        base_runs,
        study_id="study_old",
        candidate_batch_id="batch_old",
        created_at="2026-03-30T09:00:00Z",
    )
    _write_study(
        base_runs,
        study_id="study_new",
        candidate_batch_id="batch_new",
        created_at="2026-03-30T15:00:00Z",
    )

    items, _ = scan_workspace_inbox(base_runs)

    shortlist_items = [item for item in items if item.queue_type == "shortlisted_candidates"]
    assert [item.study_id for item in shortlist_items] == ["study_new", "study_old"]


def test_scan_workspace_inbox_skips_malformed_artifacts_without_crashing(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    study_dir = base_runs / "cohort_studies" / "study_bad"
    study_dir.mkdir(parents=True, exist_ok=True)
    (study_dir / "cohort_study_manifest.json").write_text("{", encoding="utf-8")
    packet_dir = base_runs / "comparisons" / "comparison_001" / "review_packets" / "packet_bad"
    packet_dir.mkdir(parents=True, exist_ok=True)
    (packet_dir / "review_packet_manifest.json").write_text("{", encoding="utf-8")

    items, warnings = scan_workspace_inbox(base_runs)

    assert items == []
    assert len(warnings) >= 2


def test_filter_workspace_inbox_items_filters_by_queue_and_search(tmp_path: Path) -> None:
    base_runs = tmp_path / "runs"
    _write_study(base_runs, candidate_batch_id="batch_focus")
    _write_baseline(
        base_runs,
        baseline_id="baseline_001",
        outcome_id="outcome_001",
        created_at="2026-03-30T14:00:00Z",
    )

    items, _ = scan_workspace_inbox(base_runs)
    filtered = filter_workspace_inbox_items(
        items,
        queue_types=["shortlisted_candidates"],
        search_text="batch_focus",
    )

    assert len(filtered) == 1
    assert filtered[0].queue_type == "shortlisted_candidates"
