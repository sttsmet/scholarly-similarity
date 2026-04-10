from __future__ import annotations

import json
from pathlib import Path

from src.workspace_smoke import run_workspace_smoke, workspace_smoke_exit_code


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, contents: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def _write_batch(repo_root: Path, *, batch_id: str = "batch_001") -> Path:
    batch_dir = repo_root / "runs" / "batches" / batch_id
    _write_json(
        batch_dir / "batch_manifest.json",
        {
            "batch_id": batch_id,
            "batch_dir": f"runs/batches/{batch_id}",
            "output_paths": {
                "batch_manifest_json": f"runs/batches/{batch_id}/batch_manifest.json",
                "aggregate_summary_json": f"runs/batches/{batch_id}/aggregate_summary.json",
            },
        },
    )
    _write_json(batch_dir / "aggregate_summary.json", {"status": "completed"})
    _write_text(batch_dir / "seed_table.jsonl")
    _write_json(batch_dir / "worst_cases.json", {"best_seeds": [], "worst_seeds": [], "failed_seeds": []})
    return batch_dir


def _write_comparison_tree(repo_root: Path) -> None:
    comparison_dir = repo_root / "runs" / "comparisons" / "comparison_001"
    _write_json(
        comparison_dir / "comparison_manifest.json",
        {
            "comparison_id": "comparison_001",
            "comparison_dir": "runs/comparisons/comparison_001",
            "primary_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "secondary_batch": {"batch_id": "batch_002", "batch_dir": "runs/batches/batch_002"},
            "output_paths": {
                "comparison_manifest_json": "runs/comparisons/comparison_001/comparison_manifest.json",
                "decision_record_json": "runs/comparisons/comparison_001/decision_record.json",
            },
        },
    )
    _write_json(comparison_dir / "decision_record.json", {"comparison_id": "comparison_001"})
    _write_text(comparison_dir / "paired_seed_table.jsonl")

    packet_dir = comparison_dir / "review_packets" / "packet_001"
    _write_json(
        packet_dir / "review_packet_manifest.json",
        {
            "packet_id": "packet_001",
            "packet_dir": "runs/comparisons/comparison_001/review_packets/packet_001",
            "comparison_id": "comparison_001",
            "primary_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "secondary_batch": {"batch_id": "batch_002", "batch_dir": "runs/batches/batch_002"},
            "output_paths": {
                "review_packet_manifest_json": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/review_packet_manifest.json"
                ),
                "evidence_summary_json": "runs/comparisons/comparison_001/review_packets/packet_001/evidence_summary.json",
                "allowed_revision_paths_json": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/allowed_revision_paths.json"
                ),
                "baseline_theory_snapshot_yaml": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/baseline_theory_snapshot.yaml"
                ),
                "candidate_reply_template_yaml": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/candidate_reply_TEMPLATE.yaml"
                ),
            },
        },
    )
    _write_json(packet_dir / "evidence_summary.json", {"ok": True})
    _write_json(packet_dir / "allowed_revision_paths.json", {"allowed_scalar_paths": []})
    _write_text(packet_dir / "baseline_theory_snapshot.yaml", "version: v1\n")
    _write_text(packet_dir / "candidate_reply_TEMPLATE.yaml", "packet_id: packet_001\n")

    candidate_dir = packet_dir / "candidate_runs" / "candidate_001"
    _write_json(
        candidate_dir / "candidate_apply_manifest.json",
        {
            "candidate_id": "candidate_001",
            "candidate_dir": str(candidate_dir),
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
            "output_batch_id": "batch_002",
            "output_batch_dir": "runs/batches/batch_002",
            "source_primary_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "copied_reply_yaml": str(candidate_dir / "candidate_reply.yaml"),
            "candidate_theory_snapshot_path": str(candidate_dir / "candidate_theory_snapshot.yaml"),
            "output_paths": {
                "candidate_apply_manifest_json": str(candidate_dir / "candidate_apply_manifest.json"),
                "batch_run_result_json": str(candidate_dir / "batch_run_result.json"),
            },
        },
    )
    _write_json(candidate_dir / "batch_run_result.json", {"batch_id": "batch_002", "batch_dir": "runs/batches/batch_002"})
    _write_json(candidate_dir / "batch_run_request.json", {"batch_id": "batch_002"})
    _write_text(candidate_dir / "candidate_reply.yaml", "rationale: ok\n")
    _write_text(candidate_dir / "candidate_theory_snapshot.yaml", "version: v1\n")
    _write_text(candidate_dir / "applied_changes.jsonl")

    outcome_dir = candidate_dir / "outcomes" / "outcome_001"
    _write_json(
        outcome_dir / "reeval_outcome_manifest.json",
        {
            "outcome_id": "outcome_001",
            "outcome_dir": str(outcome_dir),
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
            "candidate_id": "candidate_001",
            "candidate_run_dir": str(candidate_dir),
            "decision_status": "accept_candidate",
            "primary_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "secondary_batch": {"batch_id": "batch_002", "batch_dir": "runs/batches/batch_002"},
            "output_paths": {
                "reeval_outcome_manifest_json": str(outcome_dir / "reeval_outcome_manifest.json"),
                "reeval_decision_record_json": str(outcome_dir / "reeval_decision_record.json"),
            },
        },
    )
    _write_json(
        outcome_dir / "reeval_decision_record.json",
        {
            "outcome_id": "outcome_001",
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
            "candidate_id": "candidate_001",
            "decision_status": "accept_candidate",
        },
    )
    _write_text(outcome_dir / "reeval_paired_seed_table.jsonl")


def _write_baseline(repo_root: Path) -> None:
    baseline_dir = repo_root / "runs" / "accepted_baselines" / "baseline_001"
    _write_json(
        baseline_dir / "accepted_baseline_manifest.json",
        {
            "baseline_id": "baseline_001",
            "baseline_dir": "runs/accepted_baselines/baseline_001",
            "decision_status": "accept_candidate",
            "accepted_theory_snapshot_path": "runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml",
            "candidate_reply_yaml_path": "runs/accepted_baselines/baseline_001/candidate_reply.yaml",
            "applied_changes_path": "runs/accepted_baselines/baseline_001/applied_changes.jsonl",
            "source_lineage": {
                "comparison_id": "comparison_001",
                "packet_id": "packet_001",
                "candidate_id": "candidate_001",
                "outcome_id": "outcome_001",
                "candidate_run_dir": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001"
                ),
                "outcome_dir": (
                    "runs/comparisons/comparison_001/review_packets/packet_001/candidate_runs/candidate_001/outcomes/outcome_001"
                ),
            },
            "source_primary_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "source_secondary_batch": {"batch_id": "batch_002", "batch_dir": "runs/batches/batch_002"},
            "output_paths": {
                "accepted_baseline_manifest_json": "runs/accepted_baselines/baseline_001/accepted_baseline_manifest.json",
                "promotion_record_json": "runs/accepted_baselines/baseline_001/promotion_record.json",
            },
        },
    )
    _write_json(
        baseline_dir / "promotion_record.json",
        {
            "baseline_id": "baseline_001",
            "comparison_id": "comparison_001",
            "packet_id": "packet_001",
            "candidate_id": "candidate_001",
            "outcome_id": "outcome_001",
            "decision_status": "accept_candidate",
        },
    )
    _write_text(baseline_dir / "accepted_theory_snapshot.yaml", "version: v1\n")
    _write_text(baseline_dir / "candidate_reply.yaml", "rationale: ok\n")
    _write_text(baseline_dir / "applied_changes.jsonl")


def _write_presets(repo_root: Path) -> None:
    _write_text(repo_root / "data" / "benchmarks" / "seeds.csv", "query_doi,label\n10.1/example,seed\n")
    _write_json(
        repo_root / "configs" / "presets" / "benchmarks" / "benchmark_001.json",
        {"benchmark_preset_id": "benchmark_001", "seeds_csv": "data/benchmarks/seeds.csv"},
    )
    _write_json(
        repo_root / "configs" / "presets" / "evals" / "eval_001.json",
        {"eval_preset_id": "eval_001", "top_k": 10, "label_source": "silver"},
    )
    _write_json(
        repo_root / "configs" / "presets" / "launch_profiles" / "launch_001.json",
        {
            "launch_profile_id": "launch_001",
            "accepted_baseline_dir": "runs/accepted_baselines/baseline_001",
            "accepted_theory_snapshot": "runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml",
            "seeds_csv": "data/benchmarks/seeds.csv",
        },
    )


def _write_report(repo_root: Path) -> None:
    report_dir = repo_root / "runs" / "reports" / "report_001"
    _write_json(
        report_dir / "report_manifest.json",
        {
            "report_id": "report_001",
            "report_dir": "runs/reports/report_001",
            "output_paths": {
                "report_manifest_json": "runs/reports/report_001/report_manifest.json",
                "context_snapshot_json": "runs/reports/report_001/context_snapshot.json",
                "included_artifacts_json": "runs/reports/report_001/included_artifacts.json",
            },
        },
    )
    _write_json(report_dir / "context_snapshot.json", {})
    _write_json(report_dir / "included_artifacts.json", {})


def _write_benchmark_audit(repo_root: Path) -> None:
    audit_dir = repo_root / "runs" / "benchmark_audits" / "audit_001"
    _write_json(
        audit_dir / "benchmark_audit_manifest.json",
        {
            "audit_id": "audit_001",
            "audit_dir": "runs/benchmark_audits/audit_001",
            "output_paths": {
                "benchmark_audit_manifest_json": "runs/benchmark_audits/audit_001/benchmark_audit_manifest.json",
                "primary_batch_health_json": "runs/benchmark_audits/audit_001/primary_batch_health.json",
            },
        },
    )
    _write_json(audit_dir / "primary_batch_health.json", {"verdict": "usable"})


def _write_benchmark_curation(repo_root: Path) -> None:
    curation_dir = repo_root / "runs" / "benchmark_curations" / "curation_001"
    _write_json(
        curation_dir / "curation_manifest.json",
        {
            "curation_id": "curation_001",
            "curation_dir": "runs/benchmark_curations/curation_001",
            "output_paths": {
                "curation_manifest_json": "runs/benchmark_curations/curation_001/curation_manifest.json",
                "seed_decisions_jsonl": "runs/benchmark_curations/curation_001/seed_decisions.jsonl",
                "curated_seeds_csv": "runs/benchmark_curations/curation_001/curated_seeds.csv",
            },
        },
    )
    _write_text(curation_dir / "seed_decisions.jsonl")
    _write_text(curation_dir / "curated_seeds.csv", "doi\n10.1/example\n")


def _write_cohort_study(repo_root: Path) -> None:
    study_dir = repo_root / "runs" / "cohort_studies" / "study_001"
    _write_json(
        study_dir / "cohort_study_manifest.json",
        {
            "study_id": "study_001",
            "study_dir": "runs/cohort_studies/study_001",
            "reference_batch": {"batch_id": "batch_001", "batch_dir": "runs/batches/batch_001"},
            "output_paths": {
                "candidate_decisions_jsonl": "runs/cohort_studies/study_001/candidate_decisions.jsonl",
                "cohort_leaderboard_jsonl": "runs/cohort_studies/study_001/cohort_leaderboard.jsonl",
            },
        },
    )
    _write_text(study_dir / "candidate_decisions.jsonl")
    _write_text(study_dir / "cohort_leaderboard.jsonl")


def test_run_workspace_smoke_counts_major_artifact_families(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_batch(repo_root, batch_id="batch_001")
    _write_batch(repo_root, batch_id="batch_002")
    _write_comparison_tree(repo_root)
    _write_baseline(repo_root)
    _write_presets(repo_root)
    _write_report(repo_root)
    _write_benchmark_audit(repo_root)
    _write_benchmark_curation(repo_root)
    _write_cohort_study(repo_root)

    result = run_workspace_smoke(repo_root)

    assert result.errors == ()
    assert result.artifact_family_counts["batches"] == 2
    assert result.artifact_family_counts["comparisons"] == 1
    assert result.artifact_family_counts["review_packets"] == 1
    assert result.artifact_family_counts["candidate_runs"] == 1
    assert result.artifact_family_counts["reeval_outcomes"] == 1
    assert result.artifact_family_counts["accepted_baselines"] == 1
    assert result.artifact_family_counts["benchmark_presets"] == 1
    assert result.artifact_family_counts["evaluation_presets"] == 1
    assert result.artifact_family_counts["launch_profiles"] == 1
    assert result.artifact_family_counts["reports"] == 1
    assert result.artifact_family_counts["benchmark_audits"] == 1
    assert result.artifact_family_counts["benchmark_curations"] == 1
    assert result.artifact_family_counts["cohort_studies"] == 1
    assert workspace_smoke_exit_code(result) == 0


def test_run_workspace_smoke_warns_on_malformed_run_context_without_crashing(tmp_path: Path) -> None:
    repo_root = tmp_path
    batch_dir = _write_batch(repo_root, batch_id="batch_001")
    _write_text(batch_dir / "run_context.json", "{bad json")

    result = run_workspace_smoke(repo_root)

    assert result.errors == ()
    assert any("Malformed run_context.json" in warning for warning in result.warnings)
    assert workspace_smoke_exit_code(result) == 0


def test_run_workspace_smoke_detects_hard_structural_errors(tmp_path: Path) -> None:
    repo_root = tmp_path
    broken_batch_dir = repo_root / "runs" / "batches" / "batch_broken"
    broken_batch_dir.mkdir(parents=True, exist_ok=True)
    _write_json(broken_batch_dir / "aggregate_summary.json", {"status": "completed"})

    result = run_workspace_smoke(repo_root)

    assert any("missing required file: batch_manifest.json" in error for error in result.errors)
    assert workspace_smoke_exit_code(result) == 1


def test_run_workspace_smoke_detects_candidate_run_result_mismatch(tmp_path: Path) -> None:
    repo_root = tmp_path
    _write_batch(repo_root, batch_id="batch_001")
    _write_batch(repo_root, batch_id="batch_002")
    _write_comparison_tree(repo_root)
    batch_result_path = (
        repo_root
        / "runs"
        / "comparisons"
        / "comparison_001"
        / "review_packets"
        / "packet_001"
        / "candidate_runs"
        / "candidate_001"
        / "batch_run_result.json"
    )
    _write_json(batch_result_path, {"batch_id": "batch_wrong", "batch_dir": "runs/batches/batch_002"})

    result = run_workspace_smoke(repo_root)

    assert any("candidate run 'candidate_001'" in error for error in result.errors)
    assert workspace_smoke_exit_code(result) == 1
