from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.decision_guardrails import evaluate_decision_guardrails
from src.ui.baseline_promotion import (
    BaselinePromotionError,
    build_accepted_baseline_manifest_payload,
    build_baseline_promotion_request,
    build_promotion_record_payload,
    load_outcome_context,
    promotion_compatibility_errors,
    save_accepted_baseline_artifacts,
)
from src.ui.reeval_outcome_export import load_candidate_run_context


def _promotion_ready_guardrail(summary: dict[str, object] | None = None):
    return evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=summary
        or {
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
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )


def _write_batch_manifest(
    tmp_path: Path,
    *,
    batch_id: str,
    evaluation_mode: str = "independent_benchmark",
    benchmark_dataset_id: str = "benchmark_dataset_001",
    benchmark_labels_sha256: str = "labels_sha256_001",
    benchmark_maturity_tier: str | None = "promotion_ready",
    promotion_ready: bool | None = True,
    promotion_ineligibility_reasons: list[str] | None = None,
) -> Path:
    batch_dir = tmp_path / "runs" / "batches" / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    (batch_dir / "batch_manifest.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "batch_dir": str(batch_dir),
                "seeds_csv": "data/benchmarks/seeds.csv",
                "theory_config": "configs/theory_v001.yaml",
                "created_at": "2026-03-29T12:00:00Z",
                "status": "completed",
                "seed_count": 1,
                "completed_seed_count": 1,
                "failed_seed_count": 0,
                "options": {
                    "max_references": 10,
                    "max_related": 10,
                    "max_hard_negatives": 10,
                    "top_k": 10,
                    "label_source": (
                        "benchmark" if evaluation_mode == "independent_benchmark" else "silver"
                    ),
                    "evaluation_mode": evaluation_mode,
                    "benchmark_labels_path": "data/benchmarks/benchmark_labels.csv",
                    "benchmark_dataset_id": benchmark_dataset_id,
                    "benchmark_labels_sha256": benchmark_labels_sha256,
                    "benchmark_maturity_tier": benchmark_maturity_tier,
                    "promotion_ready": promotion_ready,
                    "promotion_ineligibility_reasons": list(
                        promotion_ineligibility_reasons or []
                    ),
                    "metric_scope": "local_corpus_ranking",
                    "refresh": False,
                },
                "output_paths": {
                    "batch_manifest_json": str(batch_dir / "batch_manifest.json"),
                    "seed_runs_jsonl": str(batch_dir / "seed_runs.jsonl"),
                    "aggregate_summary_json": str(batch_dir / "aggregate_summary.json"),
                    "seed_table_jsonl": str(batch_dir / "seed_table.jsonl"),
                    "worst_cases_json": str(batch_dir / "worst_cases.json"),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return batch_dir


def _write_candidate_run_dir(
    tmp_path: Path,
    *,
    include_study_source: bool = False,
    primary_evaluation_mode: str = "independent_benchmark",
    secondary_evaluation_mode: str = "independent_benchmark",
    primary_benchmark_dataset_id: str = "benchmark_dataset_001",
    secondary_benchmark_dataset_id: str = "benchmark_dataset_001",
    primary_benchmark_labels_sha256: str = "labels_sha256_001",
    secondary_benchmark_labels_sha256: str = "labels_sha256_001",
    primary_benchmark_maturity_tier: str | None = "promotion_ready",
    secondary_benchmark_maturity_tier: str | None = "promotion_ready",
    primary_promotion_ready: bool | None = True,
    secondary_promotion_ready: bool | None = True,
    primary_promotion_ineligibility_reasons: list[str] | None = None,
    secondary_promotion_ineligibility_reasons: list[str] | None = None,
) -> Path:
    primary_batch_dir = _write_batch_manifest(
        tmp_path,
        batch_id="batch_005",
        evaluation_mode=primary_evaluation_mode,
        benchmark_dataset_id=primary_benchmark_dataset_id,
        benchmark_labels_sha256=primary_benchmark_labels_sha256,
        benchmark_maturity_tier=primary_benchmark_maturity_tier,
        promotion_ready=primary_promotion_ready,
        promotion_ineligibility_reasons=primary_promotion_ineligibility_reasons,
    )
    secondary_batch_dir = _write_batch_manifest(
        tmp_path,
        batch_id="batch_007",
        evaluation_mode=secondary_evaluation_mode,
        benchmark_dataset_id=secondary_benchmark_dataset_id,
        benchmark_labels_sha256=secondary_benchmark_labels_sha256,
        benchmark_maturity_tier=secondary_benchmark_maturity_tier,
        promotion_ready=secondary_promotion_ready,
        promotion_ineligibility_reasons=secondary_promotion_ineligibility_reasons,
    )
    candidate_run_dir = (
        tmp_path
        / "runs"
        / "comparisons"
        / "comparison_001"
        / "review_packets"
        / "packet_001"
        / "candidate_runs"
        / "candidate_001"
    )
    candidate_run_dir.mkdir(parents=True)
    (candidate_run_dir / "candidate_theory_snapshot.yaml").write_text(
        "version: theory_v001\nsim_weights:\n  temporal: 0.2\n",
        encoding="utf-8",
    )
    (candidate_run_dir / "candidate_reply.yaml").write_text(
        "summary: accepted\nchanges:\n  - path: sim_weights.temporal\n    value: 0.2\n",
        encoding="utf-8",
    )
    (candidate_run_dir / "applied_changes.jsonl").write_text(
        json.dumps(
            {
                "path": "sim_weights.temporal",
                "baseline_value": 0.1,
                "proposed_value": 0.2,
                "numeric_delta": 0.1,
                "status": "applied",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (candidate_run_dir / "candidate_apply_manifest.json").write_text(
        json.dumps(
            {
                "candidate_id": "candidate_001",
                "comparison_id": "comparison_001",
                "packet_id": "packet_001",
                "candidate_dir": str(candidate_run_dir),
                "copied_reply_yaml": str(candidate_run_dir / "candidate_reply.yaml"),
                "candidate_theory_snapshot_path": str(candidate_run_dir / "candidate_theory_snapshot.yaml"),
                "output_batch_id": "batch_007",
                "output_batch_dir": str(tmp_path / "runs" / "batches" / "batch_007"),
                "source_primary_batch": {
                    "batch_id": "batch_005",
                    "batch_dir": str(primary_batch_dir),
                    "theory_config": "configs/theory_v001.yaml",
                },
                **(
                    {
                        "source_type": "cohort_study",
                        "source_study_id": "study_001",
                        "source_study_dir": "runs/cohort_studies/study_001",
                        "source_reference_batch_id": "batch_005",
                        "source_candidate_batch_id": "batch_010",
                        "source_candidate_decision": "shortlist",
                        "source_suggested_decision": "review",
                        "source_selected_metric": "ndcg_at_k",
                    }
                    if include_study_source
                    else {}
                ),
                "output_paths": {
                    "candidate_reply_yaml": str(candidate_run_dir / "candidate_reply.yaml"),
                    "candidate_theory_snapshot_yaml": str(candidate_run_dir / "candidate_theory_snapshot.yaml"),
                    "applied_changes_jsonl": str(candidate_run_dir / "applied_changes.jsonl"),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (candidate_run_dir / "batch_run_result.json").write_text(
        json.dumps(
            {
                "batch_id": "batch_007",
                "batch_dir": str(secondary_batch_dir),
                "status": "completed",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return candidate_run_dir


def _write_outcome_dir(
    tmp_path: Path,
    *,
    decision_status: str = "accept_candidate",
    include_study_source: bool = False,
) -> Path:
    outcome_dir = (
        tmp_path
        / "runs"
        / "comparisons"
        / "comparison_001"
        / "review_packets"
        / "packet_001"
        / "candidate_runs"
        / "candidate_001"
        / "outcomes"
        / "outcome_accept_001"
    )
    outcome_dir.mkdir(parents=True)
    (outcome_dir / "reeval_outcome_manifest.json").write_text(
        json.dumps(
            {
                "outcome_id": "outcome_accept_001",
                "candidate_id": "candidate_001",
                "candidate_run_dir": str(outcome_dir.parent.parent),
                "packet_id": "packet_001",
                "comparison_id": "comparison_001",
                "decision_status": decision_status,
                "selected_metric": "ndcg_at_k",
                "primary_batch": {
                    "batch_id": "batch_005",
                    "batch_dir": str(tmp_path / "runs" / "batches" / "batch_005"),
                    "theory_config": "configs/theory_v001.yaml",
                },
                "secondary_batch": {
                    "batch_id": "batch_007",
                    "batch_dir": str(tmp_path / "runs" / "batches" / "batch_007"),
                    "theory_config": "runs/candidate.yaml",
                },
                "common_doi_count": 5,
                "common_completed_seed_count": 4,
                "selected_metric_summary": {
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
                **(
                    {
                        "source_type": "cohort_study",
                        "source_study_id": "study_001",
                        "source_study_dir": "runs/cohort_studies/study_001",
                        "source_reference_batch_id": "batch_005",
                        "source_candidate_batch_id": "batch_010",
                        "source_candidate_decision": "shortlist",
                        "source_suggested_decision": "review",
                        "source_selected_metric": "ndcg_at_k",
                    }
                    if include_study_source
                    else {}
                ),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (outcome_dir / "reeval_decision_record.json").write_text(
        json.dumps(
            {
                "outcome_id": "outcome_accept_001",
                "candidate_id": "candidate_001",
                "packet_id": "packet_001",
                "comparison_id": "comparison_001",
                "decision_status": decision_status,
                "selected_metric": "ndcg_at_k",
                "primary_batch_id": "batch_005",
                "secondary_batch_id": "batch_007",
                "selected_metric_summary": {
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
                **(
                    {
                        "source_type": "cohort_study",
                        "source_study_id": "study_001",
                        "source_candidate_decision": "shortlist",
                        "source_selected_metric": "ndcg_at_k",
                    }
                    if include_study_source
                    else {}
                ),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return outcome_dir


def test_build_baseline_promotion_request_normalizes_fields(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    outcome_dir = _write_outcome_dir(tmp_path)

    request = build_baseline_promotion_request(
        candidate_run_dir=f"  {candidate_run_dir}  ",
        outcome_dir=f"  {outcome_dir}  ",
        baseline_id=" accepted_baseline_001 ",
        reviewer=" Alice ",
        notes=" Promote this baseline. ",
    )

    assert request.candidate_run_dir == candidate_run_dir
    assert request.outcome_dir == outcome_dir
    assert request.baseline_id == "accepted_baseline_001"
    assert request.reviewer == "Alice"
    assert request.notes == "Promote this baseline."


def test_promotion_compatibility_errors_detect_lineage_mismatch(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    outcome = outcome.__class__(
        outcome_dir=outcome.outcome_dir,
        manifest_path=outcome.manifest_path,
        decision_record_path=outcome.decision_record_path,
        manifest=outcome.manifest,
        decision_record=outcome.decision_record,
        outcome_id=outcome.outcome_id,
        decision_status=outcome.decision_status,
        comparison_id=outcome.comparison_id,
        packet_id=outcome.packet_id,
        candidate_id="candidate_other",
    )

    errors = promotion_compatibility_errors(candidate_run=candidate_run, outcome=outcome)

    assert errors
    assert "candidate_id" in errors[0]


def test_manifest_and_promotion_record_payloads_include_source_lineage(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="Alice",
        notes="Promote it.",
    )
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=outcome.manifest.get("selected_metric_summary"),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )

    manifest_payload = build_accepted_baseline_manifest_payload(
        request=request,
        baseline_dir=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001",
        created_at="2026-03-29T12:00:00Z",
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001" / "accepted_theory_snapshot.yaml",
        candidate_reply_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001" / "candidate_reply.yaml",
        applied_changes_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001" / "applied_changes.jsonl",
        guardrail_assessment=guardrail,
        output_paths={"accepted_baseline_manifest_json": "runs/accepted_baselines/accepted_baseline_001/accepted_baseline_manifest.json"},
    )
    record_payload = build_promotion_record_payload(
        request=request,
        created_at="2026-03-29T12:00:00Z",
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001" / "accepted_theory_snapshot.yaml",
        candidate_reply_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001" / "candidate_reply.yaml",
        guardrail_assessment=guardrail,
    )

    assert manifest_payload["source_lineage"]["candidate_id"] == "candidate_001"
    assert manifest_payload["decision_status"] == "accept_candidate"
    assert manifest_payload["outcome_summary"]["wins"] == 3
    assert manifest_payload["guardrail_verdict"] == "pass"
    assert record_payload["baseline_id"] == "accepted_baseline_001"
    assert record_payload["selected_metric"] == "ndcg_at_k"
    assert record_payload["guardrail_verdict"] == "pass"


def test_baseline_promotion_payloads_propagate_saved_study_source(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path, include_study_source=True))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path, include_study_source=True))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_002",
        reviewer="Alice",
        notes="Promote it.",
    )
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=outcome.manifest.get("selected_metric_summary"),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )

    manifest_payload = build_accepted_baseline_manifest_payload(
        request=request,
        baseline_dir=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002",
        created_at="2026-03-29T12:00:00Z",
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002" / "accepted_theory_snapshot.yaml",
        candidate_reply_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002" / "candidate_reply.yaml",
        applied_changes_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002" / "applied_changes.jsonl",
        guardrail_assessment=guardrail,
        output_paths={"accepted_baseline_manifest_json": "runs/accepted_baselines/accepted_baseline_002/accepted_baseline_manifest.json"},
    )
    record_payload = build_promotion_record_payload(
        request=request,
        created_at="2026-03-29T12:00:00Z",
        candidate_run=candidate_run,
        outcome=outcome,
        accepted_theory_snapshot_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002" / "accepted_theory_snapshot.yaml",
        candidate_reply_path=tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_002" / "candidate_reply.yaml",
        guardrail_assessment=guardrail,
    )

    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["source_candidate_decision"] == "shortlist"
    assert record_payload["source_study_id"] == "study_001"
    assert record_payload["source_selected_metric"] == "ndcg_at_k"


def test_save_accepted_baseline_artifacts_copies_required_files(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    outcome_dir = _write_outcome_dir(tmp_path)
    candidate_run = load_candidate_run_context(candidate_run_dir)
    outcome = load_outcome_context(outcome_dir)
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run_dir,
        outcome_dir=outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="Alice",
        notes="Promote after acceptance.",
    )
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=outcome.manifest.get("selected_metric_summary"),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )
    source_theory_text = (candidate_run_dir / "candidate_theory_snapshot.yaml").read_text(encoding="utf-8")
    source_reply_text = (candidate_run_dir / "candidate_reply.yaml").read_text(encoding="utf-8")
    source_changes_text = (candidate_run_dir / "applied_changes.jsonl").read_text(encoding="utf-8")

    result = save_accepted_baseline_artifacts(
        base_dir=tmp_path / "runs" / "accepted_baselines",
        request=request,
        candidate_run=candidate_run,
        outcome=outcome,
        guardrail_assessment=guardrail,
    )

    assert result.baseline_dir == tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001"
    assert result.manifest_path.exists()
    assert result.accepted_theory_snapshot_path.exists()
    assert result.candidate_reply_path.exists()
    assert result.applied_changes_path.exists()
    assert result.promotion_record_path.exists()
    assert result.accepted_theory_snapshot_path.read_text(encoding="utf-8") == source_theory_text
    assert result.candidate_reply_path.read_text(encoding="utf-8") == source_reply_text
    assert result.applied_changes_path.read_text(encoding="utf-8") == source_changes_text
    assert (candidate_run_dir / "candidate_theory_snapshot.yaml").read_text(encoding="utf-8") == source_theory_text


def test_save_accepted_baseline_artifacts_requires_accept_candidate(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path, decision_status="needs_review"))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=outcome.manifest.get("selected_metric_summary"),
        paired_seed_count=4,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="promotion_ready",
        promotion_ready=True,
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="promotion_ready",
        comparison_promotion_ready=True,
    )

    with pytest.raises(BaselinePromotionError):
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=guardrail,
        )


def test_save_accepted_baseline_artifacts_refuses_existing_baseline_dir(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=outcome.manifest.get("selected_metric_summary"),
        paired_seed_count=4,
    )
    (tmp_path / "runs" / "accepted_baselines" / "accepted_baseline_001").mkdir(parents=True)

    with pytest.raises(BaselinePromotionError):
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=guardrail,
        )


def test_save_accepted_baseline_artifacts_blocks_promotion_in_silver_mode(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(
        _write_candidate_run_dir(
            tmp_path,
            primary_evaluation_mode="silver_provenance_regression",
            secondary_evaluation_mode="silver_provenance_regression",
        )
    )
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    pass_guardrail = _promotion_ready_guardrail(outcome.manifest.get("selected_metric_summary"))

    with pytest.raises(BaselinePromotionError) as exc_info:
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=pass_guardrail,
        )

    assert "override" in str(exc_info.value).lower()


def test_save_accepted_baseline_artifacts_blocks_mismatched_benchmark_dataset_ids(
    tmp_path: Path,
) -> None:
    candidate_run = load_candidate_run_context(
        _write_candidate_run_dir(
            tmp_path,
            primary_benchmark_dataset_id="benchmark_dataset_A",
            secondary_benchmark_dataset_id="benchmark_dataset_B",
        )
    )
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    pass_guardrail = _promotion_ready_guardrail(outcome.manifest.get("selected_metric_summary"))

    with pytest.raises(BaselinePromotionError) as exc_info:
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=pass_guardrail,
        )

    assert "override" in str(exc_info.value).lower()


def test_save_accepted_baseline_artifacts_blocks_when_benchmark_is_not_promotion_ready(
    tmp_path: Path,
) -> None:
    candidate_run = load_candidate_run_context(
        _write_candidate_run_dir(
            tmp_path,
            primary_benchmark_maturity_tier="prototype",
            secondary_benchmark_maturity_tier="prototype",
            primary_promotion_ready=False,
            secondary_promotion_ready=False,
            primary_promotion_ineligibility_reasons=["Dataset is still prototype maturity."],
            secondary_promotion_ineligibility_reasons=["Dataset is still prototype maturity."],
        )
    )
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    pass_guardrail = _promotion_ready_guardrail(outcome.manifest.get("selected_metric_summary"))

    with pytest.raises(BaselinePromotionError) as exc_info:
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=pass_guardrail,
        )

    assert "override" in str(exc_info.value).lower()


def test_save_accepted_baseline_artifacts_blocks_when_maturity_fields_are_absent(
    tmp_path: Path,
) -> None:
    candidate_run = load_candidate_run_context(
        _write_candidate_run_dir(
            tmp_path,
            primary_benchmark_maturity_tier=None,
            secondary_benchmark_maturity_tier=None,
            primary_promotion_ready=None,
            secondary_promotion_ready=None,
        )
    )
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )
    pass_guardrail = _promotion_ready_guardrail(outcome.manifest.get("selected_metric_summary"))

    with pytest.raises(BaselinePromotionError) as exc_info:
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=pass_guardrail,
        )

    assert "override" in str(exc_info.value).lower()


def test_save_accepted_baseline_artifacts_requires_override_for_weak_guardrail(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    weak_guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary={
            "primary_mean": 0.7,
            "primary_median": 0.7,
            "secondary_mean": 0.7,
            "secondary_median": 0.7,
            "raw_delta_mean": 0.0,
            "raw_delta_median": 0.0,
            "improvement_delta_mean": 0.0,
            "improvement_delta_median": 0.0,
            "wins": 1,
            "losses": 1,
            "ties": 2,
        },
        paired_seed_count=4,
    )
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="",
        notes="",
    )

    with pytest.raises(BaselinePromotionError) as exc_info:
        save_accepted_baseline_artifacts(
            base_dir=tmp_path / "runs" / "accepted_baselines",
            request=request,
            candidate_run=candidate_run,
            outcome=outcome,
            guardrail_assessment=weak_guardrail,
        )

    assert "override" in str(exc_info.value).lower()


def test_save_accepted_baseline_artifacts_accepts_override_for_weak_guardrail(tmp_path: Path) -> None:
    candidate_run = load_candidate_run_context(_write_candidate_run_dir(tmp_path))
    outcome = load_outcome_context(_write_outcome_dir(tmp_path))
    weak_guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary={
            "primary_mean": 0.7,
            "primary_median": 0.7,
            "secondary_mean": 0.7,
            "secondary_median": 0.7,
            "raw_delta_mean": 0.0,
            "raw_delta_median": 0.0,
            "improvement_delta_mean": 0.0,
            "improvement_delta_median": 0.0,
            "wins": 1,
            "losses": 1,
            "ties": 2,
        },
        paired_seed_count=4,
    )
    request = build_baseline_promotion_request(
        candidate_run_dir=candidate_run.candidate_run_dir,
        outcome_dir=outcome.outcome_dir,
        baseline_id="accepted_baseline_001",
        reviewer="Alice",
        notes="Override after manual qualitative review.",
        override_used=True,
        override_reason="Manual case review supports promotion despite weak aggregate evidence.",
    )

    result = save_accepted_baseline_artifacts(
        base_dir=tmp_path / "runs" / "accepted_baselines",
        request=request,
        candidate_run=candidate_run,
        outcome=outcome,
        guardrail_assessment=weak_guardrail,
    )

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    promotion_payload = json.loads(result.promotion_record_path.read_text(encoding="utf-8"))
    assert manifest_payload["guardrail_verdict"] == "weak"
    assert manifest_payload["override_used"] is True
    assert promotion_payload["override_reason"] is not None
