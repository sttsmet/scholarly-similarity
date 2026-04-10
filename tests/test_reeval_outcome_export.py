from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ui.comparison import ComparisonMetricSummary, ComparisonMovementSummary
from src.ui.decision_guardrails import evaluate_decision_guardrails
from src.ui.reeval_outcome_export import (
    ReevalOutcomeExportError,
    build_reeval_decision_record_payload,
    build_reeval_outcome_manifest_payload,
    build_reeval_outcome_save_request,
    candidate_run_compatibility_errors,
    load_candidate_run_context,
    save_reeval_outcome_artifacts,
)


def _promotion_ready_guardrail() -> object:
    return evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary_with_movement(),
        paired_seed_count=3,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
        promotion_ineligibility_reasons=["Dataset is still prototype maturity."],
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="prototype",
        comparison_promotion_ready=False,
    )


def _summary() -> ComparisonMetricSummary:
    return ComparisonMetricSummary(
        primary_mean=0.7,
        primary_median=0.7,
        secondary_mean=0.8,
        secondary_median=0.8,
        raw_delta_mean=0.1,
        raw_delta_median=0.1,
        improvement_delta_mean=0.1,
        improvement_delta_median=0.1,
        wins=3,
        losses=1,
        ties=0,
    )


def _movement() -> ComparisonMovementSummary:
    return ComparisonMovementSummary(
        paired_seed_count=3,
        seeds_with_rank_movement=2,
        top_k_exact_match_rate=0.33,
        top_k_jaccard_at_k=1.0,
        top_k_common_items_permutation_change_count=4,
        judged_pair_rank_change_count=5,
        judged_pair_mean_abs_rank_change=0.4,
        judged_pair_max_abs_rank_change=2,
        cross_label_concordant_pair_count=6,
        cross_label_discordant_pair_count=0,
        cross_label_tied_pair_count=0,
        pairwise_label_order_accuracy=1.0,
        weighted_pairwise_label_order_accuracy=1.0,
        pairwise_label_order_accuracy_delta=0.0,
        weighted_pairwise_label_order_accuracy_delta=0.0,
        top_k_cross_label_concordant_pair_count=3,
        top_k_cross_label_discordant_pair_count=0,
        top_k_pairwise_label_order_accuracy=1.0,
        pair_order_reversal_count=2,
        cross_label_order_reversal_count=0,
        same_label_order_reversal_count=2,
        cross_label_top_k_swaps=0,
        movement_without_label_gain=True,
        headline_metric_flat_but_rank_moved=True,
        headline_flat_but_directional_gain=False,
        headline_flat_but_directional_loss=False,
        rank_moved_materially=True,
        directional_signal_strength="weak",
        movement_diagnostic_note="headline flat, same-label movement only",
    )


def _summary_with_movement() -> ComparisonMetricSummary:
    summary = _summary()
    return ComparisonMetricSummary(
        primary_mean=summary.primary_mean,
        primary_median=summary.primary_median,
        secondary_mean=summary.secondary_mean,
        secondary_median=summary.secondary_median,
        raw_delta_mean=summary.raw_delta_mean,
        raw_delta_median=summary.raw_delta_median,
        improvement_delta_mean=summary.improvement_delta_mean,
        improvement_delta_median=summary.improvement_delta_median,
        wins=summary.wins,
        losses=summary.losses,
        ties=summary.ties,
        movement_diagnostics=_movement(),
    )


def _paired_rows() -> list[dict[str, object]]:
    return [
        {
            "doi": "10.1000/a",
            "primary_status": "completed",
            "secondary_status": "completed",
            "metric_name": "ndcg_at_k",
            "primary_metric_value": 0.7,
            "secondary_metric_value": 0.8,
            "raw_delta": 0.1,
            "improvement_delta": 0.1,
            "primary_run_dir": str(Path("runs") / "primary" / "a"),
            "secondary_run_dir": str(Path("runs") / "secondary" / "a"),
            "primary_experiment_id": "batch_005",
            "secondary_experiment_id": "batch_007",
        }
    ]


def _bundle(
    batch_id: str,
    batch_dir: Path,
    theory_config: str | None = None,
    *,
    evaluation_mode: str | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    benchmark_maturity_tier: str | None = None,
    promotion_ready: bool | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        batch_dir=batch_dir,
        manifest=SimpleNamespace(
            batch_id=batch_id,
            theory_config=theory_config,
            options=SimpleNamespace(
                evaluation_mode=evaluation_mode,
                evidence_tier=evaluation_mode,
                metric_scope="local_corpus_ranking",
                benchmark_dataset_id=benchmark_dataset_id,
                benchmark_labels_sha256=benchmark_labels_sha256,
                benchmark_maturity_tier=benchmark_maturity_tier,
                promotion_ready=promotion_ready,
                promotion_ineligibility_reasons=[],
                benchmark_labels_snapshot_path="runs/batches/labels_snapshot.csv" if benchmark_dataset_id else None,
            ),
        ),
    )


def _write_candidate_run_dir(tmp_path: Path, *, include_study_source: bool = False) -> Path:
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
    (candidate_run_dir / "candidate_apply_manifest.json").write_text(
        json.dumps(
            {
                "candidate_id": "candidate_001",
                "comparison_id": "comparison_001",
                "packet_id": "packet_001",
                "candidate_dir": str(candidate_run_dir),
                "reply_yaml_path": str(candidate_run_dir / "source_reply.yaml"),
                "copied_reply_yaml": str(candidate_run_dir / "candidate_reply.yaml"),
                "candidate_theory_snapshot_path": str(candidate_run_dir / "candidate_theory_snapshot.yaml"),
                "output_batch_id": "batch_007",
                "output_batch_dir": str(tmp_path / "runs" / "batches" / "batch_007"),
                "source_primary_batch": {
                    "batch_id": "batch_005",
                    "batch_dir": str(tmp_path / "runs" / "batches" / "batch_005"),
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
                "batch_dir": str(tmp_path / "runs" / "batches" / "batch_007"),
                "status": "completed",
                "completed_seed_count": 3,
                "failed_seed_count": 1,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return candidate_run_dir


def test_build_reeval_outcome_save_request_normalizes_fields(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)

    request = build_reeval_outcome_save_request(
        candidate_run_dir=f"  {candidate_run_dir}  ",
        outcome_id=" outcome_001 ",
        reviewer=" Alice ",
        decision_status="needs_review",
        notes=" Keep looking. ",
        selected_metric=" ndcg_at_k ",
    )

    assert request.candidate_run_dir == candidate_run_dir
    assert request.outcome_id == "outcome_001"
    assert request.reviewer == "Alice"
    assert request.notes == "Keep looking."
    assert request.selected_metric == "ndcg_at_k"


def test_build_reeval_outcome_save_request_rejects_invalid_inputs(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    with pytest.raises(ReevalOutcomeExportError):
        build_reeval_outcome_save_request(
            candidate_run_dir=candidate_run_dir,
            outcome_id=" ",
            reviewer="",
            decision_status="needs_review",
            notes="",
            selected_metric="ndcg_at_k",
        )
    with pytest.raises(ReevalOutcomeExportError):
        build_reeval_outcome_save_request(
            candidate_run_dir=candidate_run_dir,
            outcome_id="outcome_001",
            reviewer="",
            decision_status="maybe",
            notes="",
            selected_metric="ndcg_at_k",
        )


def test_candidate_run_context_and_compatibility_checks(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    context = load_candidate_run_context(candidate_run_dir)
    primary_bundle = _bundle(
        "batch_005",
        tmp_path / "runs" / "batches" / "batch_005",
        "configs/theory_v001.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
    )
    secondary_bundle = _bundle(
        "batch_007",
        tmp_path / "runs" / "batches" / "batch_007",
        "runs/candidate.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
    )

    assert context.candidate_id == "candidate_001"
    assert context.packet_id == "packet_001"
    assert context.comparison_id == "comparison_001"
    assert candidate_run_compatibility_errors(
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
    ) == []

    secondary_bundle.manifest.batch_id = "batch_other"
    errors = candidate_run_compatibility_errors(
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
    )
    assert errors
    assert "secondary batch" in errors[0].lower()


def test_candidate_run_compatibility_errors_detect_incomplete_candidate_batch(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    batch_result_path = candidate_run_dir / "batch_run_result.json"
    batch_result_path.write_text(
        json.dumps(
            {
                "batch_id": "batch_007",
                "batch_dir": str(tmp_path / "runs" / "batches" / "batch_007"),
                "status": "failed",
                "completed_seed_count": 0,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    context = load_candidate_run_context(candidate_run_dir)

    errors = candidate_run_compatibility_errors(
        candidate_run=context,
        primary_bundle=_bundle("batch_005", tmp_path / "runs" / "batches" / "batch_005", "configs/theory_v001.yaml"),
        secondary_bundle=_bundle("batch_007", tmp_path / "runs" / "batches" / "batch_007", "runs/candidate.yaml"),
    )

    assert any("candidate_run_incomplete" in error for error in errors)


def test_manifest_and_decision_payloads_include_candidate_context(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    context = load_candidate_run_context(candidate_run_dir)
    request = build_reeval_outcome_save_request(
        candidate_run_dir=candidate_run_dir,
        outcome_id="outcome_001",
        reviewer="Alice",
        decision_status="accept_candidate",
        notes="Looks good.",
        selected_metric="ndcg_at_k",
    )
    primary_bundle = _bundle(
        "batch_005",
        tmp_path / "runs" / "batches" / "batch_005",
        "configs/theory_v001.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
    )
    secondary_bundle = _bundle(
        "batch_007",
        tmp_path / "runs" / "batches" / "batch_007",
        "runs/candidate.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
    )
    guardrail = _promotion_ready_guardrail()

    manifest_payload = build_reeval_outcome_manifest_payload(
        request=request,
        outcome_dir=candidate_run_dir / "outcomes" / "outcome_001",
        created_at="2026-03-29T12:00:00Z",
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary_with_movement(),
        guardrail_assessment=guardrail,
        output_paths={"reeval_outcome_manifest_json": "runs/comparisons/.../reeval_outcome_manifest.json"},
        paired_seed_count=3,
    )
    decision_payload = build_reeval_decision_record_payload(
        request=request,
        created_at="2026-03-29T12:00:00Z",
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary_with_movement(),
        guardrail_assessment=guardrail,
        paired_seed_count=3,
    )

    assert manifest_payload["candidate_id"] == "candidate_001"
    assert manifest_payload["packet_id"] == "packet_001"
    assert manifest_payload["selected_metric_summary"]["wins"] == 3
    assert manifest_payload["movement_diagnostic_note"] == "headline flat, same-label movement only"
    assert manifest_payload["movement_diagnostics"]["pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert manifest_payload["guardrail_verdict"] == "pass"
    assert manifest_payload["evaluation_mode"] == "independent_benchmark"
    assert manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert manifest_payload["benchmark_maturity_tier"] == "prototype"
    assert manifest_payload["promotion_ready"] is False
    assert manifest_payload["override_used"] is False
    assert manifest_payload["candidate_reply_yaml_path"].endswith("candidate_reply.yaml")
    assert decision_payload["outcome_id"] == "outcome_001"
    assert decision_payload["candidate_theory_snapshot_path"].endswith("candidate_theory_snapshot.yaml")
    assert decision_payload["movement_diagnostics"]["pair_order_reversal_count"] == 2
    assert decision_payload["movement_diagnostics"]["directional_signal_strength"] == "weak"
    assert decision_payload["guardrail_verdict"] == "pass"
    assert decision_payload["benchmark_labels_sha256"] == "labels_sha256_001"


def test_reeval_payloads_propagate_saved_candidate_run_study_source(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path, include_study_source=True)
    context = load_candidate_run_context(candidate_run_dir)
    request = build_reeval_outcome_save_request(
        candidate_run_dir=candidate_run_dir,
        outcome_id="outcome_002",
        reviewer="Alice",
        decision_status="accept_candidate",
        notes="Looks good.",
        selected_metric="ndcg_at_k",
    )
    primary_bundle = _bundle("batch_005", tmp_path / "runs" / "batches" / "batch_005", "configs/theory_v001.yaml")
    secondary_bundle = _bundle("batch_007", tmp_path / "runs" / "batches" / "batch_007", "runs/candidate.yaml")
    guardrail = _promotion_ready_guardrail()

    manifest_payload = build_reeval_outcome_manifest_payload(
        request=request,
        outcome_dir=candidate_run_dir / "outcomes" / "outcome_002",
        created_at="2026-03-29T12:00:00Z",
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary(),
        guardrail_assessment=guardrail,
        output_paths={"reeval_outcome_manifest_json": "runs/comparisons/.../reeval_outcome_manifest.json"},
        paired_seed_count=3,
    )
    decision_payload = build_reeval_decision_record_payload(
        request=request,
        created_at="2026-03-29T12:00:00Z",
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary(),
        guardrail_assessment=guardrail,
        paired_seed_count=3,
    )

    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["source_candidate_decision"] == "shortlist"
    assert decision_payload["source_study_id"] == "study_001"
    assert decision_payload["source_selected_metric"] == "ndcg_at_k"


def test_save_reeval_outcome_artifacts_writes_expected_files(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    context = load_candidate_run_context(candidate_run_dir)
    request = build_reeval_outcome_save_request(
        candidate_run_dir=candidate_run_dir,
        outcome_id="outcome_001",
        reviewer="Alice",
        decision_status="needs_review",
        notes="Check a few cases.",
        selected_metric="ndcg_at_k",
    )
    primary_bundle = _bundle("batch_005", tmp_path / "runs" / "batches" / "batch_005", "configs/theory_v001.yaml")
    secondary_bundle = _bundle("batch_007", tmp_path / "runs" / "batches" / "batch_007", "runs/candidate.yaml")
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary=_summary(),
        paired_seed_count=1,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
        promotion_ineligibility_reasons=["Dataset is still prototype maturity."],
        comparison_benchmark_dataset_id="benchmark_dataset_001",
        comparison_benchmark_labels_sha256="labels_sha256_001",
        comparison_benchmark_maturity_tier="prototype",
        comparison_promotion_ready=False,
    )

    result = save_reeval_outcome_artifacts(
        request=request,
        candidate_run=context,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        common_doi_count=2,
        common_completed_seed_count=2,
        paired_rows=_paired_rows(),
        summary=_summary(),
        guardrail_assessment=guardrail,
    )

    assert result.outcome_dir == candidate_run_dir / "outcomes" / "outcome_001"
    assert result.manifest_path.exists()
    assert result.paired_seed_table_path.exists()
    assert result.decision_record_path.exists()

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    decision_payload = json.loads(result.decision_record_path.read_text(encoding="utf-8"))
    paired_lines = result.paired_seed_table_path.read_text(encoding="utf-8").splitlines()

    assert manifest_payload["outcome_id"] == "outcome_001"
    assert manifest_payload["output_paths"]["reeval_paired_seed_table_jsonl"].endswith("reeval_paired_seed_table.jsonl")
    assert manifest_payload["guardrail_verdict"] == "weak"
    assert decision_payload["decision_status"] == "needs_review"
    assert decision_payload["guardrail_verdict"] == "weak"
    assert json.loads(paired_lines[0])["doi"] == "10.1000/a"


def test_save_reeval_outcome_artifacts_refuses_existing_directory(tmp_path: Path) -> None:
    candidate_run_dir = _write_candidate_run_dir(tmp_path)
    context = load_candidate_run_context(candidate_run_dir)
    request = build_reeval_outcome_save_request(
        candidate_run_dir=candidate_run_dir,
        outcome_id="outcome_001",
        reviewer="",
        decision_status="reject_candidate",
        notes="",
        selected_metric="ndcg_at_k",
    )
    (candidate_run_dir / "outcomes" / "outcome_001").mkdir(parents=True)
    primary_bundle = _bundle("batch_005", tmp_path / "runs" / "batches" / "batch_005", "configs/theory_v001.yaml")
    secondary_bundle = _bundle("batch_007", tmp_path / "runs" / "batches" / "batch_007", "runs/candidate.yaml")
    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=2,
        common_completed_seed_count=2,
        summary=_summary(),
        paired_seed_count=1,
    )

    with pytest.raises(ReevalOutcomeExportError):
        save_reeval_outcome_artifacts(
            request=request,
            candidate_run=context,
            primary_bundle=primary_bundle,
            secondary_bundle=secondary_bundle,
            common_doi_count=2,
            common_completed_seed_count=2,
            paired_rows=_paired_rows(),
            summary=_summary(),
            guardrail_assessment=guardrail,
        )
