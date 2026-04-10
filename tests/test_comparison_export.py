from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ui.comparison import ComparisonMetricSummary, ComparisonMovementSummary
from src.ui.comparison_export import (
    ComparisonExportError,
    build_comparison_manifest_payload,
    build_comparison_save_request,
    build_decision_record_payload,
    save_comparison_artifacts,
    serialize_paired_seed_rows,
)


def _bundle(
    batch_id: str,
    batch_dir: str,
    theory_config: str | None = None,
    *,
    evaluation_mode: str | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    benchmark_maturity_tier: str | None = None,
    promotion_ready: bool | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        batch_dir=Path(batch_dir),
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
        wins=2,
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
            "primary_ndcg_at_k": 0.7,
            "secondary_ndcg_at_k": 0.8,
            "primary_metric_value": 0.7,
            "secondary_metric_value": 0.8,
            "raw_delta": 0.1,
            "improvement_delta": 0.1,
            "primary_run_dir": str(Path("runs") / "primary" / "a"),
            "secondary_run_dir": str(Path("runs") / "secondary" / "a"),
            "primary_experiment_id": "batch_005",
            "secondary_experiment_id": "batch_006",
        }
    ]


def _study_source_context() -> dict[str, object]:
    return {
        "source_type": "cohort_study",
        "source_study_id": "study_001",
        "source_study_dir": str(Path("runs") / "cohort_studies" / "study_001"),
        "source_reference_batch_id": "batch_005",
        "source_candidate_batch_id": "batch_006",
        "source_candidate_decision": "shortlist",
        "source_suggested_decision": "review",
        "source_selected_metric": "ndcg_at_k",
        "source_context_active": True,
    }


def test_build_comparison_save_request_normalizes_optional_fields() -> None:
    request = build_comparison_save_request(
        comparison_id=" comparison_001 ",
        reviewer="  Alice  ",
        decision_status="needs_review",
        notes="  Needs follow-up.  ",
    )

    assert request.comparison_id == "comparison_001"
    assert request.reviewer == "Alice"
    assert request.decision_status == "needs_review"
    assert request.notes == "Needs follow-up."


def test_build_comparison_save_request_rejects_invalid_inputs() -> None:
    with pytest.raises(ComparisonExportError):
        build_comparison_save_request(
            comparison_id=" ",
            reviewer="",
            decision_status="needs_review",
            notes="",
        )
    with pytest.raises(ComparisonExportError):
        build_comparison_save_request(
            comparison_id="nested/path",
            reviewer="",
            decision_status="needs_review",
            notes="",
        )
    with pytest.raises(ComparisonExportError):
        build_comparison_save_request(
            comparison_id="comparison_001",
            reviewer="",
            decision_status="maybe",
            notes="",
        )


def test_manifest_and_decision_payloads_include_selected_metric_summary() -> None:
    primary_bundle = _bundle(
        "batch_005",
        "runs/batches/batch_005",
        "runs/theory_a.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
    )
    secondary_bundle = _bundle(
        "batch_006",
        "runs/batches/batch_006",
        None,
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
    )

    manifest_payload = build_comparison_manifest_payload(
        comparison_id="comparison_001",
        comparison_dir=Path("runs") / "comparisons" / "comparison_001",
        created_at="2026-03-29T12:00:00Z",
        reviewer="Alice",
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric="ndcg_at_k",
        status_mode="common completed only",
        common_doi_count=5,
        common_completed_seed_count=4,
        compatibility_warning_list=["Seeds CSV differs between primary and secondary batches."],
        summary=_summary_with_movement(),
        output_paths={"comparison_manifest_json": "runs/comparisons/comparison_001/comparison_manifest.json"},
        paired_seed_count=3,
        study_source_context=_study_source_context(),
    )
    decision_payload = build_decision_record_payload(
        comparison_id="comparison_001",
        created_at="2026-03-29T12:00:00Z",
        reviewer=None,
        decision_status="needs_review",
        notes=None,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric="ndcg_at_k",
        status_mode="common completed only",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary_with_movement(),
        paired_seed_count=3,
        study_source_context=_study_source_context(),
    )

    assert manifest_payload["primary_batch"]["batch_id"] == "batch_005"
    assert manifest_payload["secondary_batch"]["theory_config"] is None
    assert manifest_payload["selected_metric_summary"]["improvement_delta_mean"] == pytest.approx(0.1)
    assert manifest_payload["paired_seed_count"] == 3
    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["evaluation_mode"] == "independent_benchmark"
    assert manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert manifest_payload["benchmark_maturity_tier"] == "prototype"
    assert manifest_payload["promotion_ready"] is False
    assert manifest_payload["comparison_eligible"] is True
    assert manifest_payload["movement_diagnostic_note"] == "headline flat, same-label movement only"
    assert manifest_payload["selected_metric_summary"]["movement_diagnostics"]["pair_order_reversal_count"] == 2
    assert manifest_payload["selected_metric_summary"]["movement_diagnostics"]["pairwise_label_order_accuracy_delta"] == pytest.approx(0.0)

    assert decision_payload["decision_status"] == "needs_review"
    assert decision_payload["selected_metric_summary"]["wins"] == 2
    assert decision_payload["movement_diagnostics"]["rank_moved_materially"] is True
    assert decision_payload["movement_diagnostics"]["directional_signal_strength"] == "weak"
    assert decision_payload["secondary_theory_config"] is None
    assert decision_payload["source_candidate_decision"] == "shortlist"
    assert decision_payload["selected_comparison_metric_category"] == "headline_ranking"
    assert decision_payload["benchmark_maturity_tier"] == "prototype"
    assert decision_payload["promotion_ready"] is False


def test_serialize_paired_seed_rows_preserves_rows_and_normalizes_paths() -> None:
    serialized_rows = serialize_paired_seed_rows(_paired_rows())

    assert serialized_rows[0]["doi"] == "10.1000/a"
    assert serialized_rows[0]["primary_run_dir"] == str(Path("runs") / "primary" / "a")
    assert serialized_rows[0]["improvement_delta"] == pytest.approx(0.1)


def test_save_comparison_artifacts_writes_expected_files(tmp_path: Path) -> None:
    request = build_comparison_save_request(
        comparison_id="comparison_001",
        reviewer="Alice",
        decision_status="accept_candidate",
        notes="Looks good.",
    )
    primary_bundle = _bundle("batch_005", "runs/batches/batch_005", "runs/theory_a.yaml")
    secondary_bundle = _bundle("batch_006", "runs/batches/batch_006", "runs/theory_b.yaml")

    result = save_comparison_artifacts(
        base_dir=tmp_path,
        request=request,
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric="ndcg_at_k",
        status_mode="common completed only",
        common_doi_count=2,
        common_completed_seed_count=2,
        compatibility_warning_list=[],
        paired_rows=_paired_rows(),
        summary=_summary(),
        study_source_context=_study_source_context(),
    )

    assert result.comparison_dir == tmp_path / "comparison_001"
    assert result.manifest_path.exists()
    assert result.paired_seed_table_path.exists()
    assert result.decision_record_path.exists()

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    decision_payload = json.loads(result.decision_record_path.read_text(encoding="utf-8"))
    paired_lines = result.paired_seed_table_path.read_text(encoding="utf-8").splitlines()

    assert manifest_payload["comparison_id"] == "comparison_001"
    assert manifest_payload["output_paths"]["paired_seed_table_jsonl"].endswith("paired_seed_table.jsonl")
    assert manifest_payload["source_study_id"] == "study_001"
    assert decision_payload["decision_status"] == "accept_candidate"
    assert decision_payload["source_context_active"] is True
    assert json.loads(paired_lines[0])["doi"] == "10.1000/a"


def test_save_comparison_artifacts_refuses_existing_directory(tmp_path: Path) -> None:
    request = build_comparison_save_request(
        comparison_id="comparison_001",
        reviewer="",
        decision_status="reject_candidate",
        notes="",
    )
    comparison_dir = tmp_path / "comparison_001"
    comparison_dir.mkdir(parents=True)

    with pytest.raises(ComparisonExportError):
        save_comparison_artifacts(
            base_dir=tmp_path,
            request=request,
            primary_bundle=_bundle("batch_005", "runs/batches/batch_005"),
            secondary_bundle=_bundle("batch_006", "runs/batches/batch_006"),
            selected_metric="ndcg_at_k",
            status_mode="common completed only",
            common_doi_count=2,
            common_completed_seed_count=2,
            compatibility_warning_list=[],
            paired_rows=_paired_rows(),
            summary=_summary(),
        )
