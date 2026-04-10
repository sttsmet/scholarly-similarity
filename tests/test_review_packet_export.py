from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ui.comparison import ComparisonMetricSummary, ComparisonMovementSummary
from src.ui.review_packet_export import (
    ReviewPacketExportError,
    build_candidate_reply_template_text,
    build_evidence_summary_payload,
    build_review_packet_export_request,
    build_review_packet_manifest_payload,
    flatten_allowed_scalar_paths,
    save_review_packet_artifacts,
    select_top_packet_rows,
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
        primary_mean=0.6,
        primary_median=0.6,
        secondary_mean=0.7,
        secondary_median=0.7,
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
        paired_seed_count=4,
        seeds_with_rank_movement=3,
        top_k_exact_match_rate=0.25,
        top_k_jaccard_at_k=0.95,
        top_k_common_items_permutation_change_count=6,
        judged_pair_rank_change_count=8,
        judged_pair_mean_abs_rank_change=0.5,
        judged_pair_max_abs_rank_change=2,
        cross_label_concordant_pair_count=5,
        cross_label_discordant_pair_count=4,
        cross_label_tied_pair_count=1,
        pairwise_label_order_accuracy=0.555556,
        weighted_pairwise_label_order_accuracy=0.58,
        pairwise_label_order_accuracy_delta=-0.02,
        weighted_pairwise_label_order_accuracy_delta=-0.04,
        top_k_cross_label_concordant_pair_count=2,
        top_k_cross_label_discordant_pair_count=1,
        top_k_pairwise_label_order_accuracy=0.666667,
        pair_order_reversal_count=3,
        cross_label_order_reversal_count=1,
        same_label_order_reversal_count=2,
        cross_label_top_k_swaps=1,
        movement_without_label_gain=False,
        headline_metric_flat_but_rank_moved=False,
        headline_flat_but_directional_gain=False,
        headline_flat_but_directional_loss=False,
        rank_moved_materially=True,
        directional_signal_strength="moderate",
        movement_diagnostic_note="cross-label movement detected",
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
            "primary_metric_value": 0.9,
            "secondary_metric_value": 0.5,
            "raw_delta": -0.4,
            "improvement_delta": -0.4,
            "primary_run_dir": str(Path("runs") / "primary" / "a"),
            "primary_experiment_id": "batch_005",
            "secondary_run_dir": str(Path("runs") / "secondary" / "a"),
            "secondary_experiment_id": "batch_006",
        },
        {
            "doi": "10.1000/b",
            "primary_metric_value": 0.7,
            "secondary_metric_value": 0.8,
            "raw_delta": 0.1,
            "improvement_delta": 0.1,
            "primary_run_dir": str(Path("runs") / "primary" / "b"),
            "primary_experiment_id": "batch_005",
            "secondary_run_dir": str(Path("runs") / "secondary" / "b"),
            "secondary_experiment_id": "batch_006",
        },
        {
            "doi": "10.1000/c",
            "primary_metric_value": 0.5,
            "secondary_metric_value": 0.9,
            "raw_delta": 0.4,
            "improvement_delta": 0.4,
            "primary_run_dir": str(Path("runs") / "primary" / "c"),
            "primary_experiment_id": "batch_005",
            "secondary_run_dir": str(Path("runs") / "secondary" / "c"),
            "secondary_experiment_id": "batch_006",
        },
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
    }


def test_build_review_packet_export_request_validates_inputs() -> None:
    request = build_review_packet_export_request(
        packet_id=" packet_001 ",
        comparison_id=" comparison_001 ",
        reviewer=" Alice ",
        selected_metric="ndcg_at_k",
        max_regressions=10,
        max_improvements=5,
    )

    assert request.packet_id == "packet_001"
    assert request.comparison_id == "comparison_001"
    assert request.reviewer == "Alice"

    with pytest.raises(ReviewPacketExportError):
        build_review_packet_export_request(
            packet_id="",
            comparison_id="comparison_001",
            reviewer="",
            selected_metric="ndcg_at_k",
            max_regressions=10,
            max_improvements=5,
        )
    with pytest.raises(ReviewPacketExportError):
        build_review_packet_export_request(
            packet_id="packet_001",
            comparison_id="nested/path",
            reviewer="",
            selected_metric="ndcg_at_k",
            max_regressions=10,
            max_improvements=5,
        )
    with pytest.raises(ReviewPacketExportError):
        build_review_packet_export_request(
            packet_id="packet_001",
            comparison_id="comparison_001",
            reviewer="",
            selected_metric="",
            max_regressions=10,
            max_improvements=5,
        )


def test_flatten_allowed_scalar_paths_excludes_non_scalars() -> None:
    theory_payload = {
        "sim_weights": {
            "lineage": 1.0,
            "nested": {
                "detail": 0.5,
                "list_value": [1, 2, 3],
            },
        },
        "confidence_factors": {
            "citation_count": 0.2,
        },
        "sim_parameters": {
            "min_overlap": 2,
            "options": {"enabled": True},
        },
        "explanation": {
            "style": "compact",
            "sections": ["why", "how"],
        },
        "other_group": {
            "ignored": 123,
        },
    }

    flattened = flatten_allowed_scalar_paths(theory_payload)

    assert flattened == [
        "confidence_factors.citation_count",
        "explanation.style",
        "sim_parameters.min_overlap",
        "sim_parameters.options.enabled",
        "sim_weights.lineage",
        "sim_weights.nested.detail",
    ]


def test_select_top_packet_rows_returns_regressions_and_improvements() -> None:
    regressions, improvements = select_top_packet_rows(
        _paired_rows(),
        max_regressions=2,
        max_improvements=2,
    )

    assert [row["doi"] for row in regressions] == ["10.1000/a", "10.1000/b"]
    assert [row["doi"] for row in improvements] == ["10.1000/c", "10.1000/b"]


def test_build_packet_manifest_and_evidence_payloads_include_summary_and_counts() -> None:
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
        "runs/theory_b.yaml",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
        benchmark_labels_sha256="labels_sha256_001",
        benchmark_maturity_tier="prototype",
        promotion_ready=False,
    )
    regressions = _paired_rows()[:1]
    improvements = _paired_rows()[1:]

    manifest_payload = build_review_packet_manifest_payload(
        packet_id="packet_001",
        packet_dir=Path("runs") / "comparisons" / "comparison_001" / "review_packets" / "packet_001",
        created_at="2026-03-29T12:00:00Z",
        comparison_id="comparison_001",
        reviewer="Alice",
        primary_bundle=primary_bundle,
        secondary_bundle=secondary_bundle,
        selected_metric="ndcg_at_k",
        common_doi_count=5,
        common_completed_seed_count=4,
        summary=_summary_with_movement(),
        output_paths={"review_packet_manifest_json": "runs/comparisons/comparison_001/review_packets/packet_001/review_packet_manifest.json"},
        source_comparison_paths={"comparison_dir": "runs/comparisons/comparison_001", "comparison_manifest_json": None, "decision_record_json": None},
        study_source_context=_study_source_context(),
    )
    evidence_payload = build_evidence_summary_payload(
        selected_metric="ndcg_at_k",
        compatibility_warning_list=["Seeds CSV differs."],
        summary=_summary_with_movement(),
        regressions=regressions,
        improvements=improvements,
        study_source_context=_study_source_context(),
    )

    assert manifest_payload["packet_id"] == "packet_001"
    assert manifest_payload["wins"] == 3
    assert manifest_payload["source_comparison_paths"]["comparison_dir"] == "runs/comparisons/comparison_001"
    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["evaluation_mode"] == "independent_benchmark"
    assert manifest_payload["benchmark_maturity_tier"] == "prototype"
    assert manifest_payload["promotion_ready"] is False
    assert manifest_payload["promotion_eligible"] is False
    assert manifest_payload["movement_diagnostics"]["cross_label_order_reversal_count"] == 1
    assert evidence_payload["best_regressions_count_included"] == 1
    assert evidence_payload["movement_diagnostic_note"] == "cross-label movement detected"
    assert evidence_payload["movement_diagnostics"]["pairwise_label_order_accuracy_delta"] == pytest.approx(-0.02)
    assert evidence_payload["movement_diagnostics"]["directional_signal_strength"] == "moderate"
    assert evidence_payload["selected_metric_summary"]["movement_diagnostics"]["cross_label_top_k_swaps"] == 1
    assert evidence_payload["top_improvement_dois"] == ["10.1000/b", "10.1000/c"]
    assert evidence_payload["study_source"]["source_candidate_decision"] == "shortlist"


def test_candidate_reply_template_is_clearly_marked_template_only() -> None:
    template_text = build_candidate_reply_template_text(
        packet_id="packet_001",
        comparison_id="comparison_001",
        baseline_theory_config="baseline_theory_snapshot.yaml",
    )

    assert "TEMPLATE ONLY - not an actual generator reply" in template_text
    assert "proposed_changes: []" in template_text


def test_save_review_packet_artifacts_writes_expected_files(tmp_path: Path) -> None:
    theory_config_path = tmp_path / "theory_snapshot.yaml"
    theory_config_path.write_text(
        "\n".join(
            [
                "sim_weights:",
                "  lineage: 1.0",
                "confidence_factors:",
                "  citation_count: 0.2",
                "sim_parameters:",
                "  min_overlap: 2",
                "explanation:",
                "  style: compact",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    comparison_dir = tmp_path / "comparison_001"
    comparison_dir.mkdir(parents=True)
    (comparison_dir / "comparison_manifest.json").write_text(
        json.dumps(
            {
                "source_type": "cohort_study",
                "source_study_id": "study_001",
                "source_study_dir": "runs/cohort_studies/study_001",
                "source_reference_batch_id": "batch_005",
                "source_candidate_batch_id": "batch_006",
            }
        ),
        encoding="utf-8",
    )
    (comparison_dir / "decision_record.json").write_text(
        json.dumps(
            {
                "source_type": "cohort_study",
                "source_candidate_decision": "shortlist",
                "source_suggested_decision": "review",
                "source_selected_metric": "ndcg_at_k",
                "source_context_active": True,
            }
        ),
        encoding="utf-8",
    )

    request = build_review_packet_export_request(
        packet_id="packet_001",
        comparison_id="comparison_001",
        reviewer="Alice",
        selected_metric="ndcg_at_k",
        max_regressions=2,
        max_improvements=2,
    )
    result = save_review_packet_artifacts(
        base_dir=tmp_path,
        request=request,
        primary_bundle=_bundle("batch_005", "runs/batches/batch_005", str(theory_config_path)),
        secondary_bundle=_bundle("batch_006", "runs/batches/batch_006", "runs/theory_b.yaml"),
        compatibility_warning_list=["Seeds CSV differs."],
        common_doi_count=3,
        common_completed_seed_count=3,
        paired_rows=_paired_rows(),
        summary=_summary(),
    )

    assert result.packet_dir == comparison_dir / "review_packets" / "packet_001"
    assert result.manifest_path.exists()
    assert result.evidence_summary_path.exists()
    assert result.regressions_path.exists()
    assert result.improvements_path.exists()
    assert result.allowed_revision_paths_path.exists()
    assert result.baseline_snapshot_path.exists()
    assert result.candidate_template_path.exists()

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    evidence_payload = json.loads(result.evidence_summary_path.read_text(encoding="utf-8"))
    allowed_paths_payload = json.loads(result.allowed_revision_paths_path.read_text(encoding="utf-8"))
    template_text = result.candidate_template_path.read_text(encoding="utf-8")

    assert manifest_payload["comparison_id"] == "comparison_001"
    assert manifest_payload["source_comparison_paths"]["comparison_manifest_json"] is not None
    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["source_candidate_decision"] == "shortlist"
    assert evidence_payload["top_regression_dois"][0] == "10.1000/a"
    assert evidence_payload["study_source"]["source_selected_metric"] == "ndcg_at_k"
    assert allowed_paths_payload["allowed_scalar_paths"] == [
        "confidence_factors.citation_count",
        "explanation.style",
        "sim_parameters.min_overlap",
        "sim_weights.lineage",
    ]
    assert "TEMPLATE ONLY - not an actual generator reply" in template_text


def test_save_review_packet_artifacts_noops_when_saved_comparison_has_no_study_source(tmp_path: Path) -> None:
    theory_config_path = tmp_path / "theory_snapshot.yaml"
    theory_config_path.write_text("sim_weights:\n  lineage: 1.0\n", encoding="utf-8")
    comparison_dir = tmp_path / "comparison_001"
    comparison_dir.mkdir(parents=True)
    (comparison_dir / "comparison_manifest.json").write_text("{}", encoding="utf-8")
    (comparison_dir / "decision_record.json").write_text("{}", encoding="utf-8")

    request = build_review_packet_export_request(
        packet_id="packet_001",
        comparison_id="comparison_001",
        reviewer="Alice",
        selected_metric="ndcg_at_k",
        max_regressions=1,
        max_improvements=1,
    )
    result = save_review_packet_artifacts(
        base_dir=tmp_path,
        request=request,
        primary_bundle=_bundle("batch_005", "runs/batches/batch_005", str(theory_config_path)),
        secondary_bundle=_bundle("batch_006", "runs/batches/batch_006", "runs/theory_b.yaml"),
        compatibility_warning_list=[],
        common_doi_count=1,
        common_completed_seed_count=1,
        paired_rows=_paired_rows()[:1],
        summary=_summary(),
    )

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    evidence_payload = json.loads(result.evidence_summary_path.read_text(encoding="utf-8"))

    assert "source_study_id" not in manifest_payload
    assert "study_source" not in evidence_payload


def test_save_review_packet_artifacts_refuses_overwrite(tmp_path: Path) -> None:
    theory_config_path = tmp_path / "theory_snapshot.yaml"
    theory_config_path.write_text("sim_weights:\n  lineage: 1.0\n", encoding="utf-8")
    packet_dir = tmp_path / "comparison_001" / "review_packets" / "packet_001"
    packet_dir.mkdir(parents=True)
    request = build_review_packet_export_request(
        packet_id="packet_001",
        comparison_id="comparison_001",
        reviewer="",
        selected_metric="ndcg_at_k",
        max_regressions=1,
        max_improvements=1,
    )

    with pytest.raises(ReviewPacketExportError):
        save_review_packet_artifacts(
            base_dir=tmp_path,
            request=request,
            primary_bundle=_bundle("batch_005", "runs/batches/batch_005", str(theory_config_path)),
            secondary_bundle=_bundle("batch_006", "runs/batches/batch_006", "runs/theory_b.yaml"),
            compatibility_warning_list=[],
            common_doi_count=1,
            common_completed_seed_count=1,
            paired_rows=_paired_rows()[:1],
            summary=_summary(),
        )


def test_save_review_packet_artifacts_rejects_placeholder_candidate_evidence(tmp_path: Path) -> None:
    theory_config_path = tmp_path / "theory_snapshot.yaml"
    theory_config_path.write_text("sim_weights:\n  lineage: 1.0\n", encoding="utf-8")
    request = build_review_packet_export_request(
        packet_id="packet_002",
        comparison_id="comparison_001",
        reviewer="Alice",
        selected_metric="ndcg_at_k",
        max_regressions=1,
        max_improvements=1,
    )
    placeholder_rows = [
        {
            **_paired_rows()[0],
            "secondary_experiment_id": "batch_005",
        }
    ]

    with pytest.raises(ReviewPacketExportError) as exc_info:
        save_review_packet_artifacts(
            base_dir=tmp_path,
            request=request,
            primary_bundle=_bundle("batch_005", "runs/batches/batch_005", str(theory_config_path)),
            secondary_bundle=_bundle("batch_007", "runs/batches/batch_007", "runs/theory_b.yaml"),
            compatibility_warning_list=[],
            common_doi_count=1,
            common_completed_seed_count=1,
            paired_rows=placeholder_rows,
            summary=_summary(),
        )

    assert "candidate_run_incomplete" in str(exc_info.value)
