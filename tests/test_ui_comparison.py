from __future__ import annotations

import json
from types import SimpleNamespace
from pathlib import Path

import pytest

from src.ui.comparison import (
    align_common_seed_rows,
    choose_default_comparison_metric,
    common_completed_seed_count,
    common_diagnostic_metrics,
    common_numeric_metrics,
    comparison_metric_summary,
    compatibility_warnings,
    metric_overlap_counts,
    paired_metric_rows,
    wins_losses_ties,
)


def _primary_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 1,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": 0.8,
            "precision_at_k": 0.7,
            "brier_score": 0.20,
            "run_dir": "runs/primary/a",
            "experiment_id": "primary_batch",
        },
        {
            "batch_index": 2,
            "doi": "10.1000/b",
            "status": "failed",
            "ndcg_at_k": None,
            "precision_at_k": None,
            "brier_score": 0.40,
            "run_dir": "runs/primary/b",
            "experiment_id": "primary_batch",
        },
        {
            "batch_index": 3,
            "doi": "10.1000/c",
            "status": "completed",
            "ndcg_at_k": 0.5,
            "precision_at_k": 0.4,
            "run_dir": "runs/primary/c",
            "experiment_id": "primary_batch",
        },
    ]


def _secondary_rows() -> list[dict[str, object]]:
    return [
        {
            "batch_index": 10,
            "doi": "10.1000/b",
            "status": "completed",
            "ndcg_at_k": 0.9,
            "precision_at_k": 0.8,
            "brier_score": 0.30,
            "run_dir": "runs/secondary/b",
            "experiment_id": "secondary_batch",
        },
        {
            "batch_index": 11,
            "doi": "10.1000/a",
            "status": "completed",
            "ndcg_at_k": 0.6,
            "precision_at_k": 0.75,
            "brier_score": 0.10,
            "run_dir": "runs/secondary/a",
            "experiment_id": "secondary_batch",
        },
        {
            "batch_index": 12,
            "doi": "10.1000/d",
            "status": "completed",
            "ndcg_at_k": 0.7,
            "precision_at_k": 0.5,
            "run_dir": "runs/secondary/d",
            "experiment_id": "secondary_batch",
        },
        {
            "batch_index": 13,
            "doi": "10.1000/c",
            "status": "completed",
            "ndcg_at_k": 0.5,
            "precision_at_k": 0.3,
            "run_dir": "runs/secondary/c",
            "experiment_id": "secondary_batch",
        },
    ]


def _write_experiment(
    tmp_path: Path,
    *,
    run_name: str,
    experiment_id: str,
    scored_rows: list[dict[str, object]],
    judged_rows: list[dict[str, object]],
    ndcg_at_k: float,
    precision_at_k: float = 1.0,
    recall_at_k: float = 1.0,
    top_k_used: int = 3,
) -> dict[str, object]:
    run_dir = tmp_path / "runs" / run_name
    experiment_dir = run_dir / "experiments" / experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=True)

    scored_path = experiment_dir / "scored_candidates.jsonl"
    judged_path = experiment_dir / "judged_candidates.jsonl"
    summary_path = experiment_dir / "evaluation_summary.json"

    scored_path.write_text(
        "".join(json.dumps(row) + "\n" for row in scored_rows),
        encoding="utf-8",
    )
    judged_path.write_text(
        "".join(json.dumps(row) + "\n" for row in judged_rows),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "evaluation_mode": "silver_provenance_regression",
                "evidence_tier": "silver_provenance_regression",
                "metric_scope": "local_corpus_ranking",
                "top_k_used": top_k_used,
                "metrics": {
                    "ndcg_at_k": ndcg_at_k,
                    "precision_at_k": precision_at_k,
                    "recall_at_k": recall_at_k,
                },
                "output_paths": {
                    "judged_candidates_jsonl": str(judged_path),
                    "mode_judged_candidates_jsonl": str(judged_path),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    return {
        "batch_index": 1,
        "doi": "10.1000/movement",
        "status": "completed",
        "ndcg_at_k": ndcg_at_k,
        "precision_at_k": precision_at_k,
        "recall_at_k": recall_at_k,
        "run_dir": str(run_dir),
        "experiment_id": experiment_id,
        "evaluation_summary_json": str(summary_path),
    }


def _movement_rows_same_label(tmp_path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    baseline_scored = [
        {"openalex_id": "A", "rank": 1, "sim": 0.90},
        {"openalex_id": "B", "rank": 2, "sim": 0.80},
        {"openalex_id": "C", "rank": 3, "sim": 0.60},
        {"openalex_id": "D", "rank": 4, "sim": 0.20},
    ]
    candidate_scored = [
        {"openalex_id": "B", "rank": 1, "sim": 0.95},
        {"openalex_id": "A", "rank": 2, "sim": 0.85},
        {"openalex_id": "C", "rank": 3, "sim": 0.55},
        {"openalex_id": "D", "rank": 4, "sim": 0.10},
    ]
    baseline_judged = [
        {"candidate_openalex_id": "A", "rank": 1, "label": 2},
        {"candidate_openalex_id": "B", "rank": 2, "label": 2},
        {"candidate_openalex_id": "C", "rank": 3, "label": 1},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    candidate_judged = [
        {"candidate_openalex_id": "B", "rank": 1, "label": 2},
        {"candidate_openalex_id": "A", "rank": 2, "label": 2},
        {"candidate_openalex_id": "C", "rank": 3, "label": 1},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    return (
        [
            _write_experiment(
                tmp_path,
                run_name="primary_same_label",
                experiment_id="baseline",
                scored_rows=baseline_scored,
                judged_rows=baseline_judged,
                ndcg_at_k=1.0,
            )
        ],
        [
            _write_experiment(
                tmp_path,
                run_name="secondary_same_label",
                experiment_id="candidate",
                scored_rows=candidate_scored,
                judged_rows=candidate_judged,
                ndcg_at_k=1.0,
            )
        ],
    )


def _movement_rows_cross_label(tmp_path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    baseline_scored = [
        {"openalex_id": "A", "rank": 1, "sim": 0.90},
        {"openalex_id": "B", "rank": 2, "sim": 0.80},
        {"openalex_id": "C", "rank": 3, "sim": 0.60},
        {"openalex_id": "D", "rank": 4, "sim": 0.20},
    ]
    candidate_scored = [
        {"openalex_id": "A", "rank": 1, "sim": 0.91},
        {"openalex_id": "C", "rank": 2, "sim": 0.82},
        {"openalex_id": "B", "rank": 3, "sim": 0.81},
        {"openalex_id": "D", "rank": 4, "sim": 0.10},
    ]
    baseline_judged = [
        {"candidate_openalex_id": "A", "rank": 1, "label": 2},
        {"candidate_openalex_id": "B", "rank": 2, "label": 2},
        {"candidate_openalex_id": "C", "rank": 3, "label": 1},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    candidate_judged = [
        {"candidate_openalex_id": "A", "rank": 1, "label": 2},
        {"candidate_openalex_id": "C", "rank": 2, "label": 1},
        {"candidate_openalex_id": "B", "rank": 3, "label": 2},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    return (
        [
            _write_experiment(
                tmp_path,
                run_name="primary_cross_label",
                experiment_id="baseline",
                scored_rows=baseline_scored,
                judged_rows=baseline_judged,
                ndcg_at_k=1.0,
            )
        ],
        [
            _write_experiment(
                tmp_path,
                run_name="secondary_cross_label",
                experiment_id="candidate",
                scored_rows=candidate_scored,
                judged_rows=candidate_judged,
                ndcg_at_k=0.95,
            )
        ],
    )


def _movement_rows_directional_gain(
    tmp_path: Path,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    baseline_scored = [
        {"openalex_id": "A", "rank": 1, "sim": 0.90},
        {"openalex_id": "C", "rank": 2, "sim": 0.89},
        {"openalex_id": "B", "rank": 3, "sim": 0.88},
        {"openalex_id": "D", "rank": 4, "sim": 0.10},
    ]
    candidate_scored = [
        {"openalex_id": "A", "rank": 1, "sim": 0.91},
        {"openalex_id": "B", "rank": 2, "sim": 0.90},
        {"openalex_id": "C", "rank": 3, "sim": 0.89},
        {"openalex_id": "D", "rank": 4, "sim": 0.08},
    ]
    baseline_judged = [
        {"candidate_openalex_id": "A", "rank": 1, "label": 2},
        {"candidate_openalex_id": "C", "rank": 2, "label": 0},
        {"candidate_openalex_id": "B", "rank": 3, "label": 1},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    candidate_judged = [
        {"candidate_openalex_id": "A", "rank": 1, "label": 2},
        {"candidate_openalex_id": "B", "rank": 2, "label": 1},
        {"candidate_openalex_id": "C", "rank": 3, "label": 0},
        {"candidate_openalex_id": "D", "rank": 4, "label": 0},
    ]
    return (
        [
            _write_experiment(
                tmp_path,
                run_name="primary_directional_gain",
                experiment_id="baseline",
                scored_rows=baseline_scored,
                judged_rows=baseline_judged,
                ndcg_at_k=1.0,
            )
        ],
        [
            _write_experiment(
                tmp_path,
                run_name="secondary_directional_gain",
                experiment_id="candidate",
                scored_rows=candidate_scored,
                judged_rows=candidate_judged,
                ndcg_at_k=1.0,
            )
        ],
    )


def test_align_common_seed_rows_preserves_primary_order_and_overlap() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())

    assert [row["doi"] for row in aligned_rows] == ["10.1000/a", "10.1000/b", "10.1000/c"]
    assert aligned_rows[0]["primary_batch_index"] == 1
    assert aligned_rows[0]["secondary_batch_index"] == 11


def test_common_numeric_metrics_and_overlap_counts_ignore_missing_pairs() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())

    assert common_numeric_metrics(aligned_rows) == ["precision_at_k", "ndcg_at_k"]
    assert common_diagnostic_metrics(aligned_rows) == ["brier_score"]
    assert metric_overlap_counts(aligned_rows) == {
        "precision_at_k": 2,
        "ndcg_at_k": 2,
    }
    assert common_completed_seed_count(aligned_rows) == 2


def test_paired_metric_rows_use_correct_delta_direction_for_higher_is_better() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="ndcg_at_k",
        status_mode="common completed only",
    )

    assert [row["doi"] for row in paired_rows] == ["10.1000/a", "10.1000/c"]
    assert paired_rows[0]["raw_delta"] == pytest.approx(-0.2)
    assert paired_rows[0]["improvement_delta"] == pytest.approx(-0.2)


def test_paired_metric_rows_use_correct_delta_direction_for_lower_is_better() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="brier_score",
        status_mode="all common seeds with metric available",
    )

    assert [row["doi"] for row in paired_rows] == ["10.1000/a", "10.1000/b"]
    assert paired_rows[0]["raw_delta"] == pytest.approx(-0.1)
    assert paired_rows[0]["improvement_delta"] == pytest.approx(0.1)
    assert paired_rows[1]["raw_delta"] == pytest.approx(-0.1)
    assert paired_rows[1]["improvement_delta"] == pytest.approx(0.1)


def test_comparison_metric_summary_and_outcomes_use_improvement_delta() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())
    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="precision_at_k",
        status_mode="common completed only",
    )

    summary = comparison_metric_summary(paired_rows)

    assert wins_losses_ties(paired_rows) == (1, 1, 0)
    assert summary.primary_mean == pytest.approx(0.55)
    assert summary.secondary_mean == pytest.approx(0.525)
    assert summary.raw_delta_mean == pytest.approx(-0.025)
    assert summary.improvement_delta_mean == pytest.approx(-0.025)
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.ties == 0


def test_choose_default_comparison_metric_falls_back_cleanly() -> None:
    common_metrics = ["precision_at_k", "ndcg_at_k"]

    assert choose_default_comparison_metric("ndcg_at_k", common_metrics) == "ndcg_at_k"
    assert choose_default_comparison_metric("brier_score", common_metrics) == "ndcg_at_k"
    assert choose_default_comparison_metric(None, ["precision_at_k"]) == "precision_at_k"
    assert choose_default_comparison_metric(None, []) is None


def test_compatibility_warnings_detect_manifest_mismatches() -> None:
    primary_manifest = SimpleNamespace(
        seeds_csv="data/benchmarks/seeds.csv",
        theory_config="runs/theory_a.yaml",
        options=SimpleNamespace(
            top_k=10,
            label_source="silver",
            evaluation_mode="silver_provenance_regression",
            metric_scope="local_corpus_ranking",
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
            benchmark_dataset_id=None,
            benchmark_labels_sha256=None,
            refresh=False,
        ),
    )
    secondary_manifest = SimpleNamespace(
        seeds_csv="data/benchmarks/other.csv",
        theory_config="runs/theory_b.yaml",
        options=SimpleNamespace(
            top_k=20,
            label_source="gold",
            evaluation_mode="independent_benchmark",
            metric_scope="local_corpus_ranking",
            max_references=12,
            max_related=11,
            max_hard_negatives=8,
            benchmark_dataset_id="benchmark_dataset_002",
            benchmark_labels_sha256="labels_sha256_002",
            refresh=True,
        ),
    )

    warnings = compatibility_warnings(primary_manifest, secondary_manifest)

    assert "Seeds CSV differs between primary and secondary batches." in warnings
    assert "Theory config paths differ between primary and secondary batches." in warnings
    assert "Effective option differs: top_k." in warnings
    assert "Effective option differs: label_source." in warnings
    assert "Cross-mode comparison warning" in " ".join(warnings)
    assert "Benchmark dataset id differs" in " ".join(warnings)


def test_paired_metric_rows_capture_same_label_rank_movement_when_headline_is_flat(
    tmp_path: Path,
) -> None:
    primary_rows, secondary_rows = _movement_rows_same_label(tmp_path)
    aligned_rows = align_common_seed_rows(primary_rows, secondary_rows)

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="ndcg_at_k",
        status_mode="common completed only",
    )
    summary = comparison_metric_summary(paired_rows)

    assert len(paired_rows) == 1
    row = paired_rows[0]
    assert row["top_k_exact_match_rate"] == pytest.approx(0.0)
    assert row["top_k_jaccard_at_k"] == pytest.approx(1.0)
    assert row["top_k_common_items_permutation_change_count"] == 2
    assert row["judged_pair_rank_change_count"] == 2
    assert row["judged_pair_mean_abs_rank_change"] == pytest.approx(0.5)
    assert row["judged_pair_max_abs_rank_change"] == 1
    assert row["pair_order_reversal_count"] == 1
    assert row["cross_label_order_reversal_count"] == 0
    assert row["same_label_order_reversal_count"] == 1
    assert row["pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert row["weighted_pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert row["pairwise_label_order_accuracy_delta"] == pytest.approx(0.0)
    assert row["weighted_pairwise_label_order_accuracy_delta"] == pytest.approx(0.0)
    assert row["headline_flat_but_directional_gain"] is False
    assert row["headline_flat_but_directional_loss"] is False
    assert row["directional_signal_strength"] == "weak"
    assert row["movement_without_label_gain"] is True
    assert row["headline_metric_flat_but_rank_moved"] is True
    assert row["movement_diagnostic_note"] == "headline flat, same-label movement only"

    assert summary.movement_diagnostics is not None
    assert summary.movement_diagnostics.top_k_exact_match_rate == pytest.approx(0.0)
    assert summary.movement_diagnostics.top_k_jaccard_at_k == pytest.approx(1.0)
    assert summary.movement_diagnostics.pair_order_reversal_count == 1
    assert summary.movement_diagnostics.same_label_order_reversal_count == 1
    assert summary.movement_diagnostics.cross_label_order_reversal_count == 0
    assert summary.movement_diagnostics.headline_metric_flat_but_rank_moved is True
    assert summary.movement_diagnostics.movement_diagnostic_note == (
        "headline flat, same-label movement only"
    )


def test_paired_metric_rows_distinguish_cross_label_reversals(tmp_path: Path) -> None:
    primary_rows, secondary_rows = _movement_rows_cross_label(tmp_path)
    aligned_rows = align_common_seed_rows(primary_rows, secondary_rows)

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="ndcg_at_k",
        status_mode="common completed only",
    )
    summary = comparison_metric_summary(paired_rows)

    row = paired_rows[0]
    assert row["pair_order_reversal_count"] == 1
    assert row["cross_label_order_reversal_count"] == 1
    assert row["same_label_order_reversal_count"] == 0
    assert row["pairwise_label_order_accuracy"] == pytest.approx(0.8)
    assert row["weighted_pairwise_label_order_accuracy"] == pytest.approx(6 / 7)
    assert row["pairwise_label_order_accuracy_delta"] == pytest.approx(-0.2)
    assert row["weighted_pairwise_label_order_accuracy_delta"] == pytest.approx(-1 / 7)
    assert row["headline_flat_but_directional_gain"] is False
    assert row["headline_flat_but_directional_loss"] is False
    assert row["directional_signal_strength"] == "strong"
    assert row["movement_without_label_gain"] is False
    assert row["movement_diagnostic_note"] == "directional loss detected"

    assert summary.movement_diagnostics is not None
    assert summary.movement_diagnostics.cross_label_concordant_pair_count == 4
    assert summary.movement_diagnostics.cross_label_discordant_pair_count == 1
    assert summary.movement_diagnostics.pairwise_label_order_accuracy == pytest.approx(0.8)
    assert summary.movement_diagnostics.weighted_pairwise_label_order_accuracy == pytest.approx(6 / 7)
    assert summary.movement_diagnostics.pairwise_label_order_accuracy_delta == pytest.approx(-0.2)
    assert summary.movement_diagnostics.cross_label_order_reversal_count == 1
    assert summary.movement_diagnostics.same_label_order_reversal_count == 0
    assert summary.movement_diagnostics.movement_without_label_gain is False
    assert summary.movement_diagnostics.movement_diagnostic_note == "directional loss detected"


def test_paired_metric_rows_capture_directional_gain_when_headline_is_flat(
    tmp_path: Path,
) -> None:
    primary_rows, secondary_rows = _movement_rows_directional_gain(tmp_path)
    aligned_rows = align_common_seed_rows(primary_rows, secondary_rows)

    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="ndcg_at_k",
        status_mode="common completed only",
    )
    summary = comparison_metric_summary(paired_rows)

    row = paired_rows[0]
    assert row["cross_label_concordant_pair_count"] == 5
    assert row["cross_label_discordant_pair_count"] == 0
    assert row["cross_label_tied_pair_count"] == 0
    assert row["pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert row["weighted_pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert row["pairwise_label_order_accuracy_delta"] == pytest.approx(0.2)
    assert row["weighted_pairwise_label_order_accuracy_delta"] == pytest.approx(1 / 7)
    assert row["top_k_cross_label_concordant_pair_count"] == 3
    assert row["top_k_cross_label_discordant_pair_count"] == 0
    assert row["top_k_pairwise_label_order_accuracy"] == pytest.approx(1.0)
    assert row["headline_flat_but_directional_gain"] is True
    assert row["headline_flat_but_directional_loss"] is False
    assert row["directional_signal_strength"] == "strong"
    assert row["movement_diagnostic_note"] == "headline flat, directional gain"

    assert summary.movement_diagnostics is not None
    assert summary.movement_diagnostics.pairwise_label_order_accuracy == pytest.approx(1.0)
    assert summary.movement_diagnostics.weighted_pairwise_label_order_accuracy == pytest.approx(1.0)
    assert summary.movement_diagnostics.pairwise_label_order_accuracy_delta == pytest.approx(0.2)
    assert summary.movement_diagnostics.weighted_pairwise_label_order_accuracy_delta == pytest.approx(1 / 7)
    assert summary.movement_diagnostics.top_k_cross_label_concordant_pair_count == 3
    assert summary.movement_diagnostics.top_k_cross_label_discordant_pair_count == 0
    assert summary.movement_diagnostics.top_k_pairwise_label_order_accuracy == pytest.approx(1.0)
    assert summary.movement_diagnostics.headline_flat_but_directional_gain is True
    assert summary.movement_diagnostics.headline_flat_but_directional_loss is False
    assert summary.movement_diagnostics.directional_signal_strength == "strong"
    assert summary.movement_diagnostics.movement_diagnostic_note == (
        "headline flat, directional gain"
    )


def test_comparison_metric_summary_remains_backward_compatible_without_artifact_paths() -> None:
    aligned_rows = align_common_seed_rows(_primary_rows(), _secondary_rows())
    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="precision_at_k",
        status_mode="common completed only",
    )

    summary = comparison_metric_summary(paired_rows)

    assert summary.primary_mean == pytest.approx(0.55)
    assert summary.movement_diagnostics is None
