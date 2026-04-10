from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.eval.metrics import (
    count_pair_order_reversals,
    count_position_changes,
    jaccard_similarity,
    pairwise_label_order_stats,
)


COMPARISON_METRICS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
)
DIAGNOSTIC_COMPARISON_METRICS = (
    "brier_score",
    "expected_calibration_error",
)
ALL_COMPARISON_METRICS = COMPARISON_METRICS + DIAGNOSTIC_COMPARISON_METRICS
HIGHER_IS_BETTER_METRICS = {
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
}
LOWER_IS_BETTER_METRICS = {
    "brier_score",
    "expected_calibration_error",
}
COMPARISON_STATUS_OPTIONS = (
    "common completed only",
    "all common seeds with metric available",
)
TIE_TOLERANCE = 1e-9
DIAGNOSTIC_METRIC_LABELS = {
    "brier_score": "confidence_brier_score_diag",
    "expected_calibration_error": "confidence_ece_diag",
}
MOVEMENT_ROW_FIELDS = (
    "top_k_exact_match_rate",
    "top_k_jaccard_at_k",
    "top_k_common_items_permutation_change_count",
    "judged_pair_rank_change_count",
    "judged_pair_mean_abs_rank_change",
    "judged_pair_max_abs_rank_change",
    "cross_label_concordant_pair_count",
    "cross_label_discordant_pair_count",
    "cross_label_tied_pair_count",
    "pairwise_label_order_accuracy",
    "weighted_pairwise_label_order_accuracy",
    "pairwise_label_order_accuracy_delta",
    "weighted_pairwise_label_order_accuracy_delta",
    "top_k_cross_label_concordant_pair_count",
    "top_k_cross_label_discordant_pair_count",
    "top_k_pairwise_label_order_accuracy",
    "pair_order_reversal_count",
    "cross_label_order_reversal_count",
    "same_label_order_reversal_count",
    "cross_label_top_k_swaps",
    "movement_without_label_gain",
    "headline_metric_flat_but_rank_moved",
    "headline_flat_but_directional_gain",
    "headline_flat_but_directional_loss",
    "rank_moved_materially",
    "directional_signal_strength",
    "movement_diagnostic_note",
)
MOVEMENT_INTERNAL_ROW_FIELDS = (
    "judged_pair_observation_count",
    "baseline_cross_label_concordant_pair_count",
    "baseline_cross_label_discordant_pair_count",
    "baseline_weighted_cross_label_concordant_pair_count",
    "baseline_weighted_cross_label_discordant_pair_count",
    "candidate_weighted_cross_label_concordant_pair_count",
    "candidate_weighted_cross_label_discordant_pair_count",
    "top_k_weighted_cross_label_concordant_pair_count",
    "top_k_weighted_cross_label_discordant_pair_count",
)
TRUE_NEAR_INVARIANCE_NOTE = "true near-invariance"
SAME_LABEL_MOVEMENT_NOTE = "headline flat, same-label movement only"
CROSS_LABEL_MOVEMENT_NOTE = "headline flat, cross-label movement detected"
DIRECTIONAL_GAIN_NOTE = "headline flat, directional gain"
DIRECTIONAL_LOSS_NOTE = "headline flat, directional loss"
MIXED_DIRECTIONAL_MOVEMENT_NOTE = "headline flat, mixed directional movement"


@dataclass(frozen=True, slots=True)
class ComparisonMovementSummary:
    paired_seed_count: int
    seeds_with_rank_movement: int
    top_k_exact_match_rate: float | None
    top_k_jaccard_at_k: float | None
    top_k_common_items_permutation_change_count: int
    judged_pair_rank_change_count: int
    judged_pair_mean_abs_rank_change: float | None
    judged_pair_max_abs_rank_change: int | None
    cross_label_concordant_pair_count: int
    cross_label_discordant_pair_count: int
    cross_label_tied_pair_count: int
    pairwise_label_order_accuracy: float | None
    weighted_pairwise_label_order_accuracy: float | None
    pairwise_label_order_accuracy_delta: float | None
    weighted_pairwise_label_order_accuracy_delta: float | None
    top_k_cross_label_concordant_pair_count: int
    top_k_cross_label_discordant_pair_count: int
    top_k_pairwise_label_order_accuracy: float | None
    pair_order_reversal_count: int
    cross_label_order_reversal_count: int
    same_label_order_reversal_count: int
    cross_label_top_k_swaps: int
    movement_without_label_gain: bool
    headline_metric_flat_but_rank_moved: bool
    headline_flat_but_directional_gain: bool
    headline_flat_but_directional_loss: bool
    rank_moved_materially: bool
    directional_signal_strength: str
    movement_diagnostic_note: str


@dataclass(frozen=True, slots=True)
class ComparisonMetricSummary:
    primary_mean: float | None
    primary_median: float | None
    secondary_mean: float | None
    secondary_median: float | None
    raw_delta_mean: float | None
    raw_delta_median: float | None
    improvement_delta_mean: float | None
    improvement_delta_median: float | None
    wins: int
    losses: int
    ties: int
    movement_diagnostics: ComparisonMovementSummary | None = None


def align_common_seed_rows(
    primary_rows: list[dict[str, Any]],
    secondary_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    primary_by_doi = {
        str(row.get("doi")): row
        for row in primary_rows
        if row.get("doi") not in (None, "")
    }
    secondary_by_doi = {
        str(row.get("doi")): row
        for row in secondary_rows
        if row.get("doi") not in (None, "")
    }
    common_dois = [
        str(row.get("doi"))
        for row in primary_rows
        if row.get("doi") not in (None, "")
        and str(row.get("doi")) in secondary_by_doi
    ]

    aligned_rows: list[dict[str, Any]] = []
    for doi in common_dois:
        primary_row = primary_by_doi[doi]
        secondary_row = secondary_by_doi[doi]
        aligned_row = {
            "doi": doi,
            "primary_batch_index": primary_row.get("batch_index"),
            "secondary_batch_index": secondary_row.get("batch_index"),
            "primary_status": primary_row.get("status"),
            "secondary_status": secondary_row.get("status"),
            "primary_run_dir": primary_row.get("run_dir"),
            "secondary_run_dir": secondary_row.get("run_dir"),
            "primary_experiment_id": primary_row.get("experiment_id"),
            "secondary_experiment_id": secondary_row.get("experiment_id"),
            "primary_evaluation_summary_json": primary_row.get("evaluation_summary_json"),
            "secondary_evaluation_summary_json": secondary_row.get("evaluation_summary_json"),
            "primary_evaluation_mode": primary_row.get("evaluation_mode"),
            "secondary_evaluation_mode": secondary_row.get("evaluation_mode"),
            "primary_evidence_tier": primary_row.get("evidence_tier"),
            "secondary_evidence_tier": secondary_row.get("evidence_tier"),
            "primary_metric_scope": primary_row.get("metric_scope"),
            "secondary_metric_scope": secondary_row.get("metric_scope"),
            "primary_benchmark_dataset_id": primary_row.get("benchmark_dataset_id"),
            "secondary_benchmark_dataset_id": secondary_row.get("benchmark_dataset_id"),
            "primary_benchmark_labels_sha256": primary_row.get("benchmark_labels_sha256"),
            "secondary_benchmark_labels_sha256": secondary_row.get("benchmark_labels_sha256"),
            "primary_benchmark_maturity_tier": primary_row.get("benchmark_maturity_tier"),
            "secondary_benchmark_maturity_tier": secondary_row.get("benchmark_maturity_tier"),
            "primary_promotion_ready": primary_row.get("promotion_ready"),
            "secondary_promotion_ready": secondary_row.get("promotion_ready"),
        }
        for metric_name in ALL_COMPARISON_METRICS:
            aligned_row[f"primary_{metric_name}"] = primary_row.get(metric_name)
            aligned_row[f"secondary_{metric_name}"] = secondary_row.get(metric_name)
        aligned_rows.append(aligned_row)
    return aligned_rows


def common_numeric_metrics(aligned_rows: list[dict[str, Any]]) -> list[str]:
    return [
        metric_name
        for metric_name in COMPARISON_METRICS
        if any(
            _numeric_value(row.get(f"primary_{metric_name}")) is not None
            and _numeric_value(row.get(f"secondary_{metric_name}")) is not None
            for row in aligned_rows
        )
    ]


def common_diagnostic_metrics(aligned_rows: list[dict[str, Any]]) -> list[str]:
    return [
        metric_name
        for metric_name in DIAGNOSTIC_COMPARISON_METRICS
        if any(
            _numeric_value(row.get(f"primary_{metric_name}")) is not None
            and _numeric_value(row.get(f"secondary_{metric_name}")) is not None
            for row in aligned_rows
        )
    ]


def choose_default_comparison_metric(
    primary_ranking_metric: str | None,
    common_metrics: list[str],
) -> str | None:
    if primary_ranking_metric in common_metrics:
        return primary_ranking_metric
    if "ndcg_at_k" in common_metrics:
        return "ndcg_at_k"
    return common_metrics[0] if common_metrics else None


def paired_metric_rows(
    aligned_rows: list[dict[str, Any]],
    *,
    metric_name: str,
    status_mode: str,
    tie_tolerance: float = TIE_TOLERANCE,
) -> list[dict[str, Any]]:
    paired_rows: list[dict[str, Any]] = []
    for row in aligned_rows:
        primary_value = _numeric_value(row.get(f"primary_{metric_name}"))
        secondary_value = _numeric_value(row.get(f"secondary_{metric_name}"))
        if primary_value is None or secondary_value is None:
            continue
        if status_mode == "common completed only" and not _is_common_completed(row):
            continue
        raw_delta = secondary_value - primary_value
        if metric_name in LOWER_IS_BETTER_METRICS:
            improvement_delta = primary_value - secondary_value
        else:
            improvement_delta = raw_delta
        paired_row = {
            **row,
            "metric_name": metric_name,
            "primary_metric_value": primary_value,
            "secondary_metric_value": secondary_value,
            "raw_delta": raw_delta,
            "improvement_delta": improvement_delta,
        }
        paired_row.update(
            _movement_diagnostics_for_row(
                paired_row,
                tie_tolerance=tie_tolerance,
            )
        )
        paired_rows.append(paired_row)
    return paired_rows


def common_completed_seed_count(aligned_rows: list[dict[str, Any]]) -> int:
    return sum(1 for row in aligned_rows if _is_common_completed(row))


def metric_overlap_counts(aligned_rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        metric_name: len(
            paired_metric_rows(
                aligned_rows,
                metric_name=metric_name,
                status_mode="common completed only",
            )
        )
        for metric_name in common_numeric_metrics(aligned_rows)
    }


def comparison_metric_summary(
    paired_rows: list[dict[str, Any]],
    *,
    tie_tolerance: float = TIE_TOLERANCE,
) -> ComparisonMetricSummary:
    primary_values = [row["primary_metric_value"] for row in paired_rows]
    secondary_values = [row["secondary_metric_value"] for row in paired_rows]
    raw_deltas = [row["raw_delta"] for row in paired_rows]
    improvement_deltas = [row["improvement_delta"] for row in paired_rows]
    wins, losses, ties = wins_losses_ties(paired_rows, tie_tolerance=tie_tolerance)

    improvement_delta_mean = _mean_or_none(improvement_deltas)
    movement_diagnostics = _movement_summary_from_paired_rows(
        paired_rows,
        improvement_delta_mean=improvement_delta_mean,
        tie_tolerance=tie_tolerance,
    )
    return ComparisonMetricSummary(
        primary_mean=_mean_or_none(primary_values),
        primary_median=_median_or_none(primary_values),
        secondary_mean=_mean_or_none(secondary_values),
        secondary_median=_median_or_none(secondary_values),
        raw_delta_mean=_mean_or_none(raw_deltas),
        raw_delta_median=_median_or_none(raw_deltas),
        improvement_delta_mean=improvement_delta_mean,
        improvement_delta_median=_median_or_none(improvement_deltas),
        wins=wins,
        losses=losses,
        ties=ties,
        movement_diagnostics=movement_diagnostics,
    )


def wins_losses_ties(
    paired_rows: list[dict[str, Any]],
    *,
    tie_tolerance: float = TIE_TOLERANCE,
) -> tuple[int, int, int]:
    wins = 0
    losses = 0
    ties = 0
    for row in paired_rows:
        improvement_delta = float(row.get("improvement_delta", 0.0))
        if abs(improvement_delta) <= tie_tolerance:
            ties += 1
        elif improvement_delta > 0:
            wins += 1
        else:
            losses += 1
    return wins, losses, ties


def compatibility_warnings(primary_manifest: Any, secondary_manifest: Any) -> list[str]:
    warnings: list[str] = []
    if getattr(primary_manifest, "seeds_csv", None) != getattr(secondary_manifest, "seeds_csv", None):
        warnings.append("Seeds CSV differs between primary and secondary batches.")
    if getattr(primary_manifest, "theory_config", None) != getattr(secondary_manifest, "theory_config", None):
        warnings.append("Theory config paths differ between primary and secondary batches.")

    primary_options = getattr(primary_manifest, "options", None)
    secondary_options = getattr(secondary_manifest, "options", None)
    if primary_options is None or secondary_options is None:
        return warnings

    option_pairs = (
        ("top_k", "top_k"),
        ("label_source", "label_source"),
        ("evaluation_mode", "evaluation_mode"),
        ("metric_scope", "metric_scope"),
        ("max_references", "max_references"),
        ("max_related", "max_related"),
        ("max_hard_negatives", "max_hard_negatives"),
        ("refresh", "refresh"),
    )
    for label, field_name in option_pairs:
        if getattr(primary_options, field_name, None) != getattr(secondary_options, field_name, None):
            warnings.append(f"Effective option differs: {label}.")
    primary_mode = _optional_str(getattr(primary_options, "evaluation_mode", None))
    secondary_mode = _optional_str(getattr(secondary_options, "evaluation_mode", None))
    if primary_mode is not None and secondary_mode is not None and primary_mode != secondary_mode:
        warnings.append(
            "Cross-mode comparison warning: primary and secondary batches use different evaluation_mode values."
        )
    primary_dataset_id = _optional_str(getattr(primary_options, "benchmark_dataset_id", None))
    secondary_dataset_id = _optional_str(getattr(secondary_options, "benchmark_dataset_id", None))
    if primary_dataset_id != secondary_dataset_id:
        warnings.append("Benchmark dataset id differs between primary and secondary batches.")
    primary_labels_sha256 = _optional_str(getattr(primary_options, "benchmark_labels_sha256", None))
    secondary_labels_sha256 = _optional_str(getattr(secondary_options, "benchmark_labels_sha256", None))
    if primary_labels_sha256 != secondary_labels_sha256:
        warnings.append("Benchmark labels SHA256 differs between primary and secondary batches.")
    return warnings


def movement_diagnostics_payload(
    diagnostics: ComparisonMovementSummary | None,
) -> dict[str, Any] | None:
    if diagnostics is None:
        return None
    return {
        "paired_seed_count": diagnostics.paired_seed_count,
        "seeds_with_rank_movement": diagnostics.seeds_with_rank_movement,
        "top_k_exact_match_rate": diagnostics.top_k_exact_match_rate,
        "top_k_jaccard_at_k": diagnostics.top_k_jaccard_at_k,
        "top_k_common_items_permutation_change_count": diagnostics.top_k_common_items_permutation_change_count,
        "judged_pair_rank_change_count": diagnostics.judged_pair_rank_change_count,
        "judged_pair_mean_abs_rank_change": diagnostics.judged_pair_mean_abs_rank_change,
        "judged_pair_max_abs_rank_change": diagnostics.judged_pair_max_abs_rank_change,
        "cross_label_concordant_pair_count": diagnostics.cross_label_concordant_pair_count,
        "cross_label_discordant_pair_count": diagnostics.cross_label_discordant_pair_count,
        "cross_label_tied_pair_count": diagnostics.cross_label_tied_pair_count,
        "pairwise_label_order_accuracy": diagnostics.pairwise_label_order_accuracy,
        "weighted_pairwise_label_order_accuracy": diagnostics.weighted_pairwise_label_order_accuracy,
        "pairwise_label_order_accuracy_delta": diagnostics.pairwise_label_order_accuracy_delta,
        "weighted_pairwise_label_order_accuracy_delta": diagnostics.weighted_pairwise_label_order_accuracy_delta,
        "top_k_cross_label_concordant_pair_count": diagnostics.top_k_cross_label_concordant_pair_count,
        "top_k_cross_label_discordant_pair_count": diagnostics.top_k_cross_label_discordant_pair_count,
        "top_k_pairwise_label_order_accuracy": diagnostics.top_k_pairwise_label_order_accuracy,
        "pair_order_reversal_count": diagnostics.pair_order_reversal_count,
        "cross_label_order_reversal_count": diagnostics.cross_label_order_reversal_count,
        "same_label_order_reversal_count": diagnostics.same_label_order_reversal_count,
        "cross_label_top_k_swaps": diagnostics.cross_label_top_k_swaps,
        "movement_without_label_gain": diagnostics.movement_without_label_gain,
        "headline_metric_flat_but_rank_moved": diagnostics.headline_metric_flat_but_rank_moved,
        "headline_flat_but_directional_gain": diagnostics.headline_flat_but_directional_gain,
        "headline_flat_but_directional_loss": diagnostics.headline_flat_but_directional_loss,
        "rank_moved_materially": diagnostics.rank_moved_materially,
        "directional_signal_strength": diagnostics.directional_signal_strength,
        "movement_diagnostic_note": diagnostics.movement_diagnostic_note,
    }


def comparison_metric_summary_payload(summary: ComparisonMetricSummary) -> dict[str, Any]:
    return {
        "primary_mean": summary.primary_mean,
        "primary_median": summary.primary_median,
        "secondary_mean": summary.secondary_mean,
        "secondary_median": summary.secondary_median,
        "raw_delta_mean": summary.raw_delta_mean,
        "raw_delta_median": summary.raw_delta_median,
        "improvement_delta_mean": summary.improvement_delta_mean,
        "improvement_delta_median": summary.improvement_delta_median,
        "wins": summary.wins,
        "losses": summary.losses,
        "ties": summary.ties,
        "movement_diagnostics": movement_diagnostics_payload(summary.movement_diagnostics),
    }


def _shared_label_map(
    primary_labels: dict[str, int],
    secondary_labels: dict[str, int],
) -> dict[str, int]:
    return {
        item: primary_labels[item]
        for item in sorted(set(primary_labels) & set(secondary_labels))
        if primary_labels[item] == secondary_labels[item]
    }


def _accuracy_from_counts(concordant_count: int, discordant_count: int) -> float | None:
    strict_pair_count = concordant_count + discordant_count
    if strict_pair_count <= 0:
        return None
    return concordant_count / strict_pair_count


def _weighted_accuracy_from_counts(
    weighted_concordant_count: float,
    weighted_discordant_count: float,
) -> float | None:
    strict_pair_weight = weighted_concordant_count + weighted_discordant_count
    if strict_pair_weight <= 0:
        return None
    return weighted_concordant_count / strict_pair_weight


def _delta_or_none(candidate_value: Any, baseline_value: Any) -> float | None:
    candidate_numeric = _numeric_value(candidate_value)
    baseline_numeric = _numeric_value(baseline_value)
    if candidate_numeric is None or baseline_numeric is None:
        return None
    return candidate_numeric - baseline_numeric


def _preferred_directional_delta(
    weighted_delta: float | None,
    unweighted_delta: float | None,
) -> float | None:
    if weighted_delta is not None:
        return weighted_delta
    return unweighted_delta


def _strict_pair_count(stats_payload: dict[str, Any]) -> int:
    return _int_or_zero(stats_payload.get("concordant_pair_count")) + _int_or_zero(
        stats_payload.get("discordant_pair_count")
    )


def _directional_signal_strength(
    directional_delta: float | None,
    *,
    strict_pair_count: int,
) -> str:
    if strict_pair_count <= 0 or directional_delta is None:
        return "weak"
    magnitude = abs(directional_delta)
    if magnitude >= 0.10:
        return "strong"
    if magnitude >= 0.03:
        return "moderate"
    return "weak"


def _movement_diagnostics_for_row(
    row: dict[str, Any],
    *,
    tie_tolerance: float,
) -> dict[str, Any]:
    primary_summary_path = _summary_path_from_row(row, prefix="primary")
    secondary_summary_path = _summary_path_from_row(row, prefix="secondary")
    if primary_summary_path is None or secondary_summary_path is None:
        return {}

    primary_artifacts = _experiment_artifacts(str(primary_summary_path))
    secondary_artifacts = _experiment_artifacts(str(secondary_summary_path))
    if primary_artifacts is None or secondary_artifacts is None:
        return {}

    primary_ranked = _ordered_candidate_ids(primary_artifacts["scored_rows"])
    secondary_ranked = _ordered_candidate_ids(secondary_artifacts["scored_rows"])
    if not primary_ranked and not secondary_ranked:
        return {}

    top_k = _shared_top_k(
        primary_artifacts.get("top_k_used"),
        secondary_artifacts.get("top_k_used"),
        fallback=min(len(primary_ranked), len(secondary_ranked)),
    )
    primary_top_k = primary_ranked[:top_k]
    secondary_top_k = secondary_ranked[:top_k]
    shared_top_k_items = sorted(set(primary_top_k) & set(secondary_top_k))

    primary_judged_ranks, primary_labels = _judged_rank_and_label_maps(
        primary_artifacts["judged_rows"]
    )
    secondary_judged_ranks, secondary_labels = _judged_rank_and_label_maps(
        secondary_artifacts["judged_rows"]
    )
    shared_labels = _shared_label_map(primary_labels, secondary_labels)
    common_judged_items = sorted(
        set(primary_judged_ranks)
        & set(secondary_judged_ranks)
        & set(shared_labels)
    )
    judged_rank_deltas = [
        abs(primary_judged_ranks[item] - secondary_judged_ranks[item])
        for item in common_judged_items
    ]
    baseline_pairwise = pairwise_label_order_stats(
        primary_judged_ranks,
        shared_labels,
        items=common_judged_items,
    )
    candidate_pairwise = pairwise_label_order_stats(
        secondary_judged_ranks,
        shared_labels,
        items=common_judged_items,
    )
    pair_reversal_count, cross_label_reversal_count, same_label_reversal_count = (
        count_pair_order_reversals(
            {item: primary_judged_ranks[item] for item in common_judged_items},
            {item: secondary_judged_ranks[item] for item in common_judged_items},
            labels_by_item=shared_labels,
        )
    )

    top_k_common_judged_items = [
        item
        for item in shared_top_k_items
        if item in primary_judged_ranks and item in secondary_judged_ranks and item in shared_labels
    ]
    _, cross_label_top_k_swaps, _ = count_pair_order_reversals(
        {item: primary_judged_ranks[item] for item in top_k_common_judged_items},
        {item: secondary_judged_ranks[item] for item in top_k_common_judged_items},
        labels_by_item={item: shared_labels[item] for item in top_k_common_judged_items},
    )
    top_k_candidate_pairwise = pairwise_label_order_stats(
        secondary_judged_ranks,
        shared_labels,
        items=top_k_common_judged_items,
    )

    top_k_exact_match_rate = 1.0 if primary_top_k == secondary_top_k else 0.0
    top_k_jaccard = jaccard_similarity(primary_top_k, secondary_top_k)
    permutation_change_count = count_position_changes(
        primary_top_k,
        secondary_top_k,
        items=shared_top_k_items,
    )
    judged_pair_rank_change_count = sum(1 for delta in judged_rank_deltas if delta > 0)
    judged_pair_mean_abs_rank_change = (
        statistics.mean(judged_rank_deltas) if judged_rank_deltas else None
    )
    judged_pair_max_abs_rank_change = max(judged_rank_deltas) if judged_rank_deltas else None
    rank_moved_materially = bool(
        permutation_change_count > 0
        or pair_reversal_count > 0
        or judged_pair_rank_change_count > 0
        or top_k_exact_match_rate < 1.0
    )
    headline_flat = abs(float(row.get("improvement_delta", 0.0))) <= tie_tolerance
    pairwise_label_order_accuracy_delta = _delta_or_none(
        candidate_pairwise.get("pairwise_label_order_accuracy"),
        baseline_pairwise.get("pairwise_label_order_accuracy"),
    )
    weighted_pairwise_label_order_accuracy_delta = _delta_or_none(
        candidate_pairwise.get("weighted_pairwise_label_order_accuracy"),
        baseline_pairwise.get("weighted_pairwise_label_order_accuracy"),
    )
    directional_delta = _preferred_directional_delta(
        weighted_pairwise_label_order_accuracy_delta,
        pairwise_label_order_accuracy_delta,
    )
    headline_flat_but_directional_gain = bool(
        headline_flat
        and directional_delta is not None
        and directional_delta > tie_tolerance
    )
    headline_flat_but_directional_loss = bool(
        headline_flat
        and directional_delta is not None
        and directional_delta < -tie_tolerance
    )
    movement_without_label_gain = bool(
        rank_moved_materially
        and cross_label_reversal_count == 0
        and cross_label_top_k_swaps == 0
        and not headline_flat_but_directional_gain
        and not headline_flat_but_directional_loss
    )

    return {
        "top_k_exact_match_rate": top_k_exact_match_rate,
        "top_k_jaccard_at_k": top_k_jaccard,
        "top_k_common_items_permutation_change_count": permutation_change_count,
        "judged_pair_rank_change_count": judged_pair_rank_change_count,
        "judged_pair_mean_abs_rank_change": judged_pair_mean_abs_rank_change,
        "judged_pair_max_abs_rank_change": judged_pair_max_abs_rank_change,
        "cross_label_concordant_pair_count": _int_or_zero(
            candidate_pairwise.get("concordant_pair_count")
        ),
        "cross_label_discordant_pair_count": _int_or_zero(
            candidate_pairwise.get("discordant_pair_count")
        ),
        "cross_label_tied_pair_count": _int_or_zero(
            candidate_pairwise.get("tied_pair_count")
        ),
        "pairwise_label_order_accuracy": _numeric_value(
            candidate_pairwise.get("pairwise_label_order_accuracy")
        ),
        "weighted_pairwise_label_order_accuracy": _numeric_value(
            candidate_pairwise.get("weighted_pairwise_label_order_accuracy")
        ),
        "pairwise_label_order_accuracy_delta": pairwise_label_order_accuracy_delta,
        "weighted_pairwise_label_order_accuracy_delta": (
            weighted_pairwise_label_order_accuracy_delta
        ),
        "top_k_cross_label_concordant_pair_count": _int_or_zero(
            top_k_candidate_pairwise.get("concordant_pair_count")
        ),
        "top_k_cross_label_discordant_pair_count": _int_or_zero(
            top_k_candidate_pairwise.get("discordant_pair_count")
        ),
        "top_k_pairwise_label_order_accuracy": _numeric_value(
            top_k_candidate_pairwise.get("pairwise_label_order_accuracy")
        ),
        "pair_order_reversal_count": pair_reversal_count,
        "cross_label_order_reversal_count": cross_label_reversal_count,
        "same_label_order_reversal_count": same_label_reversal_count,
        "cross_label_top_k_swaps": cross_label_top_k_swaps,
        "movement_without_label_gain": movement_without_label_gain,
        "headline_metric_flat_but_rank_moved": bool(headline_flat and rank_moved_materially),
        "headline_flat_but_directional_gain": headline_flat_but_directional_gain,
        "headline_flat_but_directional_loss": headline_flat_but_directional_loss,
        "rank_moved_materially": rank_moved_materially,
        "directional_signal_strength": _directional_signal_strength(
            directional_delta,
            strict_pair_count=_strict_pair_count(candidate_pairwise),
        ),
        "movement_diagnostic_note": _movement_diagnostic_note(
            headline_flat=headline_flat,
            rank_moved_materially=rank_moved_materially,
            cross_label_order_reversal_count=cross_label_reversal_count,
            cross_label_top_k_swaps=cross_label_top_k_swaps,
            directional_delta=directional_delta,
            movement_without_label_gain=movement_without_label_gain,
            tie_tolerance=tie_tolerance,
        ),
        "judged_pair_observation_count": len(judged_rank_deltas),
        "baseline_cross_label_concordant_pair_count": _int_or_zero(
            baseline_pairwise.get("concordant_pair_count")
        ),
        "baseline_cross_label_discordant_pair_count": _int_or_zero(
            baseline_pairwise.get("discordant_pair_count")
        ),
        "baseline_weighted_cross_label_concordant_pair_count": _numeric_value(
            baseline_pairwise.get("weighted_concordant_pair_count")
        )
        or 0.0,
        "baseline_weighted_cross_label_discordant_pair_count": _numeric_value(
            baseline_pairwise.get("weighted_discordant_pair_count")
        )
        or 0.0,
        "candidate_weighted_cross_label_concordant_pair_count": _numeric_value(
            candidate_pairwise.get("weighted_concordant_pair_count")
        )
        or 0.0,
        "candidate_weighted_cross_label_discordant_pair_count": _numeric_value(
            candidate_pairwise.get("weighted_discordant_pair_count")
        )
        or 0.0,
        "top_k_weighted_cross_label_concordant_pair_count": _numeric_value(
            top_k_candidate_pairwise.get("weighted_concordant_pair_count")
        )
        or 0.0,
        "top_k_weighted_cross_label_discordant_pair_count": _numeric_value(
            top_k_candidate_pairwise.get("weighted_discordant_pair_count")
        )
        or 0.0,
    }


def _movement_summary_from_paired_rows(
    paired_rows: list[dict[str, Any]],
    *,
    improvement_delta_mean: float | None,
    tie_tolerance: float,
) -> ComparisonMovementSummary | None:
    movement_rows = [
        row
        for row in paired_rows
        if _numeric_value(row.get("top_k_exact_match_rate")) is not None
    ]
    if not movement_rows:
        return None

    judged_pair_observation_count = sum(
        _int_or_zero(row.get("judged_pair_observation_count"))
        for row in movement_rows
    )
    weighted_rank_change_total = sum(
        (_numeric_value(row.get("judged_pair_mean_abs_rank_change")) or 0.0)
        * _int_or_zero(row.get("judged_pair_observation_count"))
        for row in movement_rows
    )
    max_abs_rank_changes = [
        int(value)
        for value in (
            row.get("judged_pair_max_abs_rank_change")
            for row in movement_rows
        )
        if value is not None
    ]
    cross_label_order_reversal_count = sum(
        _int_or_zero(row.get("cross_label_order_reversal_count"))
        for row in movement_rows
    )
    cross_label_top_k_swaps = sum(
        _int_or_zero(row.get("cross_label_top_k_swaps"))
        for row in movement_rows
    )
    cross_label_concordant_pair_count = sum(
        _int_or_zero(row.get("cross_label_concordant_pair_count"))
        for row in movement_rows
    )
    cross_label_discordant_pair_count = sum(
        _int_or_zero(row.get("cross_label_discordant_pair_count"))
        for row in movement_rows
    )
    cross_label_tied_pair_count = sum(
        _int_or_zero(row.get("cross_label_tied_pair_count"))
        for row in movement_rows
    )
    top_k_cross_label_concordant_pair_count = sum(
        _int_or_zero(row.get("top_k_cross_label_concordant_pair_count"))
        for row in movement_rows
    )
    top_k_cross_label_discordant_pair_count = sum(
        _int_or_zero(row.get("top_k_cross_label_discordant_pair_count"))
        for row in movement_rows
    )
    baseline_cross_label_concordant_pair_count = sum(
        _int_or_zero(row.get("baseline_cross_label_concordant_pair_count"))
        for row in movement_rows
    )
    baseline_cross_label_discordant_pair_count = sum(
        _int_or_zero(row.get("baseline_cross_label_discordant_pair_count"))
        for row in movement_rows
    )
    baseline_weighted_cross_label_concordant_pair_count = sum(
        _numeric_value(row.get("baseline_weighted_cross_label_concordant_pair_count")) or 0.0
        for row in movement_rows
    )
    baseline_weighted_cross_label_discordant_pair_count = sum(
        _numeric_value(row.get("baseline_weighted_cross_label_discordant_pair_count")) or 0.0
        for row in movement_rows
    )
    candidate_weighted_cross_label_concordant_pair_count = sum(
        _numeric_value(row.get("candidate_weighted_cross_label_concordant_pair_count")) or 0.0
        for row in movement_rows
    )
    candidate_weighted_cross_label_discordant_pair_count = sum(
        _numeric_value(row.get("candidate_weighted_cross_label_discordant_pair_count")) or 0.0
        for row in movement_rows
    )
    top_k_weighted_cross_label_concordant_pair_count = sum(
        _numeric_value(row.get("top_k_weighted_cross_label_concordant_pair_count")) or 0.0
        for row in movement_rows
    )
    top_k_weighted_cross_label_discordant_pair_count = sum(
        _numeric_value(row.get("top_k_weighted_cross_label_discordant_pair_count")) or 0.0
        for row in movement_rows
    )
    seeds_with_rank_movement = sum(
        1 for row in movement_rows if bool(row.get("rank_moved_materially"))
    )
    rank_moved_materially = seeds_with_rank_movement > 0
    headline_flat = (
        improvement_delta_mean is not None
        and abs(improvement_delta_mean) <= tie_tolerance
    )
    pairwise_label_order_accuracy = _accuracy_from_counts(
        cross_label_concordant_pair_count,
        cross_label_discordant_pair_count,
    )
    weighted_pairwise_label_order_accuracy = _weighted_accuracy_from_counts(
        candidate_weighted_cross_label_concordant_pair_count,
        candidate_weighted_cross_label_discordant_pair_count,
    )
    pairwise_label_order_accuracy_delta = _delta_or_none(
        pairwise_label_order_accuracy,
        _accuracy_from_counts(
            baseline_cross_label_concordant_pair_count,
            baseline_cross_label_discordant_pair_count,
        ),
    )
    weighted_pairwise_label_order_accuracy_delta = _delta_or_none(
        weighted_pairwise_label_order_accuracy,
        _weighted_accuracy_from_counts(
            baseline_weighted_cross_label_concordant_pair_count,
            baseline_weighted_cross_label_discordant_pair_count,
        ),
    )
    directional_delta = _preferred_directional_delta(
        weighted_pairwise_label_order_accuracy_delta,
        pairwise_label_order_accuracy_delta,
    )
    headline_flat_but_directional_gain = bool(
        headline_flat
        and directional_delta is not None
        and directional_delta > tie_tolerance
    )
    headline_flat_but_directional_loss = bool(
        headline_flat
        and directional_delta is not None
        and directional_delta < -tie_tolerance
    )
    movement_without_label_gain = bool(
        rank_moved_materially
        and cross_label_order_reversal_count == 0
        and cross_label_top_k_swaps == 0
        and not headline_flat_but_directional_gain
        and not headline_flat_but_directional_loss
    )

    return ComparisonMovementSummary(
        paired_seed_count=len(movement_rows),
        seeds_with_rank_movement=seeds_with_rank_movement,
        top_k_exact_match_rate=_mean_or_none(
            [
                float(row["top_k_exact_match_rate"])
                for row in movement_rows
            ]
        ),
        top_k_jaccard_at_k=_mean_or_none(
            [
                float(row["top_k_jaccard_at_k"])
                for row in movement_rows
                if _numeric_value(row.get("top_k_jaccard_at_k")) is not None
            ]
        ),
        top_k_common_items_permutation_change_count=sum(
            _int_or_zero(row.get("top_k_common_items_permutation_change_count"))
            for row in movement_rows
        ),
        judged_pair_rank_change_count=sum(
            _int_or_zero(row.get("judged_pair_rank_change_count"))
            for row in movement_rows
        ),
        judged_pair_mean_abs_rank_change=(
            weighted_rank_change_total / judged_pair_observation_count
            if judged_pair_observation_count > 0
            else None
        ),
        judged_pair_max_abs_rank_change=(
            max(max_abs_rank_changes) if max_abs_rank_changes else None
        ),
        cross_label_concordant_pair_count=cross_label_concordant_pair_count,
        cross_label_discordant_pair_count=cross_label_discordant_pair_count,
        cross_label_tied_pair_count=cross_label_tied_pair_count,
        pairwise_label_order_accuracy=pairwise_label_order_accuracy,
        weighted_pairwise_label_order_accuracy=weighted_pairwise_label_order_accuracy,
        pairwise_label_order_accuracy_delta=pairwise_label_order_accuracy_delta,
        weighted_pairwise_label_order_accuracy_delta=(
            weighted_pairwise_label_order_accuracy_delta
        ),
        top_k_cross_label_concordant_pair_count=top_k_cross_label_concordant_pair_count,
        top_k_cross_label_discordant_pair_count=top_k_cross_label_discordant_pair_count,
        top_k_pairwise_label_order_accuracy=_accuracy_from_counts(
            top_k_cross_label_concordant_pair_count,
            top_k_cross_label_discordant_pair_count,
        ),
        pair_order_reversal_count=sum(
            _int_or_zero(row.get("pair_order_reversal_count"))
            for row in movement_rows
        ),
        cross_label_order_reversal_count=cross_label_order_reversal_count,
        same_label_order_reversal_count=sum(
            _int_or_zero(row.get("same_label_order_reversal_count"))
            for row in movement_rows
        ),
        cross_label_top_k_swaps=cross_label_top_k_swaps,
        movement_without_label_gain=movement_without_label_gain,
        headline_metric_flat_but_rank_moved=bool(headline_flat and rank_moved_materially),
        headline_flat_but_directional_gain=headline_flat_but_directional_gain,
        headline_flat_but_directional_loss=headline_flat_but_directional_loss,
        rank_moved_materially=rank_moved_materially,
        directional_signal_strength=_directional_signal_strength(
            directional_delta,
            strict_pair_count=(
                cross_label_concordant_pair_count + cross_label_discordant_pair_count
            ),
        ),
        movement_diagnostic_note=_movement_diagnostic_note(
            headline_flat=headline_flat,
            rank_moved_materially=rank_moved_materially,
            cross_label_order_reversal_count=cross_label_order_reversal_count,
            cross_label_top_k_swaps=cross_label_top_k_swaps,
            directional_delta=directional_delta,
            movement_without_label_gain=movement_without_label_gain,
            tie_tolerance=tie_tolerance,
        ),
    )


def _movement_diagnostic_note(
    *,
    headline_flat: bool,
    rank_moved_materially: bool,
    cross_label_order_reversal_count: int,
    cross_label_top_k_swaps: int,
    directional_delta: float | None,
    movement_without_label_gain: bool,
    tie_tolerance: float,
) -> str:
    if not rank_moved_materially:
        return TRUE_NEAR_INVARIANCE_NOTE
    if headline_flat and directional_delta is not None and directional_delta > tie_tolerance:
        return DIRECTIONAL_GAIN_NOTE
    if headline_flat and directional_delta is not None and directional_delta < -tie_tolerance:
        return DIRECTIONAL_LOSS_NOTE
    if headline_flat and movement_without_label_gain:
        return SAME_LABEL_MOVEMENT_NOTE
    if headline_flat and (cross_label_order_reversal_count > 0 or cross_label_top_k_swaps > 0):
        return MIXED_DIRECTIONAL_MOVEMENT_NOTE
    if directional_delta is not None and directional_delta > tie_tolerance:
        return "directional gain detected"
    if directional_delta is not None and directional_delta < -tie_tolerance:
        return "directional loss detected"
    if cross_label_order_reversal_count > 0 or cross_label_top_k_swaps > 0:
        return CROSS_LABEL_MOVEMENT_NOTE if headline_flat else "cross-label movement detected"
    return "same-label movement detected"


def _summary_path_from_row(row: dict[str, Any], *, prefix: str) -> Path | None:
    explicit_path = _resolve_path(row.get(f"{prefix}_evaluation_summary_json"))
    if explicit_path is not None and explicit_path.exists():
        return explicit_path
    run_dir = _resolve_path(row.get(f"{prefix}_run_dir"))
    experiment_id = _optional_str(row.get(f"{prefix}_experiment_id"))
    if run_dir is None or experiment_id is None:
        return None
    derived_path = run_dir / "experiments" / experiment_id / "evaluation_summary.json"
    return derived_path if derived_path.exists() else None


@lru_cache(maxsize=512)
def _experiment_artifacts(summary_path_str: str) -> dict[str, Any] | None:
    summary_path = _resolve_path(summary_path_str)
    if summary_path is None or not summary_path.exists():
        return None
    summary_payload = _load_json_object(summary_path)
    if not isinstance(summary_payload, dict):
        return None
    experiment_dir = summary_path.parent
    scored_path = experiment_dir / "scored_candidates.jsonl"
    if not scored_path.exists():
        return None
    judged_path = _judged_candidates_path(summary_payload, summary_path=summary_path)
    return {
        "top_k_used": _positive_int(summary_payload.get("top_k_used")),
        "scored_rows": _load_jsonl_records(scored_path),
        "judged_rows": _load_jsonl_records(judged_path) if judged_path is not None and judged_path.exists() else [],
    }


def _judged_candidates_path(
    summary_payload: dict[str, Any],
    *,
    summary_path: Path,
) -> Path | None:
    output_paths = summary_payload.get("output_paths")
    if isinstance(output_paths, dict):
        preferred = _resolve_path(output_paths.get("mode_judged_candidates_jsonl"))
        if preferred is not None:
            return preferred
        fallback = _resolve_path(output_paths.get("judged_candidates_jsonl"))
        if fallback is not None:
            return fallback
    default_path = summary_path.parent / "judged_candidates.jsonl"
    return default_path if default_path.exists() else None


def _ordered_candidate_ids(rows: list[dict[str, Any]]) -> list[str]:
    ranked_items: list[tuple[int, int, str]] = []
    for index, row in enumerate(rows, start=1):
        candidate_id = _candidate_id(row)
        if candidate_id is None:
            continue
        ranked_items.append((_rank_value(row.get("rank"), default=index), index, candidate_id))
    ranked_items.sort()
    return [candidate_id for _, _, candidate_id in ranked_items]


def _judged_rank_and_label_maps(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, int], dict[str, int]]:
    rank_by_item: dict[str, int] = {}
    label_by_item: dict[str, int] = {}
    for index, row in enumerate(rows, start=1):
        candidate_id = _candidate_id(row)
        if candidate_id is None:
            continue
        rank_by_item[candidate_id] = _rank_value(row.get("rank"), default=index)
        label_value = _label_value(row.get("label"))
        if label_value is not None:
            label_by_item[candidate_id] = label_value
    return rank_by_item, label_by_item


def _candidate_id(row: dict[str, Any]) -> str | None:
    return _optional_str(row.get("candidate_openalex_id")) or _optional_str(row.get("openalex_id"))


def _label_value(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _rank_value(value: Any, *, default: int) -> int:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return default


def _shared_top_k(primary_top_k: int | None, secondary_top_k: int | None, *, fallback: int) -> int:
    candidates = [value for value in (primary_top_k, secondary_top_k) if value is not None and value > 0]
    if candidates:
        return min(candidates)
    return max(fallback, 0)


def _is_common_completed(row: dict[str, Any]) -> bool:
    return (
        str(row.get("primary_status", "")).strip().lower() == "completed"
        and str(row.get("secondary_status", "")).strip().lower() == "completed"
    )


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    try:
        raw_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _resolve_path(value: Any) -> Path | None:
    raw_value = _optional_str(value)
    if raw_value is None:
        return None
    path = Path(raw_value).expanduser()
    if path.is_absolute():
        return path
    return (REPO_ROOT / path).resolve()


def _positive_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float) and value.is_integer():
        converted = int(value)
        return converted if converted > 0 else None
    return None


def _int_or_zero(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return 0


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _mean_or_none(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def _median_or_none(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
