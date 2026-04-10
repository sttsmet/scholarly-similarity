from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.eval.metrics import count_pair_order_reversals, mean_value_by_label, pairwise_label_order_stats  # noqa: E402
from src.ui.batch_loader import BatchUiBundle, load_batch_bundle  # noqa: E402
from src.ui.comparison import (  # noqa: E402
    align_common_seed_rows,
    common_completed_seed_count,
    comparison_metric_summary,
    compatibility_warnings,
    movement_diagnostics_payload,
    paired_metric_rows,
)


LINEAGE_STRATUMS = (
    "strong_lineage",
    "indirect_lineage",
    "ambiguous_middle",
    "provenance_weak",
    "hard_negative_or_distractor",
)
LABEL_PAIRS = ((2, 1), (2, 0), (1, 0))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit graph-path separability and benchmark-directional behavior from existing batch artifacts.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--batch-dir", required=True, help="Primary batch directory to audit.")
    parser.add_argument(
        "--comparison-batch-dir",
        default="",
        help="Optional secondary batch directory for directional comparison.",
    )
    parser.add_argument(
        "--output-path",
        default="",
        help="Optional JSON output path. Defaults under runs/benchmark_audits/.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    primary_bundle = load_batch_bundle(args.batch_dir)
    secondary_bundle = load_batch_bundle(args.comparison_batch_dir) if str(args.comparison_batch_dir).strip() else None
    report = build_audit_report(primary_bundle=primary_bundle, secondary_bundle=secondary_bundle)

    output_path = _resolve_output_path(
        batch_id=primary_bundle.manifest.batch_id,
        comparison_batch_id=secondary_bundle.manifest.batch_id if secondary_bundle is not None else None,
        provided_path=args.output_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(output_path)
    return 0


def build_audit_report(
    *,
    primary_bundle: BatchUiBundle,
    secondary_bundle: BatchUiBundle | None,
) -> dict[str, Any]:
    primary_rows = _load_judged_rows(primary_bundle)
    report: dict[str, Any] = {
        "generated_at": _utc_timestamp(),
        "primary_batch": _batch_metadata(primary_bundle),
        "overall_metrics": _aggregate_metric_payload(primary_bundle),
        "slice_metrics": _slice_metrics(primary_rows),
        "direct_citation_zero_conditioned": _slice_metrics(
            [row for row in primary_rows if _direct_citation_zero(row)]
        ),
        "feature_separability_by_label_pair": _feature_separability_by_label_pair(primary_rows),
    }

    if secondary_bundle is not None:
        secondary_rows = _load_judged_rows(secondary_bundle)
        report["comparison_batch"] = _batch_metadata(secondary_bundle)
        report["directional_comparison"] = _directional_comparison(primary_bundle, secondary_bundle)
        report["slice_directional_metrics"] = _slice_directional_metrics(
            primary_rows=primary_rows,
            secondary_rows=secondary_rows,
        )

    return report


def _load_judged_rows(bundle: BatchUiBundle) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for seed_run in bundle.seed_run_rows:
        if str(seed_run.get("status", "")).strip().lower() != "completed":
            continue
        judged_path = (
            _optional_path(seed_run.get("mode_judged_candidates_jsonl"))
            or _optional_path(seed_run.get("judged_candidates_jsonl"))
        )
        if judged_path is None or not judged_path.exists():
            continue
        for raw_line in judged_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            payload["_batch_id"] = bundle.manifest.batch_id
            payload["_doi"] = seed_run.get("doi")
            payload["_stratum"] = _lineage_stratum(payload)
            rows.append(payload)
    return rows


def _batch_metadata(bundle: BatchUiBundle) -> dict[str, Any]:
    options = getattr(bundle.manifest, "options", None)
    return {
        "batch_id": bundle.manifest.batch_id,
        "batch_dir": str(bundle.batch_dir),
        "seed_count": bundle.manifest.seed_count,
        "completed_seed_count": bundle.manifest.completed_seed_count,
        "failed_seed_count": bundle.manifest.failed_seed_count,
        "evaluation_mode": getattr(options, "evaluation_mode", None),
        "benchmark_dataset_id": getattr(options, "benchmark_dataset_id", None),
        "benchmark_maturity_tier": getattr(options, "benchmark_maturity_tier", None),
        "promotion_ready": getattr(options, "promotion_ready", None),
    }


def _aggregate_metric_payload(bundle: BatchUiBundle) -> dict[str, Any]:
    metric_aggregates = getattr(bundle.aggregate_summary, "metric_aggregates", {})
    payload: dict[str, Any] = {}
    for metric_name in (
        "precision_at_k",
        "recall_at_k",
        "ndcg_at_k",
        "brier_score",
        "expected_calibration_error",
    ):
        stats = metric_aggregates.get(metric_name)
        if stats is None:
            continue
        payload[metric_name] = stats.model_dump(mode="json")
    return payload


def _slice_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped_rows: dict[str, list[dict[str, Any]]] = {name: [] for name in LINEAGE_STRATUMS}
    for row in rows:
        grouped_rows.setdefault(_lineage_stratum(row), []).append(row)

    return {
        slice_name: _slice_summary(grouped_rows.get(slice_name, []))
        for slice_name in LINEAGE_STRATUMS
    }


def _slice_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    label_distribution: dict[str, int] = defaultdict(int)
    scored_pairs: list[tuple[int, float]] = []
    graph_pairs: list[tuple[int, float]] = []
    for row in rows:
        label = row.get("label")
        if label is None:
            continue
        label_distribution[str(int(label))] += 1
        scored_pairs.append((int(label), float(row.get("sim", 0.0))))
        graph_value = _feature_value(row, "graph_path")
        if graph_value is not None:
            graph_pairs.append((int(label), graph_value))

    pairwise_counts = _aggregate_pairwise_by_seed(rows)
    return {
        "pair_count": len(rows),
        "label_distribution": dict(sorted(label_distribution.items())),
        "mean_sim_by_label": mean_value_by_label(scored_pairs),
        "mean_graph_path_by_label": mean_value_by_label(graph_pairs),
        "pairwise_label_order_accuracy": pairwise_counts["pairwise_label_order_accuracy"],
        "weighted_pairwise_label_order_accuracy": pairwise_counts["weighted_pairwise_label_order_accuracy"],
        "cross_label_order_reversal_count": pairwise_counts["cross_label_order_reversal_count"],
    }


def _directional_comparison(primary_bundle: BatchUiBundle, secondary_bundle: BatchUiBundle) -> dict[str, Any]:
    aligned_rows = align_common_seed_rows(
        primary_bundle.seed_table_rows,
        secondary_bundle.seed_table_rows,
    )
    paired_rows = paired_metric_rows(
        aligned_rows,
        metric_name="ndcg_at_k",
        status_mode="common completed only",
    )
    summary = comparison_metric_summary(paired_rows)
    primary_aggregate = getattr(primary_bundle.aggregate_summary, "metric_aggregates", {})
    secondary_aggregate = getattr(secondary_bundle.aggregate_summary, "metric_aggregates", {})

    metric_deltas: dict[str, float | None] = {}
    for metric_name in (
        "precision_at_k",
        "recall_at_k",
        "ndcg_at_k",
        "brier_score",
        "expected_calibration_error",
    ):
        primary_stats = primary_aggregate.get(metric_name)
        secondary_stats = secondary_aggregate.get(metric_name)
        primary_mean = getattr(primary_stats, "mean", None) if primary_stats is not None else None
        secondary_mean = getattr(secondary_stats, "mean", None) if secondary_stats is not None else None
        if primary_mean is None or secondary_mean is None:
            metric_deltas[metric_name] = None
        else:
            metric_deltas[metric_name] = round(float(secondary_mean) - float(primary_mean), 6)

    return {
        "metric_deltas": metric_deltas,
        "common_completed_seed_count": common_completed_seed_count(aligned_rows),
        "compatibility_warnings": compatibility_warnings(
            primary_bundle.manifest,
            secondary_bundle.manifest,
        ),
        "selected_metric_summary": {
            "primary_mean": summary.primary_mean,
            "secondary_mean": summary.secondary_mean,
            "improvement_delta_mean": summary.improvement_delta_mean,
            "wins": summary.wins,
            "losses": summary.losses,
            "ties": summary.ties,
        },
        "movement_diagnostics": movement_diagnostics_payload(summary.movement_diagnostics),
    }


def _slice_directional_metrics(
    *,
    primary_rows: list[dict[str, Any]],
    secondary_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    secondary_lookup = {
        (_seed_key(row), _candidate_key(row)): row
        for row in secondary_rows
    }
    rows_by_slice_and_seed: dict[str, dict[str, list[tuple[dict[str, Any], dict[str, Any]]]]] = {
        slice_name: defaultdict(list)
        for slice_name in LINEAGE_STRATUMS
    }

    for primary_row in primary_rows:
        lookup_key = (_seed_key(primary_row), _candidate_key(primary_row))
        secondary_row = secondary_lookup.get(lookup_key)
        if secondary_row is None or primary_row.get("label") is None:
            continue
        rows_by_slice_and_seed[_lineage_stratum(primary_row)][_seed_key(primary_row)].append(
            (primary_row, secondary_row)
        )

    return {
        slice_name: _slice_directional_summary(rows_by_seed)
        for slice_name, rows_by_seed in rows_by_slice_and_seed.items()
    }


def _slice_directional_summary(rows_by_seed: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]]) -> dict[str, Any]:
    concordant = 0
    discordant = 0
    weighted_concordant = 0.0
    weighted_discordant = 0.0
    cross_label_reversals = 0

    baseline_concordant = 0
    baseline_discordant = 0
    baseline_weighted_concordant = 0.0
    baseline_weighted_discordant = 0.0

    for paired_rows in rows_by_seed.values():
        labels_by_item = {
            _candidate_key(primary_row): int(primary_row["label"])
            for primary_row, _ in paired_rows
        }
        baseline_ranks = {
            _candidate_key(primary_row): int(primary_row["rank"])
            for primary_row, _ in paired_rows
        }
        candidate_ranks = {
            _candidate_key(secondary_row): int(secondary_row["rank"])
            for _, secondary_row in paired_rows
        }

        baseline_stats = pairwise_label_order_stats(baseline_ranks, labels_by_item)
        candidate_stats = pairwise_label_order_stats(candidate_ranks, labels_by_item)
        baseline_concordant += int(baseline_stats["concordant_pair_count"])
        baseline_discordant += int(baseline_stats["discordant_pair_count"])
        baseline_weighted_concordant += float(baseline_stats["weighted_concordant_pair_count"])
        baseline_weighted_discordant += float(baseline_stats["weighted_discordant_pair_count"])
        concordant += int(candidate_stats["concordant_pair_count"])
        discordant += int(candidate_stats["discordant_pair_count"])
        weighted_concordant += float(candidate_stats["weighted_concordant_pair_count"])
        weighted_discordant += float(candidate_stats["weighted_discordant_pair_count"])
        _, cross_label, _ = count_pair_order_reversals(
            baseline_ranks,
            candidate_ranks,
            labels_by_item=labels_by_item,
        )
        cross_label_reversals += cross_label

    baseline_accuracy = _accuracy(baseline_concordant, baseline_discordant)
    candidate_accuracy = _accuracy(concordant, discordant)
    baseline_weighted_accuracy = _weighted_accuracy(
        baseline_weighted_concordant,
        baseline_weighted_discordant,
    )
    candidate_weighted_accuracy = _weighted_accuracy(
        weighted_concordant,
        weighted_discordant,
    )
    return {
        "pairwise_label_order_accuracy": candidate_accuracy,
        "pairwise_label_order_accuracy_delta": _delta(candidate_accuracy, baseline_accuracy),
        "weighted_pairwise_label_order_accuracy": candidate_weighted_accuracy,
        "weighted_pairwise_label_order_accuracy_delta": _delta(
            candidate_weighted_accuracy,
            baseline_weighted_accuracy,
        ),
        "cross_label_order_reversal_count": cross_label_reversals,
    }


def _feature_separability_by_label_pair(rows: list[dict[str, Any]]) -> dict[str, Any]:
    feature_names = sorted(
        {
            feature_name
            for row in rows
            for feature_name in dict(row.get("feature_values") or {}).keys()
        }
    )
    payload: dict[str, Any] = {}
    for feature_name in feature_names:
        payload[feature_name] = {}
        for higher_label, lower_label in LABEL_PAIRS:
            payload[feature_name][f"{higher_label}_over_{lower_label}"] = _feature_pair_separability(
                rows=rows,
                feature_name=feature_name,
                higher_label=higher_label,
                lower_label=lower_label,
            )
    return payload


def _feature_pair_separability(
    *,
    rows: list[dict[str, Any]],
    feature_name: str,
    higher_label: int,
    lower_label: int,
) -> dict[str, Any]:
    higher_values = [
        value
        for row in rows
        if row.get("label") == higher_label
        for value in [_feature_value(row, feature_name)]
        if value is not None
    ]
    lower_values = [
        value
        for row in rows
        if row.get("label") == lower_label
        for value in [_feature_value(row, feature_name)]
        if value is not None
    ]
    concordant = 0
    discordant = 0
    tied = 0
    for higher_value in higher_values:
        for lower_value in lower_values:
            if higher_value > lower_value:
                concordant += 1
            elif higher_value < lower_value:
                discordant += 1
            else:
                tied += 1
    return {
        "higher_label_count": len(higher_values),
        "lower_label_count": len(lower_values),
        "higher_label_mean": _mean_or_none(higher_values),
        "lower_label_mean": _mean_or_none(lower_values),
        "mean_gap": (
            round(_mean_or_none(higher_values) - _mean_or_none(lower_values), 6)
            if higher_values and lower_values
            else None
        ),
        "concordant_pair_count": concordant,
        "discordant_pair_count": discordant,
        "tied_pair_count": tied,
        "pairwise_order_accuracy": _accuracy(concordant, discordant),
    }


def _aggregate_pairwise_by_seed(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("label") is None:
            continue
        grouped_rows[_seed_key(row)].append(row)

    concordant = 0
    discordant = 0
    weighted_concordant = 0.0
    weighted_discordant = 0.0
    for seed_rows in grouped_rows.values():
        ranks = {_candidate_key(row): int(row["rank"]) for row in seed_rows}
        labels = {_candidate_key(row): int(row["label"]) for row in seed_rows}
        stats = pairwise_label_order_stats(ranks, labels)
        concordant += int(stats["concordant_pair_count"])
        discordant += int(stats["discordant_pair_count"])
        weighted_concordant += float(stats["weighted_concordant_pair_count"])
        weighted_discordant += float(stats["weighted_discordant_pair_count"])

    return {
        "pairwise_label_order_accuracy": _accuracy(concordant, discordant),
        "weighted_pairwise_label_order_accuracy": _weighted_accuracy(
            weighted_concordant,
            weighted_discordant,
        ),
        "cross_label_order_reversal_count": 0,
    }


def _lineage_stratum(row: dict[str, Any]) -> str:
    origins = {
        str(origin).strip()
        for origin in (row.get("candidate_origins") or row.get("origin_flags") or [])
        if str(origin).strip()
    }
    rank = int(row.get("rank") or 0)
    if "seed_reference" in origins or "direct_neighbor" in origins:
        return "strong_lineage"
    if "hard_negative" in origins:
        return "hard_negative_or_distractor"
    if "seed_related" in origins and rank <= 5:
        return "indirect_lineage"
    if "seed_related" in origins:
        return "ambiguous_middle"
    if rank <= 10:
        return "ambiguous_middle"
    return "provenance_weak"


def _feature_value(row: dict[str, Any], feature_name: str) -> float | None:
    feature_values = row.get("feature_values")
    if not isinstance(feature_values, dict):
        return None
    value = feature_values.get(feature_name)
    if value is None:
        return None
    return float(value)


def _direct_citation_zero(row: dict[str, Any]) -> bool:
    direct_value = _feature_value(row, "direct_citation")
    return direct_value is not None and direct_value == 0.0


def _seed_key(row: dict[str, Any]) -> str:
    return str(row.get("seed_openalex_id"))


def _candidate_key(row: dict[str, Any]) -> str:
    return str(row.get("candidate_openalex_id"))


def _optional_path(value: object) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path


def _resolve_output_path(
    *,
    batch_id: str,
    comparison_batch_id: str | None,
    provided_path: str,
) -> Path:
    if provided_path.strip():
        candidate = Path(provided_path).expanduser()
        if not candidate.is_absolute():
            candidate = (REPO_ROOT / candidate).resolve()
        return candidate
    comparison_suffix = f"__vs__{comparison_batch_id}" if comparison_batch_id else ""
    return (
        REPO_ROOT
        / "runs"
        / "benchmark_audits"
        / f"graph_path_separability_{batch_id}{comparison_suffix}.json"
    )


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _accuracy(concordant: int, discordant: int) -> float | None:
    strict = concordant + discordant
    if strict <= 0:
        return None
    return round(concordant / strict, 6)


def _weighted_accuracy(concordant: float, discordant: float) -> float | None:
    strict = concordant + discordant
    if strict <= 0:
        return None
    return round(concordant / strict, 6)


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return round(current - baseline, 6)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
