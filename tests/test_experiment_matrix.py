from __future__ import annotations

import json
from pathlib import Path

from src.ui.experiment_matrix import (
    UNCLASSIFIED_COHORT_KEY,
    build_cohort_rows,
    build_experiment_batch_row,
    build_experiment_cohort_key,
    build_experiment_detail,
    build_experiment_table_rows,
    choose_default_leaderboard_metric,
    filter_experiment_rows,
    find_experiment_row,
    group_experiment_cohorts,
    scan_experiment_batches,
    sort_experiment_rows,
)


def test_build_experiment_batch_row_prefers_run_context_for_cohort_fields(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_010"
    batch_dir.mkdir(parents=True)
    row = build_experiment_batch_row(
        batch_dir=batch_dir,
        manifest_payload={
            "batch_id": "batch_010",
            "created_at": "2026-04-01T10:00:00Z",
            "status": "completed",
            "seed_count": 10,
            "completed_seed_count": 10,
            "failed_seed_count": 0,
            "theory_config": "configs/theory_v001.yaml",
            "seeds_csv": "data/benchmarks/original.csv",
            "options": {
                "max_references": 5,
                "max_related": 5,
                "max_hard_negatives": 5,
                "top_k": 5,
                "label_source": "silver",
            },
        },
        aggregate_payload={
            "metric_aggregates": {
                "ndcg_at_k": {"mean": 0.9, "median": 0.92},
                "precision_at_k": {"mean": 0.8, "median": 0.8},
            }
        },
        run_context_payload={
            "seeds_csv": "runs/benchmark_curations/curation_001/curated_seeds.csv",
            "max_references": 10,
            "max_related": 10,
            "max_hard_negatives": 10,
            "top_k": 10,
            "label_source": "silver",
            "launch_source_type": "launch_profile",
            "accepted_baseline_id": "baseline_001",
            "benchmark_preset_id": "benchmark_curated_001",
            "eval_preset_id": "eval_micro_001",
            "launch_profile_id": "launch_baseline_001",
            "source_curation_id": "curation_001",
        },
        manifest_mtime=1711965600.0,
    )

    assert row.seeds_csv == "runs/benchmark_curations/curation_001/curated_seeds.csv"
    assert row.max_references == 10
    assert row.top_k == 10
    assert row.benchmark_preset_id == "benchmark_curated_001"
    assert row.evaluation_mode == "silver_provenance_regression"
    assert row.metric_means["ndcg_at_k"] == 0.9
    assert row.metric_medians["ndcg_at_k"] == 0.92
    assert row.comparable is True


def test_build_experiment_cohort_key_requires_complete_comparability_fields() -> None:
    cohort_key, summary, missing = build_experiment_cohort_key(
        seeds_csv="data/benchmarks/seeds.csv",
        max_references=10,
        max_related=None,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
    )

    assert cohort_key is None
    assert "Unclassified" in summary
    assert missing == ["max_related"]


def test_group_experiment_cohorts_includes_unclassified_bucket(tmp_path: Path) -> None:
    comparable = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_010",
        manifest_payload={
            "batch_id": "batch_010",
            "created_at": "2026-04-01T10:00:00Z",
            "status": "completed",
            "seeds_csv": "data/benchmarks/seeds.csv",
            "options": {
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={},
        run_context_payload=None,
        manifest_mtime=1711965600.0,
    )
    unclassified = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_011",
        manifest_payload={
            "batch_id": "batch_011",
            "created_at": "2026-04-01T11:00:00Z",
            "status": "completed",
            "seeds_csv": "data/benchmarks/seeds.csv",
            "options": {
                "max_references": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={},
        run_context_payload=None,
        manifest_mtime=1711969200.0,
    )

    cohorts = group_experiment_cohorts([comparable, unclassified])
    cohort_rows = build_cohort_rows(cohorts)

    assert len(cohorts) == 2
    assert any(cohort.cohort_key == UNCLASSIFIED_COHORT_KEY for cohort in cohorts)
    assert any(row["comparable"] == "no" for row in cohort_rows)


def test_choose_default_leaderboard_metric_prefers_ndcg_when_available(tmp_path: Path) -> None:
    rows = [
        build_experiment_batch_row(
            batch_dir=tmp_path / "runs" / "batches" / "batch_010",
            manifest_payload={"batch_id": "batch_010", "created_at": "2026-04-01T10:00:00Z"},
            aggregate_payload={"metric_aggregates": {"precision_at_k": {"mean": 0.7}, "ndcg_at_k": {"mean": 0.8}}},
            run_context_payload=None,
            manifest_mtime=1711965600.0,
        )
    ]

    assert choose_default_leaderboard_metric(rows) == "ndcg_at_k"


def test_sort_experiment_rows_respects_metric_direction_and_missing_values(tmp_path: Path) -> None:
    better_higher = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_high",
        manifest_payload={"batch_id": "batch_high", "created_at": "2026-04-01T10:00:00Z", "status": "completed"},
        aggregate_payload={"metric_aggregates": {"ndcg_at_k": {"mean": 0.9}, "brier_score": {"mean": 0.2}}},
        run_context_payload=None,
        manifest_mtime=1711965600.0,
    )
    worse_higher = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_low",
        manifest_payload={"batch_id": "batch_low", "created_at": "2026-04-01T11:00:00Z", "status": "completed"},
        aggregate_payload={"metric_aggregates": {"ndcg_at_k": {"mean": 0.7}, "brier_score": {"mean": 0.4}}},
        run_context_payload=None,
        manifest_mtime=1711969200.0,
    )
    missing_metric = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_missing",
        manifest_payload={"batch_id": "batch_missing", "created_at": "2026-04-01T12:00:00Z", "status": "completed"},
        aggregate_payload={},
        run_context_payload=None,
        manifest_mtime=1711972800.0,
    )

    sorted_ndcg = sort_experiment_rows([worse_higher, missing_metric, better_higher], leaderboard_metric="ndcg_at_k")
    sorted_brier = sort_experiment_rows([worse_higher, missing_metric, better_higher], leaderboard_metric="brier_score")

    assert [row.batch_id for row in sorted_ndcg] == ["batch_high", "batch_low", "batch_missing"]
    assert [row.batch_id for row in sorted_brier] == ["batch_high", "batch_low", "batch_missing"]


def test_filter_find_and_detail_helpers_work_with_selected_cohort(tmp_path: Path) -> None:
    comparable = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_010",
        manifest_payload={
            "batch_id": "batch_010",
            "created_at": "2026-04-01T10:00:00Z",
            "status": "completed",
            "theory_config": "configs/theory_a.yaml",
            "seeds_csv": "data/benchmarks/seeds.csv",
            "options": {
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={"metric_aggregates": {"ndcg_at_k": {"mean": 0.8, "median": 0.82}}},
        run_context_payload={"accepted_baseline_id": "baseline_001", "launch_source_type": "run_batch_form"},
        manifest_mtime=1711965600.0,
    )
    other = build_experiment_batch_row(
        batch_dir=tmp_path / "runs" / "batches" / "batch_011",
        manifest_payload={
            "batch_id": "batch_011",
            "created_at": "2026-04-01T11:00:00Z",
            "status": "running",
            "theory_config": "configs/theory_b.yaml",
            "seeds_csv": "data/benchmarks/other.csv",
            "options": {
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
            },
        },
        aggregate_payload={},
        run_context_payload=None,
        manifest_mtime=1711969200.0,
    )

    filtered = filter_experiment_rows(
        [comparable, other],
        cohort_key=comparable.cohort_key,
        statuses=["completed"],
        search_text="baseline_001",
    )
    found = find_experiment_row([comparable, other], "batch_010")
    detail = build_experiment_detail(comparable)
    table_rows = build_experiment_table_rows(filtered, leaderboard_metric="ndcg_at_k")

    assert filtered == [comparable]
    assert found == comparable
    assert detail["run_context_summary"]["accepted_baseline_id"] == "baseline_001"
    assert table_rows[0]["ndcg_at_k_mean"] == 0.8


def test_scan_experiment_batches_skips_malformed_manifests_and_keeps_old_batches(tmp_path: Path) -> None:
    good_dir = tmp_path / "runs" / "batches" / "batch_010"
    good_dir.mkdir(parents=True)
    (good_dir / "batch_manifest.json").write_text(
        json.dumps(
            {
                "batch_id": "batch_010",
                "created_at": "2026-04-01T10:00:00Z",
                "status": "completed",
                "seed_count": 10,
                "completed_seed_count": 10,
                "failed_seed_count": 0,
                "theory_config": "configs/theory_v001.yaml",
                "seeds_csv": "data/benchmarks/seeds.csv",
                "options": {
                    "max_references": 10,
                    "max_related": 10,
                    "max_hard_negatives": 10,
                    "top_k": 10,
                    "label_source": "silver",
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (good_dir / "aggregate_summary.json").write_text(
        json.dumps({"metric_aggregates": {"ndcg_at_k": {"mean": 0.8, "median": 0.81}}}, indent=2),
        encoding="utf-8",
    )
    broken_dir = tmp_path / "runs" / "batches" / "broken_batch"
    broken_dir.mkdir(parents=True)
    (broken_dir / "batch_manifest.json").write_text("{not json", encoding="utf-8")

    rows, warnings = scan_experiment_batches(tmp_path)

    assert len(rows) == 1
    assert rows[0].batch_id == "batch_010"
    assert rows[0].run_context_payload is None
    assert warnings
    assert "broken_batch" in warnings[0]
