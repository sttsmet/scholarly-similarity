from __future__ import annotations

import json
from pathlib import Path

from src.eval.benchmark import (
    AggregateMetricStats,
    BatchAggregateEvalResult,
    BatchOutputPaths,
    SeedBatchManifest,
    SeedBatchOptions,
)
from src.ui.batch_loader import BatchLoadError, load_batch_bundle


def _write_valid_batch(batch_dir: Path, *, include_seed_runs: bool = True) -> None:
    output_paths = BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )
    manifest = SeedBatchManifest(
        batch_id="batch_ui",
        batch_dir=str(batch_dir),
        seeds_csv="data/benchmarks/seeds.csv",
        theory_config="runs/example/theory_snapshot.yaml",
        created_at="2026-03-28T00:00:00+00:00",
        completed_at="2026-03-28T00:05:00+00:00",
        status="completed",
        seed_count=2,
        completed_seed_count=1,
        failed_seed_count=1,
        options=SeedBatchOptions(
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
            top_k=10,
            label_source="silver",
            refresh=False,
        ),
        output_paths=output_paths,
    )
    aggregate_summary = BatchAggregateEvalResult(
        batch_id="batch_ui",
        batch_dir=str(batch_dir),
        aggregated_at="2026-03-28T00:06:00+00:00",
        seed_count=2,
        completed_seed_count=1,
        failed_seed_count=1,
        ranking_metric="ndcg_at_k",
        metric_aggregates={
            "precision_at_k": AggregateMetricStats(count=1, mean=0.8, median=0.8, std=None, spread=0.0, min=0.8, max=0.8),
            "recall_at_k": AggregateMetricStats(count=1, mean=0.5, median=0.5, std=None, spread=0.0, min=0.5, max=0.5),
            "ndcg_at_k": AggregateMetricStats(count=1, mean=0.7, median=0.7, std=None, spread=0.0, min=0.7, max=0.7),
            "brier_score": AggregateMetricStats(count=1, mean=0.1, median=0.1, std=None, spread=0.0, min=0.1, max=0.1),
            "expected_calibration_error": AggregateMetricStats(count=1, mean=0.2, median=0.2, std=None, spread=0.0, min=0.2, max=0.2),
        },
        best_seeds=[
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
                "experiment_id": "batch_ui",
                "seed_openalex_id": "https://openalex.org/WSEED1",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 0.7,
                "precision_at_k": 0.8,
                "recall_at_k": 0.5,
                "ndcg_at_k": 0.7,
                "brier_score": 0.1,
                "expected_calibration_error": 0.2,
                "evaluation_summary_json": str(batch_dir / "seed_one_evaluation_summary.json"),
                "evaluation_cases_json": str(batch_dir / "seed_one_evaluation_cases.json"),
            }
        ],
        worst_seeds=[
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
                "experiment_id": "batch_ui",
                "seed_openalex_id": "https://openalex.org/WSEED1",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 0.7,
                "precision_at_k": 0.8,
                "recall_at_k": 0.5,
                "ndcg_at_k": 0.7,
                "brier_score": 0.1,
                "expected_calibration_error": 0.2,
                "evaluation_summary_json": str(batch_dir / "seed_one_evaluation_summary.json"),
                "evaluation_cases_json": str(batch_dir / "seed_one_evaluation_cases.json"),
            }
        ],
        failed_seeds=[
            {
                "batch_index": 2,
                "doi": "<doi_2>",
                "status": "failed",
                "run_dir": str(batch_dir / ".." / ".." / "seed_two"),
                "experiment_id": "batch_ui",
                "theory_config": "runs/example/theory_snapshot.yaml",
                "error_type": "OpenAlexNotFoundError",
                "error_message": "OpenAlex work not found for DOI: <doi_2>",
            }
        ],
        output_paths=output_paths,
    )
    seed_table_rows = [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
            "experiment_id": "batch_ui",
            "theory_config": "runs/example/theory_snapshot.yaml",
            "seed_openalex_id": "https://openalex.org/WSEED1",
            "candidate_count": 20,
            "judged_count": 20,
            "evaluation_summary_json": str(batch_dir / "seed_one_evaluation_summary.json"),
            "evaluation_cases_json": str(batch_dir / "seed_one_evaluation_cases.json"),
            "precision_at_k": 0.8,
            "recall_at_k": 0.5,
            "ndcg_at_k": 0.7,
            "brier_score": 0.1,
            "expected_calibration_error": 0.2,
            "error_type": None,
            "error_message": None,
        },
        {
            "batch_index": 2,
            "doi": "<doi_2>",
            "status": "failed",
            "run_dir": str(batch_dir / ".." / ".." / "seed_two"),
            "experiment_id": "batch_ui",
            "theory_config": "runs/example/theory_snapshot.yaml",
            "precision_at_k": None,
            "recall_at_k": None,
            "ndcg_at_k": None,
            "brier_score": None,
            "expected_calibration_error": None,
            "error_type": "OpenAlexNotFoundError",
            "error_message": "OpenAlex work not found for DOI: <doi_2>",
        },
    ]
    seed_runs_rows = [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "run_id": "seed_one",
            "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
            "experiment_id": "batch_ui",
            "theory_config": "runs/example/theory_snapshot.yaml",
            "started_at": "2026-03-28T00:00:00+00:00",
            "completed_at": "2026-03-28T00:01:00+00:00",
            "duration_seconds": 60.0,
            "reused_existing_run": True,
            "corpus_manifest_json": str(batch_dir / "seed_one_manifest.json"),
            "silver_labels_csv": str(batch_dir / "seed_one_silver_labels.csv"),
            "experiment_dir": str(batch_dir / ".." / ".." / "seed_one" / "experiments" / "batch_ui"),
            "experiment_manifest_json": str(batch_dir / "seed_one_experiment_manifest.json"),
            "evaluation_summary_json": str(batch_dir / "seed_one_evaluation_summary.json"),
            "evaluation_cases_json": str(batch_dir / "seed_one_evaluation_cases.json"),
            "seed_openalex_id": "https://openalex.org/WSEED1",
            "candidate_count": 20,
            "judged_count": 20,
            "metrics": {"ndcg_at_k": 0.7, "precision_at_k": 0.8},
            "failed_stage": None,
            "error_type": None,
            "error_message": None,
        },
        {
            "batch_index": 2,
            "doi": "<doi_2>",
            "status": "failed",
            "run_id": "seed_two",
            "run_dir": str(batch_dir / ".." / ".." / "seed_two"),
            "experiment_id": "batch_ui",
            "theory_config": "runs/example/theory_snapshot.yaml",
            "started_at": "2026-03-28T00:01:00+00:00",
            "completed_at": "2026-03-28T00:01:10+00:00",
            "duration_seconds": 10.0,
            "reused_existing_run": False,
            "corpus_manifest_json": None,
            "silver_labels_csv": None,
            "experiment_dir": None,
            "experiment_manifest_json": None,
            "evaluation_summary_json": None,
            "evaluation_cases_json": None,
            "seed_openalex_id": None,
            "candidate_count": None,
            "judged_count": None,
            "metrics": None,
            "failed_stage": "build-local-corpus",
            "error_type": "OpenAlexNotFoundError",
            "error_message": "OpenAlex work not found for DOI: <doi_2>",
        },
    ]
    worst_cases_payload = {
        "batch_id": "batch_ui",
        "batch_dir": str(batch_dir),
        "generated_at": "2026-03-28T00:06:00+00:00",
        "ranking_metric": "ndcg_at_k",
        "best_seeds": [
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 0.7,
                "cases": {"top_false_positives": [], "top_strong_relevants": []},
            }
        ],
        "worst_seeds": [
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 0.7,
                "cases": {"top_false_positives": [], "top_strong_relevants": []},
            }
        ],
        "failed_seeds": [
            {
                "batch_index": 2,
                "doi": "<doi_2>",
                "status": "failed",
                "error_type": "OpenAlexNotFoundError",
                "error_message": "OpenAlex work not found for DOI: <doi_2>",
            }
        ],
    }

    batch_dir.mkdir(parents=True, exist_ok=True)
    (batch_dir / "batch_manifest.json").write_text(
        json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (batch_dir / "aggregate_summary.json").write_text(
        json.dumps(aggregate_summary.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    with (batch_dir / "seed_table.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in seed_table_rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    (batch_dir / "worst_cases.json").write_text(
        json.dumps(worst_cases_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    if include_seed_runs:
        with (batch_dir / "seed_runs.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
            for row in seed_runs_rows:
                handle.write(json.dumps(row, sort_keys=True))
                handle.write("\n")


def test_load_batch_bundle_reads_valid_directory(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ui"
    _write_valid_batch(batch_dir)

    bundle = load_batch_bundle(batch_dir)

    assert bundle.manifest.batch_id == "batch_ui"
    assert bundle.aggregate_summary.completed_seed_count == 1
    assert len(bundle.seed_table_rows) == 2
    assert bundle.seed_rows_by_batch_index[2]["failed_stage"] == "build-local-corpus"
    assert bundle.seed_runs_by_batch_index[1]["reused_existing_run"] is True


def test_load_batch_bundle_fails_when_required_file_is_missing(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ui"
    _write_valid_batch(batch_dir)
    (batch_dir / "worst_cases.json").unlink()

    try:
        load_batch_bundle(batch_dir)
    except BatchLoadError as exc:
        assert "worst_cases.json" in str(exc)
    else:
        raise AssertionError("Expected BatchLoadError for missing worst_cases.json")


def test_load_batch_bundle_fails_on_malformed_json(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ui"
    _write_valid_batch(batch_dir)
    (batch_dir / "aggregate_summary.json").write_text("{not-valid-json", encoding="utf-8")

    try:
        load_batch_bundle(batch_dir)
    except BatchLoadError as exc:
        assert "aggregate_summary.json" in str(exc)
        assert "Malformed JSON" in str(exc)
    else:
        raise AssertionError("Expected BatchLoadError for malformed aggregate_summary.json")


def test_load_batch_bundle_fails_on_malformed_jsonl(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ui"
    _write_valid_batch(batch_dir)
    (batch_dir / "seed_table.jsonl").write_text('{"batch_index": 1}\n{bad-jsonl\n', encoding="utf-8")

    try:
        load_batch_bundle(batch_dir)
    except BatchLoadError as exc:
        assert "seed_table.jsonl" in str(exc)
        assert "Malformed JSONL" in str(exc)
    else:
        raise AssertionError("Expected BatchLoadError for malformed seed_table.jsonl")


def test_load_batch_bundle_allows_partial_metric_rows(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_ui"
    _write_valid_batch(batch_dir, include_seed_runs=False)
    with (batch_dir / "seed_table.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "batch_index": 1,
                    "doi": "10.1038/nphys1170",
                    "status": "completed",
                    "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
                    "experiment_id": "batch_ui",
                    "precision_at_k": 0.8,
                },
                sort_keys=True,
            )
        )
        handle.write("\n")

    bundle = load_batch_bundle(batch_dir)

    row = bundle.seed_table_rows[0]
    assert row["precision_at_k"] == 0.8
    assert row["recall_at_k"] is None
    assert row["ndcg_at_k"] is None
    assert row["metrics"] == {"precision_at_k": 0.8}
