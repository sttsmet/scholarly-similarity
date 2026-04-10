from __future__ import annotations

import json
from pathlib import Path

import src.ui.batch_runner as batch_runner
from src.config import RuntimeConfig
from src.eval.benchmark import (
    AggregateMetricStats,
    BatchAggregateEvalResult,
    BatchOutputPaths,
    SeedBatchManifest,
    SeedBatchOptions,
)
from src.ui.batch_loader import load_batch_bundle
from src.ui.batch_runner import BatchRunValidationError, build_batch_run_request, run_batch_request


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        app_name="scholarly-similarity",
        openalex_base_url="https://api.openalex.org",
        use_network=False,
        cache_dir="data/cache",
        runs_dir="runs",
        request_timeout_seconds=10.0,
    )


def _write_ui_batch(batch_dir: Path, *, batch_id: str) -> SeedBatchManifest:
    output_paths = BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )
    manifest = SeedBatchManifest(
        batch_id=batch_id,
        batch_dir=str(batch_dir),
        seeds_csv="data/benchmarks/seeds.csv",
        theory_config="configs/theory_v001.yaml",
        created_at="2026-03-28T00:00:00+00:00",
        completed_at="2026-03-28T00:05:00+00:00",
        status="completed",
        seed_count=1,
        completed_seed_count=1,
        failed_seed_count=0,
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
    aggregate = BatchAggregateEvalResult(
        batch_id=batch_id,
        batch_dir=str(batch_dir),
        aggregated_at="2026-03-28T00:05:00+00:00",
        seed_count=1,
        completed_seed_count=1,
        failed_seed_count=0,
        ranking_metric="ndcg_at_k",
        metric_aggregates={
            "precision_at_k": AggregateMetricStats(count=1, mean=0.9, median=0.9, std=None, spread=0.0, min=0.9, max=0.9),
            "recall_at_k": AggregateMetricStats(count=1, mean=0.5, median=0.5, std=None, spread=0.0, min=0.5, max=0.5),
            "ndcg_at_k": AggregateMetricStats(count=1, mean=1.0, median=1.0, std=None, spread=0.0, min=1.0, max=1.0),
            "brier_score": AggregateMetricStats(count=1, mean=0.1, median=0.1, std=None, spread=0.0, min=0.1, max=0.1),
            "expected_calibration_error": AggregateMetricStats(count=1, mean=0.2, median=0.2, std=None, spread=0.0, min=0.2, max=0.2),
        },
        best_seeds=[
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
                "experiment_id": batch_id,
                "seed_openalex_id": "https://openalex.org/WSEED1",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 1.0,
                "precision_at_k": 0.9,
                "recall_at_k": 0.5,
                "ndcg_at_k": 1.0,
                "brier_score": 0.1,
                "expected_calibration_error": 0.2,
                "evaluation_summary_json": str(batch_dir / "evaluation_summary.json"),
                "evaluation_cases_json": str(batch_dir / "evaluation_cases.json"),
            }
        ],
        worst_seeds=[
            {
                "batch_index": 1,
                "doi": "10.1038/nphys1170",
                "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
                "experiment_id": batch_id,
                "seed_openalex_id": "https://openalex.org/WSEED1",
                "ranking_metric": "ndcg_at_k",
                "ranking_value": 1.0,
                "precision_at_k": 0.9,
                "recall_at_k": 0.5,
                "ndcg_at_k": 1.0,
                "brier_score": 0.1,
                "expected_calibration_error": 0.2,
                "evaluation_summary_json": str(batch_dir / "evaluation_summary.json"),
                "evaluation_cases_json": str(batch_dir / "evaluation_cases.json"),
            }
        ],
        failed_seeds=[],
        output_paths=output_paths,
    )
    seed_table_rows = [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
            "experiment_id": batch_id,
            "theory_config": "configs/theory_v001.yaml",
            "seed_openalex_id": "https://openalex.org/WSEED1",
            "candidate_count": 20,
            "judged_count": 20,
            "evaluation_summary_json": str(batch_dir / "evaluation_summary.json"),
            "evaluation_cases_json": str(batch_dir / "evaluation_cases.json"),
            "precision_at_k": 0.9,
            "recall_at_k": 0.5,
            "ndcg_at_k": 1.0,
            "brier_score": 0.1,
            "expected_calibration_error": 0.2,
            "error_type": None,
            "error_message": None,
        }
    ]
    seed_runs_rows = [
        {
            "batch_index": 1,
            "doi": "10.1038/nphys1170",
            "status": "completed",
            "started_at": "2026-03-28T00:00:00+00:00",
            "completed_at": "2026-03-28T00:05:00+00:00",
            "duration_seconds": 300.0,
            "run_id": "seed_one",
            "run_dir": str(batch_dir / ".." / ".." / "seed_one"),
            "experiment_id": batch_id,
            "theory_config": "configs/theory_v001.yaml",
            "reused_existing_run": True,
            "corpus_manifest_json": str(batch_dir / "manifest.json"),
            "silver_labels_csv": str(batch_dir / "silver_labels.csv"),
            "experiment_dir": str(batch_dir / ".." / ".." / "seed_one" / "experiments" / batch_id),
            "experiment_manifest_json": str(batch_dir / "experiment_manifest.json"),
            "evaluation_summary_json": str(batch_dir / "evaluation_summary.json"),
            "evaluation_cases_json": str(batch_dir / "evaluation_cases.json"),
            "seed_openalex_id": "https://openalex.org/WSEED1",
            "candidate_count": 20,
            "judged_count": 20,
            "metrics": {"precision_at_k": 0.9, "ndcg_at_k": 1.0},
            "failed_stage": None,
            "error_type": None,
            "error_message": None,
        }
    ]
    worst_cases = {
        "batch_id": batch_id,
        "batch_dir": str(batch_dir),
        "generated_at": "2026-03-28T00:05:00+00:00",
        "ranking_metric": "ndcg_at_k",
        "best_seeds": [{"batch_index": 1, "doi": "10.1038/nphys1170", "ranking_metric": "ndcg_at_k", "ranking_value": 1.0, "cases": {}}],
        "worst_seeds": [{"batch_index": 1, "doi": "10.1038/nphys1170", "ranking_metric": "ndcg_at_k", "ranking_value": 1.0, "cases": {}}],
        "failed_seeds": [],
    }

    batch_dir.mkdir(parents=True, exist_ok=True)
    (batch_dir / "batch_manifest.json").write_text(json.dumps(manifest.model_dump(mode="json"), indent=2), encoding="utf-8")
    (batch_dir / "aggregate_summary.json").write_text(json.dumps(aggregate.model_dump(mode="json"), indent=2), encoding="utf-8")
    (batch_dir / "worst_cases.json").write_text(json.dumps(worst_cases, indent=2), encoding="utf-8")
    with (batch_dir / "seed_table.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in seed_table_rows:
            handle.write(json.dumps(row))
            handle.write("\n")
    with (batch_dir / "seed_runs.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for row in seed_runs_rows:
            handle.write(json.dumps(row))
            handle.write("\n")
    return manifest


def test_build_batch_run_request_resolves_paths_and_defaults(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)
    theory_path = tmp_path / "configs" / "theory.yaml"
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    theory_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    theory_path.write_text("version: v1\n", encoding="utf-8")
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_batch_run_request(
        initial_doi_context=" 10.1038/nphys1170 ",
        theory_config_path="configs/theory.yaml",
        seeds_csv_path="data/benchmarks/seeds.csv",
        batch_id="batch_003",
        runtime_loader=_runtime_config,
    )

    assert request.initial_doi_context == "10.1038/nphys1170"
    assert request.theory_config_path == theory_path.resolve()
    assert request.seeds_csv_path == seeds_path.resolve()
    assert request.batch_dir == tmp_path / "runs" / "batches" / "batch_003"
    assert request.label_source == "silver"
    assert request.top_k == 10


def test_build_batch_run_request_collects_validation_errors(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)

    try:
        build_batch_run_request(
            initial_doi_context="",
            theory_config_path="missing_theory.yaml",
            seeds_csv_path="missing_seeds.csv",
            batch_id="bad id",
            top_k=0,
            label_source="manual",
            runtime_loader=_runtime_config,
        )
    except BatchRunValidationError as exc:
        message = str(exc)
        assert "Theory config path does not exist" in message
        assert "Seeds CSV path does not exist" in message
        assert "Batch ID may contain only" in message
        assert "top_k must be >= 1." in message
        assert "Label source must be one of: silver, benchmark." in message
    else:
        raise AssertionError("Expected BatchRunValidationError")


def test_build_batch_run_request_allows_empty_initial_doi_context(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)
    theory_path = tmp_path / "configs" / "theory.yaml"
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    theory_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    theory_path.write_text("version: v1\n", encoding="utf-8")
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_batch_run_request(
        initial_doi_context="   ",
        theory_config_path=theory_path,
        seeds_csv_path=seeds_path,
        batch_id="batch_003",
        runtime_loader=_runtime_config,
    )

    assert request.initial_doi_context == ""


def test_run_batch_request_success_loads_new_bundle(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)
    theory_path = tmp_path / "configs" / "theory.yaml"
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    theory_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    theory_path.write_text("placeholder", encoding="utf-8")
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    request = build_batch_run_request(
        initial_doi_context="10.1038/nphys1170",
        theory_config_path=theory_path,
        seeds_csv_path=seeds_path,
        batch_id="batch_003",
        runtime_loader=_runtime_config,
    )

    def fake_theory_loader(path: str | Path) -> object:
        assert Path(path) == theory_path
        return object()

    def fake_run_batch_service(**kwargs) -> SeedBatchManifest:
        assert kwargs["batch_id"] == "batch_003"
        return _write_ui_batch(request.batch_dir, batch_id="batch_003")

    outcome = run_batch_request(
        request,
        theory_loader=fake_theory_loader,
        run_batch_service=fake_run_batch_service,
        batch_loader=load_batch_bundle,
    )

    assert outcome.success is True
    assert outcome.loaded_bundle is not None
    assert outcome.loaded_bundle.manifest.batch_id == "batch_003"
    assert outcome.summary is not None
    assert outcome.summary.batch_dir == str(request.batch_dir)
    assert outcome.summary.initial_doi_context == "10.1038/nphys1170"


def test_build_batch_run_request_requires_external_labels_for_independent_benchmark(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)
    theory_path = tmp_path / "configs" / "theory.yaml"
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    labels_path = tmp_path / "data" / "benchmarks" / "benchmark_labels.csv"
    theory_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    theory_path.write_text("version: v1\n", encoding="utf-8")
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    labels_path.write_text(
        "seed_openalex_id,candidate_openalex_id,label\n"
        "https://openalex.org/WSEED,https://openalex.org/WCAND1,2\n",
        encoding="utf-8",
    )

    request = build_batch_run_request(
        initial_doi_context="10.1038/nphys1170",
        theory_config_path=theory_path,
        seeds_csv_path=seeds_path,
        batch_id="batch_independent_001",
        evaluation_mode="independent_benchmark",
        benchmark_labels_path=labels_path,
        benchmark_dataset_id="benchmark_dataset_001",
        runtime_loader=_runtime_config,
    )

    assert request.evaluation_mode == "independent_benchmark"
    assert request.label_source == "benchmark"
    assert request.benchmark_labels_path == labels_path.resolve()
    assert request.benchmark_dataset_id == "benchmark_dataset_001"
    assert request.benchmark_labels_sha256 is not None


def test_run_batch_request_failure_preserves_previous_bundle_and_surfaces_partial_bundle(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(batch_runner, "REPO_ROOT", tmp_path)
    theory_path = tmp_path / "configs" / "theory.yaml"
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    theory_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    theory_path.write_text("placeholder", encoding="utf-8")
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    previous_manifest = _write_ui_batch(tmp_path / "runs" / "batches" / "batch_prev", batch_id="batch_prev")
    previous_bundle = load_batch_bundle(previous_manifest.batch_dir)

    request = build_batch_run_request(
        initial_doi_context="10.1038/nphys1170",
        theory_config_path=theory_path,
        seeds_csv_path=seeds_path,
        batch_id="batch_004",
        runtime_loader=_runtime_config,
    )

    def fake_theory_loader(path: str | Path) -> object:
        return object()

    def fake_run_batch_service(**kwargs) -> SeedBatchManifest:
        _write_ui_batch(request.batch_dir, batch_id="batch_004")
        raise RuntimeError("batch execution failed")

    outcome = run_batch_request(
        request,
        previous_bundle=previous_bundle,
        theory_loader=fake_theory_loader,
        run_batch_service=fake_run_batch_service,
        batch_loader=load_batch_bundle,
    )

    assert outcome.success is False
    assert outcome.loaded_bundle is previous_bundle
    assert outcome.partial_bundle is not None
    assert outcome.partial_bundle.manifest.batch_id == "batch_004"
    assert outcome.summary is not None
    assert outcome.summary.batch_id == "batch_004"
    assert outcome.error_message == "batch execution failed"
