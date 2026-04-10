from __future__ import annotations

import json
from pathlib import Path

import src.eval.benchmark as benchmark
from src.config import DEFAULT_THEORY_PATH, RuntimeConfig, load_theory_config
from src.eval.benchmark import (
    BENCHMARK_SCHEMA_VERSION_V1,
    BatchOutputPaths,
    SeedBatchManifest,
    SeedBatchOptions,
    SeedBatchRunRecord,
    aggregate_batch_eval,
    load_benchmark_seeds,
    run_seed_batch,
)
from src.graph.build_local_corpus import build_local_corpus_run_id
from src.ui.batch_loader import load_batch_bundle
from src.ui.comparison import ComparisonMetricSummary
from src.ui.comparison_export import build_comparison_manifest_payload
from src.ui.decision_guardrails import evaluate_decision_guardrails


def _make_record(
    *,
    openalex_id: str,
    doi: str | None,
    title: str,
    publication_year: int | None = 2020,
    referenced_works: list[str] | None = None,
    related_works: list[str] | None = None,
    primary_topic: str | None = "Physics",
    topics: list[str] | None = None,
    abstract_text: str | None = "Quantum measurement experiment",
    candidate_origins: list[str] | None = None,
) -> dict[str, object]:
    return {
        "openalex_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_year": publication_year,
        "cited_by_count": 10,
        "referenced_works": referenced_works or [],
        "related_works": related_works or [],
        "primary_topic": primary_topic,
        "topics": topics or [],
        "abstract_text": abstract_text,
        "candidate_origins": candidate_origins or [],
        "source": "openalex",
    }


def _write_batch_ready_run(run_dir: Path, *, doi: str, seed_openalex_id: str = "https://openalex.org/WSEED") -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    seed = _make_record(
        openalex_id=seed_openalex_id,
        doi=doi,
        title="Seed paper",
        publication_year=2018,
        referenced_works=["https://openalex.org/WCAND1"],
        related_works=["https://openalex.org/WCAND2"],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol for coupled systems",
    )
    candidate_one = _make_record(
        openalex_id="https://openalex.org/WCAND1",
        doi=None,
        title="Reference match",
        publication_year=2019,
        referenced_works=["https://openalex.org/W1", "https://openalex.org/W2"],
        related_works=[],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol with related systems",
        candidate_origins=["seed_reference", "direct_neighbor"],
    )
    candidate_two = _make_record(
        openalex_id="https://openalex.org/WCAND2",
        doi=None,
        title="Related match",
        publication_year=2017,
        referenced_works=["https://openalex.org/W2"],
        related_works=[],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Measurement protocol in a related system",
        candidate_origins=["seed_related"],
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed_openalex_id,
                    "dst": candidate_one["openalex_id"],
                    "edge_type": "seed_references",
                }
            )
        )
        handle.write("\n")
        handle.write(
            json.dumps(
                {
                    "src": seed_openalex_id,
                    "dst": candidate_two["openalex_id"],
                    "edge_type": "seed_related",
                }
            )
        )
        handle.write("\n")


def _write_evaluation_artifacts(
    experiment_dir: Path,
    *,
    seed_openalex_id: str,
    precision: float,
    recall: float,
    ndcg: float,
    brier: float,
    ece: float,
) -> tuple[Path, Path]:
    experiment_dir.mkdir(parents=True, exist_ok=True)
    summary_path = experiment_dir / "evaluation_summary.json"
    cases_path = experiment_dir / "evaluation_cases.json"
    judged_path = experiment_dir / "judged_candidates.jsonl"
    judged_path.write_text("", encoding="utf-8")
    cases_path.write_text(
        json.dumps(
            {
                "top_false_positives": [{"candidate_openalex_id": "https://openalex.org/WFP"}],
                "top_strong_relevants": [{"candidate_openalex_id": "https://openalex.org/WPOS"}],
                "unlabeled_top_candidates": [],
                "top_ranked_hard_negatives": [],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(
            {
                "run_dir": str(experiment_dir),
                "labels_path": str(experiment_dir / "silver_labels.csv"),
                "label_source": "silver",
                "seed_openalex_id": seed_openalex_id,
                "top_k_used": 10,
                "candidate_count": 20,
                "judged_count": 20,
                "judged_fraction": 1.0,
                "metrics": {
                    "judged_count": 20,
                    "judged_fraction": 1.0,
                    "precision_at_k": precision,
                    "recall_at_k": recall,
                    "dcg_at_k": 5.0,
                    "ndcg_at_k": ndcg,
                    "brier_score": brier,
                    "expected_calibration_error": ece,
                    "mean_sim_by_label": {"0": 0.1, "1": 0.2, "2": 0.3},
                    "mean_conf_by_label": {"0": 0.4, "1": 0.5, "2": 0.6},
                },
                "output_paths": {
                    "evaluation_summary_json": str(summary_path),
                    "judged_candidates_jsonl": str(judged_path),
                    "evaluation_cases_json": str(cases_path),
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return summary_path, cases_path


def test_load_benchmark_seeds_accepts_comments_and_deduplicates(tmp_path: Path) -> None:
    seeds_path = tmp_path / "seeds.csv"
    seeds_path.write_text(
        "\n".join(
            [
                "# seed list",
                "DOI,tag",
                "10.1038/nphys1170,alpha",
                "",
                "https://doi.org/10.1038/nphys1170,beta",
                "10.1145/example,gamma",
            ]
        ),
        encoding="utf-8",
    )

    seeds = load_benchmark_seeds(seeds_path)

    assert [seed.query_doi for seed in seeds] == ["10.1038/nphys1170", "10.1145/example"]
    assert [seed.label for seed in seeds] == ["alpha", "gamma"]


def test_load_benchmark_seeds_uses_first_column_without_header(tmp_path: Path) -> None:
    seeds_path = tmp_path / "seeds_no_header.csv"
    seeds_path.write_text(
        "\n".join(
            [
                "# comment",
                "10.1038/nphys1170,alpha",
                "10.1145/example,beta",
            ]
        ),
        encoding="utf-8",
    )

    seeds = load_benchmark_seeds(seeds_path)

    assert [seed.query_doi for seed in seeds] == ["10.1038/nphys1170", "10.1145/example"]
    assert [seed.label for seed in seeds] == ["alpha", "beta"]


def test_load_benchmark_seeds_handles_utf8_bom_header(tmp_path: Path) -> None:
    seeds_path = tmp_path / "seeds_bom.csv"
    seeds_path.write_text(
        "\ufeffdoi,tag\n10.1038/nphys1170,alpha\n<doi_2>,beta\n",
        encoding="utf-8",
    )

    seeds = load_benchmark_seeds(seeds_path)

    assert [seed.query_doi for seed in seeds] == ["10.1038/nphys1170", "<doi_2>"]
    assert [seed.label for seed in seeds] == ["alpha", "beta"]


def test_aggregate_batch_eval_computes_stats_and_best_worst(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_stats"
    output_paths = BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )
    manifest = SeedBatchManifest(
        batch_id="batch_stats",
        batch_dir=str(batch_dir),
        seeds_csv=str(tmp_path / "seeds.csv"),
        theory_config=str(DEFAULT_THEORY_PATH),
        created_at="2026-03-28T00:00:00+00:00",
        completed_at="2026-03-28T00:05:00+00:00",
        status="completed",
        seed_count=3,
        completed_seed_count=2,
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
    batch_dir.mkdir(parents=True, exist_ok=True)
    Path(output_paths.batch_manifest_json).write_text(
        json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True),
        encoding="utf-8",
    )

    summary_one, cases_one = _write_evaluation_artifacts(
        tmp_path / "runs" / "seed_one" / "experiments" / "batch_stats",
        seed_openalex_id="https://openalex.org/WSEED1",
        precision=0.9,
        recall=0.7,
        ndcg=0.8,
        brier=0.1,
        ece=0.2,
    )
    summary_two, cases_two = _write_evaluation_artifacts(
        tmp_path / "runs" / "seed_two" / "experiments" / "batch_stats",
        seed_openalex_id="https://openalex.org/WSEED2",
        precision=0.5,
        recall=0.3,
        ndcg=0.2,
        brier=0.4,
        ece=0.5,
    )

    records = [
        SeedBatchRunRecord(
            batch_index=1,
            doi="10.1038/nphys1170",
            status="completed",
            started_at="2026-03-28T00:00:00+00:00",
            completed_at="2026-03-28T00:00:10+00:00",
            duration_seconds=10.0,
            run_id="seed_one",
            run_dir=str(tmp_path / "runs" / "seed_one"),
            experiment_id="batch_stats",
            theory_config=str(DEFAULT_THEORY_PATH),
            experiment_dir=str(summary_one.parent),
            experiment_manifest_json=str(summary_one.parent / "experiment_manifest.json"),
            evaluation_summary_json=str(summary_one),
            evaluation_cases_json=str(cases_one),
            seed_openalex_id="https://openalex.org/WSEED1",
            candidate_count=20,
            judged_count=20,
        ),
        SeedBatchRunRecord(
            batch_index=2,
            doi="10.2000/example",
            status="completed",
            started_at="2026-03-28T00:00:10+00:00",
            completed_at="2026-03-28T00:00:20+00:00",
            duration_seconds=10.0,
            run_id="seed_two",
            run_dir=str(tmp_path / "runs" / "seed_two"),
            experiment_id="batch_stats",
            theory_config=str(DEFAULT_THEORY_PATH),
            experiment_dir=str(summary_two.parent),
            experiment_manifest_json=str(summary_two.parent / "experiment_manifest.json"),
            evaluation_summary_json=str(summary_two),
            evaluation_cases_json=str(cases_two),
            seed_openalex_id="https://openalex.org/WSEED2",
            candidate_count=20,
            judged_count=20,
        ),
        SeedBatchRunRecord(
            batch_index=3,
            doi="<doi_3>",
            status="failed",
            started_at="2026-03-28T00:00:20+00:00",
            completed_at="2026-03-28T00:00:21+00:00",
            duration_seconds=1.0,
            run_id="seed_three",
            run_dir=str(tmp_path / "runs" / "seed_three"),
            experiment_id="batch_stats",
            theory_config=str(DEFAULT_THEORY_PATH),
            failed_stage="build-local-corpus",
            error_type="ValueError",
            error_message="Invalid DOI",
        ),
    ]
    with Path(output_paths.seed_runs_jsonl).open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record.model_dump(mode="json"), sort_keys=True))
            handle.write("\n")

    result = aggregate_batch_eval(batch_dir=batch_dir)

    assert result.seed_count == 3
    assert result.completed_seed_count == 2
    assert result.failed_seed_count == 1
    assert result.ranking_metric == "ndcg_at_k"
    assert result.metric_aggregates["precision_at_k"].mean == 0.7
    assert result.metric_aggregates["precision_at_k"].median == 0.7
    assert result.metric_aggregates["precision_at_k"].spread == 0.4
    assert result.metric_aggregates["precision_at_k"].std == 0.282843
    assert result.best_seeds[0].doi == "10.1038/nphys1170"
    assert result.worst_seeds[0].doi == "10.2000/example"
    assert Path(result.output_paths.aggregate_summary_json).exists()
    assert Path(result.output_paths.seed_table_jsonl).exists()
    assert Path(result.output_paths.worst_cases_json).exists()

    worst_cases_payload = json.loads(Path(result.output_paths.worst_cases_json).read_text(encoding="utf-8"))
    assert len(worst_cases_payload["best_seeds"]) == 2
    assert len(worst_cases_payload["failed_seeds"]) == 1


def test_run_seed_batch_reuses_existing_run_and_writes_outputs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(benchmark, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        benchmark,
        "load_runtime_config",
        lambda: RuntimeConfig(
            app_name="scholarly-similarity",
            openalex_base_url="https://api.openalex.org",
            use_network=False,
            cache_dir="data/cache",
            runs_dir="runs",
            request_timeout_seconds=10.0,
        ),
    )

    existing_run_id = build_local_corpus_run_id(
        doi="10.1038/nphys1170",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
    )
    existing_run_dir = tmp_path / "runs" / existing_run_id
    _write_batch_ready_run(existing_run_dir, doi="10.1038/nphys1170")

    called_dois: list[str] = []

    def fake_build_local_corpus(**kwargs):
        called_dois.append(str(kwargs["doi"]))
        raise ValueError(f"Cannot build corpus for {kwargs['doi']}")

    monkeypatch.setattr(benchmark, "build_local_corpus", fake_build_local_corpus)

    seeds_path = tmp_path / "seeds.csv"
    seeds_path.write_text(
        "\n".join(
            [
                "doi,tag",
                "10.1038/nphys1170,alpha",
                "<doi_2>,beta",
            ]
        ),
        encoding="utf-8",
    )

    result = run_seed_batch(
        seeds_path=seeds_path,
        theory_config_path=DEFAULT_THEORY_PATH,
        theory=load_theory_config(DEFAULT_THEORY_PATH),
        batch_id="batch_001",
    )

    assert result.completed_seed_count == 1
    assert result.failed_seed_count == 1
    assert called_dois == ["<doi_2>"]

    batch_dir = tmp_path / "runs" / "batches" / "batch_001"
    assert (batch_dir / "batch_manifest.json").exists()
    assert (batch_dir / "seed_runs.jsonl").exists()
    assert (batch_dir / "aggregate_summary.json").exists()
    assert (batch_dir / "seed_table.jsonl").exists()
    assert (batch_dir / "worst_cases.json").exists()

    seed_runs = [
        json.loads(line)
        for line in (batch_dir / "seed_runs.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert seed_runs[0]["status"] == "completed"
    assert seed_runs[0]["reused_existing_run"] is True
    assert seed_runs[0]["experiment_id"] == "batch_001"
    assert seed_runs[0]["evaluation_summary_json"].endswith("evaluation_summary.json")
    assert seed_runs[1]["status"] == "failed"
    assert seed_runs[1]["failed_stage"] == "build-local-corpus"

    aggregate_payload = json.loads((batch_dir / "aggregate_summary.json").read_text(encoding="utf-8"))
    assert aggregate_payload["completed_seed_count"] == 1
    assert aggregate_payload["failed_seed_count"] == 1


def test_run_seed_batch_independent_benchmark_freezes_labels_and_surfaces_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    benchmark_preset_payload = json.loads(
        (repo_root / "configs" / "presets" / "benchmarks" / "benchmark_preset_independent_smoke.json").read_text(
            encoding="utf-8"
        )
    )
    seeds_path = repo_root / str(benchmark_preset_payload["seeds_csv"])
    labels_path = repo_root / str(benchmark_preset_payload["benchmark_labels_path"])

    monkeypatch.setattr(benchmark, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        benchmark,
        "load_runtime_config",
        lambda: RuntimeConfig(
            app_name="scholarly-similarity",
            openalex_base_url="https://api.openalex.org",
            use_network=False,
            cache_dir="data/cache",
            runs_dir="runs",
            request_timeout_seconds=10.0,
        ),
    )

    existing_run_id = build_local_corpus_run_id(
        doi="10.1038/nphys1170",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
    )
    existing_run_dir = tmp_path / "runs" / existing_run_id
    _write_batch_ready_run(existing_run_dir, doi="10.1038/nphys1170")

    result = run_seed_batch(
        seeds_path=seeds_path,
        theory_config_path=DEFAULT_THEORY_PATH,
        theory=load_theory_config(DEFAULT_THEORY_PATH),
        batch_id="batch_independent_smoke",
        label_source="benchmark",
        evaluation_mode="independent_benchmark",
        benchmark_labels_path=labels_path,
        benchmark_dataset_id=str(benchmark_preset_payload["benchmark_dataset_id"]),
    )

    batch_dir = tmp_path / "runs" / "batches" / "batch_independent_smoke"
    manifest_payload = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    options_payload = manifest_payload["options"]
    snapshot_path = Path(options_payload["benchmark_labels_snapshot_path"])
    if not snapshot_path.is_absolute():
        snapshot_path = (tmp_path / snapshot_path).resolve()

    assert result.completed_seed_count == 1
    assert options_payload["evaluation_mode"] == "independent_benchmark"
    assert options_payload["evidence_tier"] == "independent_benchmark"
    assert options_payload["benchmark_dataset_id"] == "benchmark_dataset_independent_smoke"
    assert options_payload["benchmark_schema_version"] == BENCHMARK_SCHEMA_VERSION_V1
    assert options_payload["benchmark_labels_row_count"] == 3
    assert snapshot_path.exists()

    seed_runs = [
        json.loads(line)
        for line in (batch_dir / "seed_runs.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert seed_runs[0]["benchmark_labels_snapshot_path"] == options_payload["benchmark_labels_snapshot_path"]
    assert seed_runs[0]["benchmark_labels_sha256"] == options_payload["benchmark_labels_sha256"]

    mode_summary_path = Path(seed_runs[0]["mode_evaluation_summary_json"])
    assert mode_summary_path.exists()

    bundle = load_batch_bundle(batch_dir)
    comparison_manifest_payload = build_comparison_manifest_payload(
        comparison_id="comparison_independent_smoke",
        comparison_dir=tmp_path / "runs" / "comparisons" / "comparison_independent_smoke",
        created_at="2026-04-07T00:00:00Z",
        reviewer="tester",
        primary_bundle=bundle,
        secondary_bundle=bundle,
        selected_metric="ndcg_at_k",
        status_mode="common completed only",
        common_doi_count=1,
        common_completed_seed_count=1,
        compatibility_warning_list=[],
        summary=ComparisonMetricSummary(
            primary_mean=1.0,
            primary_median=1.0,
            secondary_mean=1.0,
            secondary_median=1.0,
            raw_delta_mean=0.0,
            raw_delta_median=0.0,
            improvement_delta_mean=0.0,
            improvement_delta_median=0.0,
            wins=0,
            losses=0,
            ties=1,
        ),
        output_paths={},
        paired_seed_count=1,
    )
    assert comparison_manifest_payload["evaluation_mode"] == "independent_benchmark"
    assert comparison_manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_independent_smoke"

    guardrail = evaluate_decision_guardrails(
        selected_metric="ndcg_at_k",
        common_doi_count=1,
        common_completed_seed_count=1,
        summary=ComparisonMetricSummary(
            primary_mean=0.8,
            primary_median=0.8,
            secondary_mean=0.9,
            secondary_median=0.9,
            raw_delta_mean=0.1,
            raw_delta_median=0.1,
            improvement_delta_mean=0.1,
            improvement_delta_median=0.1,
            wins=1,
            losses=0,
            ties=0,
        ),
        paired_seed_count=1,
        evaluation_mode="independent_benchmark",
        metric_scope="local_corpus_ranking",
        benchmark_dataset_id="benchmark_dataset_independent_smoke",
        benchmark_labels_sha256=options_payload["benchmark_labels_sha256"],
        comparison_benchmark_dataset_id="benchmark_dataset_other",
        comparison_benchmark_labels_sha256=options_payload["benchmark_labels_sha256"],
    )
    assert guardrail.promotion_eligible is False
