from __future__ import annotations

import json
from pathlib import Path

import yaml
from typer.testing import CliRunner

import src.cli.main as cli_main
from src.config import DEFAULT_THEORY_PATH
from src.eval.benchmark import (
    AggregateMetricStats,
    BatchAggregateEvalResult,
    BatchOutputPaths,
    SeedBatchManifest,
    SeedBatchOptions,
    generate_silver_labels,
    run_theory_eval,
)
from src.ingest.openalex_client import OpenAlexLookupResult


runner = CliRunner()

SAMPLE_WORK_PAYLOAD = {
    "id": "https://openalex.org/W2741809807",
    "doi": "https://doi.org/10.1038/nphys1170",
    "display_name": "Observation of the dynamical Casimir effect",
    "publication_year": 2011,
    "cited_by_count": 321,
    "referenced_works": [
        "https://openalex.org/W1",
        "https://openalex.org/W2",
    ],
    "related_works": [
        "https://openalex.org/W3",
    ],
    "primary_topic": {"display_name": "Quantum optics"},
    "topics": [
        {"display_name": "Quantum optics"},
        {"display_name": "Casimir effect"},
    ],
    "abstract_inverted_index": {
        "Observation": [0],
        "of": [1],
        "the": [2],
        "dynamical": [3],
        "Casimir": [4],
        "effect": [5],
    },
}


def _write_rank_ready_run(run_dir: Path) -> None:
    seed = {
        "openalex_id": "https://openalex.org/WSEED",
        "doi": "10.1038/nphys1170",
        "title": "Seed paper",
        "publication_year": 2018,
        "cited_by_count": 20,
        "referenced_works": ["https://openalex.org/WCAND1"],
        "related_works": ["https://openalex.org/WCAND2"],
        "primary_topic": "Physics",
        "topics": ["Physics", "Quantum"],
        "abstract_text": "Quantum measurement protocol",
        "candidate_origins": [],
        "source": "openalex",
    }
    candidate_one = {
        **seed,
        "openalex_id": "https://openalex.org/WCAND1",
        "doi": None,
        "title": "Strong match",
        "publication_year": 2019,
        "referenced_works": ["https://openalex.org/W1", "https://openalex.org/W2"],
        "related_works": [],
        "abstract_text": "Quantum measurement protocol for related systems",
        "candidate_origins": ["seed_reference", "direct_neighbor"],
    }
    candidate_two = {
        **seed,
        "openalex_id": "https://openalex.org/WCAND2",
        "doi": None,
        "title": "Weaker match",
        "publication_year": 2010,
        "referenced_works": [],
        "related_works": [],
        "primary_topic": "Biology",
        "topics": ["Biology"],
        "abstract_text": "Cell growth observations",
        "candidate_origins": ["seed_related"],
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "seed_record.json").write_text(json.dumps(seed), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": candidate_one["openalex_id"], "edge_type": "seed_references"}))
        handle.write("\n")


def test_cli_help_runs() -> None:
    result = runner.invoke(cli_main.app, ["--help"])
    assert result.exit_code == 0
    assert "fetch-doi" in result.stdout
    assert "print-tree-info" in result.stdout
    assert "rank-local-corpus" in result.stdout
    assert "export-label-template" in result.stdout
    assert "generate-silver-labels" in result.stdout
    assert "evaluate-local-ranking" in result.stdout
    assert "run-theory-eval" in result.stdout
    assert "run-seed-batch" in result.stdout
    assert "aggregate-batch-eval" in result.stdout
    assert "build-generator-packet" in result.stdout
    assert "apply-generator-reply" in result.stdout
    assert "build-verifier-packet" in result.stdout
    assert "record-verifier-reply" in result.stdout


def test_fetch_doi_json_output(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_main, "REPO_ROOT", tmp_path)

    def fake_fetch(self, normalized_doi: str) -> OpenAlexLookupResult:
        return OpenAlexLookupResult(
            doi=normalized_doi,
            request_url=f"https://api.openalex.org/works/https://doi.org/{normalized_doi}",
            payload=SAMPLE_WORK_PAYLOAD,
        )

    monkeypatch.setattr(cli_main.OpenAlexClient, "fetch_work_by_doi", fake_fetch)

    result = runner.invoke(cli_main.app, ["fetch-doi", "10.1038/nphys1170", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["cache_hit"] is False
    assert payload["record"]["doi"] == "10.1038/nphys1170"
    assert payload["record"]["title"] == "Observation of the dynamical Casimir effect"
    assert (tmp_path / "data" / "cache" / "openalex").exists()


def test_build_local_corpus_json_output(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_main, "REPO_ROOT", tmp_path)

    seed_payload = {
        **SAMPLE_WORK_PAYLOAD,
        "id": "https://openalex.org/WSEED",
        "referenced_works": ["https://openalex.org/WREF1"],
        "related_works": ["https://openalex.org/WREL1"],
    }
    neighbor_payloads = {
        "WREF1": {
            **SAMPLE_WORK_PAYLOAD,
            "id": "https://openalex.org/WREF1",
            "display_name": "Referenced neighbor",
            "doi": None,
            "referenced_works": [],
            "related_works": [],
        },
        "WREL1": {
            **SAMPLE_WORK_PAYLOAD,
            "id": "https://openalex.org/WREL1",
            "display_name": "Related neighbor",
            "doi": "https://doi.org/10.1000/related",
            "referenced_works": [],
            "related_works": [],
        },
    }

    def fake_fetch_by_doi(self, normalized_doi: str) -> OpenAlexLookupResult:
        return OpenAlexLookupResult(
            doi=normalized_doi,
            request_url=f"https://api.openalex.org/works/https://doi.org/{normalized_doi}",
            payload=seed_payload,
        )

    def fake_fetch_by_openalex_id(self, openalex_id: str) -> OpenAlexLookupResult:
        return OpenAlexLookupResult(
            openalex_id=openalex_id,
            request_url=f"https://api.openalex.org/works/{openalex_id}",
            payload=neighbor_payloads[openalex_id],
        )

    monkeypatch.setattr(cli_main.OpenAlexClient, "fetch_work_by_doi", fake_fetch_by_doi)
    monkeypatch.setattr(cli_main.OpenAlexClient, "fetch_work_by_openalex_id", fake_fetch_by_openalex_id)

    result = runner.invoke(
        cli_main.app,
        ["build-local-corpus", "10.1038/nphys1170", "--max-references", "1", "--max-related", "1", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["seed_openalex_id"] == "https://openalex.org/WSEED"
    assert payload["counts"]["papers"] == 3
    assert payload["counts"]["edges"] == 2
    assert (tmp_path / "runs").exists()


def test_rank_local_corpus_json_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)

    result = runner.invoke(cli_main.app, ["rank-local-corpus", str(run_dir), "--top-k", "1", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["candidate_count"] == 2
    assert payload["top_results"][0]["openalex_id"] == "https://openalex.org/WCAND1"
    assert Path(payload["output_paths"]["scored_candidates_jsonl"]).exists()


def test_export_label_template_cli_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)
    rank_result = runner.invoke(cli_main.app, ["rank-local-corpus", str(run_dir), "--top-k", "2", "--json"])
    assert rank_result.exit_code == 0

    output_path = run_dir / "manual_labels.csv"
    result = runner.invoke(
        cli_main.app,
        ["export-label-template", str(run_dir), "--top-k", "1", "--output", str(output_path)],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    rows = output_path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 2


def test_evaluate_local_ranking_cli_json_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)
    rank_result = runner.invoke(cli_main.app, ["rank-local-corpus", str(run_dir), "--top-k", "2", "--json"])
    assert rank_result.exit_code == 0

    labels_path = run_dir / "labels.csv"
    labels_path.write_text(
        "\n".join(
            [
                "seed_openalex_id,candidate_openalex_id,rank,title,sim,conf,suggested_summary,label,notes",
                "https://openalex.org/WSEED,https://openalex.org/WCAND1,1,Strong match,0.0,0.0,,2,strong",
                "https://openalex.org/WSEED,https://openalex.org/WCAND2,2,Weaker match,0.0,0.0,,0,weak",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        cli_main.app,
        ["evaluate-local-ranking", str(run_dir), "--labels", str(labels_path), "--top-k", "2", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["judged_count"] == 2
    assert payload["metrics"]["precision_at_k"] == 0.5
    assert Path(payload["output_paths"]["evaluation_summary_json"]).exists()


def test_generate_silver_labels_cli_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)

    result = runner.invoke(cli_main.app, ["generate-silver-labels", str(run_dir)])

    assert result.exit_code == 0
    silver_csv_path = run_dir / "silver_labels.csv"
    assert silver_csv_path.exists()
    assert (run_dir / "silver_labels.jsonl").exists()
    rows = silver_csv_path.read_text(encoding="utf-8").splitlines()
    assert any("https://openalex.org/WCAND1,2," in row for row in rows)
    assert any("https://openalex.org/WCAND2,1," in row for row in rows)


def test_evaluate_local_ranking_silver_cli_json_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)
    rank_result = runner.invoke(cli_main.app, ["rank-local-corpus", str(run_dir), "--top-k", "2", "--json"])
    assert rank_result.exit_code == 0
    silver_result = runner.invoke(cli_main.app, ["generate-silver-labels", str(run_dir)])
    assert silver_result.exit_code == 0
    silver_csv_path = run_dir / "silver_labels.csv"
    rows = silver_csv_path.read_text(encoding="utf-8").splitlines()
    silver_csv_path.write_text(
        "\n".join(
            [
                rows[0],
                rows[1].replace("https://openalex.org/WSEED", "WSEED").replace(
                    "https://openalex.org/WCAND1", "WCAND1"
                ),
                rows[2].replace("https://openalex.org/WSEED", "WSEED").replace(
                    "https://openalex.org/WCAND2", "WCAND2"
                ),
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        cli_main.app,
        ["evaluate-local-ranking", str(run_dir), "--label-source", "silver", "--top-k", "2", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["label_source"] == "silver"
    assert payload["judged_count"] == 2
    assert payload["metrics"]["brier_score"] >= 0.0


def test_run_theory_eval_cli_json_output(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)
    silver_result = runner.invoke(cli_main.app, ["generate-silver-labels", str(run_dir)])
    assert silver_result.exit_code == 0

    result = runner.invoke(
        cli_main.app,
        [
            "run-theory-eval",
            str(run_dir),
            "--theory-config",
            str(DEFAULT_THEORY_PATH),
            "--experiment-id",
            "trial_a",
            "--label-source",
            "silver",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["experiment_id"] == "trial_a"
    assert Path(payload["output_paths"]["theory_snapshot_yaml"]).exists()
    assert Path(payload["output_paths"]["evaluation_summary_json"]).exists()


def test_run_seed_batch_cli_json_output(monkeypatch, tmp_path: Path) -> None:
    seeds_path = tmp_path / "seeds.csv"
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    batch_dir = tmp_path / "runs" / "batches" / "batch_001"
    output_paths = BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )
    manifest = SeedBatchManifest(
        batch_id="batch_001",
        batch_dir=str(batch_dir),
        seeds_csv=str(seeds_path),
        theory_config=str(DEFAULT_THEORY_PATH),
        created_at="2026-03-28T00:00:00+00:00",
        completed_at="2026-03-28T00:01:00+00:00",
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
    monkeypatch.setattr(cli_main, "run_seed_batch", lambda **kwargs: manifest)

    result = runner.invoke(
        cli_main.app,
        [
            "run-seed-batch",
            "--seeds",
            str(seeds_path),
            "--theory-config",
            str(DEFAULT_THEORY_PATH),
            "--batch-id",
            "batch_001",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["batch_id"] == "batch_001"
    assert payload["completed_seed_count"] == 1
    assert payload["output_paths"]["aggregate_summary_json"].endswith("aggregate_summary.json")


def test_aggregate_batch_eval_cli_json_output(monkeypatch, tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_001"
    output_paths = BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )
    aggregate_result = BatchAggregateEvalResult(
        batch_id="batch_001",
        batch_dir=str(batch_dir),
        aggregated_at="2026-03-28T00:02:00+00:00",
        seed_count=2,
        completed_seed_count=1,
        failed_seed_count=1,
        ranking_metric="ndcg_at_k",
        metric_aggregates={
            "precision_at_k": AggregateMetricStats(count=1, mean=0.9, median=0.9, std=None, spread=0.0, min=0.9, max=0.9),
            "recall_at_k": AggregateMetricStats(count=1, mean=0.5, median=0.5, std=None, spread=0.0, min=0.5, max=0.5),
            "ndcg_at_k": AggregateMetricStats(count=1, mean=0.8, median=0.8, std=None, spread=0.0, min=0.8, max=0.8),
            "brier_score": AggregateMetricStats(count=1, mean=0.1, median=0.1, std=None, spread=0.0, min=0.1, max=0.1),
            "expected_calibration_error": AggregateMetricStats(count=1, mean=0.2, median=0.2, std=None, spread=0.0, min=0.2, max=0.2),
        },
        best_seeds=[],
        worst_seeds=[],
        failed_seeds=[],
        output_paths=output_paths,
    )
    monkeypatch.setattr(cli_main, "aggregate_batch_eval", lambda **kwargs: aggregate_result)

    result = runner.invoke(
        cli_main.app,
        ["aggregate-batch-eval", str(batch_dir), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["batch_id"] == "batch_001"
    assert payload["ranking_metric"] == "ndcg_at_k"
    assert payload["failed_seed_count"] == 1


def test_agent_loop_cli_commands(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "sample_run"
    _write_rank_ready_run(run_dir)
    silver_result = runner.invoke(cli_main.app, ["generate-silver-labels", str(run_dir)])
    assert silver_result.exit_code == 0

    baseline_result = run_theory_eval(
        run_dir=run_dir,
        theory_config_path=DEFAULT_THEORY_PATH,
        theory=cli_main.load_theory_config(DEFAULT_THEORY_PATH),
        experiment_id="baseline",
        label_source="silver",
        top_k=2,
    )
    assert Path(baseline_result.output_paths.evaluation_summary_json).exists()

    candidate_theory_path = tmp_path / "candidate_theory.yaml"
    candidate_payload = yaml.safe_load(DEFAULT_THEORY_PATH.read_text(encoding="utf-8"))
    candidate_payload["sim_weights"]["temporal"] = 0.25
    candidate_payload["sim_weights"]["semantic"] = 0.0
    candidate_theory_path.write_text(yaml.safe_dump(candidate_payload, sort_keys=False), encoding="utf-8")
    candidate_result = run_theory_eval(
        run_dir=run_dir,
        theory_config_path=candidate_theory_path,
        theory=cli_main.load_theory_config(candidate_theory_path),
        experiment_id="candidate_a",
        label_source="silver",
        top_k=2,
    )
    assert Path(candidate_result.output_paths.evaluation_summary_json).exists()

    generator_packet_result = runner.invoke(
        cli_main.app,
        [
            "build-generator-packet",
            str(run_dir),
            "--baseline-experiment",
            "baseline",
            "--packet-id",
            "pkt1",
        ],
    )
    assert generator_packet_result.exit_code == 0
    assert (run_dir / "agent_loops" / "pkt1" / "generator_packet.md").exists()

    generator_reply_path = tmp_path / "generator_reply.yaml"
    generator_reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Slightly increase temporal weight.",
                "expected_effect": "Favor nearer lineage neighbors.",
                "risks": ["Could under-rank older foundational papers."],
                "changes": [
                    {"path": "sim_weights.temporal", "value": 0.2},
                    {"path": "sim_weights.semantic", "value": 0.0},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    apply_reply_result = runner.invoke(
        cli_main.app,
        [
            "apply-generator-reply",
            str(run_dir),
            "--baseline-experiment",
            "baseline",
            "--packet-id",
            "pkt1",
            "--reply",
            str(generator_reply_path),
            "--candidate-id",
            "cand1",
        ],
    )
    assert apply_reply_result.exit_code == 0
    assert (run_dir / "agent_loops" / "pkt1" / "cand1" / "candidate_theory.yaml").exists()

    verifier_packet_result = runner.invoke(
        cli_main.app,
        [
            "build-verifier-packet",
            str(run_dir),
            "--baseline-experiment",
            "baseline",
            "--candidate-experiment",
            "candidate_a",
            "--packet-id",
            "pkt1",
        ],
    )
    assert verifier_packet_result.exit_code == 0
    assert (run_dir / "agent_loops" / "pkt1" / "verifier_packet.md").exists()

    verifier_reply_path = tmp_path / "verifier_reply.yaml"
    verifier_reply_path.write_text(
        yaml.safe_dump(
            {
                "pass": True,
                "score": 0.8,
                "issues": ["Improvement is small but consistent."],
                "next_change": "Adjust direct citation weight carefully next.",
                "notes": "Acceptable for another round.",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    record_result = runner.invoke(
        cli_main.app,
        [
            "record-verifier-reply",
            str(run_dir),
            "--packet-id",
            "pkt1",
            "--reply",
            str(verifier_reply_path),
        ],
    )
    assert record_result.exit_code == 0
    assert (run_dir / "agent_loops" / "pkt1" / "decision.json").exists()
