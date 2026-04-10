from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from src.config import load_theory_config
from src.eval.benchmark import evaluate_local_ranking, generate_silver_labels
from src.rank.ranker import rank_local_corpus


def _make_record(
    *,
    openalex_id: str,
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
        "doi": None,
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


def _write_ranked_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "silver_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    seed = _make_record(
        openalex_id="https://openalex.org/WSEED",
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
        title="Reference match",
        publication_year=2019,
        referenced_works=["https://openalex.org/W1", "https://openalex.org/W2"],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol with related systems",
        candidate_origins=["seed_reference", "direct_neighbor"],
    )
    candidate_two = _make_record(
        openalex_id="https://openalex.org/WCAND2",
        title="Related match",
        publication_year=2017,
        referenced_works=["https://openalex.org/W2"],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Measurement protocol in a related system",
        candidate_origins=["seed_related"],
    )
    candidate_three = _make_record(
        openalex_id="https://openalex.org/WCAND3",
        title="Hard negative",
        publication_year=2018,
        referenced_works=["https://openalex.org/W9"],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="A different but nearby topic",
        candidate_origins=["hard_negative"],
    )
    candidate_four = _make_record(
        openalex_id="https://openalex.org/WCAND4",
        title="Legacy unlabeled",
        publication_year=2015,
        referenced_works=[],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Legacy candidate without provenance",
        candidate_origins=[],
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two, candidate_three, candidate_four):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": candidate_one["openalex_id"], "edge_type": "seed_references"}))
        handle.write("\n")
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": candidate_two["openalex_id"], "edge_type": "seed_related"}))
        handle.write("\n")

    rank_local_corpus(run_dir=run_dir, theory=load_theory_config(), top_k=4)
    return run_dir


def test_generate_silver_labels_from_provenance(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)

    result = generate_silver_labels(run_dir=run_dir)

    assert result.candidate_count == 4
    assert result.judged_count == 3

    with Path(result.output_paths.silver_labels_csv).open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    row_map = {row["candidate_openalex_id"]: row for row in rows}
    assert row_map["https://openalex.org/WCAND1"]["label"] == "2"
    assert row_map["https://openalex.org/WCAND2"]["label"] == "1"
    assert row_map["https://openalex.org/WCAND3"]["label"] == "0"
    assert row_map["https://openalex.org/WCAND4"]["label"] == ""
    assert float(row_map["https://openalex.org/WCAND1"]["label_confidence"]) > float(
        row_map["https://openalex.org/WCAND2"]["label_confidence"]
    )


def test_evaluate_local_ranking_with_silver_labels(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    generate_silver_labels(run_dir=run_dir)

    result = evaluate_local_ranking(run_dir=run_dir, top_k=None, label_source="silver")

    assert result.label_source == "silver"
    assert result.evaluation_mode == "silver_provenance_regression"
    assert result.judged_count == 3
    assert result.judged_fraction == 0.75
    assert result.metrics["precision_at_k"] == 0.666667
    assert result.metrics["recall_at_k"] == 1.0
    assert result.metrics["brier_score"] >= 0.0
    assert result.metrics["expected_calibration_error"] >= 0.0
    assert result.metrics["mean_sim_by_label"]["2"] is not None
    assert result.metrics["mean_sim_by_label"]["1"] is not None
    assert result.metrics["mean_sim_by_label"]["0"] is not None
    assert result.metrics["mean_conf_by_label"]["2"] is not None
    assert result.metrics["mean_conf_by_label"]["1"] is not None
    assert result.metrics["mean_conf_by_label"]["0"] is not None
    assert result.provenance_slice_summaries is not None
    assert result.provenance_slice_summaries["strong_lineage"]["candidate_count"] == 1
    assert Path(result.output_paths.mode_evaluation_summary_json).name == (
        "silver_provenance_regression_summary.json"
    )
    assert Path(result.output_paths.mode_evaluation_summary_json).exists()
    assert Path(result.output_paths.mode_evaluation_cases_json).exists()

    cases_payload = json.loads(Path(result.output_paths.evaluation_cases_json).read_text(encoding="utf-8"))
    assert cases_payload["top_false_positives"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND3"
    assert cases_payload["top_strong_relevants"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND1"
    assert cases_payload["unlabeled_top_candidates"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND4"
    assert "provenance_slice_summaries" in cases_payload


def test_evaluate_local_ranking_joins_silver_labels_with_short_ids(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    silver_result = generate_silver_labels(run_dir=run_dir)
    csv_path = Path(silver_result.output_paths.silver_labels_csv)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            row["seed_openalex_id"] = row["seed_openalex_id"].rsplit("/", 1)[-1]
            row["candidate_openalex_id"] = row["candidate_openalex_id"].rsplit("/", 1)[-1]
            writer.writerow(row)

    result = evaluate_local_ranking(run_dir=run_dir, top_k=None, label_source="silver")

    assert result.judged_count == 3
    assert result.metrics["mean_sim_by_label"]["0"] is not None
    assert result.metrics["mean_conf_by_label"]["0"] is not None


def test_evaluate_local_ranking_fails_when_zero_silver_labels_match(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    silver_result = generate_silver_labels(run_dir=run_dir)
    csv_path = Path(silver_result.output_paths.silver_labels_csv)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        for index, row in enumerate(rows, start=1):
            row["candidate_openalex_id"] = f"WUNKNOWN{index}"
            writer.writerow(row)

    with pytest.raises(
        ValueError,
        match="No labels matched scored candidates after OpenAlex id normalization",
    ):
        evaluate_local_ranking(run_dir=run_dir, top_k=None, label_source="silver")


def test_evaluate_local_ranking_independent_benchmark_loads_external_labels(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    benchmark_labels_path = tmp_path / "data" / "benchmarks" / "benchmark_labels.csv"
    benchmark_labels_path.parent.mkdir(parents=True, exist_ok=True)
    benchmark_labels_path.write_text(
        "\n".join(
            [
                "seed_openalex_id,candidate_openalex_id,label,label_confidence,label_reason,notes",
                "https://openalex.org/WSEED,https://openalex.org/WCAND1,2,0.90,independent strong,",
                "https://openalex.org/WSEED,https://openalex.org/WCAND2,1,0.70,independent moderate,",
                "https://openalex.org/WSEED,https://openalex.org/WCAND3,0,0.60,independent negative,",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = evaluate_local_ranking(
        run_dir=run_dir,
        labels_path=benchmark_labels_path,
        top_k=None,
        label_source="benchmark",
        evaluation_mode="independent_benchmark",
        benchmark_dataset_id="benchmark_dataset_001",
    )

    assert result.label_source == "benchmark"
    assert result.evaluation_mode == "independent_benchmark"
    assert result.benchmark_dataset_id == "benchmark_dataset_001"
    assert result.benchmark_labels_sha256 is not None
    assert result.benchmark_labels_snapshot_path == str(benchmark_labels_path)
    assert result.benchmark_labels_row_count == 3
    assert result.benchmark_schema_version == "benchmark_labels.v1"
    assert result.provenance_slice_summaries is None
    assert Path(result.output_paths.mode_evaluation_summary_json).name == "independent_benchmark_summary.json"
    assert Path(result.output_paths.mode_evaluation_summary_json).exists()
    assert Path(result.output_paths.mode_evaluation_cases_json).exists()
