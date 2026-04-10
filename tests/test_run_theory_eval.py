from __future__ import annotations

import json
from pathlib import Path

from src.config import DEFAULT_THEORY_PATH, load_theory_config
from src.eval.benchmark import evaluate_local_ranking, generate_silver_labels, run_theory_eval
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


def _write_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "experiment_run"
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

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two, candidate_three):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate_one["openalex_id"],
                    "edge_type": "seed_references",
                }
            )
        )
        handle.write("\n")
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate_two["openalex_id"],
                    "edge_type": "seed_related",
                }
            )
        )
        handle.write("\n")

    return run_dir


def _write_theory_copy(tmp_path: Path, filename: str = "theory_experiment.yaml") -> Path:
    destination = tmp_path / filename
    destination.write_text(DEFAULT_THEORY_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    return destination


def test_run_theory_eval_writes_isolated_outputs_without_overwriting_base_run(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    theory_path = _write_theory_copy(tmp_path)

    generate_silver_labels(run_dir=run_dir)
    rank_local_corpus(run_dir=run_dir, theory=load_theory_config(), top_k=3)
    evaluate_local_ranking(
        run_dir=run_dir,
        labels_path=run_dir / "silver_labels.csv",
        top_k=3,
        label_source="silver",
    )

    base_scored_text = (run_dir / "scored_candidates.jsonl").read_text(encoding="utf-8")
    base_eval_text = (run_dir / "evaluation_summary.json").read_text(encoding="utf-8")

    result = run_theory_eval(
        run_dir=run_dir,
        theory_config_path=theory_path,
        theory=load_theory_config(theory_path),
        experiment_id="revision_a",
        label_source="silver",
        top_k=3,
    )

    experiment_dir = Path(result.experiment_dir)
    assert experiment_dir.exists()
    assert Path(result.output_paths.theory_snapshot_yaml).read_text(encoding="utf-8") == theory_path.read_text(
        encoding="utf-8"
    )
    assert Path(result.output_paths.scored_candidates_jsonl).exists()
    assert Path(result.output_paths.ranking_summary_json).exists()
    assert Path(result.output_paths.evaluation_summary_json).exists()
    assert Path(result.output_paths.judged_candidates_jsonl).exists()
    assert Path(result.output_paths.evaluation_cases_json).exists()
    assert Path(result.output_paths.experiment_manifest_json).exists()
    assert result.output_paths.metrics_delta_json is None

    assert (run_dir / "scored_candidates.jsonl").read_text(encoding="utf-8") == base_scored_text
    assert (run_dir / "evaluation_summary.json").read_text(encoding="utf-8") == base_eval_text


def test_run_theory_eval_writes_metrics_delta_when_baseline_exists(tmp_path: Path) -> None:
    run_dir = _write_run(tmp_path)
    theory_path = _write_theory_copy(tmp_path)

    generate_silver_labels(run_dir=run_dir)
    run_theory_eval(
        run_dir=run_dir,
        theory_config_path=theory_path,
        theory=load_theory_config(theory_path),
        experiment_id="baseline",
        label_source="silver",
        top_k=3,
    )
    result = run_theory_eval(
        run_dir=run_dir,
        theory_config_path=theory_path,
        theory=load_theory_config(theory_path),
        experiment_id="revision_b",
        label_source="silver",
        top_k=3,
    )

    assert result.output_paths.metrics_delta_json is not None
    delta_payload = json.loads(Path(result.output_paths.metrics_delta_json).read_text(encoding="utf-8"))
    assert delta_payload["baseline_experiment_id"] == "baseline"
    assert delta_payload["current_experiment_id"] == "revision_b"
