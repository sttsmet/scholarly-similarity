from __future__ import annotations

import csv
import json
from pathlib import Path

from src.config import load_theory_config
from src.eval.benchmark import evaluate_local_ranking, export_label_template
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
        "source": "openalex",
    }


def _write_ranked_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "eval_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    seed = _make_record(
        openalex_id="https://openalex.org/WSEED",
        title="Seed paper",
        publication_year=2018,
        referenced_works=[
            "https://openalex.org/W1",
            "https://openalex.org/W2",
            "https://openalex.org/WCAND1",
        ],
        related_works=["https://openalex.org/WCAND2"],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol for coupled systems",
    )
    candidate_one = _make_record(
        openalex_id="https://openalex.org/WCAND1",
        title="Strong match",
        publication_year=2019,
        referenced_works=[
            "https://openalex.org/W1",
            "https://openalex.org/W2",
            "https://openalex.org/W3",
        ],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol with related systems",
    )
    candidate_two = _make_record(
        openalex_id="https://openalex.org/WCAND2",
        title="Moderate match",
        publication_year=2017,
        referenced_works=["https://openalex.org/W2"],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Measurement protocol in a related system",
    )
    candidate_three = _make_record(
        openalex_id="https://openalex.org/WCAND3",
        title="Weak match",
        publication_year=2010,
        referenced_works=["https://openalex.org/W9"],
        primary_topic="Biology",
        topics=["Biology"],
        abstract_text="Cell growth observations in a lab",
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two, candidate_three):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": candidate_one["openalex_id"], "edge_type": "seed_references"}))
        handle.write("\n")
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": candidate_two["openalex_id"], "edge_type": "seed_related"}))
        handle.write("\n")

    rank_local_corpus(run_dir=run_dir, theory=load_theory_config(), top_k=3)
    return run_dir


def _write_labels_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "seed_openalex_id",
                "candidate_openalex_id",
                "title",
                "publication_year",
                "label",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_export_label_template_writes_csv(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)

    result = export_label_template(run_dir=run_dir, top_k=2, output_path=None)

    assert result.exported_count == 2
    label_path = Path(result.output_path)
    assert label_path.exists()

    with label_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert len(rows) == 2
    assert reader.fieldnames == [
        "seed_openalex_id",
        "seed_title",
        "candidate_openalex_id",
        "title",
        "publication_year",
        "label",
        "label_confidence",
        "aspect",
        "annotator_id",
        "notes",
        "adjudicated_label",
        "adjudication_notes",
    ]
    assert rows[0]["seed_title"] == "Seed paper"
    assert rows[0]["candidate_openalex_id"] == "https://openalex.org/WCAND2"
    assert rows[0]["publication_year"] == "2017"
    assert rows[0]["label"] == ""
    assert rows[0]["label_confidence"] == ""
    assert rows[0]["aspect"] == "lineage"
    assert rows[0]["annotator_id"] == ""
    assert rows[0]["notes"] == ""
    assert rows[0]["adjudicated_label"] == ""
    assert rows[0]["adjudication_notes"] == ""


def test_evaluate_local_ranking_metrics_and_artifacts(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    labels_path = run_dir / "labels.csv"
    _write_labels_csv(
        labels_path,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Strong match",
                "publication_year": "2019",
                "label": "2",
                "notes": "Clearly related",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND2",
                "title": "Moderate match",
                "publication_year": "2017",
                "label": "1",
                "notes": "Somewhat related",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND3",
                "title": "Weak match",
                "publication_year": "2010",
                "label": "0",
                "notes": "Not relevant",
            },
        ],
    )

    result = evaluate_local_ranking(run_dir=run_dir, labels_path=labels_path, top_k=None)

    assert result.judged_count == 3
    assert result.judged_fraction == 1.0
    assert result.metrics["precision_at_k"] == 0.666667
    assert result.metrics["recall_at_k"] == 1.0
    assert result.metrics["ndcg_at_k"] == 1.0
    assert result.metrics["mean_sim_by_label"]["2"] is not None

    summary_path = Path(result.output_paths.evaluation_summary_json)
    cases_path = Path(result.output_paths.evaluation_cases_json)
    judged_path = Path(result.output_paths.judged_candidates_jsonl)

    assert summary_path.exists()
    assert cases_path.exists()
    assert judged_path.exists()

    cases_payload = json.loads(cases_path.read_text(encoding="utf-8"))
    assert cases_payload["top_false_positives"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND3"
    assert cases_payload["top_strong_relevants"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND1"


def test_evaluate_local_ranking_handles_missing_labels(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    labels_path = run_dir / "partial_labels.csv"
    _write_labels_csv(
        labels_path,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Strong match",
                "publication_year": "2019",
                "label": "2",
                "notes": "Strongly related",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND2",
                "title": "Moderate match",
                "publication_year": "2017",
                "label": "",
                "notes": "",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "candidate_openalex_id": "https://openalex.org/WCAND3",
                "title": "Weak match",
                "publication_year": "2010",
                "label": "0",
                "notes": "",
            },
        ],
    )

    result = evaluate_local_ranking(run_dir=run_dir, labels_path=labels_path, top_k=2)

    assert result.judged_count == 2
    assert result.judged_fraction == 0.666667
    assert result.metrics["precision_at_k"] == 1.0
    assert result.metrics["recall_at_k"] == 1.0

    cases_payload = json.loads(Path(result.output_paths.evaluation_cases_json).read_text(encoding="utf-8"))
    assert cases_payload["unlabeled_top_candidates"][0]["candidate_openalex_id"] == "https://openalex.org/WCAND2"


def test_evaluate_local_ranking_joins_manual_labels_with_short_ids(tmp_path: Path) -> None:
    run_dir = _write_ranked_run(tmp_path)
    labels_path = run_dir / "short_id_labels.csv"
    _write_labels_csv(
        labels_path,
        [
            {
                "seed_openalex_id": "WSEED",
                "candidate_openalex_id": "WCAND1",
                "title": "Strong match",
                "publication_year": "2019",
                "label": "2",
                "notes": "Strongly related",
            },
            {
                "seed_openalex_id": "WSEED",
                "candidate_openalex_id": "WCAND2",
                "title": "Moderate match",
                "publication_year": "2017",
                "label": "1",
                "notes": "Related",
            },
            {
                "seed_openalex_id": "WSEED",
                "candidate_openalex_id": "WCAND3",
                "title": "Weak match",
                "publication_year": "2010",
                "label": "0",
                "notes": "Not relevant",
            },
        ],
    )

    result = evaluate_local_ranking(run_dir=run_dir, labels_path=labels_path, top_k=None)

    assert result.judged_count == 3
    assert result.metrics["mean_sim_by_label"]["0"] is not None
