from __future__ import annotations

import json
from math import exp, sqrt
from pathlib import Path

import pytest

from src.config import load_theory_config
from src.features import bibliographic_coupling, direct_citation, temporal
from src.ingest.doi_resolver import NormalizedOpenAlexRecord
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


def _write_run_artifacts(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "sample_run"
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
        title="Weak match",
        publication_year=2010,
        referenced_works=["https://openalex.org/W9"],
        primary_topic="Biology",
        topics=["Biology"],
        abstract_text="Cell growth observations in a lab",
    )
    candidate_three = _make_record(
        openalex_id="https://openalex.org/WCAND3",
        title="Masked temporal and topical",
        publication_year=None,
        referenced_works=["https://openalex.org/W2"],
        primary_topic=None,
        topics=[],
        abstract_text="Quantum measurement systems",
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
        handle.write(json.dumps({"src": seed["openalex_id"], "dst": "https://openalex.org/WMISSING", "edge_type": "seed_related"}))
        handle.write("\n")
    return run_dir


def test_bibliographic_coupling_formula() -> None:
    theory = load_theory_config()
    seed = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WSEED",
            title="Seed",
            referenced_works=["https://openalex.org/W1", "https://openalex.org/W2", "https://openalex.org/W3"],
        )
    )
    candidate = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WCAND",
            title="Candidate",
            referenced_works=["https://openalex.org/W2", "https://openalex.org/W3"],
        )
    )

    score = bibliographic_coupling.score(seed, candidate, theory)
    assert score == pytest.approx(2 / sqrt(3 * 2), abs=1e-6)


def test_direct_citation_feature() -> None:
    theory = load_theory_config()
    seed = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WSEED",
            title="Seed",
            referenced_works=["https://openalex.org/WCAND"],
        )
    )
    candidate = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WCAND",
            title="Candidate",
            referenced_works=[],
        )
    )

    assert direct_citation.score(seed, candidate, theory) == 1.0


def test_temporal_feature_uses_tau_from_config() -> None:
    theory = load_theory_config()
    seed = NormalizedOpenAlexRecord.model_validate(
        _make_record(openalex_id="https://openalex.org/WSEED", title="Seed", publication_year=2018)
    )
    candidate = NormalizedOpenAlexRecord.model_validate(
        _make_record(openalex_id="https://openalex.org/WCAND", title="Candidate", publication_year=2020)
    )

    score = temporal.score(seed, candidate, theory)
    assert score == pytest.approx(exp(-2 / theory.sim_parameters.temporal_tau), abs=1e-6)


def test_rank_local_corpus_writes_artifacts(tmp_path: Path) -> None:
    theory = load_theory_config()
    run_dir = _write_run_artifacts(tmp_path)

    summary = rank_local_corpus(run_dir=run_dir, theory=theory, top_k=2)

    scored_path = Path(summary.output_paths.scored_candidates_jsonl)
    summary_path = Path(summary.output_paths.ranking_summary_json)

    assert scored_path.exists()
    assert summary_path.exists()
    assert summary.candidate_count == 3
    assert summary.ignored_orphan_edges == 1
    assert summary.top_results[0].openalex_id == "https://openalex.org/WCAND1"

    scored_rows = [json.loads(line) for line in scored_path.read_text(encoding="utf-8").splitlines()]
    assert len(scored_rows) == 3
    assert scored_rows[0]["rank"] == 1
    assert scored_rows[0]["feature_values"]["direct_citation"] == 1.0
    masked_candidate = next(row for row in scored_rows if row["openalex_id"] == "https://openalex.org/WCAND3")
    assert masked_candidate["feature_values"]["temporal"] is None
    assert masked_candidate["sim"] > 0.0

    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["score_ranges"]["sim_max"] >= summary_payload["score_ranges"]["sim_min"]
