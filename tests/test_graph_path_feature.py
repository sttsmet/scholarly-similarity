from __future__ import annotations

import json
from math import isclose
from pathlib import Path

from src.config import TheoryConfig, load_theory_config
from src.features.confidence import score as confidence_score
from src.features.graph_path import score as graph_path_score
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
    abstract_text: str | None = "Quantum bridge system",
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
        "topics": topics or ["Physics"],
        "abstract_text": abstract_text,
        "candidate_origins": candidate_origins or [],
        "source": "openalex",
    }


def _theory_with_graph_weight(weight: float) -> TheoryConfig:
    base = load_theory_config()
    payload = base.model_dump(mode="json")
    payload["sim_weights"]["graph_path"] = weight
    return TheoryConfig.model_validate(payload)


def test_graph_path_score_detects_length_two_bridge() -> None:
    theory = load_theory_config()
    seed = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WSEED",
            title="Seed",
            referenced_works=["https://openalex.org/WBRIDGE"],
        )
    )
    candidate = NormalizedOpenAlexRecord.model_validate(
        _make_record(
            openalex_id="https://openalex.org/WCAND",
            title="Candidate",
            referenced_works=["https://openalex.org/WBRIDGE"],
        )
    )

    score = graph_path_score(seed, candidate, theory)

    assert isclose(score, 0.632121, abs_tol=1e-6)


def test_graph_path_zero_weight_preserves_legacy_behavior_and_masking(tmp_path: Path) -> None:
    theory = load_theory_config()
    run_dir = tmp_path / "runs" / "graph_path_zero_weight"
    run_dir.mkdir(parents=True, exist_ok=True)

    seed = _make_record(
        openalex_id="https://openalex.org/WSEED",
        title="Seed paper",
        publication_year=2018,
        referenced_works=["https://openalex.org/WBRIDGE", "https://openalex.org/WREF"],
        related_works=["https://openalex.org/WMID"],
        topics=["Physics", "Quantum"],
        abstract_text="Quantum bridge protocol",
    )
    candidate = _make_record(
        openalex_id="https://openalex.org/WCAND",
        title="Bridge candidate",
        publication_year=2018,
        referenced_works=["https://openalex.org/WBRIDGE", "https://openalex.org/WREF"],
        related_works=[],
        topics=["Physics", "Quantum"],
        abstract_text="Quantum bridge protocol",
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate["openalex_id"],
                    "edge_type": "seed_references",
                }
            )
        )
        handle.write("\n")

    summary = rank_local_corpus(run_dir=run_dir, theory=theory, top_k=1)
    scored_rows = [
        json.loads(line)
        for line in Path(summary.output_paths.scored_candidates_jsonl).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    row = scored_rows[0]

    assert row["feature_values"]["graph_path"] is None
    assert "graph_path" not in row["exp"]["masked_features"]
    assert row["sim"] > 0.0


def test_graph_path_reason_and_confidence_remain_deterministic(tmp_path: Path) -> None:
    theory = _theory_with_graph_weight(0.2)
    run_dir = tmp_path / "runs" / "graph_path_active"
    run_dir.mkdir(parents=True, exist_ok=True)

    seed = _make_record(
        openalex_id="https://openalex.org/WSEED",
        title="Seed paper",
        publication_year=2018,
        referenced_works=["https://openalex.org/WBRIDGE"],
        related_works=["https://openalex.org/WMID"],
        topics=["Physics", "Quantum"],
        abstract_text="Quantum bridge protocol",
    )
    candidate = _make_record(
        openalex_id="https://openalex.org/WCAND",
        title="Bridge candidate",
        publication_year=2018,
        referenced_works=["https://openalex.org/WBRIDGE"],
        related_works=[],
        topics=["Physics"],
        abstract_text="Bridge protocol",
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate["openalex_id"],
                    "edge_type": "seed_related",
                }
            )
        )
        handle.write("\n")

    summary = rank_local_corpus(run_dir=run_dir, theory=theory, top_k=1)
    scored_rows = [
        json.loads(line)
        for line in Path(summary.output_paths.scored_candidates_jsonl).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    row = scored_rows[0]

    expected_confidence = confidence_score(
        NormalizedOpenAlexRecord.model_validate(seed),
        NormalizedOpenAlexRecord.model_validate(candidate),
        {
            key: value
            for key, value in row["feature_values"].items()
            if key != "graph_path"
        },
        load_theory_config(),
    ).score

    assert row["feature_values"]["graph_path"] is not None
    graph_factor = next(
        factor
        for factor in row["exp"]["top_factors"]
        if factor["name"] == "graph_path"
    )
    assert "paths=" in graph_factor["reason"]
    assert isclose(row["conf"], expected_confidence, abs_tol=1e-6)
