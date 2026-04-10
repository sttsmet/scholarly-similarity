from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from src.config import TheoryConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord
from src.rank.scorer import CandidateScorer, ScoredCandidateRecord


class RankingOutputPaths(BaseModel):
    """Absolute artifact paths written by the local corpus ranker."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    scored_candidates_jsonl: str
    ranking_summary_json: str


class ScoreRanges(BaseModel):
    """Minimum and maximum score values across the ranked candidates."""

    model_config = ConfigDict(extra="forbid")

    sim_min: float | None = Field(default=None, ge=0.0, le=1.0)
    sim_max: float | None = Field(default=None, ge=0.0, le=1.0)
    conf_min: float | None = Field(default=None, ge=0.0, le=1.0)
    conf_max: float | None = Field(default=None, ge=0.0, le=1.0)


class TopResultSummary(BaseModel):
    """Small ranked preview entry shown in CLI and summary JSON."""

    model_config = ConfigDict(extra="forbid")

    rank: int
    openalex_id: str
    title: str
    sim: float = Field(ge=0.0, le=1.0)
    conf: float = Field(ge=0.0, le=1.0)


class RankingSummary(BaseModel):
    """Structured result returned by the local corpus ranking command."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    seed_openalex_id: str
    candidate_count: int
    scored_count: int
    top_k: int
    ignored_orphan_edges: int
    output_paths: RankingOutputPaths
    score_ranges: ScoreRanges
    top_results: list[TopResultSummary]


class Ranker:
    """Compatibility wrapper for deterministic in-memory ranking."""

    def __init__(self, pool_builder: object | None, scorer: CandidateScorer) -> None:
        self.pool_builder = pool_builder
        self.scorer = scorer

    def rank(
        self,
        seed: NormalizedOpenAlexRecord,
        candidates: list[NormalizedOpenAlexRecord],
    ) -> list[ScoredCandidateRecord]:
        pooled = candidates
        if self.pool_builder is not None and hasattr(self.pool_builder, "build"):
            pooled = self.pool_builder.build(seed, candidates)

        scorer = self.scorer.for_local_records(seed=seed, local_records=pooled)
        ranked = [scorer.score(seed, candidate) for candidate in pooled]
        ranked.sort(
            key=lambda item: (
                -item.sim,
                -item.conf,
                -(item.publication_year or 0),
                item.title.lower(),
                item.openalex_id,
            )
        )
        return [
            candidate.model_copy(update={"rank": index})
            for index, candidate in enumerate(ranked, start=1)
        ]


def rank_local_corpus(
    *,
    run_dir: Path,
    theory: TheoryConfig,
    top_k: int,
    output_dir: Path | None = None,
) -> RankingSummary:
    """Rank the local one-hop corpus using only existing run artifacts."""

    output_root = output_dir or run_dir
    output_root.mkdir(parents=True, exist_ok=True)
    seed_record = _load_record(run_dir / "seed_record.json")
    paper_records = _load_papers(run_dir / "papers.jsonl")
    known_ids = {seed_record.openalex_id, *(record.openalex_id for record in paper_records)}
    ignored_orphan_edges = _count_orphan_edges(run_dir / "edges.jsonl", known_ids)

    candidates = [record for record in paper_records if record.openalex_id != seed_record.openalex_id]
    scorer = CandidateScorer(
        theory,
        seed=seed_record,
        local_records=candidates,
    )
    ranked = [scorer.score(seed_record, candidate) for candidate in candidates]
    ranked.sort(
        key=lambda item: (
            -item.sim,
            -item.conf,
            -(item.publication_year or 0),
            item.title.lower(),
            item.openalex_id,
        )
    )
    ranked = [
        candidate.model_copy(update={"rank": index})
        for index, candidate in enumerate(ranked, start=1)
    ]

    scored_candidates_path = output_root / "scored_candidates.jsonl"
    ranking_summary_path = output_root / "ranking_summary.json"
    _write_jsonl(scored_candidates_path, [candidate.model_dump(mode="json") for candidate in ranked])

    summary = RankingSummary(
        run_dir=str(output_root),
        seed_openalex_id=seed_record.openalex_id,
        candidate_count=len(candidates),
        scored_count=len(ranked),
        top_k=top_k,
        ignored_orphan_edges=ignored_orphan_edges,
        output_paths=RankingOutputPaths(
            run_dir=str(output_root),
            scored_candidates_jsonl=str(scored_candidates_path),
            ranking_summary_json=str(ranking_summary_path),
        ),
        score_ranges=_build_score_ranges(ranked),
        top_results=[
            TopResultSummary(
                rank=candidate.rank,
                openalex_id=candidate.openalex_id,
                title=candidate.title,
                sim=candidate.sim,
                conf=candidate.conf,
            )
            for candidate in ranked[:top_k]
        ],
    )
    _write_json(ranking_summary_path, summary.model_dump(mode="json"))
    return summary


def _load_record(path: Path) -> NormalizedOpenAlexRecord:
    if not path.exists():
        raise FileNotFoundError(path)
    return NormalizedOpenAlexRecord.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _load_papers(path: Path) -> list[NormalizedOpenAlexRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    ordered_records: dict[str, NormalizedOpenAlexRecord] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            record = NormalizedOpenAlexRecord.model_validate(json.loads(line))
            ordered_records.setdefault(record.openalex_id, record)
    return list(ordered_records.values())


def _count_orphan_edges(path: Path, known_ids: set[str]) -> int:
    if not path.exists():
        raise FileNotFoundError(path)

    orphan_count = 0
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                orphan_count += 1
                continue
            src = payload.get("src")
            dst = payload.get("dst")
            if not isinstance(src, str) or not isinstance(dst, str):
                orphan_count += 1
                continue
            if src not in known_ids or dst not in known_ids:
                orphan_count += 1
    return orphan_count


def _build_score_ranges(ranked: list[ScoredCandidateRecord]) -> ScoreRanges:
    if not ranked:
        return ScoreRanges()

    sim_values = [candidate.sim for candidate in ranked]
    conf_values = [candidate.conf for candidate in ranked]
    return ScoreRanges(
        sim_min=round(min(sim_values), 6),
        sim_max=round(max(sim_values), 6),
        conf_min=round(min(conf_values), 6),
        conf_max=round(max(conf_values), 6),
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
