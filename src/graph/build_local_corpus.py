from __future__ import annotations

import json
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from src.ingest.cache_store import CacheStore, CacheStoreError
from src.ingest.doi_resolver import DOIResolver, NORMALIZATION_VERSION, NormalizedOpenAlexRecord, build_normalized_record, normalize_doi
from src.ingest.openalex_client import OpenAlexClient, OpenAlexError, normalize_openalex_work_id


HARD_NEGATIVE_YEAR_WINDOW = 5


class CorpusFailure(BaseModel):
    """Minimal failure record for neighbor fetch or normalization problems."""

    model_config = ConfigDict(extra="forbid")

    openalex_id: str
    edge_type: Literal["seed_references", "seed_related", "hard_negative"]
    error: str


class CorpusCounts(BaseModel):
    """Summary counts for one local corpus build."""

    model_config = ConfigDict(extra="forbid")

    papers: int
    edges: int
    failures: int
    selected_references: int
    selected_related: int
    selected_hard_negatives: int = 0
    eligible_hard_negative_pool_size: int = 0
    considered_hard_negative_pool_size: int = 0
    hard_negative_shortfall: int = 0


class CorpusOutputPaths(BaseModel):
    """Absolute artifact paths written for one local corpus build."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    manifest: str
    seed_record: str
    papers_jsonl: str
    edges_jsonl: str
    failures_json: str | None = None


class LocalCorpusResult(BaseModel):
    """Structured result returned by the local corpus builder."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    input_doi: str
    normalized_doi: str
    seed_openalex_id: str
    created_at: str
    max_references: int
    max_related: int
    max_hard_negatives: int = 0
    counts: CorpusCounts
    output_paths: CorpusOutputPaths
    completed_stages: list[str] = Field(default_factory=list)


@dataclass
class HardNegativeSelectionResult:
    """Deterministic hard-negative selection summary for one seed."""

    records: list[NormalizedOpenAlexRecord]
    eligible_pool_size: int
    considered_pool_size: int


def build_local_corpus_run_id(
    *,
    doi: str,
    max_references: int,
    max_related: int,
    max_hard_negatives: int = 0,
) -> str:
    """Build the deterministic local run id for a DOI and corpus parameters."""

    normalized_doi = normalize_doi(doi)
    return _build_run_id(
        normalized_doi,
        max_references,
        max_related,
        max_hard_negatives,
    )


def build_local_corpus(
    *,
    doi: str,
    max_references: int,
    max_related: int,
    max_hard_negatives: int = 0,
    refresh: bool,
    runs_root: Path,
    doi_resolver: DOIResolver,
    openalex_client: OpenAlexClient,
    cache_store: CacheStore,
) -> LocalCorpusResult:
    """Build a deterministic local corpus rooted at a seed DOI."""

    completed_stages: list[str] = []
    seed_resolution = doi_resolver.resolve(doi, refresh=refresh)
    seed_record = seed_resolution.record
    completed_stages.append("seed_record_loaded")
    selected_references = seed_record.referenced_works[:max_references]
    selected_related = seed_record.related_works[:max_related]
    run_id = _build_run_id(
        seed_resolution.normalized_doi,
        max_references,
        max_related,
        max_hard_negatives,
    )
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    paper_records: OrderedDict[str, NormalizedOpenAlexRecord] = OrderedDict()
    paper_records[seed_record.openalex_id] = seed_record
    origin_map: dict[str, set[str]] = {seed_record.openalex_id: set()}
    edge_rows: list[dict[str, str]] = []
    failures: list[CorpusFailure] = []
    neighbor_cache: dict[str, NormalizedOpenAlexRecord | None] = {}
    positive_neighbors: list[NormalizedOpenAlexRecord] = []

    for openalex_id in selected_references:
        edge_rows.append(_build_edge(seed_record.openalex_id, openalex_id, "seed_references"))
        record = _load_neighbor_record(
            openalex_id=openalex_id,
            refresh=refresh,
            openalex_client=openalex_client,
            cache_store=cache_store,
            failures=failures,
            edge_type="seed_references",
            neighbor_cache=neighbor_cache,
        )
        if record is None:
            continue
        _merge_candidate_record(
            paper_records=paper_records,
            origin_map=origin_map,
            seed_record=seed_record,
            record=record,
            base_origins={"seed_reference"},
        )
        if record.openalex_id != seed_record.openalex_id:
            positive_neighbors.append(record)
    completed_stages.append("references_selected")

    for openalex_id in selected_related:
        edge_rows.append(_build_edge(seed_record.openalex_id, openalex_id, "seed_related"))
        record = _load_neighbor_record(
            openalex_id=openalex_id,
            refresh=refresh,
            openalex_client=openalex_client,
            cache_store=cache_store,
            failures=failures,
            edge_type="seed_related",
            neighbor_cache=neighbor_cache,
        )
        if record is None:
            continue
        _merge_candidate_record(
            paper_records=paper_records,
            origin_map=origin_map,
            seed_record=seed_record,
            record=record,
            base_origins={"seed_related"},
        )
        if record.openalex_id != seed_record.openalex_id:
            positive_neighbors.append(record)
    completed_stages.append("related_selected")

    hard_negative_selection = _collect_hard_negatives(
        seed_record=seed_record,
        positive_neighbors=positive_neighbors,
        max_hard_negatives=max_hard_negatives,
        refresh=refresh,
        openalex_client=openalex_client,
        cache_store=cache_store,
        failures=failures,
        neighbor_cache=neighbor_cache,
        seen_paper_ids=set(paper_records.keys()),
    )
    completed_stages.append("hard_negative_pool_built")
    hard_negative_records = hard_negative_selection.records
    for record in hard_negative_records:
        _merge_candidate_record(
            paper_records=paper_records,
            origin_map=origin_map,
            seed_record=seed_record,
            record=record,
            base_origins={"hard_negative"},
        )
    completed_stages.append("hard_negatives_selected")

    enriched_records = [
        _apply_candidate_origins(record, origin_map.get(record.openalex_id, set()))
        for record in paper_records.values()
    ]
    completed_stages.append("candidate_records_resolved")

    seed_record_path = run_dir / "seed_record.json"
    papers_path = run_dir / "papers.jsonl"
    edges_path = run_dir / "edges.jsonl"
    failures_path = run_dir / "failures.json"
    manifest_path = run_dir / "manifest.json"

    _write_json(seed_record_path, enriched_records[0].model_dump(mode="json"))
    _write_jsonl(papers_path, [record.model_dump(mode="json") for record in enriched_records])
    _write_jsonl(edges_path, edge_rows)

    failures_json_path: Path | None = None
    if failures:
        _write_json(failures_path, [failure.model_dump(mode="json") for failure in failures])
        failures_json_path = failures_path

    created_at = datetime.now(timezone.utc).isoformat()
    result = LocalCorpusResult(
        run_id=run_id,
        input_doi=doi,
        normalized_doi=seed_resolution.normalized_doi,
        seed_openalex_id=seed_record.openalex_id,
        created_at=created_at,
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
        counts=CorpusCounts(
            papers=len(enriched_records),
            edges=len(edge_rows),
            failures=len(failures),
            selected_references=len(selected_references),
            selected_related=len(selected_related),
            selected_hard_negatives=len(hard_negative_records),
            eligible_hard_negative_pool_size=hard_negative_selection.eligible_pool_size,
            considered_hard_negative_pool_size=hard_negative_selection.considered_pool_size,
            hard_negative_shortfall=max(0, max_hard_negatives - len(hard_negative_records)),
        ),
        output_paths=CorpusOutputPaths(
            run_dir=str(run_dir),
            manifest=str(manifest_path),
            seed_record=str(seed_record_path),
            papers_jsonl=str(papers_path),
            edges_jsonl=str(edges_path),
            failures_json=str(failures_json_path) if failures_json_path is not None else None,
        ),
        completed_stages=[*completed_stages, "corpus_manifest_written"],
    )

    _write_json(manifest_path, result.model_dump(mode="json"))
    return result


def _collect_hard_negatives(
    *,
    seed_record: NormalizedOpenAlexRecord,
    positive_neighbors: list[NormalizedOpenAlexRecord],
    max_hard_negatives: int,
    refresh: bool,
    openalex_client: OpenAlexClient,
    cache_store: CacheStore,
    failures: list[CorpusFailure],
    neighbor_cache: dict[str, NormalizedOpenAlexRecord | None],
    seen_paper_ids: set[str],
) -> HardNegativeSelectionResult:
    if max_hard_negatives <= 0:
        return HardNegativeSelectionResult(records=[], eligible_pool_size=0, considered_pool_size=0)

    excluded_ids = _canonicalize_openalex_id_set(
        {
            seed_record.openalex_id,
            *seed_record.referenced_works,
            *seed_record.related_works,
            *seen_paper_ids,
        }
    )
    eligible_pool_ids = _collect_second_hop_ids(records=positive_neighbors, excluded_ids=excluded_ids)
    candidate_ids = eligible_pool_ids[:max_hard_negatives]

    scored_candidates: list[tuple[int, int, int, str, NormalizedOpenAlexRecord]] = []
    for openalex_id in candidate_ids:
        record = _load_neighbor_record(
            openalex_id=openalex_id,
            refresh=refresh,
            openalex_client=openalex_client,
            cache_store=cache_store,
            failures=failures,
            edge_type="hard_negative",
            neighbor_cache=neighbor_cache,
        )
        if record is None or _canonicalize_openalex_id(record.openalex_id) in excluded_ids:
            continue
        if _has_direct_citation(seed_record, record):
            continue

        hard_negative_rank = _hard_negative_rank(seed_record, record)
        if hard_negative_rank is None:
            continue
        scored_candidates.append((*hard_negative_rank, record.openalex_id, record))

    scored_candidates.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
    selected = [record for _, _, _, _, record in scored_candidates[:max_hard_negatives]]
    return HardNegativeSelectionResult(
        records=selected,
        eligible_pool_size=len(eligible_pool_ids),
        considered_pool_size=len(candidate_ids),
    )


def _collect_second_hop_ids(
    *,
    records: list[NormalizedOpenAlexRecord],
    excluded_ids: set[str],
) -> list[str]:
    ordered_ids: OrderedDict[str, str] = OrderedDict()
    for record in records:
        for openalex_id in record.related_works:
            _add_second_hop_id(
                ordered_ids=ordered_ids,
                openalex_id=openalex_id,
                excluded_ids=excluded_ids,
            )
        for openalex_id in record.referenced_works:
            _add_second_hop_id(
                ordered_ids=ordered_ids,
                openalex_id=openalex_id,
                excluded_ids=excluded_ids,
            )
    return list(ordered_ids.values())


def _add_second_hop_id(
    *,
    ordered_ids: OrderedDict[str, str],
    openalex_id: str,
    excluded_ids: set[str],
) -> None:
    canonical_id = _canonicalize_openalex_id(openalex_id)
    if canonical_id in excluded_ids:
        return
    ordered_ids.setdefault(canonical_id, openalex_id.strip().rstrip("/") or openalex_id)


def _canonicalize_openalex_id_set(openalex_ids: set[str]) -> set[str]:
    return {_canonicalize_openalex_id(openalex_id) for openalex_id in openalex_ids}


def _canonicalize_openalex_id(openalex_id: str) -> str:
    stripped = openalex_id.strip().rstrip("/") or openalex_id
    try:
        return normalize_openalex_work_id(stripped)
    except ValueError:
        return stripped


def _hard_negative_rank(
    seed_record: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
) -> tuple[int, int, int] | None:
    primary_match = int(
        bool(seed_record.primary_topic)
        and bool(candidate.primary_topic)
        and seed_record.primary_topic == candidate.primary_topic
    )
    topic_overlap = len(set(seed_record.topics) & set(candidate.topics))
    if primary_match == 0 and topic_overlap == 0:
        return None

    if seed_record.publication_year is not None and candidate.publication_year is not None:
        year_gap = abs(seed_record.publication_year - candidate.publication_year)
        if year_gap > HARD_NEGATIVE_YEAR_WINDOW:
            return None
    else:
        year_gap = HARD_NEGATIVE_YEAR_WINDOW + 1

    return primary_match, topic_overlap, year_gap


def _merge_candidate_record(
    *,
    paper_records: OrderedDict[str, NormalizedOpenAlexRecord],
    origin_map: dict[str, set[str]],
    seed_record: NormalizedOpenAlexRecord,
    record: NormalizedOpenAlexRecord,
    base_origins: set[str],
) -> None:
    paper_records.setdefault(record.openalex_id, record)
    origin_map.setdefault(record.openalex_id, set()).update(base_origins)
    if _has_direct_citation(seed_record, record):
        origin_map[record.openalex_id].add("direct_neighbor")


def _apply_candidate_origins(
    record: NormalizedOpenAlexRecord,
    origins: set[str],
) -> NormalizedOpenAlexRecord:
    return record.model_copy(update={"candidate_origins": sorted(origins)})


def _has_direct_citation(seed_record: NormalizedOpenAlexRecord, candidate: NormalizedOpenAlexRecord) -> bool:
    return (
        candidate.openalex_id in seed_record.referenced_works
        or seed_record.openalex_id in candidate.referenced_works
    )


def _load_neighbor_record(
    *,
    openalex_id: str,
    refresh: bool,
    openalex_client: OpenAlexClient,
    cache_store: CacheStore,
    failures: list[CorpusFailure],
    edge_type: Literal["seed_references", "seed_related", "hard_negative"],
    neighbor_cache: dict[str, NormalizedOpenAlexRecord | None],
) -> NormalizedOpenAlexRecord | None:
    canonical_id = openalex_id.strip().rstrip("/") or openalex_id
    try:
        canonical_id = normalize_openalex_work_id(openalex_id)
        if canonical_id in neighbor_cache:
            return neighbor_cache[canonical_id]

        if refresh:
            raw_payload = None
        else:
            raw_payload = cache_store.get_work_raw(canonical_id)

        if raw_payload is None:
            lookup = openalex_client.fetch_work_by_openalex_id(canonical_id)
            raw_payload = lookup.payload
            cache_store.set_work_raw(canonical_id, raw_payload)

        record = build_normalized_record(raw_payload, None)
        cache_store.set_work_record(
            canonical_id,
            record.model_dump(mode="json"),
            normalization_version=NORMALIZATION_VERSION,
        )
        neighbor_cache[canonical_id] = record
        return record
    except (ValueError, CacheStoreError, OpenAlexError) as exc:
        failure = CorpusFailure(
            openalex_id=openalex_id,
            edge_type=edge_type,
            error=str(exc),
        )
        failures.append(failure)
        neighbor_cache[canonical_id] = None
        return None


def _build_edge(src: str, dst: str, edge_type: Literal["seed_references", "seed_related"]) -> dict[str, str]:
    return {
        "src": src,
        "dst": dst,
        "edge_type": edge_type,
    }


def _build_run_id(
    normalized_doi: str,
    max_references: int,
    max_related: int,
    max_hard_negatives: int,
) -> str:
    safe_doi = re.sub(r"[^A-Za-z0-9]+", "_", normalized_doi).strip("_").lower()
    run_id = f"doi_{safe_doi}__refs_{max_references}__related_{max_related}"
    if max_hard_negatives > 0:
        run_id = f"{run_id}__hardneg_{max_hard_negatives}"
    return run_id


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
