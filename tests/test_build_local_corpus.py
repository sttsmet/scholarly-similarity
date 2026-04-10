from __future__ import annotations

import json
from pathlib import Path

import httpx

from src.graph.build_local_corpus import build_local_corpus
from src.ingest.cache_store import CacheStore
from src.ingest.doi_resolver import DOIResolver
from src.ingest.openalex_client import OpenAlexClient


def _make_payload(
    *,
    work_id: str,
    title: str,
    doi: str | None,
    referenced_works: list[str] | None = None,
    related_works: list[str] | None = None,
) -> dict:
    return {
        "id": f"https://openalex.org/{work_id}",
        "doi": doi,
        "display_name": title,
        "publication_year": 2020,
        "cited_by_count": 12,
        "referenced_works": referenced_works or [],
        "related_works": related_works or [],
        "primary_topic": {"display_name": "Physics"},
        "topics": [{"display_name": "Physics"}],
        "abstract_inverted_index": {
            "Example": [0],
            "abstract": [1],
        },
    }


def test_build_local_corpus_writes_artifacts(tmp_path: Path) -> None:
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1", "https://openalex.org/WREF2"],
        related_works=["https://openalex.org/WREL1", "https://openalex.org/WREF1"],
    )
    payloads = {
        "https://doi.org/10.1038/nphys1170": seed_payload,
        "WREF1": _make_payload(work_id="WREF1", title="Ref one", doi=None),
        "WREF2": _make_payload(work_id="WREF2", title="Ref two", doi="https://doi.org/10.1000/ref2"),
        "WREL1": _make_payload(work_id="WREL1", title="Related one", doi="https://doi.org/10.1000/rel1"),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=payloads["https://doi.org/10.1038/nphys1170"], request=request)
        return httpx.Response(200, json=payloads[path.rsplit("/", 1)[-1]], request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    result = build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=2,
        max_related=2,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    manifest_path = Path(result.output_paths.manifest)
    papers_path = Path(result.output_paths.papers_jsonl)
    edges_path = Path(result.output_paths.edges_jsonl)

    assert manifest_path.exists()
    assert papers_path.exists()
    assert edges_path.exists()
    assert result.counts.papers == 4
    assert result.counts.edges == 4
    assert result.counts.failures == 0
    assert result.counts.eligible_hard_negative_pool_size == 0
    assert result.counts.considered_hard_negative_pool_size == 0
    assert result.counts.hard_negative_shortfall == 0
    assert result.completed_stages == [
        "seed_record_loaded",
        "references_selected",
        "related_selected",
        "hard_negative_pool_built",
        "hard_negatives_selected",
        "candidate_records_resolved",
        "corpus_manifest_written",
    ]

    papers = [json.loads(line) for line in papers_path.read_text(encoding="utf-8").splitlines()]
    edges = [json.loads(line) for line in edges_path.read_text(encoding="utf-8").splitlines()]

    assert papers[0]["openalex_id"] == "https://openalex.org/WSEED"
    assert any(paper["openalex_id"] == "https://openalex.org/WREF1" for paper in papers)
    assert any(edge["edge_type"] == "seed_related" for edge in edges)


def test_build_local_corpus_uses_cache_on_repeated_runs(tmp_path: Path) -> None:
    call_count = 0
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1"],
        related_works=["https://openalex.org/WREL1"],
    )
    payloads = {
        "https://doi.org/10.1038/nphys1170": seed_payload,
        "WREF1": _make_payload(work_id="WREF1", title="Ref one", doi=None),
        "WREL1": _make_payload(work_id="WREL1", title="Related one", doi="https://doi.org/10.1000/rel1"),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=payloads["https://doi.org/10.1038/nphys1170"], request=request)
        return httpx.Response(200, json=payloads[path.rsplit("/", 1)[-1]], request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )
    build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    assert call_count == 3


def test_build_local_corpus_records_partial_failures(tmp_path: Path) -> None:
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1"],
        related_works=["https://openalex.org/WMISSING"],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=seed_payload, request=request)
        if path.endswith("/works/WREF1"):
            return httpx.Response(200, json=_make_payload(work_id="WREF1", title="Ref one", doi=None), request=request)
        return httpx.Response(404, json={"error": "not found"}, request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    result = build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    assert result.counts.failures == 1
    assert result.output_paths.failures_json is not None
    failures = json.loads(Path(result.output_paths.failures_json).read_text(encoding="utf-8"))
    assert failures[0]["openalex_id"] == "https://openalex.org/WMISSING"


def test_build_local_corpus_adds_hard_negatives_deterministically(tmp_path: Path) -> None:
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1"],
        related_works=["https://openalex.org/WREL1"],
    )
    payloads = {
        "https://doi.org/10.1038/nphys1170": seed_payload,
        "WREF1": _make_payload(
            work_id="WREF1",
            title="Ref one",
            doi=None,
            referenced_works=["https://openalex.org/WHARD1", "https://openalex.org/WDIRECT"],
        ),
        "WREL1": _make_payload(
            work_id="WREL1",
            title="Related one",
            doi="https://doi.org/10.1000/rel1",
            related_works=["https://openalex.org/WHARD1", "https://openalex.org/WOUTTOPIC"],
        ),
        "WHARD1": _make_payload(work_id="WHARD1", title="Hard negative", doi=None),
        "WDIRECT": {
            **_make_payload(work_id="WDIRECT", title="Direct inbound", doi=None),
            "referenced_works": ["https://openalex.org/WSEED"],
        },
        "WOUTTOPIC": {
            **_make_payload(work_id="WOUTTOPIC", title="Topic mismatch", doi=None),
            "primary_topic": {"display_name": "Biology"},
            "topics": [{"display_name": "Biology"}],
        },
    }

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=payloads["https://doi.org/10.1038/nphys1170"], request=request)
        return httpx.Response(200, json=payloads[path.rsplit("/", 1)[-1]], request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    result = build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        max_hard_negatives=1,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    papers = [
        json.loads(line)
        for line in Path(result.output_paths.papers_jsonl).read_text(encoding="utf-8").splitlines()
    ]

    assert result.counts.selected_hard_negatives == 1
    assert any(
        paper["openalex_id"] == "https://openalex.org/WHARD1"
        and paper["candidate_origins"] == ["hard_negative"]
        for paper in papers
    )
    assert not any(paper["openalex_id"] == "https://openalex.org/WDIRECT" for paper in papers)
    assert result.counts.eligible_hard_negative_pool_size == 3
    assert result.counts.considered_hard_negative_pool_size == 1
    assert result.counts.hard_negative_shortfall == 0


def test_build_local_corpus_hard_negative_shortfall_uses_finite_pool(tmp_path: Path) -> None:
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1"],
        related_works=["https://openalex.org/WREL1"],
    )
    payloads = {
        "https://doi.org/10.1038/nphys1170": seed_payload,
        "WREF1": _make_payload(
            work_id="WREF1",
            title="Ref one",
            doi=None,
            referenced_works=["https://openalex.org/WHARD1", "https://openalex.org/WHARD2"],
            related_works=["https://openalex.org/WREF1"],
        ),
        "WREL1": _make_payload(
            work_id="WREL1",
            title="Related one",
            doi="https://doi.org/10.1000/rel1",
            referenced_works=["https://openalex.org/WHARD2"],
            related_works=["https://openalex.org/WSEED", "https://openalex.org/WREL1"],
        ),
        "WHARD1": _make_payload(work_id="WHARD1", title="Hard one", doi=None),
        "WHARD2": _make_payload(work_id="WHARD2", title="Hard two", doi=None),
    }
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=payloads["https://doi.org/10.1038/nphys1170"], request=request)
        return httpx.Response(200, json=payloads[path.rsplit("/", 1)[-1]], request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    result = build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        max_hard_negatives=3,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    papers = [
        json.loads(line)
        for line in Path(result.output_paths.papers_jsonl).read_text(encoding="utf-8").splitlines()
    ]

    assert result.counts.selected_hard_negatives == 2
    assert result.counts.eligible_hard_negative_pool_size == 2
    assert result.counts.considered_hard_negative_pool_size == 2
    assert result.counts.hard_negative_shortfall == 1
    assert call_count == 5
    assert any(paper["openalex_id"] == "https://openalex.org/WHARD1" for paper in papers)
    assert any(paper["openalex_id"] == "https://openalex.org/WHARD2" for paper in papers)


def test_build_local_corpus_deduplicates_hard_negative_pool_before_resolution(tmp_path: Path) -> None:
    seed_payload = _make_payload(
        work_id="WSEED",
        title="Seed paper",
        doi="https://doi.org/10.1038/nphys1170",
        referenced_works=["https://openalex.org/WREF1"],
        related_works=["https://openalex.org/WREL1"],
    )
    payloads = {
        "https://doi.org/10.1038/nphys1170": seed_payload,
        "WREF1": _make_payload(
            work_id="WREF1",
            title="Ref one",
            doi=None,
            referenced_works=["https://openalex.org/WHARD1", "WHARD1", "https://openalex.org/WREF1"],
            related_works=["https://openalex.org/WREL1"],
        ),
        "WREL1": _make_payload(
            work_id="WREL1",
            title="Related one",
            doi="https://doi.org/10.1000/rel1",
            referenced_works=["WHARD1/"],
            related_works=["https://openalex.org/WSEED"],
        ),
        "WHARD1": _make_payload(work_id="WHARD1", title="Hard one", doi=None),
    }
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        path = request.url.path
        if path.endswith("/works/https://doi.org/10.1038/nphys1170"):
            return httpx.Response(200, json=payloads["https://doi.org/10.1038/nphys1170"], request=request)
        return httpx.Response(200, json=payloads[path.rsplit("/", 1)[-1]], request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    cache_store = CacheStore(tmp_path / "data" / "cache")
    openalex_client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    result = build_local_corpus(
        doi="10.1038/nphys1170",
        max_references=1,
        max_related=1,
        max_hard_negatives=3,
        refresh=False,
        runs_root=tmp_path / "runs",
        doi_resolver=doi_resolver,
        openalex_client=openalex_client,
        cache_store=cache_store,
    )

    assert result.counts.selected_hard_negatives == 1
    assert result.counts.eligible_hard_negative_pool_size == 1
    assert result.counts.considered_hard_negative_pool_size == 1
    assert result.counts.hard_negative_shortfall == 2
    assert call_count == 4
