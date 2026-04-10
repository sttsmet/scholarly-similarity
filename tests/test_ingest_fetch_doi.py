from __future__ import annotations

from pathlib import Path

import httpx

from src.ingest.cache_store import CacheStore
from src.ingest.doi_resolver import DOIResolver, build_normalized_record, normalize_doi
from src.ingest.openalex_client import OpenAlexClient


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


def test_normalize_doi_strips_prefixes_and_validates() -> None:
    assert normalize_doi(" doi:10.1038/nphys1170 ") == "10.1038/nphys1170"
    assert normalize_doi("https://doi.org/10.1038/nphys1170") == "10.1038/nphys1170"

    try:
        normalize_doi(" doi: ")
    except ValueError as exc:
        assert "must not be empty" in str(exc)
    else:
        raise AssertionError("Expected normalize_doi to reject empty values")


def test_cache_store_roundtrip(tmp_path: Path) -> None:
    cache_store = CacheStore(tmp_path / "data" / "cache")
    normalized_doi = "10.1038/nphys1170"

    assert cache_store.exists(normalized_doi) is False

    raw_path = cache_store.set_raw(normalized_doi, SAMPLE_WORK_PAYLOAD)
    record_path = cache_store.set_record(
        normalized_doi,
        {"title": SAMPLE_WORK_PAYLOAD["display_name"]},
        normalization_version="openalex_work_v1",
    )

    assert raw_path.name == "raw.json"
    assert record_path.name == "record.json"
    assert cache_store.exists(normalized_doi) is True
    assert cache_store.get_raw(normalized_doi) == SAMPLE_WORK_PAYLOAD
    assert cache_store.get_record(normalized_doi)["normalization_version"] == "openalex_work_v1"


def test_build_normalized_record_from_payload() -> None:
    record = build_normalized_record(SAMPLE_WORK_PAYLOAD, "10.1038/nphys1170")

    assert record.openalex_id == "https://openalex.org/W2741809807"
    assert record.doi == "10.1038/nphys1170"
    assert record.primary_topic == "Quantum optics"
    assert record.topics == ["Quantum optics", "Casimir effect"]
    assert record.abstract_text == "Observation of the dynamical Casimir effect"


def test_openalex_client_uses_single_work_endpoint_and_select() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json=SAMPLE_WORK_PAYLOAD, request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )

    result = client.fetch_work_by_doi("10.1038/nphys1170")

    assert result.payload["id"] == SAMPLE_WORK_PAYLOAD["id"]
    assert "/works/https://doi.org/10.1038/nphys1170" in seen["url"]
    assert "select=" in seen["url"]


def test_openalex_client_fetches_single_work_by_openalex_id() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json=SAMPLE_WORK_PAYLOAD, request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = OpenAlexClient(
        base_url="https://api.openalex.org",
        timeout_seconds=5.0,
        http_client=http_client,
    )

    result = client.fetch_work_by_openalex_id("https://openalex.org/W2741809807")

    assert result.openalex_id == "W2741809807"
    assert "/works/W2741809807" in seen["url"]
    assert "select=" in seen["url"]


def test_cache_hit_skips_http(tmp_path: Path) -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=SAMPLE_WORK_PAYLOAD, request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    resolver = DOIResolver(
        client=OpenAlexClient(
            base_url="https://api.openalex.org",
            timeout_seconds=5.0,
            http_client=http_client,
        ),
        cache_store=CacheStore(tmp_path / "data" / "cache"),
    )

    first = resolver.resolve("10.1038/nphys1170")
    second = resolver.resolve("10.1038/nphys1170")

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert call_count == 1


def test_refresh_forces_refetch(tmp_path: Path) -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json=SAMPLE_WORK_PAYLOAD, request=request)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    resolver = DOIResolver(
        client=OpenAlexClient(
            base_url="https://api.openalex.org",
            timeout_seconds=5.0,
            http_client=http_client,
        ),
        cache_store=CacheStore(tmp_path / "data" / "cache"),
    )

    resolver.resolve("10.1038/nphys1170")
    refreshed = resolver.resolve("10.1038/nphys1170", refresh=True)

    assert refreshed.cache_hit is False
    assert call_count == 2
