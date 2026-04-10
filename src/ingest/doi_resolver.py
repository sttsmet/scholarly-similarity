from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field

from src.ingest.cache_store import CacheStore
from src.ingest.openalex_client import OpenAlexClient


DOI_PREFIXES = ("https://doi.org/", "http://doi.org/", "doi:")
NORMALIZATION_VERSION = "openalex_work_v1"


class NormalizedOpenAlexRecord(BaseModel):
    """Normalized local record used by the first ingest vertical slice."""

    model_config = ConfigDict(extra="forbid")

    openalex_id: str
    doi: str | None
    title: str
    publication_year: int | None
    cited_by_count: int
    referenced_works: list[str]
    related_works: list[str]
    primary_topic: str | None
    topics: list[str]
    abstract_text: str | None
    candidate_origins: list[str] = Field(default_factory=list)
    source: Literal["openalex"] = "openalex"


class DoiResolution(BaseModel):
    """Structured result returned by DOI resolution and normalization."""

    model_config = ConfigDict(extra="forbid")

    input_doi: str
    normalized_doi: str
    cache_hit: bool
    raw_cache_path: str
    record_cache_path: str
    record: NormalizedOpenAlexRecord


def normalize_doi(value: str) -> str:
    """Normalize a DOI string for lookup and local storage."""

    normalized = value.strip()
    for prefix in DOI_PREFIXES:
        if normalized.lower().startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    normalized = normalized.strip().lower()
    if not normalized:
        raise ValueError("DOI must not be empty")
    return normalized


def build_normalized_record(
    payload: Mapping[str, Any],
    normalized_doi: str | None,
) -> NormalizedOpenAlexRecord:
    """Build a normalized local record from a raw OpenAlex work payload."""

    openalex_id = _require_string(payload, "id")
    title = _require_string(payload, "display_name")

    publication_year_raw = payload.get("publication_year")
    publication_year = int(publication_year_raw) if publication_year_raw is not None else None
    cited_by_count = int(payload.get("cited_by_count") or 0)

    primary_topic = _extract_topic_name(payload.get("primary_topic"))
    topics = _extract_topic_names(payload.get("topics"))
    abstract_text = build_abstract_text(payload.get("abstract_inverted_index"))

    return NormalizedOpenAlexRecord(
        openalex_id=openalex_id,
        doi=_extract_payload_doi(payload.get("doi"), normalized_doi),
        title=title,
        publication_year=publication_year,
        cited_by_count=cited_by_count,
        referenced_works=_extract_string_list(payload.get("referenced_works")),
        related_works=_extract_string_list(payload.get("related_works")),
        primary_topic=primary_topic,
        topics=topics,
        abstract_text=abstract_text,
        candidate_origins=[],
        source="openalex",
    )


def build_abstract_text(abstract_inverted_index: Any) -> str | None:
    """Reconstruct plain abstract text from OpenAlex inverted index data."""

    if not isinstance(abstract_inverted_index, Mapping) or not abstract_inverted_index:
        return None

    word_positions: dict[int, str] = {}
    max_position = -1

    for token, positions in abstract_inverted_index.items():
        if not isinstance(token, str) or not isinstance(positions, list):
            continue
        for position in positions:
            if not isinstance(position, int) or position < 0:
                continue
            word_positions[position] = token
            if position > max_position:
                max_position = position

    if max_position < 0:
        return None

    words = [word_positions.get(position, "") for position in range(max_position + 1)]
    text = " ".join(word for word in words if word)
    return text or None


class DOIResolver:
    """Coordinate DOI normalization, raw payload caching, and record normalization."""

    def __init__(self, client: OpenAlexClient, cache_store: CacheStore) -> None:
        self.client = client
        self.cache_store = cache_store

    def resolve(self, doi: str, *, refresh: bool = False) -> DoiResolution:
        """Resolve a DOI into a normalized local record."""

        normalized_doi = normalize_doi(doi)
        cache_hit = False

        if not refresh:
            raw_payload = self.cache_store.get_raw(normalized_doi)
            cache_hit = raw_payload is not None
        else:
            raw_payload = None

        if raw_payload is None:
            lookup = self.client.fetch_work_by_doi(normalized_doi)
            raw_payload = lookup.payload
            self.cache_store.set_raw(normalized_doi, raw_payload)

        record = build_normalized_record(raw_payload, normalized_doi)
        raw_cache_path = self.cache_store.raw_path(normalized_doi)
        record_cache_path = self.cache_store.set_record(
            normalized_doi,
            record.model_dump(mode="json"),
            normalization_version=NORMALIZATION_VERSION,
        )

        return DoiResolution(
            input_doi=doi,
            normalized_doi=normalized_doi,
            cache_hit=cache_hit,
            raw_cache_path=str(raw_cache_path),
            record_cache_path=str(record_cache_path),
            record=record,
        )


def _require_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"OpenAlex payload missing required string field: {field_name}")
    return value


def _extract_payload_doi(payload_doi: Any, normalized_doi: str | None) -> str | None:
    if isinstance(payload_doi, str) and payload_doi.strip():
        return normalize_doi(payload_doi)
    return normalized_doi


def _extract_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item]


def _extract_topic_name(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    display_name = value.get("display_name")
    if not isinstance(display_name, str) or not display_name.strip():
        return None
    return display_name


def _extract_topic_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    topic_names: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        display_name = item.get("display_name")
        if isinstance(display_name, str) and display_name.strip():
            topic_names.append(display_name)
    return topic_names
