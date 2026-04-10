from __future__ import annotations

import json
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict


OPENALEX_WORK_SELECT_FIELDS = [
    "id",
    "doi",
    "display_name",
    "publication_year",
    "cited_by_count",
    "referenced_works",
    "related_works",
    "primary_topic",
    "topics",
    "abstract_inverted_index",
]


class OpenAlexError(RuntimeError):
    """Base error raised for OpenAlex request failures."""


class OpenAlexNotFoundError(OpenAlexError):
    """Raised when OpenAlex does not contain a work for the DOI."""


class OpenAlexLookupResult(BaseModel):
    """Successful raw work lookup result from OpenAlex."""

    model_config = ConfigDict(extra="forbid")

    doi: str | None = None
    openalex_id: str | None = None
    request_url: str
    payload: dict[str, Any]


class OpenAlexClient:
    """Minimal OpenAlex client for fetching a single work by external DOI id."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.http_client = http_client

    def fetch_work_by_doi(self, normalized_doi: str) -> OpenAlexLookupResult:
        """Fetch a single OpenAlex work using the external DOI id endpoint."""

        url = self._build_work_url(normalized_doi)
        payload, request_url = self._get_single_work_payload(
            url=url,
            not_found_message=f"OpenAlex work not found for DOI: {normalized_doi}",
            failure_context=f"DOI: {normalized_doi}",
        )
        return OpenAlexLookupResult(
            doi=normalized_doi,
            request_url=request_url,
            payload=payload,
        )

    def fetch_work_by_openalex_id(self, openalex_id: str) -> OpenAlexLookupResult:
        """Fetch a single OpenAlex work using the OpenAlex work id route."""

        normalized_work_id = normalize_openalex_work_id(openalex_id)
        url = self._build_work_id_url(normalized_work_id)
        payload, request_url = self._get_single_work_payload(
            url=url,
            not_found_message=f"OpenAlex work not found for id: {normalized_work_id}",
            failure_context=f"work id: {normalized_work_id}",
        )
        return OpenAlexLookupResult(
            openalex_id=normalized_work_id,
            request_url=request_url,
            payload=payload,
        )

    def _build_work_url(self, normalized_doi: str) -> str:
        """Build the single-work DOI endpoint URL."""

        return f"{self.base_url}/works/https://doi.org/{normalized_doi}"

    def _build_work_id_url(self, normalized_work_id: str) -> str:
        """Build the single-work OpenAlex id endpoint URL."""

        return f"{self.base_url}/works/{normalized_work_id}"

    def _get_single_work_payload(
        self,
        *,
        url: str,
        not_found_message: str,
        failure_context: str,
    ) -> tuple[dict[str, Any], str]:
        params = {"select": ",".join(OPENALEX_WORK_SELECT_FIELDS)}
        client = self.http_client or httpx.Client(timeout=self.timeout_seconds)
        created_client = self.http_client is None

        try:
            response = client.get(
                url,
                params=params,
                headers={"Accept": "application/json"},
                timeout=self.timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            raise OpenAlexError(f"OpenAlex request timed out for {failure_context}") from exc
        except httpx.RequestError as exc:
            raise OpenAlexError(f"OpenAlex request failed for {failure_context}") from exc
        finally:
            if created_client:
                client.close()

        if response.status_code == 404:
            raise OpenAlexNotFoundError(not_found_message)
        if response.status_code != 200:
            raise OpenAlexError(
                f"OpenAlex returned status {response.status_code} for {failure_context}"
            )

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise OpenAlexError("OpenAlex returned malformed JSON") from exc

        if not isinstance(payload, dict):
            raise OpenAlexError("OpenAlex returned a non-object payload")

        return payload, str(response.request.url)


def normalize_openalex_work_id(value: str) -> str:
    """Normalize a work identifier to the short OpenAlex work id form."""

    normalized = value.strip().rstrip("/")
    if normalized.startswith("https://openalex.org/"):
        normalized = normalized.rsplit("/", 1)[-1]
    if not normalized:
        raise ValueError("OpenAlex work id must not be empty")
    return normalized
