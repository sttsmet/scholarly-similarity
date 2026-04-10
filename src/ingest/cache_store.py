from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote


class CacheStoreError(RuntimeError):
    """Raised when cached files cannot be read or written safely."""


class CacheStore:
    """Readable on-disk cache for OpenAlex DOI payloads and normalized records."""

    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def exists(self, normalized_doi: str) -> bool:
        """Return whether a raw cached payload exists for the DOI."""

        return self.raw_path(normalized_doi).exists()

    def get_raw(self, normalized_doi: str) -> dict[str, Any] | None:
        """Load a cached raw OpenAlex payload if present."""

        path = self.raw_path(normalized_doi)
        if not path.exists():
            return None
        payload = self._read_json(path)
        if not isinstance(payload, dict):
            raise CacheStoreError(f"Cached raw payload is not a JSON object: {path}")
        return payload

    def set_raw(self, normalized_doi: str, payload: Mapping[str, Any]) -> Path:
        """Persist a raw OpenAlex payload to disk."""

        path = self.raw_path(normalized_doi)
        self._write_json(path, dict(payload))
        return path

    def get_record(self, normalized_doi: str) -> dict[str, Any] | None:
        """Load a cached normalized record wrapper if present."""

        path = self.record_path(normalized_doi)
        if not path.exists():
            return None
        payload = self._read_json(path)
        if not isinstance(payload, dict):
            raise CacheStoreError(f"Cached normalized record is not a JSON object: {path}")
        return payload

    def set_record(
        self,
        normalized_doi: str,
        record: Mapping[str, Any],
        *,
        normalization_version: str,
    ) -> Path:
        """Persist a normalized record wrapper to disk."""

        path = self.record_path(normalized_doi)
        payload = {
            "normalization_version": normalization_version,
            "record": dict(record),
        }
        self._write_json(path, payload)
        return path

    def doi_cache_dir(self, normalized_doi: str) -> Path:
        """Return the cache directory for one normalized DOI."""

        safe_doi = quote(normalized_doi, safe="")
        return self.root_dir / "openalex" / "doi" / safe_doi

    def raw_path(self, normalized_doi: str) -> Path:
        """Return the path for the raw cached payload."""

        return self.doi_cache_dir(normalized_doi) / "raw.json"

    def record_path(self, normalized_doi: str) -> Path:
        """Return the path for the cached normalized record."""

        return self.doi_cache_dir(normalized_doi) / "record.json"

    def work_exists(self, openalex_id: str) -> bool:
        """Return whether a raw cached payload exists for the work id."""

        return self.work_raw_path(openalex_id).exists()

    def get_work_raw(self, openalex_id: str) -> dict[str, Any] | None:
        """Load a cached raw OpenAlex work payload if present."""

        path = self.work_raw_path(openalex_id)
        if not path.exists():
            return None
        payload = self._read_json(path)
        if not isinstance(payload, dict):
            raise CacheStoreError(f"Cached raw work payload is not a JSON object: {path}")
        return payload

    def set_work_raw(self, openalex_id: str, payload: Mapping[str, Any]) -> Path:
        """Persist a raw OpenAlex work payload to disk."""

        path = self.work_raw_path(openalex_id)
        self._write_json(path, dict(payload))
        return path

    def get_work_record(self, openalex_id: str) -> dict[str, Any] | None:
        """Load a cached normalized work record wrapper if present."""

        path = self.work_record_path(openalex_id)
        if not path.exists():
            return None
        payload = self._read_json(path)
        if not isinstance(payload, dict):
            raise CacheStoreError(f"Cached work record is not a JSON object: {path}")
        return payload

    def set_work_record(
        self,
        openalex_id: str,
        record: Mapping[str, Any],
        *,
        normalization_version: str,
    ) -> Path:
        """Persist a normalized work record wrapper to disk."""

        path = self.work_record_path(openalex_id)
        payload = {
            "normalization_version": normalization_version,
            "record": dict(record),
        }
        self._write_json(path, payload)
        return path

    def _read_json(self, path: Path) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except OSError as exc:
            raise CacheStoreError(f"Failed to read cache file: {path}") from exc
        except json.JSONDecodeError as exc:
            raise CacheStoreError(f"Failed to decode JSON cache file: {path}") from exc

    def _write_json(self, path: Path, payload: Mapping[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        except OSError as exc:
            raise CacheStoreError(f"Failed to write cache file: {path}") from exc

    def work_cache_dir(self, openalex_id: str) -> Path:
        """Return the cache directory for one normalized OpenAlex work id."""

        safe_work_id = quote(_safe_openalex_id(openalex_id), safe="")
        return self.root_dir / "openalex" / "work" / safe_work_id

    def work_raw_path(self, openalex_id: str) -> Path:
        """Return the path for the cached raw work payload."""

        return self.work_cache_dir(openalex_id) / "raw.json"

    def work_record_path(self, openalex_id: str) -> Path:
        """Return the path for the cached normalized work record."""

        return self.work_cache_dir(openalex_id) / "record.json"


def _safe_openalex_id(openalex_id: str) -> str:
    normalized = openalex_id.strip().rstrip("/")
    if normalized.startswith("https://openalex.org/"):
        normalized = normalized.rsplit("/", 1)[-1]
    return normalized
