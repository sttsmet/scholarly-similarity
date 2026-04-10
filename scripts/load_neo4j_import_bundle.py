#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


class Neo4jImportBundleLoadError(Exception):
    pass


WORKS_HEADERS = [
    "openalex_id",
    "doi",
    "title",
    "publication_year",
    "type",
    "language",
    "is_retracted",
    "is_paratext",
    "cited_by_count",
    "referenced_works_count",
    "related_works_count",
    "openalex_resolved",
    "primary_topic_id",
    "primary_topic_name",
    "primary_subfield_id",
    "primary_subfield_name",
    "primary_field_id",
    "primary_field_name",
    "primary_domain_id",
    "primary_domain_name",
    "source_snapshot_id",
]

TOPICS_HEADERS = [
    "topic_id",
    "topic_name",
    "subfield_id",
    "subfield_name",
    "field_id",
    "field_name",
    "domain_id",
    "domain_name",
    "source_snapshot_id",
]

PRIMARY_TOPIC_REL_HEADERS = [
    "work_openalex_id",
    "topic_id",
    "score",
    "source_snapshot_id",
]

TOPIC_REL_HEADERS = [
    "work_openalex_id",
    "topic_id",
    "score",
    "is_primary",
    "source_snapshot_id",
]

REFERENCE_EDGE_HEADERS = [
    "source_openalex_id",
    "target_openalex_id",
    "source_snapshot_id",
]

RELATED_EDGE_HEADERS = [
    "source_openalex_id",
    "target_openalex_id",
    "source_snapshot_id",
]

EXPECTED_MANIFEST_OUTPUT_KEYS = {
    "works_csv": WORKS_HEADERS,
    "topics_csv": TOPICS_HEADERS,
    "work_primary_topic_csv": PRIMARY_TOPIC_REL_HEADERS,
    "work_topic_csv": TOPIC_REL_HEADERS,
    "work_reference_edges_csv": REFERENCE_EDGE_HEADERS,
    "work_related_edges_csv": RELATED_EDGE_HEADERS,
}

TRUE_VALUES = {"1", "true", "yes", "y"}
FALSE_VALUES = {"0", "false", "no", "n"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Neo4jCredentials:
    uri: str
    username: str
    password: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a previously built OpenAlex Neo4j import bundle via cypher-shell batched UNWIND queries.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--bundle-dir", required=True, help="Bundle directory containing import_bundle_manifest.json")
    parser.add_argument("--load-id", required=True, help="Logical load identifier for runs/neo4j_loads/<load_id>")
    parser.add_argument("--database", default="neo4j", help="Target Neo4j database name")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per Cypher UNWIND batch")
    parser.add_argument(
        "--allow-nonempty-db",
        action="store_true",
        help="Allow loading into a non-empty Neo4j database",
    )
    args = parser.parse_args()

    if not args.bundle_dir.strip():
        parser.error("--bundle-dir must be non-empty")
    if not args.load_id.strip():
        parser.error("--load-id must be non-empty")
    if args.batch_size <= 0:
        parser.error("--batch-size must be greater than 0")

    return args


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def repo_relative_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path.resolve())


def resolve_input_path(raw_path: str, repo_root: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = repo_root / candidate
    return candidate.resolve()


def parse_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not env_path.is_file():
        return values

    with env_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            if text.startswith("export "):
                text = text[len("export ") :].strip()
            if "=" not in text:
                raise Neo4jImportBundleLoadError(
                    f"Invalid .env line at {env_path}:{line_number}; expected KEY=VALUE format"
                )
            key, value = text.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise Neo4jImportBundleLoadError(
                    f"Invalid .env line at {env_path}:{line_number}; missing variable name"
                )
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            values[key] = value

    return values


def load_neo4j_credentials(repo_root: Path) -> Neo4jCredentials:
    env_path = repo_root / ".env"
    env_values = parse_env_file(env_path)

    resolved: dict[str, str] = {}
    missing: list[str] = []
    for key in ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"):
        value = os.getenv(key) or env_values.get(key, "")
        if not value:
            missing.append(key)
        resolved[key] = value

    if missing:
        raise Neo4jImportBundleLoadError(
            "Missing Neo4j credentials. Set environment variables or repo-root .env values for: "
            + ", ".join(missing)
        )

    return Neo4jCredentials(
        uri=resolved["NEO4J_URI"],
        username=resolved["NEO4J_USERNAME"],
        password=resolved["NEO4J_PASSWORD"],
    )


def require_cypher_shell() -> str:
    cypher_shell_path = shutil.which("cypher-shell")
    if not cypher_shell_path:
        raise Neo4jImportBundleLoadError(
            "cypher-shell was not found on PATH. Install Neo4j tools and ensure cypher-shell is available."
        )
    return cypher_shell_path


def load_bundle_manifest(bundle_dir: Path, repo_root: Path) -> tuple[Path, dict[str, Any], dict[str, Path]]:
    if not bundle_dir.is_dir():
        raise Neo4jImportBundleLoadError(f"Bundle directory does not exist: {bundle_dir}")

    manifest_path = bundle_dir / "import_bundle_manifest.json"
    if not manifest_path.is_file():
        raise Neo4jImportBundleLoadError(f"Bundle manifest not found: {manifest_path}")

    try:
        with manifest_path.open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
    except json.JSONDecodeError as exc:
        raise Neo4jImportBundleLoadError(f"Invalid JSON in manifest: {manifest_path}") from exc

    if not isinstance(manifest, dict):
        raise Neo4jImportBundleLoadError(f"Manifest must contain a JSON object: {manifest_path}")

    output_paths = manifest.get("output_paths")
    if not isinstance(output_paths, dict):
        raise Neo4jImportBundleLoadError(
            f"Manifest is missing output_paths mapping: {manifest_path}"
        )

    csv_paths: dict[str, Path] = {}
    for key in EXPECTED_MANIFEST_OUTPUT_KEYS:
        raw_value = output_paths.get(key)
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise Neo4jImportBundleLoadError(
                f"Manifest output_paths is missing a valid {key} entry: {manifest_path}"
            )
        csv_path = resolve_manifest_output_path(raw_value, manifest_path.parent, repo_root)
        if not csv_path.is_file():
            raise Neo4jImportBundleLoadError(f"Expected CSV file does not exist: {csv_path}")
        csv_paths[key] = csv_path

    return manifest_path, manifest, csv_paths


def resolve_manifest_output_path(raw_path: str, manifest_dir: Path, repo_root: Path) -> Path:
    candidate = Path(raw_path).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()

    for resolved in ((repo_root / candidate), (manifest_dir / candidate)):
        if resolved.is_file():
            return resolved.resolve()

    return (repo_root / candidate).resolve()


def sanitize_output(text: str, credentials: Neo4jCredentials | None) -> str:
    sanitized = text.strip()
    if credentials and credentials.password:
        sanitized = sanitized.replace(credentials.password, "<redacted>")
    return sanitized


def run_cypher_shell(
    cypher_shell_path: str,
    credentials: Neo4jCredentials,
    database: str,
    cypher_text: str,
) -> str:
    process = subprocess.run(
        [
            cypher_shell_path,
            "--format",
            "plain",
            "-a",
            credentials.uri,
            "-u",
            credentials.username,
            "-p",
            credentials.password,
            "-d",
            database,
        ],
        input=cypher_text,
        text=True,
        capture_output=True,
        check=False,
    )

    stdout = sanitize_output(process.stdout, credentials)
    stderr = sanitize_output(process.stderr, credentials)
    if process.returncode != 0:
        message = stderr or stdout or "cypher-shell exited with a non-zero status"
        raise Neo4jImportBundleLoadError(f"cypher-shell query failed: {message}")

    return stdout


def parse_last_output_value(output_text: str, description: str) -> str:
    lines = [line.strip() for line in output_text.splitlines() if line.strip()]
    if not lines:
        raise Neo4jImportBundleLoadError(f"Could not parse {description} from empty cypher-shell output")
    return lines[-1]


def parse_int_output(output_text: str, description: str) -> int:
    last_value = parse_last_output_value(output_text, description)
    try:
        return int(last_value)
    except ValueError as exc:
        raise Neo4jImportBundleLoadError(
            f"Could not parse integer {description} from cypher-shell output value: {last_value!r}"
        ) from exc


def ensure_headers(path: Path, expected_headers: list[str]) -> None:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        headers = next(reader, None)
    if headers != expected_headers:
        raise Neo4jImportBundleLoadError(
            f"Unexpected CSV headers in {path}. Expected {expected_headers!r}, got {headers!r}"
        )


def optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def required_text(value: str | None, field_name: str, context: str) -> str:
    text = optional_text(value)
    if text is None:
        raise Neo4jImportBundleLoadError(f"Missing required {field_name} at {context}")
    return text


def optional_int(value: str | None, field_name: str, context: str) -> int | None:
    text = optional_text(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise Neo4jImportBundleLoadError(
            f"Invalid integer for {field_name} at {context}: {text!r}"
        ) from exc


def optional_float(value: str | None, field_name: str, context: str) -> float | None:
    text = optional_text(value)
    if text is None:
        return None
    try:
        number = float(text)
    except ValueError as exc:
        raise Neo4jImportBundleLoadError(
            f"Invalid float for {field_name} at {context}: {text!r}"
        ) from exc
    if not math.isfinite(number):
        raise Neo4jImportBundleLoadError(f"Non-finite float for {field_name} at {context}: {text!r}")
    return number


def optional_bool(value: str | None, field_name: str, context: str) -> bool | None:
    text = optional_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in TRUE_VALUES:
        return True
    if lowered in FALSE_VALUES:
        return False
    raise Neo4jImportBundleLoadError(f"Invalid boolean for {field_name} at {context}: {text!r}")


def cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Neo4jImportBundleLoadError(f"Cannot serialize non-finite float into Cypher: {value!r}")
        return repr(value)
    if isinstance(value, str):
        escaped = (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )
        return f"'{escaped}'"
    if isinstance(value, list):
        return "[" + ", ".join(cypher_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        parts: list[str] = []
        for key, item in value.items():
            if not IDENTIFIER_RE.match(key):
                raise Neo4jImportBundleLoadError(f"Unsafe Cypher map key: {key!r}")
            parts.append(f"{key}: {cypher_literal(item)}")
        return "{" + ", ".join(parts) + "}"
    raise Neo4jImportBundleLoadError(f"Unsupported Cypher literal type: {type(value).__name__}")


def build_schema_query() -> str:
    return """
CREATE CONSTRAINT work_openalex_id_unique IF NOT EXISTS
FOR (work:Work)
REQUIRE work.openalex_id IS UNIQUE;

CREATE CONSTRAINT topic_topic_id_unique IF NOT EXISTS
FOR (topic:Topic)
REQUIRE topic.topic_id IS UNIQUE;

CREATE INDEX work_doi_idx IF NOT EXISTS
FOR (work:Work)
ON (work.doi);

CREATE INDEX topic_topic_name_idx IF NOT EXISTS
FOR (topic:Topic)
ON (topic.topic_name);
""".strip()


def build_works_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MERGE (work:Work {{openalex_id: row.openalex_id}})
SET work.doi = row.doi,
    work.title = row.title,
    work.publication_year = row.publication_year,
    work.type = row.type,
    work.language = row.language,
    work.is_retracted = row.is_retracted,
    work.is_paratext = row.is_paratext,
    work.cited_by_count = row.cited_by_count,
    work.referenced_works_count = row.referenced_works_count,
    work.related_works_count = row.related_works_count,
    work.openalex_resolved = row.openalex_resolved,
    work.primary_topic_id = row.primary_topic_id,
    work.primary_topic_name = row.primary_topic_name,
    work.primary_subfield_id = row.primary_subfield_id,
    work.primary_subfield_name = row.primary_subfield_name,
    work.primary_field_id = row.primary_field_id,
    work.primary_field_name = row.primary_field_name,
    work.primary_domain_id = row.primary_domain_id,
    work.primary_domain_name = row.primary_domain_name,
    work.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def build_topics_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MERGE (topic:Topic {{topic_id: row.topic_id}})
SET topic.topic_name = row.topic_name,
    topic.subfield_id = row.subfield_id,
    topic.subfield_name = row.subfield_name,
    topic.field_id = row.field_id,
    topic.field_name = row.field_name,
    topic.domain_id = row.domain_id,
    topic.domain_name = row.domain_name,
    topic.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def build_primary_topic_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MATCH (work:Work {{openalex_id: row.work_openalex_id}})
MATCH (topic:Topic {{topic_id: row.topic_id}})
MERGE (work)-[rel:HAS_PRIMARY_TOPIC]->(topic)
SET rel.score = row.score,
    rel.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def build_topic_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MATCH (work:Work {{openalex_id: row.work_openalex_id}})
MATCH (topic:Topic {{topic_id: row.topic_id}})
MERGE (work)-[rel:HAS_TOPIC]->(topic)
SET rel.score = row.score,
    rel.is_primary = row.is_primary,
    rel.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def build_reference_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MATCH (source:Work {{openalex_id: row.source_openalex_id}})
MATCH (target:Work {{openalex_id: row.target_openalex_id}})
MERGE (source)-[rel:REFERENCES]->(target)
SET rel.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def build_related_query(rows: list[dict[str, Any]]) -> str:
    return f"""
UNWIND {cypher_literal(rows)} AS row
MATCH (source:Work {{openalex_id: row.source_openalex_id}})
MATCH (target:Work {{openalex_id: row.target_openalex_id}})
MERGE (source)-[rel:RELATED_TO]->(target)
SET rel.source_snapshot_id = row.source_snapshot_id
RETURN count(*) AS processed_count;
""".strip()


def transform_work_row(row: dict[str, str | None], context: str) -> dict[str, Any]:
    return {
        "openalex_id": required_text(row.get("openalex_id"), "openalex_id", context),
        "doi": optional_text(row.get("doi")),
        "title": optional_text(row.get("title")),
        "publication_year": optional_int(row.get("publication_year"), "publication_year", context),
        "type": optional_text(row.get("type")),
        "language": optional_text(row.get("language")),
        "is_retracted": optional_bool(row.get("is_retracted"), "is_retracted", context),
        "is_paratext": optional_bool(row.get("is_paratext"), "is_paratext", context),
        "cited_by_count": optional_int(row.get("cited_by_count"), "cited_by_count", context),
        "referenced_works_count": optional_int(
            row.get("referenced_works_count"),
            "referenced_works_count",
            context,
        ),
        "related_works_count": optional_int(
            row.get("related_works_count"),
            "related_works_count",
            context,
        ),
        "openalex_resolved": optional_bool(row.get("openalex_resolved"), "openalex_resolved", context),
        "primary_topic_id": optional_text(row.get("primary_topic_id")),
        "primary_topic_name": optional_text(row.get("primary_topic_name")),
        "primary_subfield_id": optional_text(row.get("primary_subfield_id")),
        "primary_subfield_name": optional_text(row.get("primary_subfield_name")),
        "primary_field_id": optional_text(row.get("primary_field_id")),
        "primary_field_name": optional_text(row.get("primary_field_name")),
        "primary_domain_id": optional_text(row.get("primary_domain_id")),
        "primary_domain_name": optional_text(row.get("primary_domain_name")),
        "source_snapshot_id": optional_text(row.get("source_snapshot_id")),
    }


def transform_topic_row(row: dict[str, str | None], context: str) -> dict[str, Any]:
    return {
        "topic_id": required_text(row.get("topic_id"), "topic_id", context),
        "topic_name": optional_text(row.get("topic_name")),
        "subfield_id": optional_text(row.get("subfield_id")),
        "subfield_name": optional_text(row.get("subfield_name")),
        "field_id": optional_text(row.get("field_id")),
        "field_name": optional_text(row.get("field_name")),
        "domain_id": optional_text(row.get("domain_id")),
        "domain_name": optional_text(row.get("domain_name")),
        "source_snapshot_id": optional_text(row.get("source_snapshot_id")),
    }


def transform_primary_topic_row(row: dict[str, str | None], context: str) -> dict[str, Any]:
    return {
        "work_openalex_id": required_text(row.get("work_openalex_id"), "work_openalex_id", context),
        "topic_id": required_text(row.get("topic_id"), "topic_id", context),
        "score": optional_float(row.get("score"), "score", context),
        "source_snapshot_id": optional_text(row.get("source_snapshot_id")),
    }


def transform_topic_rel_row(row: dict[str, str | None], context: str) -> dict[str, Any]:
    return {
        "work_openalex_id": required_text(row.get("work_openalex_id"), "work_openalex_id", context),
        "topic_id": required_text(row.get("topic_id"), "topic_id", context),
        "score": optional_float(row.get("score"), "score", context),
        "is_primary": optional_bool(row.get("is_primary"), "is_primary", context),
        "source_snapshot_id": optional_text(row.get("source_snapshot_id")),
    }


def transform_edge_row(row: dict[str, str | None], context: str) -> dict[str, Any]:
    return {
        "source_openalex_id": required_text(row.get("source_openalex_id"), "source_openalex_id", context),
        "target_openalex_id": required_text(row.get("target_openalex_id"), "target_openalex_id", context),
        "source_snapshot_id": optional_text(row.get("source_snapshot_id")),
    }


def load_csv_in_batches(
    category_key: str,
    csv_path: Path,
    expected_headers: list[str],
    batch_size: int,
    transform_row: Callable[[dict[str, str | None], str], dict[str, Any]],
    build_query: Callable[[list[dict[str, Any]]], str],
    cypher_shell_path: str,
    credentials: Neo4jCredentials,
    database: str,
    counts_attempted: dict[str, int],
) -> None:
    batch: list[dict[str, Any]] = []
    batch_number = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != expected_headers:
            raise Neo4jImportBundleLoadError(
                f"Unexpected CSV headers in {csv_path}. Expected {expected_headers!r}, got {reader.fieldnames!r}"
            )

        for row_number, row in enumerate(reader, start=2):
            context = f"{csv_path}:{row_number}"
            batch.append(transform_row(row, context))
            if len(batch) >= batch_size:
                batch_number += 1
                submit_batch(
                    category_key=category_key,
                    batch_number=batch_number,
                    rows=batch,
                    build_query=build_query,
                    cypher_shell_path=cypher_shell_path,
                    credentials=credentials,
                    database=database,
                    counts_attempted=counts_attempted,
                )
                batch = []

    if batch:
        batch_number += 1
        submit_batch(
            category_key=category_key,
            batch_number=batch_number,
            rows=batch,
            build_query=build_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=database,
            counts_attempted=counts_attempted,
        )


def submit_batch(
    category_key: str,
    batch_number: int,
    rows: list[dict[str, Any]],
    build_query: Callable[[list[dict[str, Any]]], str],
    cypher_shell_path: str,
    credentials: Neo4jCredentials,
    database: str,
    counts_attempted: dict[str, int],
) -> None:
    counts_attempted[category_key] += len(rows)
    processed_count = parse_int_output(
        run_cypher_shell(
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=database,
            cypher_text=build_query(rows),
        ),
        description=f"{category_key} batch {batch_number} processed_count",
    )
    if processed_count != len(rows):
        raise Neo4jImportBundleLoadError(
            f"Cypher batch processed fewer rows than expected for {category_key} batch {batch_number}: "
            f"expected {len(rows)}, got {processed_count}"
        )


def write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def build_summary_markdown(report: dict[str, Any]) -> str:
    preflight = report["preflight_database_emptiness"]
    counts = report["counts_attempted"]
    output_paths = report["output_paths"]
    bundle_csv_paths = report.get("bundle_csv_paths", {})

    lines = [
        f"# Neo4j Load Summary: {report['load_id']}",
        "",
        f"- status: `{report['status']}`",
        f"- database: `{report['database']}`",
        f"- bundle_dir: `{report['bundle_dir']}`",
        f"- bundle_manifest_path: `{report['bundle_manifest_path']}`",
        f"- batch_size: `{report['batch_size']}`",
        f"- allow_nonempty_db: `{str(report['allow_nonempty_db']).lower()}`",
        f"- preflight_node_count: `{preflight['node_count']}`",
        f"- preflight_is_empty: `{str(preflight['is_empty']).lower()}`",
        f"- preflight_allowed_to_proceed: `{str(preflight['allowed_to_proceed']).lower()}`",
        "",
        "## Counts Attempted",
        "",
        f"- `works_csv`: `{counts['works_csv']}`",
        f"- `topics_csv`: `{counts['topics_csv']}`",
        f"- `work_primary_topic_csv`: `{counts['work_primary_topic_csv']}`",
        f"- `work_topic_csv`: `{counts['work_topic_csv']}`",
        f"- `work_reference_edges_csv`: `{counts['work_reference_edges_csv']}`",
        f"- `work_related_edges_csv`: `{counts['work_related_edges_csv']}`",
        "",
        "## Bundle CSV Paths",
        "",
        f"- `works_csv`: `{bundle_csv_paths.get('works_csv')}`",
        f"- `topics_csv`: `{bundle_csv_paths.get('topics_csv')}`",
        f"- `work_primary_topic_csv`: `{bundle_csv_paths.get('work_primary_topic_csv')}`",
        f"- `work_topic_csv`: `{bundle_csv_paths.get('work_topic_csv')}`",
        f"- `work_reference_edges_csv`: `{bundle_csv_paths.get('work_reference_edges_csv')}`",
        f"- `work_related_edges_csv`: `{bundle_csv_paths.get('work_related_edges_csv')}`",
        "",
        "## Output Files",
        "",
        f"- `neo4j_load_report_json`: `{output_paths['neo4j_load_report_json']}`",
        f"- `neo4j_load_summary_md`: `{output_paths['neo4j_load_summary_md']}`",
    ]

    error_message = report.get("error_message")
    if error_message:
        lines.extend(
            [
                "",
                "## Failure",
                "",
                f"- message: `{error_message}`",
            ]
        )

    return "\n".join(lines) + "\n"


def write_summary(path: Path, report: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        handle.write(build_summary_markdown(report))


def write_report_files(report_json_path: Path, summary_path: Path, report: dict[str, Any]) -> None:
    write_json(report_json_path, report)
    write_summary(summary_path, report)


def build_initial_report(
    args: argparse.Namespace,
    repo_root: Path,
    report_json_path: Path,
    summary_path: Path,
) -> dict[str, Any]:
    counts_attempted = {key: 0 for key in EXPECTED_MANIFEST_OUTPUT_KEYS}
    return {
        "load_id": args.load_id,
        "bundle_dir": repo_relative_path(resolve_input_path(args.bundle_dir, repo_root), repo_root),
        "bundle_manifest_path": repo_relative_path(
            resolve_input_path(args.bundle_dir, repo_root) / "import_bundle_manifest.json",
            repo_root,
        ),
        "database": args.database,
        "batch_size": args.batch_size,
        "allow_nonempty_db": args.allow_nonempty_db,
        "preflight_database_emptiness": {
            "node_count": None,
            "is_empty": None,
            "allowed_to_proceed": False,
        },
        "counts_attempted": counts_attempted,
        "status": "failure",
        "created_at_utc": utc_now_iso(),
        "completed_at_utc": None,
        "output_paths": {
            "neo4j_load_report_json": repo_relative_path(report_json_path, repo_root),
            "neo4j_load_summary_md": repo_relative_path(summary_path, repo_root),
        },
        "bundle_csv_paths": {},
    }


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    report_dir = repo_root / "runs" / "neo4j_loads" / args.load_id
    if report_dir.exists():
        raise SystemExit(f"ERROR: Load report directory already exists: {repo_relative_path(report_dir, repo_root)}")
    report_dir.mkdir(parents=True, exist_ok=False)

    report_json_path = report_dir / "neo4j_load_report.json"
    summary_path = report_dir / "neo4j_load_summary.md"
    report = build_initial_report(args, repo_root, report_json_path, summary_path)
    credentials: Neo4jCredentials | None = None

    try:
        cypher_shell_path = require_cypher_shell()
        credentials = load_neo4j_credentials(repo_root)

        bundle_dir = resolve_input_path(args.bundle_dir, repo_root)
        manifest_path, _manifest, csv_paths = load_bundle_manifest(bundle_dir, repo_root)
        report["bundle_dir"] = repo_relative_path(bundle_dir, repo_root)
        report["bundle_manifest_path"] = repo_relative_path(manifest_path, repo_root)
        report["bundle_csv_paths"] = {
            key: repo_relative_path(path, repo_root) for key, path in csv_paths.items()
        }

        for key, expected_headers in EXPECTED_MANIFEST_OUTPUT_KEYS.items():
            ensure_headers(csv_paths[key], expected_headers)

        connection_output = run_cypher_shell(
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            cypher_text="RETURN 1 AS ok;\n",
        )
        if parse_int_output(connection_output, "connectivity check") != 1:
            raise Neo4jImportBundleLoadError("cypher-shell connection check returned an unexpected result")

        node_count = parse_int_output(
            run_cypher_shell(
                cypher_shell_path=cypher_shell_path,
                credentials=credentials,
                database=args.database,
                cypher_text="MATCH (n) RETURN count(n) AS node_count;\n",
            ),
            description="database node count",
        )
        is_empty = node_count == 0
        allowed_to_proceed = is_empty or args.allow_nonempty_db
        report["preflight_database_emptiness"] = {
            "node_count": node_count,
            "is_empty": is_empty,
            "allowed_to_proceed": allowed_to_proceed,
        }

        if not allowed_to_proceed:
            raise Neo4jImportBundleLoadError(
                f"Target database '{args.database}' is not empty ({node_count} nodes). "
                "Re-run with --allow-nonempty-db to bypass this guard."
            )

        run_cypher_shell(
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            cypher_text=build_schema_query() + "\n",
        )

        load_csv_in_batches(
            category_key="works_csv",
            csv_path=csv_paths["works_csv"],
            expected_headers=WORKS_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_work_row,
            build_query=build_works_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )
        load_csv_in_batches(
            category_key="topics_csv",
            csv_path=csv_paths["topics_csv"],
            expected_headers=TOPICS_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_topic_row,
            build_query=build_topics_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )
        load_csv_in_batches(
            category_key="work_primary_topic_csv",
            csv_path=csv_paths["work_primary_topic_csv"],
            expected_headers=PRIMARY_TOPIC_REL_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_primary_topic_row,
            build_query=build_primary_topic_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )
        load_csv_in_batches(
            category_key="work_topic_csv",
            csv_path=csv_paths["work_topic_csv"],
            expected_headers=TOPIC_REL_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_topic_rel_row,
            build_query=build_topic_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )
        load_csv_in_batches(
            category_key="work_reference_edges_csv",
            csv_path=csv_paths["work_reference_edges_csv"],
            expected_headers=REFERENCE_EDGE_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_edge_row,
            build_query=build_reference_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )
        load_csv_in_batches(
            category_key="work_related_edges_csv",
            csv_path=csv_paths["work_related_edges_csv"],
            expected_headers=RELATED_EDGE_HEADERS,
            batch_size=args.batch_size,
            transform_row=transform_edge_row,
            build_query=build_related_query,
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            counts_attempted=report["counts_attempted"],
        )

        report["status"] = "success"
        report["completed_at_utc"] = utc_now_iso()
        write_report_files(report_json_path, summary_path, report)

        print(f"load_id: {report['load_id']}")
        print(f"database: {report['database']}")
        print(f"bundle_dir: {report['bundle_dir']}")
        print(f"works_loaded: {report['counts_attempted']['works_csv']}")
        print(f"topics_loaded: {report['counts_attempted']['topics_csv']}")
        print(f"reference_edges_loaded: {report['counts_attempted']['work_reference_edges_csv']}")
        print(f"related_edges_loaded: {report['counts_attempted']['work_related_edges_csv']}")
        print(f"load_report_json: {report['output_paths']['neo4j_load_report_json']}")
    except Exception as exc:
        report["status"] = "failure"
        report["completed_at_utc"] = utc_now_iso()
        report["error_message"] = sanitize_output(str(exc), credentials)
        write_report_files(report_json_path, summary_path, report)
        raise SystemExit(
            "ERROR: "
            + report["error_message"]
            + f" See {report['output_paths']['neo4j_load_report_json']}"
        ) from exc


if __name__ == "__main__":
    main()
