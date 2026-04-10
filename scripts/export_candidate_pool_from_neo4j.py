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
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


CANDIDATE_POOL_COLUMNS = [
    "candidate_id",
    "doi",
    "title",
    "proposed_tag",
    "secondary_tag_hints",
    "publication_year",
    "type",
    "openalex_resolved",
    "citation_count",
    "referenced_works_count",
    "graph_boundary_score",
    "graph_centrality_score",
    "duplicate_cluster_id",
    "source_snapshot_id",
]

DOI_PREFIXES = ("https://doi.org/", "http://doi.org/", "doi:")
TAG_RE = re.compile(r"^[a-z0-9_]+$")
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
INT_RE = re.compile(r"^-?\d+$")
FLOAT_RE = re.compile(r"^-?(?:\d+\.\d*|\d*\.\d+)(?:[eE][-+]?\d+)?$")
MUTATING_CYPHER_RE = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP|LOAD\s+CSV|FOREACH|INSERT)\b",
    re.IGNORECASE,
)


class CandidatePoolExportError(Exception):
    pass


@dataclass(frozen=True)
class Neo4jCredentials:
    uri: str
    username: str
    password: str


@dataclass(frozen=True)
class ExportFilters:
    require_doi: bool
    min_publication_year: int
    min_cited_by_count: int
    min_referenced_works_count: int


@dataclass(frozen=True)
class TagRule:
    tag: str
    primary_topic_names: tuple[str, ...]
    primary_subfield_names: tuple[str, ...]
    primary_field_names: tuple[str, ...]
    primary_domain_names: tuple[str, ...]
    topic_names: tuple[str, ...]


@dataclass(frozen=True)
class ExportSpec:
    export_type: str
    schema_version: int
    export_id: str
    source_snapshot_id: str
    filters: ExportFilters
    tag_rules: tuple[TagRule, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a deterministic seed-selection candidate_pool.csv from a local Neo4j graph via cypher-shell.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--export-spec", required=True, help="Path to the candidate-pool export spec YAML")
    parser.add_argument("--candidate-pool-id", required=True, help="Candidate pool identifier")
    parser.add_argument("--database", default="neo4j", help="Target Neo4j database name")
    args = parser.parse_args()

    if not args.export_spec.strip():
        parser.error("--export-spec must be non-empty")
    if not args.candidate_pool_id.strip():
        parser.error("--candidate-pool-id must be non-empty")

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
                raise CandidatePoolExportError(
                    f"Invalid .env line at {env_path}:{line_number}; expected KEY=VALUE format"
                )
            key, value = text.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                raise CandidatePoolExportError(
                    f"Invalid .env line at {env_path}:{line_number}; missing variable name"
                )
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            values[key] = value

    return values


def load_neo4j_credentials(repo_root: Path) -> Neo4jCredentials:
    env_values = parse_env_file(repo_root / ".env")
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for key in ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD"):
        value = os.getenv(key) or env_values.get(key, "")
        if not value:
            missing.append(key)
        resolved[key] = value

    if missing:
        raise CandidatePoolExportError(
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
        raise CandidatePoolExportError(
            "cypher-shell was not found on PATH. Install Neo4j tools and ensure cypher-shell is available."
        )
    return cypher_shell_path


def sanitize_output(text: str, credentials: Neo4jCredentials | None) -> str:
    sanitized = text.strip()
    if credentials and credentials.password:
        sanitized = sanitized.replace(credentials.password, "<redacted>")
    return sanitized


def require_mapping(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CandidatePoolExportError(f"Expected YAML object for {context}")
    return value


def require_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CandidatePoolExportError(f"Expected non-empty string for {context}")
    return value.strip()


def require_bool(value: Any, context: str) -> bool:
    if type(value) is not bool:
        raise CandidatePoolExportError(f"Expected boolean for {context}")
    return value


def require_int(value: Any, context: str) -> int:
    if type(value) is not int:
        raise CandidatePoolExportError(f"Expected integer for {context}")
    return value


def require_string_list(value: Any, context: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise CandidatePoolExportError(f"Expected list for {context}")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(require_string(item, f"{context}[{index}]"))
    return tuple(result)


def load_export_spec(spec_path: Path) -> ExportSpec:
    if not spec_path.is_file():
        raise CandidatePoolExportError(f"Export spec does not exist: {spec_path}")

    with spec_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    root = require_mapping(data, str(spec_path))
    export_type = require_string(root.get("export_type"), "export_type")
    schema_version = require_int(root.get("schema_version"), "schema_version")
    export_id = require_string(root.get("export_id"), "export_id")
    source_snapshot_id = require_string(root.get("source_snapshot_id"), "source_snapshot_id")

    if export_type != "neo4j_candidate_pool_export":
        raise CandidatePoolExportError(
            f"Unsupported export_type: {export_type!r}. Expected 'neo4j_candidate_pool_export'."
        )
    if schema_version != 1:
        raise CandidatePoolExportError(
            f"Unsupported schema_version: {schema_version!r}. Expected 1."
        )

    filters_data = require_mapping(root.get("filters"), "filters")
    filters = ExportFilters(
        require_doi=require_bool(filters_data.get("require_doi"), "filters.require_doi"),
        min_publication_year=require_int(
            filters_data.get("min_publication_year"),
            "filters.min_publication_year",
        ),
        min_cited_by_count=require_int(
            filters_data.get("min_cited_by_count"),
            "filters.min_cited_by_count",
        ),
        min_referenced_works_count=require_int(
            filters_data.get("min_referenced_works_count"),
            "filters.min_referenced_works_count",
        ),
    )

    if filters.min_publication_year < 0:
        raise CandidatePoolExportError("filters.min_publication_year must be >= 0")
    if filters.min_cited_by_count < 0:
        raise CandidatePoolExportError("filters.min_cited_by_count must be >= 0")
    if filters.min_referenced_works_count < 0:
        raise CandidatePoolExportError("filters.min_referenced_works_count must be >= 0")

    tag_rules_value = root.get("tag_rules")
    if not isinstance(tag_rules_value, list) or not tag_rules_value:
        raise CandidatePoolExportError("tag_rules must be a non-empty list")

    tag_rules: list[TagRule] = []
    seen_tags: set[str] = set()
    for index, item in enumerate(tag_rules_value):
        context = f"tag_rules[{index}]"
        tag_rule_data = require_mapping(item, context)
        tag = require_string(tag_rule_data.get("tag"), f"{context}.tag")
        if not TAG_RE.match(tag):
            raise CandidatePoolExportError(
                f"Invalid tag name for {context}.tag: {tag!r}. Expected /^[a-z0-9_]+$/."
            )
        if tag in seen_tags:
            raise CandidatePoolExportError(f"Duplicate tag rule tag: {tag}")
        seen_tags.add(tag)
        for field_name in (
            "primary_topic_names",
            "primary_subfield_names",
            "primary_field_names",
            "primary_domain_names",
            "topic_names",
        ):
            if field_name not in tag_rule_data:
                raise CandidatePoolExportError(f"Missing required field for {context}: {field_name}")
        tag_rules.append(
            TagRule(
                tag=tag,
                primary_topic_names=require_string_list(
                    tag_rule_data.get("primary_topic_names"),
                    f"{context}.primary_topic_names",
                ),
                primary_subfield_names=require_string_list(
                    tag_rule_data.get("primary_subfield_names"),
                    f"{context}.primary_subfield_names",
                ),
                primary_field_names=require_string_list(
                    tag_rule_data.get("primary_field_names"),
                    f"{context}.primary_field_names",
                ),
                primary_domain_names=require_string_list(
                    tag_rule_data.get("primary_domain_names"),
                    f"{context}.primary_domain_names",
                ),
                topic_names=require_string_list(
                    tag_rule_data.get("topic_names"),
                    f"{context}.topic_names",
                ),
            )
        )

    return ExportSpec(
        export_type=export_type,
        schema_version=schema_version,
        export_id=export_id,
        source_snapshot_id=source_snapshot_id,
        filters=filters,
        tag_rules=tuple(tag_rules),
    )


def cypher_string_literal(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def ensure_read_only_query(query_name: str, query: str) -> None:
    if MUTATING_CYPHER_RE.search(query):
        raise CandidatePoolExportError(
            f"Blocked non-read-only Cypher for {query_name}. Export step must not mutate Neo4j."
        )


def run_read_only_query(
    cypher_shell_path: str,
    credentials: Neo4jCredentials,
    database: str,
    query_name: str,
    query: str,
) -> str:
    ensure_read_only_query(query_name, query)
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
            query,
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=300,
    )

    stdout = sanitize_output(process.stdout, credentials)
    stderr = sanitize_output(process.stderr, credentials)
    if process.returncode != 0:
        message = stderr or stdout or "cypher-shell exited with a non-zero status"
        raise CandidatePoolExportError(f"cypher-shell read query failed for {query_name}: {message}")

    return stdout


def parse_plain_scalar(value: str) -> Any:
    if value == "NULL":
        return None
    if value == "TRUE":
        return True
    if value == "FALSE":
        return False
    if INT_RE.match(value):
        return int(value)
    if FLOAT_RE.match(value):
        return float(value)
    return value


def parse_cypher_plain_output(output_text: str, query_name: str) -> list[dict[str, Any]]:
    lines = [line for line in output_text.splitlines() if line.strip()]
    if not lines:
        raise CandidatePoolExportError(
            f"Parser mismatch with cypher-shell plain output for {query_name}: no rows were returned."
        )

    parsed_rows = list(csv.reader(lines, skipinitialspace=True))
    header = [item.strip() for item in parsed_rows[0]]
    if not header or any(not item for item in header):
        raise CandidatePoolExportError(
            f"Parser mismatch with cypher-shell plain output for {query_name}: invalid header row."
        )

    records: list[dict[str, Any]] = []
    for row_number, row in enumerate(parsed_rows[1:], start=2):
        if len(row) != len(header):
            raise CandidatePoolExportError(
                f"Parser mismatch with cypher-shell plain output for {query_name} at output row {row_number}: "
                f"expected {len(header)} columns, got {len(row)}."
            )
        record = {
            header[index]: parse_plain_scalar(row[index].strip())
            for index in range(len(header))
        }
        records.append(record)

    return records


def expect_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise CandidatePoolExportError(f"Expected non-empty string for {context}")
    return value


def optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def optional_int(value: Any, context: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise CandidatePoolExportError(f"Expected integer for {context}, got boolean")
    if isinstance(value, int):
        return value
    raise CandidatePoolExportError(f"Expected integer for {context}, got {type(value).__name__}")


def normalize_doi(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    for prefix in DOI_PREFIXES:
        if normalized.lower().startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    normalized = normalized.strip().lower()
    return normalized or None


def build_work_query(spec: ExportSpec) -> str:
    snapshot = cypher_string_literal(spec.source_snapshot_id)
    return f"""
MATCH (w:Work)
WHERE w.source_snapshot_id = {snapshot}
RETURN
  w.openalex_id AS openalex_id,
  w.doi AS doi,
  w.title AS title,
  w.publication_year AS publication_year,
  w.type AS type,
  w.cited_by_count AS cited_by_count,
  w.referenced_works_count AS referenced_works_count,
  w.primary_topic_name AS primary_topic_name,
  w.primary_subfield_name AS primary_subfield_name,
  w.primary_field_name AS primary_field_name,
  w.primary_domain_name AS primary_domain_name,
  w.source_snapshot_id AS source_snapshot_id
ORDER BY openalex_id
""".strip()


def build_topic_attachment_query(spec: ExportSpec) -> str:
    snapshot = cypher_string_literal(spec.source_snapshot_id)
    return f"""
MATCH (w:Work)-[r:HAS_TOPIC]->(t:Topic)
WHERE w.source_snapshot_id = {snapshot}
  AND r.source_snapshot_id = {snapshot}
  AND t.source_snapshot_id = {snapshot}
RETURN
  w.openalex_id AS work_openalex_id,
  t.topic_name AS topic_name
ORDER BY work_openalex_id, topic_name
""".strip()


def build_reference_query(spec: ExportSpec) -> str:
    snapshot = cypher_string_literal(spec.source_snapshot_id)
    return f"""
MATCH (source:Work)-[r:REFERENCES]->(target:Work)
WHERE source.source_snapshot_id = {snapshot}
  AND target.source_snapshot_id = {snapshot}
  AND r.source_snapshot_id = {snapshot}
RETURN
  source.openalex_id AS source_openalex_id,
  target.openalex_id AS target_openalex_id
ORDER BY source_openalex_id, target_openalex_id
""".strip()


def build_related_query(spec: ExportSpec) -> str:
    snapshot = cypher_string_literal(spec.source_snapshot_id)
    return f"""
MATCH (source:Work)-[r:RELATED_TO]->(target:Work)
WHERE source.source_snapshot_id = {snapshot}
  AND target.source_snapshot_id = {snapshot}
  AND r.source_snapshot_id = {snapshot}
RETURN
  source.openalex_id AS source_openalex_id,
  target.openalex_id AS target_openalex_id
ORDER BY source_openalex_id, target_openalex_id
""".strip()


def rule_matches(rule: TagRule, work: dict[str, Any], topic_names: set[str]) -> bool:
    primary_topic_name = work["primary_topic_name"]
    primary_subfield_name = work["primary_subfield_name"]
    primary_field_name = work["primary_field_name"]
    primary_domain_name = work["primary_domain_name"]

    return any(
        (
            primary_topic_name in rule.primary_topic_names if primary_topic_name else False,
            primary_subfield_name in rule.primary_subfield_names if primary_subfield_name else False,
            primary_field_name in rule.primary_field_names if primary_field_name else False,
            primary_domain_name in rule.primary_domain_names if primary_domain_name else False,
            bool(topic_names.intersection(rule.topic_names)),
        )
    )


def build_summary_markdown(manifest: dict[str, Any]) -> str:
    lines = [
        f"# Candidate Pool Summary: {manifest['candidate_pool_id']}",
        "",
        f"- export_id: `{manifest['export_id']}`",
        f"- source_snapshot_id: `{manifest['source_snapshot_id']}`",
        f"- database: `{manifest['database']}`",
        f"- export_spec_path: `{manifest['export_spec_path']}`",
        f"- total_work_nodes_seen: `{manifest['total_work_nodes_seen']}`",
        f"- total_topic_attachments_seen: `{manifest['total_topic_attachments_seen']}`",
        f"- total_reference_edges_seen: `{manifest['total_reference_edges_seen']}`",
        f"- total_related_edges_seen: `{manifest['total_related_edges_seen']}`",
        f"- exported_candidate_count: `{manifest['exported_candidate_count']}`",
        "",
        "## Per-Tag Candidate Counts",
        "",
    ]

    for tag, count in manifest["per_tag_candidate_counts"].items():
        lines.append(f"- `{tag}`: `{count}`")

    lines.extend(
        [
            "",
            "## Dropped Reason Counts",
            "",
        ]
    )

    if manifest["dropped_reason_counts"]:
        for reason, count in manifest["dropped_reason_counts"].items():
            lines.append(f"- `{reason}`: `{count}`")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Output Files",
            "",
            f"- `candidate_pool_csv`: `{manifest['output_paths']['candidate_pool_csv']}`",
            f"- `pool_manifest_json`: `{manifest['output_paths']['pool_manifest_json']}`",
            f"- `pool_summary_md`: `{manifest['output_paths']['pool_summary_md']}`",
        ]
    )

    return "\n".join(lines) + "\n"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False)
        handle.write("\n")


def write_candidate_pool_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_POOL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def format_score(value: float) -> str:
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    if "." not in text:
        text += ".0"
    return text


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = repo_root / "runs" / "seed_pools" / args.candidate_pool_id
    if output_dir.exists():
        raise SystemExit(
            f"ERROR: Candidate pool output directory already exists: {repo_relative_path(output_dir, repo_root)}"
        )

    output_dir.mkdir(parents=True, exist_ok=False)
    candidate_pool_csv_path = output_dir / "candidate_pool.csv"
    pool_manifest_path = output_dir / "pool_manifest.json"
    pool_summary_path = output_dir / "pool_summary.md"

    credentials: Neo4jCredentials | None = None
    try:
        cypher_shell_path = require_cypher_shell()
        credentials = load_neo4j_credentials(repo_root)
        export_spec_path = resolve_input_path(args.export_spec, repo_root)
        export_spec = load_export_spec(export_spec_path)

        connection_output = run_read_only_query(
            cypher_shell_path=cypher_shell_path,
            credentials=credentials,
            database=args.database,
            query_name="connectivity_check",
            query="RETURN 1 AS ok",
        )
        connection_rows = parse_cypher_plain_output(connection_output, "connectivity_check")
        if len(connection_rows) != 1 or connection_rows[0].get("ok") != 1:
            raise CandidatePoolExportError("cypher-shell connection check returned an unexpected result")

        work_rows = parse_cypher_plain_output(
            run_read_only_query(
                cypher_shell_path=cypher_shell_path,
                credentials=credentials,
                database=args.database,
                query_name="works",
                query=build_work_query(export_spec),
            ),
            "works",
        )
        topic_rows = parse_cypher_plain_output(
            run_read_only_query(
                cypher_shell_path=cypher_shell_path,
                credentials=credentials,
                database=args.database,
                query_name="topic_attachments",
                query=build_topic_attachment_query(export_spec),
            ),
            "topic_attachments",
        )
        reference_rows = parse_cypher_plain_output(
            run_read_only_query(
                cypher_shell_path=cypher_shell_path,
                credentials=credentials,
                database=args.database,
                query_name="reference_edges",
                query=build_reference_query(export_spec),
            ),
            "reference_edges",
        )
        related_rows = parse_cypher_plain_output(
            run_read_only_query(
                cypher_shell_path=cypher_shell_path,
                credentials=credentials,
                database=args.database,
                query_name="related_edges",
                query=build_related_query(export_spec),
            ),
            "related_edges",
        )

        works_by_openalex_id: dict[str, dict[str, Any]] = {}
        for row in work_rows:
            openalex_id = expect_string(row.get("openalex_id"), "works.openalex_id")
            if openalex_id in works_by_openalex_id:
                raise CandidatePoolExportError(f"Duplicate Work node returned for openalex_id: {openalex_id}")
            works_by_openalex_id[openalex_id] = {
                "openalex_id": openalex_id,
                "doi": optional_string(row.get("doi")),
                "title": optional_string(row.get("title")) or "",
                "publication_year": optional_int(row.get("publication_year"), f"{openalex_id}.publication_year"),
                "type": optional_string(row.get("type")) or "",
                "cited_by_count": optional_int(row.get("cited_by_count"), f"{openalex_id}.cited_by_count") or 0,
                "referenced_works_count": optional_int(
                    row.get("referenced_works_count"),
                    f"{openalex_id}.referenced_works_count",
                )
                or 0,
                "primary_topic_name": optional_string(row.get("primary_topic_name")),
                "primary_subfield_name": optional_string(row.get("primary_subfield_name")),
                "primary_field_name": optional_string(row.get("primary_field_name")),
                "primary_domain_name": optional_string(row.get("primary_domain_name")),
                "source_snapshot_id": optional_string(row.get("source_snapshot_id")) or export_spec.source_snapshot_id,
            }

        topics_by_work: dict[str, set[str]] = defaultdict(set)
        for row in topic_rows:
            work_openalex_id = expect_string(row.get("work_openalex_id"), "topic_attachments.work_openalex_id")
            topic_name = expect_string(row.get("topic_name"), "topic_attachments.topic_name")
            if work_openalex_id not in works_by_openalex_id:
                raise CandidatePoolExportError(
                    f"HAS_TOPIC row references unknown Work openalex_id: {work_openalex_id}"
                )
            topics_by_work[work_openalex_id].add(topic_name)

        neighbor_map: dict[str, set[str]] = defaultdict(set)
        reference_edges_seen = len(reference_rows)
        related_edges_seen = len(related_rows)

        for row in reference_rows:
            source_openalex_id = expect_string(row.get("source_openalex_id"), "reference_edges.source_openalex_id")
            target_openalex_id = expect_string(row.get("target_openalex_id"), "reference_edges.target_openalex_id")
            if source_openalex_id not in works_by_openalex_id or target_openalex_id not in works_by_openalex_id:
                raise CandidatePoolExportError(
                    f"REFERENCES edge references unknown Work ids: {source_openalex_id} -> {target_openalex_id}"
                )
            if source_openalex_id != target_openalex_id:
                neighbor_map[source_openalex_id].add(target_openalex_id)
                neighbor_map[target_openalex_id].add(source_openalex_id)

        for row in related_rows:
            source_openalex_id = expect_string(row.get("source_openalex_id"), "related_edges.source_openalex_id")
            target_openalex_id = expect_string(row.get("target_openalex_id"), "related_edges.target_openalex_id")
            if source_openalex_id not in works_by_openalex_id or target_openalex_id not in works_by_openalex_id:
                raise CandidatePoolExportError(
                    f"RELATED_TO edge references unknown Work ids: {source_openalex_id} -> {target_openalex_id}"
                )
            if source_openalex_id != target_openalex_id:
                neighbor_map[source_openalex_id].add(target_openalex_id)
                neighbor_map[target_openalex_id].add(source_openalex_id)

        dropped_reason_counts: Counter[str] = Counter()
        candidate_rows_internal: list[dict[str, Any]] = []

        for openalex_id in sorted(works_by_openalex_id):
            work = works_by_openalex_id[openalex_id]
            topic_names = topics_by_work.get(openalex_id, set())
            matching_tags = [
                rule.tag for rule in export_spec.tag_rules if rule_matches(rule, work, topic_names)
            ]
            if not matching_tags:
                dropped_reason_counts["no_matching_tag_rules"] += 1
                continue

            normalized_doi = normalize_doi(work["doi"])
            if export_spec.filters.require_doi and not normalized_doi:
                dropped_reason_counts["missing_required_doi"] += 1
                continue

            publication_year = work["publication_year"]
            if publication_year is None or publication_year < export_spec.filters.min_publication_year:
                dropped_reason_counts["below_min_publication_year"] += 1
                continue

            citation_count = int(work["cited_by_count"])
            if citation_count < export_spec.filters.min_cited_by_count:
                dropped_reason_counts["below_min_cited_by_count"] += 1
                continue

            referenced_works_count = int(work["referenced_works_count"])
            if referenced_works_count < export_spec.filters.min_referenced_works_count:
                dropped_reason_counts["below_min_referenced_works_count"] += 1
                continue

            candidate_rows_internal.append(
                {
                    "openalex_id": openalex_id,
                    "doi": normalized_doi or "",
                    "title": work["title"],
                    "proposed_tag": matching_tags[0],
                    "secondary_tag_hints": matching_tags[1:],
                    "publication_year": publication_year,
                    "type": work["type"],
                    "openalex_resolved": bool(openalex_id),
                    "citation_count": citation_count,
                    "referenced_works_count": referenced_works_count,
                    "duplicate_cluster_id": (normalized_doi.lower() if normalized_doi else openalex_id),
                    "source_snapshot_id": work["source_snapshot_id"],
                }
            )

        candidate_by_openalex_id = {
            row["openalex_id"]: row for row in candidate_rows_internal
        }
        exported_ids = set(candidate_by_openalex_id)

        for row in candidate_rows_internal:
            exported_neighbors = sorted(neighbor_map.get(row["openalex_id"], set()).intersection(exported_ids))
            total_neighbor_count = len(exported_neighbors)
            if total_neighbor_count == 0:
                row["graph_boundary_score"] = 0.0
            else:
                cross_tag_count = sum(
                    1
                    for neighbor_openalex_id in exported_neighbors
                    if candidate_by_openalex_id[neighbor_openalex_id]["proposed_tag"] != row["proposed_tag"]
                )
                row["graph_boundary_score"] = cross_tag_count / total_neighbor_count

        if candidate_rows_internal:
            log_citation_values = {
                row["openalex_id"]: math.log1p(row["citation_count"])
                for row in candidate_rows_internal
            }
            min_log_value = min(log_citation_values.values())
            max_log_value = max(log_citation_values.values())
            for row in candidate_rows_internal:
                if max_log_value == min_log_value:
                    row["graph_centrality_score"] = 0.0
                else:
                    row["graph_centrality_score"] = (
                        (log_citation_values[row["openalex_id"]] - min_log_value)
                        / (max_log_value - min_log_value)
                    )

        candidate_rows_internal.sort(
            key=lambda row: (
                row["proposed_tag"],
                -row["graph_boundary_score"],
                -row["graph_centrality_score"],
                -row["citation_count"],
                -row["referenced_works_count"],
                -row["publication_year"],
                row["doi"],
                row["openalex_id"],
            )
        )

        per_tag_candidate_counts: dict[str, int] = {}
        tag_count_lookup = Counter(row["proposed_tag"] for row in candidate_rows_internal)
        for rule in export_spec.tag_rules:
            per_tag_candidate_counts[rule.tag] = tag_count_lookup.get(rule.tag, 0)

        candidate_rows_csv: list[dict[str, str]] = []
        for index, row in enumerate(candidate_rows_internal, start=1):
            candidate_rows_csv.append(
                {
                    "candidate_id": f"C{index:04d}",
                    "doi": row["doi"],
                    "title": row["title"],
                    "proposed_tag": row["proposed_tag"],
                    "secondary_tag_hints": "|".join(row["secondary_tag_hints"]),
                    "publication_year": str(row["publication_year"]),
                    "type": row["type"],
                    "openalex_resolved": "true" if row["openalex_resolved"] else "false",
                    "citation_count": str(row["citation_count"]),
                    "referenced_works_count": str(row["referenced_works_count"]),
                    "graph_boundary_score": format_score(row["graph_boundary_score"]),
                    "graph_centrality_score": format_score(row["graph_centrality_score"]),
                    "duplicate_cluster_id": row["duplicate_cluster_id"],
                    "source_snapshot_id": row["source_snapshot_id"],
                }
            )

        manifest = {
            "candidate_pool_id": args.candidate_pool_id,
            "export_spec_path": repo_relative_path(export_spec_path, repo_root),
            "export_id": export_spec.export_id,
            "source_snapshot_id": export_spec.source_snapshot_id,
            "database": args.database,
            "total_work_nodes_seen": len(work_rows),
            "total_topic_attachments_seen": len(topic_rows),
            "total_reference_edges_seen": reference_edges_seen,
            "total_related_edges_seen": related_edges_seen,
            "exported_candidate_count": len(candidate_rows_csv),
            "per_tag_candidate_counts": per_tag_candidate_counts,
            "dropped_reason_counts": dict(sorted(dropped_reason_counts.items())),
            "output_paths": {
                "candidate_pool_csv": repo_relative_path(candidate_pool_csv_path, repo_root),
                "pool_manifest_json": repo_relative_path(pool_manifest_path, repo_root),
                "pool_summary_md": repo_relative_path(pool_summary_path, repo_root),
            },
            "created_at_utc": utc_now_iso(),
        }

        write_candidate_pool_csv(candidate_pool_csv_path, candidate_rows_csv)
        write_json(pool_manifest_path, manifest)
        with pool_summary_path.open("w", encoding="utf-8") as handle:
            handle.write(build_summary_markdown(manifest))

        print(f"candidate_pool_id: {args.candidate_pool_id}")
        print(f"export_id: {export_spec.export_id}")
        print(f"exported_candidate_count: {len(candidate_rows_csv)}")
        print(f"per_tag_counts: {json.dumps(per_tag_candidate_counts, sort_keys=False)}")
        print(f"candidate_pool_csv: {repo_relative_path(candidate_pool_csv_path, repo_root)}")
    except Exception as exc:
        raise SystemExit(f"ERROR: {sanitize_output(str(exc), credentials)}") from exc


if __name__ == "__main__":
    main()
