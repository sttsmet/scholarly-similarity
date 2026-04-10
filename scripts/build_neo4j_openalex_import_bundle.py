#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


class Neo4jImportBundleError(Exception):
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic Neo4j import bundle from raw OpenAlex JSON/JSONL work snapshots."
    )
    parser.add_argument(
        "--input-glob",
        action="append",
        required=True,
        help="Repeatable glob pattern for JSON/JSONL work snapshot files.",
    )
    parser.add_argument(
        "--snapshot-id",
        required=True,
        help="Logical snapshot identifier, e.g. openalex_snapshot_20260406",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Optional explicit output directory. Defaults to runs/neo4j_import_bundles/<snapshot_id>",
    )
    return parser.parse_args()


def ensure_nonempty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Neo4jImportBundleError(f"{field_name} must be a non-empty string")
    return value.strip()


def optional_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def optional_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return int(float(text))
        except ValueError as exc:
            raise Neo4jImportBundleError(f"Cannot parse integer from value: {value!r}") from exc
    raise Neo4jImportBundleError(f"Cannot parse integer from value: {value!r}")


def optional_bool_string(value: Any) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return "true"
        if lowered in {"false", "0", "no", "n"}:
            return "false"
        return lowered
    return "true" if bool(value) else "false"


def normalize_openalex_id(value: Any) -> str:
    identifier = optional_str(value)
    if not identifier:
        return ""
    return identifier


def iter_input_paths(patterns: List[str]) -> List[Path]:
    paths: List[Path] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern, recursive=True))
        for match in matches:
            path = Path(match)
            if path.is_file():
                paths.append(path)

    unique_paths: List[Path] = []
    seen = set()
    for path in paths:
        key = str(path.resolve())
        if key not in seen:
            seen.add(key)
            unique_paths.append(path)

    if not unique_paths:
        raise Neo4jImportBundleError("No input files matched the supplied --input-glob patterns")

    return unique_paths


def load_json_file(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def iter_records_from_path(path: Path) -> Iterable[Dict[str, Any]]:
    lower_name = path.name.lower()

    if lower_name.endswith(".jsonl"):
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise Neo4jImportBundleError(
                        f"Invalid JSONL record in {path} at line {line_number}"
                    ) from exc
                if isinstance(data, dict):
                    yield data
                else:
                    raise Neo4jImportBundleError(
                        f"Expected JSON object in {path} at line {line_number}"
                    )
        return

    data = load_json_file(path)

    if isinstance(data, dict):
        if "results" in data and isinstance(data["results"], list):
            for item in data["results"]:
                if isinstance(item, dict):
                    yield item
                else:
                    raise Neo4jImportBundleError(
                        f"Expected object items in results list in {path}"
                    )
            return
        yield data
        return

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
            else:
                raise Neo4jImportBundleError(
                    f"Expected object items in top-level list in {path}"
                )
        return

    raise Neo4jImportBundleError(
        f"Unsupported JSON top-level structure in {path}: {type(data).__name__}"
    )


def extract_topic(topic: Any) -> Optional[Dict[str, str]]:
    if not isinstance(topic, dict):
        return None

    topic_id = optional_str(topic.get("id"))
    topic_name = optional_str(topic.get("display_name"))

    subfield = topic.get("subfield") if isinstance(topic.get("subfield"), dict) else {}
    field = topic.get("field") if isinstance(topic.get("field"), dict) else {}
    domain = topic.get("domain") if isinstance(topic.get("domain"), dict) else {}

    topic_row = {
        "topic_id": topic_id,
        "topic_name": topic_name,
        "subfield_id": optional_str(subfield.get("id")),
        "subfield_name": optional_str(subfield.get("display_name")),
        "field_id": optional_str(field.get("id")),
        "field_name": optional_str(field.get("display_name")),
        "domain_id": optional_str(domain.get("id")),
        "domain_name": optional_str(domain.get("display_name")),
    }

    if not topic_row["topic_id"] and not topic_row["topic_name"]:
        return None

    return topic_row


def extract_topic_score(topic: Any) -> str:
    if not isinstance(topic, dict):
        return ""
    score = topic.get("score")
    if score is None or score == "":
        return ""
    if isinstance(score, (int, float)):
        return str(float(score))
    return optional_str(score)


def extract_work_record(record: Dict[str, Any], snapshot_id: str) -> Optional[Dict[str, Any]]:
    openalex_id = normalize_openalex_id(record.get("id"))
    if not openalex_id:
        return None

    doi = optional_str(record.get("doi"))
    title = optional_str(record.get("display_name")) or optional_str(record.get("title"))
    publication_year = optional_int(record.get("publication_year"), default=0)
    work_type = optional_str(record.get("type"))
    language = optional_str(record.get("language"))
    is_retracted = optional_bool_string(record.get("is_retracted"))
    is_paratext = optional_bool_string(record.get("is_paratext"))
    cited_by_count = optional_int(record.get("cited_by_count"), default=0)

    referenced_works_raw = record.get("referenced_works")
    if referenced_works_raw is None:
        referenced_works: List[str] = []
    elif isinstance(referenced_works_raw, list):
        referenced_works = [normalize_openalex_id(item) for item in referenced_works_raw if optional_str(item)]
    else:
        raise Neo4jImportBundleError(
            f"referenced_works must be a list for work {openalex_id}"
        )

    related_works_raw = record.get("related_works")
    if related_works_raw is None:
        related_works: List[str] = []
    elif isinstance(related_works_raw, list):
        related_works = [normalize_openalex_id(item) for item in related_works_raw if optional_str(item)]
    else:
        raise Neo4jImportBundleError(
            f"related_works must be a list for work {openalex_id}"
        )

    primary_topic_raw = record.get("primary_topic")
    primary_topic = extract_topic(primary_topic_raw)
    primary_topic_score = extract_topic_score(primary_topic_raw)

    topics_raw = record.get("topics")
    topics: List[Tuple[Dict[str, str], str]] = []
    if topics_raw is None:
        topics = []
    elif isinstance(topics_raw, list):
        for topic in topics_raw:
            topic_row = extract_topic(topic)
            if topic_row is None:
                continue
            topics.append((topic_row, extract_topic_score(topic)))
    else:
        raise Neo4jImportBundleError(f"topics must be a list for work {openalex_id}")

    work_row = {
        "openalex_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_year": publication_year,
        "type": work_type,
        "language": language,
        "is_retracted": is_retracted,
        "is_paratext": is_paratext,
        "cited_by_count": cited_by_count,
        "referenced_works_count": len(referenced_works),
        "related_works_count": len(related_works),
        "openalex_resolved": "true",
        "primary_topic_id": primary_topic["topic_id"] if primary_topic else "",
        "primary_topic_name": primary_topic["topic_name"] if primary_topic else "",
        "primary_subfield_id": primary_topic["subfield_id"] if primary_topic else "",
        "primary_subfield_name": primary_topic["subfield_name"] if primary_topic else "",
        "primary_field_id": primary_topic["field_id"] if primary_topic else "",
        "primary_field_name": primary_topic["field_name"] if primary_topic else "",
        "primary_domain_id": primary_topic["domain_id"] if primary_topic else "",
        "primary_domain_name": primary_topic["domain_name"] if primary_topic else "",
        "source_snapshot_id": snapshot_id,
    }

    return {
        "work_row": work_row,
        "primary_topic": primary_topic,
        "primary_topic_score": primary_topic_score,
        "topics": topics,
        "referenced_works": referenced_works,
        "related_works": related_works,
    }


def write_csv(path: Path, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_summary_md(path: Path, manifest: Dict[str, Any]) -> None:
    lines = [
        f"# Neo4j Import Bundle Summary: {manifest['snapshot_id']}",
        "",
        f"- input_file_count: `{manifest['input_file_count']}`",
        f"- raw_record_count: `{manifest['raw_record_count']}`",
        f"- imported_work_count: `{manifest['imported_work_count']}`",
        f"- topic_count: `{manifest['topic_count']}`",
        f"- primary_topic_rel_count: `{manifest['primary_topic_rel_count']}`",
        f"- topic_rel_count: `{manifest['topic_rel_count']}`",
        f"- reference_edge_count: `{manifest['reference_edge_count']}`",
        f"- related_edge_count: `{manifest['related_edge_count']}`",
        f"- works_with_doi_count: `{manifest['works_with_doi_count']}`",
        f"- works_without_doi_count: `{manifest['works_without_doi_count']}`",
        "",
        "## Skip / duplicate counts",
        "",
    ]

    skip_counts = manifest.get("skip_counts", {})
    if skip_counts:
        for key, value in sorted(skip_counts.items()):
            lines.append(f"- `{key}`: `{value}`")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Output files",
            "",
        ]
    )

    for key, value in manifest["output_paths"].items():
        lines.append(f"- `{key}`: `{value}`")

    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()

    snapshot_id = ensure_nonempty_str(args.snapshot_id, "snapshot_id")
    input_paths = iter_input_paths(args.input_glob)

    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        out_dir = Path("runs/neo4j_import_bundles") / snapshot_id

    if out_dir.exists():
        raise Neo4jImportBundleError(f"Refusing to overwrite existing output dir: {out_dir}")

    works_by_id: Dict[str, Dict[str, Any]] = {}
    topics_by_id: Dict[str, Dict[str, Any]] = {}
    primary_topic_rels = set()
    topic_rels = set()
    raw_reference_edges: List[Tuple[str, str]] = []
    raw_related_edges: List[Tuple[str, str]] = []

    counts: Counter[str] = Counter()

    for path in input_paths:
        for record in iter_records_from_path(path):
            counts["raw_record_count"] += 1
            if not isinstance(record, dict):
                counts["skipped_non_object_record"] += 1
                continue

            extracted = extract_work_record(record, snapshot_id)
            if extracted is None:
                counts["skipped_missing_openalex_id"] += 1
                continue

            work_row = extracted["work_row"]
            openalex_id = work_row["openalex_id"]

            if openalex_id in works_by_id:
                counts["duplicate_work_id_skipped"] += 1
                continue

            works_by_id[openalex_id] = work_row

            if work_row["doi"]:
                counts["works_with_doi_count"] += 1
            else:
                counts["works_without_doi_count"] += 1

            primary_topic = extracted["primary_topic"]
            primary_topic_score = extracted["primary_topic_score"]

            if primary_topic and primary_topic["topic_id"]:
                topic_id = primary_topic["topic_id"]
                if topic_id not in topics_by_id:
                    topics_by_id[topic_id] = {
                        **primary_topic,
                        "source_snapshot_id": snapshot_id,
                    }

                primary_topic_rels.add(
                    (
                        openalex_id,
                        topic_id,
                        primary_topic_score,
                        snapshot_id,
                    )
                )
                topic_rels.add(
                    (
                        openalex_id,
                        topic_id,
                        primary_topic_score,
                        "true",
                        snapshot_id,
                    )
                )

            for topic_row, topic_score in extracted["topics"]:
                topic_id = topic_row["topic_id"]
                if not topic_id:
                    continue
                if topic_id not in topics_by_id:
                    topics_by_id[topic_id] = {
                        **topic_row,
                        "source_snapshot_id": snapshot_id,
                    }

                is_primary = "true" if primary_topic and topic_id == primary_topic.get("topic_id") else "false"
                topic_rels.add(
                    (
                        openalex_id,
                        topic_id,
                        topic_score,
                        is_primary,
                        snapshot_id,
                    )
                )

            for target in extracted["referenced_works"]:
                if target and target != openalex_id:
                    raw_reference_edges.append((openalex_id, target))

            for target in extracted["related_works"]:
                if target and target != openalex_id:
                    raw_related_edges.append((openalex_id, target))

    imported_work_ids = set(works_by_id.keys())

    reference_edges = sorted(
        {
            (source, target, snapshot_id)
            for source, target in raw_reference_edges
            if source in imported_work_ids and target in imported_work_ids
        }
    )
    related_edges = sorted(
        {
            (source, target, snapshot_id)
            for source, target in raw_related_edges
            if source in imported_work_ids and target in imported_work_ids
        }
    )

    works_rows = sorted(works_by_id.values(), key=lambda row: (row["openalex_id"], row["doi"], row["title"]))
    topics_rows = sorted(topics_by_id.values(), key=lambda row: (row["topic_id"], row["topic_name"]))
    primary_topic_rows = [
        {
            "work_openalex_id": work_openalex_id,
            "topic_id": topic_id,
            "score": score,
            "source_snapshot_id": source_snapshot_id,
        }
        for work_openalex_id, topic_id, score, source_snapshot_id in sorted(primary_topic_rels)
    ]
    topic_rows = [
        {
            "work_openalex_id": work_openalex_id,
            "topic_id": topic_id,
            "score": score,
            "is_primary": is_primary,
            "source_snapshot_id": source_snapshot_id,
        }
        for work_openalex_id, topic_id, score, is_primary, source_snapshot_id in sorted(topic_rels)
    ]
    reference_rows = [
        {
            "source_openalex_id": source,
            "target_openalex_id": target,
            "source_snapshot_id": source_snapshot_id,
        }
        for source, target, source_snapshot_id in reference_edges
    ]
    related_rows = [
        {
            "source_openalex_id": source,
            "target_openalex_id": target,
            "source_snapshot_id": source_snapshot_id,
        }
        for source, target, source_snapshot_id in related_edges
    ]

    out_dir.mkdir(parents=True, exist_ok=False)

    works_csv = out_dir / "works.csv"
    topics_csv = out_dir / "topics.csv"
    work_primary_topic_csv = out_dir / "work_primary_topic.csv"
    work_topic_csv = out_dir / "work_topic.csv"
    work_reference_edges_csv = out_dir / "work_reference_edges.csv"
    work_related_edges_csv = out_dir / "work_related_edges.csv"
    manifest_json = out_dir / "import_bundle_manifest.json"
    summary_md = out_dir / "import_bundle_summary.md"

    write_csv(works_csv, WORKS_HEADERS, works_rows)
    write_csv(topics_csv, TOPICS_HEADERS, topics_rows)
    write_csv(work_primary_topic_csv, PRIMARY_TOPIC_REL_HEADERS, primary_topic_rows)
    write_csv(work_topic_csv, TOPIC_REL_HEADERS, topic_rows)
    write_csv(work_reference_edges_csv, REFERENCE_EDGE_HEADERS, reference_rows)
    write_csv(work_related_edges_csv, RELATED_EDGE_HEADERS, related_rows)

    manifest = {
        "snapshot_id": snapshot_id,
        "input_globs": args.input_glob,
        "input_paths": [str(path) for path in input_paths],
        "input_file_count": len(input_paths),
        "raw_record_count": counts["raw_record_count"],
        "imported_work_count": len(works_rows),
        "topic_count": len(topics_rows),
        "primary_topic_rel_count": len(primary_topic_rows),
        "topic_rel_count": len(topic_rows),
        "reference_edge_count": len(reference_rows),
        "related_edge_count": len(related_rows),
        "works_with_doi_count": counts["works_with_doi_count"],
        "works_without_doi_count": counts["works_without_doi_count"],
        "skip_counts": {
            key: value
            for key, value in sorted(counts.items())
            if key not in {"raw_record_count", "works_with_doi_count", "works_without_doi_count"}
        },
        "output_paths": {
            "works_csv": str(works_csv),
            "topics_csv": str(topics_csv),
            "work_primary_topic_csv": str(work_primary_topic_csv),
            "work_topic_csv": str(work_topic_csv),
            "work_reference_edges_csv": str(work_reference_edges_csv),
            "work_related_edges_csv": str(work_related_edges_csv),
            "import_bundle_manifest_json": str(manifest_json),
            "import_bundle_summary_md": str(summary_md),
        },
    }

    write_json(manifest_json, manifest)
    write_summary_md(summary_md, manifest)

    print("Neo4j import bundle built successfully.")
    print(f"snapshot_id: {snapshot_id}")
    print(f"input_file_count: {len(input_paths)}")
    print(f"raw_record_count: {counts['raw_record_count']}")
    print(f"imported_work_count: {len(works_rows)}")
    print(f"topic_count: {len(topics_rows)}")
    print(f"reference_edge_count: {len(reference_rows)}")
    print(f"related_edge_count: {len(related_rows)}")
    print(f"output_dir: {out_dir}")


if __name__ == "__main__":
    try:
        main()
    except Neo4jImportBundleError as exc:
        raise SystemExit(f"ERROR: {exc}")
