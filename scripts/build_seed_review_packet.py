#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

REQUIRED_CSV_COLUMNS = [
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

CANDIDATE_ID_RE = re.compile(r"^C[0-9]{4,}$")
DOI_RE = re.compile(r"^10\..+/.+$")
TAG_RE = re.compile(r"^[a-z0-9_]+$")
BOOL_TRUE_VALUES = {"1", "true", "yes", "y"}
BOOL_FALSE_VALUES = {"0", "false", "no", "n", ""}


class PacketBuildError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a deterministic seed-selection review packet from a local candidate_pool.csv."
    )
    parser.add_argument(
        "--candidate-pool-csv",
        required=True,
        help="Path to the candidate_pool.csv artifact.",
    )
    parser.add_argument(
        "--policy",
        default="configs/presets/seed_policies/seed_selection_policy_v1.yaml",
        help="Path to the seed-selection policy YAML.",
    )
    parser.add_argument(
        "--packet-id",
        required=True,
        help="Packet identifier, e.g. seed_packet_001.",
    )
    parser.add_argument(
        "--created-at",
        required=True,
        help="Packet creation timestamp in ISO-8601 format.",
    )
    parser.add_argument(
        "--source-snapshot-id",
        required=True,
        help="Source snapshot identifier for the candidate pool.",
    )
    parser.add_argument(
        "--candidate-pool-id",
        required=True,
        help="Logical identifier for the candidate pool.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for packet artifacts.",
    )
    parser.add_argument(
        "--tag-target",
        action="append",
        required=True,
        help="Repeatable tag target in the form tag:min:max . Example: state_tomography:3:5",
    )
    parser.add_argument(
        "--final-target-total",
        type=int,
        required=True,
        help="Final desired total selected seeds for the downstream cycle.",
    )
    parser.add_argument(
        "--min-total",
        type=int,
        required=True,
        help="Minimum accepted total selected seeds for the downstream cycle.",
    )
    parser.add_argument(
        "--max-total",
        type=int,
        required=True,
        help="Maximum accepted total selected seeds for the downstream cycle.",
    )
    parser.add_argument(
        "--anchor-min",
        type=int,
        required=True,
        help="Minimum anchor role count requested from the verifier.",
    )
    parser.add_argument(
        "--boundary-min",
        type=int,
        required=True,
        help="Minimum boundary role count requested from the verifier.",
    )
    parser.add_argument(
        "--sentinel-min",
        type=int,
        required=True,
        help="Minimum sentinel role count requested from the verifier.",
    )
    parser.add_argument(
        "--max-candidates-per-tag",
        type=int,
        default=25,
        help="Maximum number of packet candidates to include per proposed_tag.",
    )
    return parser.parse_args()


def validate_iso_datetime(value: str) -> str:
    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise PacketBuildError(f"Invalid ISO-8601 datetime for --created-at: {value}") from exc
    return value


def parse_tag_name(value: str) -> str:
    if not TAG_RE.match(value):
        raise PacketBuildError(f"Invalid tag name: {value}")
    return value


def parse_tag_target(value: str) -> Tuple[str, int, int]:
    parts = value.split(":")
    if len(parts) != 3:
        raise PacketBuildError(
            f"Invalid --tag-target value '{value}'. Expected format tag:min:max"
        )
    tag = parse_tag_name(parts[0].strip())
    try:
        min_count = int(parts[1])
        max_count = int(parts[2])
    except ValueError as exc:
        raise PacketBuildError(
            f"Invalid --tag-target value '{value}'. min and max must be integers."
        ) from exc
    if min_count < 0 or max_count < 0 or max_count < min_count:
        raise PacketBuildError(
            f"Invalid --tag-target value '{value}'. Must satisfy 0 <= min <= max."
        )
    return tag, min_count, max_count


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise PacketBuildError(f"Expected YAML object in {path}")
    return data


def parse_bool(value: str, field_name: str) -> bool:
    normalized = (value or "").strip().lower()
    if normalized in BOOL_TRUE_VALUES:
        return True
    if normalized in BOOL_FALSE_VALUES:
        return False
    raise PacketBuildError(f"Invalid boolean value for {field_name}: {value!r}")


def parse_int(value: str, field_name: str) -> int:
    try:
        return int((value or "").strip())
    except ValueError as exc:
        raise PacketBuildError(f"Invalid integer value for {field_name}: {value!r}") from exc


def parse_float(value: str, field_name: str) -> float:
    try:
        return float((value or "").strip())
    except ValueError as exc:
        raise PacketBuildError(f"Invalid float value for {field_name}: {value!r}") from exc


def parse_secondary_tag_hints(value: str) -> List[str]:
    if not (value or "").strip():
        return []
    items = [item.strip() for item in value.split("|")]
    result: List[str] = []
    for item in items:
        if not item:
            continue
        result.append(parse_tag_name(item))
    return result


def validate_doi(value: str) -> str:
    doi = (value or "").strip()
    if not DOI_RE.match(doi):
        raise PacketBuildError(f"Invalid DOI value: {value!r}")
    return doi


def validate_candidate_id(value: str) -> str:
    candidate_id = (value or "").strip()
    if not CANDIDATE_ID_RE.match(candidate_id):
        raise PacketBuildError(f"Invalid candidate_id value: {value!r}")
    return candidate_id


def load_candidate_rows(csv_path: Path) -> List[Dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise PacketBuildError(f"CSV has no header: {csv_path}")
        missing = [column for column in REQUIRED_CSV_COLUMNS if column not in reader.fieldnames]
        if missing:
            raise PacketBuildError(
                f"CSV is missing required columns: {', '.join(missing)}"
            )

        rows: List[Dict[str, Any]] = []
        for raw in reader:
            row = {
                "candidate_id": validate_candidate_id(raw["candidate_id"]),
                "doi": validate_doi(raw["doi"]),
                "title": (raw["title"] or "").strip(),
                "proposed_tag": parse_tag_name((raw["proposed_tag"] or "").strip()),
                "secondary_tag_hints": parse_secondary_tag_hints(raw["secondary_tag_hints"]),
                "publication_year": parse_int(raw["publication_year"], "publication_year"),
                "type": ((raw["type"] or "").strip() or "unknown"),
                "openalex_resolved": parse_bool(raw["openalex_resolved"], "openalex_resolved"),
                "citation_count": parse_int(raw["citation_count"], "citation_count"),
                "referenced_works_count": parse_int(
                    raw["referenced_works_count"], "referenced_works_count"
                ),
                "graph_boundary_score": parse_float(
                    raw["graph_boundary_score"], "graph_boundary_score"
                ),
                "graph_centrality_score": parse_float(
                    raw["graph_centrality_score"], "graph_centrality_score"
                ),
                "duplicate_cluster_id": (raw["duplicate_cluster_id"] or "").strip(),
                "source_snapshot_id": (raw["source_snapshot_id"] or "").strip(),
            }

            if not row["type"]:
                raise PacketBuildError(
                    f"candidate_id {row['candidate_id']} has empty type"
                )
            if not row["duplicate_cluster_id"]:
                raise PacketBuildError(
                    f"candidate_id {row['candidate_id']} has empty duplicate_cluster_id"
                )
            if not row["source_snapshot_id"]:
                raise PacketBuildError(
                    f"candidate_id {row['candidate_id']} has empty source_snapshot_id"
                )
            if not (0.0 <= row["graph_boundary_score"] <= 1.0):
                raise PacketBuildError(
                    f"candidate_id {row['candidate_id']} has graph_boundary_score outside [0,1]"
                )
            if not (0.0 <= row["graph_centrality_score"] <= 1.0):
                raise PacketBuildError(
                    f"candidate_id {row['candidate_id']} has graph_centrality_score outside [0,1]"
                )

            rows.append(row)

    return rows


def row_filter_reason(
    row: Dict[str, Any],
    policy: Dict[str, Any],
) -> str | None:
    source_constraints = policy["source_constraints"]
    requirements = policy["candidate_pool_requirements"]

    if not row["title"]:
        return "empty_title"

    if source_constraints.get("require_doi", False) and not row["doi"]:
        return "missing_doi"

    if source_constraints.get("require_openalex_resolved", False) and not row["openalex_resolved"]:
        return "openalex_unresolved"

    allowed_types = set(requirements.get("allowed_types", []))
    exclude_types = set(requirements.get("exclude_types", []))

    if row["type"] in exclude_types:
        return "excluded_type"

    if allowed_types and row["type"] not in allowed_types:
        return "disallowed_type"

    if row["publication_year"] < int(requirements.get("min_publication_year", 0)):
        return "below_min_publication_year"

    if row["referenced_works_count"] < int(requirements.get("min_referenced_works_count", 0)):
        return "below_min_referenced_works_count"

    if row["citation_count"] < int(requirements.get("min_cited_by_count", 0)):
        return "below_min_cited_by_count"

    return None


def sort_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        -row["graph_boundary_score"],
        -row["graph_centrality_score"],
        -row["citation_count"],
        -row["referenced_works_count"],
        -row["publication_year"],
        row["doi"],
        row["candidate_id"],
    )


def build_packet_candidates(
    eligible_rows: List[Dict[str, Any]],
    tag_targets: List[Tuple[str, int, int]],
    max_candidates_per_tag: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    if max_candidates_per_tag <= 0:
        raise PacketBuildError("--max-candidates-per-tag must be > 0")

    packet_rows: List[Dict[str, Any]] = []
    counts_by_tag: Dict[str, int] = {}
    seen_candidate_ids = set()
    seen_dois = set()

    for tag, min_count, _max_count in tag_targets:
        rows_for_tag = [row for row in eligible_rows if row["proposed_tag"] == tag]
        rows_for_tag.sort(key=sort_key)

        if len(rows_for_tag) < min_count:
            raise PacketBuildError(
                f"Tag '{tag}' has only {len(rows_for_tag)} eligible rows, which is below required minimum {min_count}"
            )

        selected_rows = rows_for_tag[:max_candidates_per_tag]

        if len(selected_rows) < min_count:
            raise PacketBuildError(
                f"Tag '{tag}' selected only {len(selected_rows)} packet rows, which is below required minimum {min_count}"
            )

        counts_by_tag[tag] = len(selected_rows)

        for row in selected_rows:
            if row["candidate_id"] in seen_candidate_ids:
                raise PacketBuildError(f"Duplicate candidate_id across packet rows: {row['candidate_id']}")
            if row["doi"] in seen_dois:
                raise PacketBuildError(f"Duplicate DOI across packet rows: {row['doi']}")
            seen_candidate_ids.add(row["candidate_id"])
            seen_dois.add(row["doi"])
            packet_rows.append(row)

    return packet_rows, counts_by_tag


def candidate_to_packet_entry(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "candidate_id": row["candidate_id"],
        "doi": row["doi"],
        "title": row["title"],
        "proposed_tag": row["proposed_tag"],
        "secondary_tag_hints": row["secondary_tag_hints"],
        "year": row["publication_year"],
        "type": row["type"],
        "openalex_resolved": row["openalex_resolved"],
        "citation_count": row["citation_count"],
        "referenced_works_count": row["referenced_works_count"],
        "graph_boundary_score": row["graph_boundary_score"],
        "graph_centrality_score": row["graph_centrality_score"],
        "duplicate_cluster_id": row["duplicate_cluster_id"],
    }


def write_yaml(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False, allow_unicode=True)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_candidate_snapshot_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = REQUIRED_CSV_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "candidate_id": row["candidate_id"],
                    "doi": row["doi"],
                    "title": row["title"],
                    "proposed_tag": row["proposed_tag"],
                    "secondary_tag_hints": "|".join(row["secondary_tag_hints"]),
                    "publication_year": row["publication_year"],
                    "type": row["type"],
                    "openalex_resolved": str(row["openalex_resolved"]).lower(),
                    "citation_count": row["citation_count"],
                    "referenced_works_count": row["referenced_works_count"],
                    "graph_boundary_score": row["graph_boundary_score"],
                    "graph_centrality_score": row["graph_centrality_score"],
                    "duplicate_cluster_id": row["duplicate_cluster_id"],
                    "source_snapshot_id": row["source_snapshot_id"],
                }
            )


def write_summary_md(
    path: Path,
    packet_id: str,
    manifest: Dict[str, Any],
) -> None:
    lines = [
        f"# Seed Review Packet Summary: {packet_id}",
        "",
        f"- candidate_pool_csv: `{manifest['candidate_pool_csv']}`",
        f"- policy_path: `{manifest['policy_path']}`",
        f"- source_snapshot_id: `{manifest['source_snapshot_id']}`",
        f"- candidate_pool_id: `{manifest['candidate_pool_id']}`",
        f"- total_input_rows: `{manifest['total_input_rows']}`",
        f"- eligible_row_count: `{manifest['eligible_row_count']}`",
        f"- packet_candidate_count: `{manifest['packet_candidate_count']}`",
        f"- max_candidates_per_tag: `{manifest['max_candidates_per_tag']}`",
        "",
        "## Per-tag packet counts",
        "",
    ]

    for tag, count in manifest["per_tag_packet_counts"].items():
        lines.append(f"- `{tag}`: `{count}`")

    lines.extend(
        [
            "",
            "## Dropped row counts",
            "",
        ]
    )

    if manifest["dropped_reason_counts"]:
        for reason, count in manifest["dropped_reason_counts"].items():
            lines.append(f"- `{reason}`: `{count}`")
    else:
        lines.append("- none")

    lines.append("")
    path.write_text("\\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    validate_iso_datetime(args.created_at)

    tag_targets = [parse_tag_target(item) for item in args.tag_target]

    policy_path = Path(args.policy)
    candidate_pool_csv = Path(args.candidate_pool_csv)
    out_dir = Path(args.out_dir)

    if not policy_path.exists():
        raise PacketBuildError(f"Policy file does not exist: {policy_path}")
    if not candidate_pool_csv.exists():
        raise PacketBuildError(f"Candidate pool CSV does not exist: {candidate_pool_csv}")

    policy = load_yaml(policy_path)
    rows = load_candidate_rows(candidate_pool_csv)

    dropped_reason_counts: Counter[str] = Counter()
    eligible_rows: List[Dict[str, Any]] = []

    for row in rows:
        reason = row_filter_reason(row, policy)
        if reason is None:
            eligible_rows.append(row)
        else:
            dropped_reason_counts[reason] += 1

    packet_rows, per_tag_packet_counts = build_packet_candidates(
        eligible_rows=eligible_rows,
        tag_targets=tag_targets,
        max_candidates_per_tag=args.max_candidates_per_tag,
    )

    packet = {
        "packet_type": "seed_selection_review_packet",
        "schema_version": 1,
        "packet_id": args.packet_id,
        "created_at": args.created_at,
        "source": {
            "source_system": policy["source_constraints"]["source_system"],
            "source_snapshot_id": args.source_snapshot_id,
            "candidate_pool_id": args.candidate_pool_id,
        },
        "selection_policy": {
            "final_target_total": args.final_target_total,
            "min_total": args.min_total,
            "max_total": args.max_total,
            "allow_only_candidate_ids": True,
            "allow_freeform_doi_additions": False,
        },
        "tag_targets": [
            {"tag": tag, "min": min_count, "max": max_count}
            for tag, min_count, max_count in tag_targets
        ],
        "role_targets": {
            "anchor_min": args.anchor_min,
            "boundary_min": args.boundary_min,
            "sentinel_min": args.sentinel_min,
        },
        "hard_rules": {
            "exclude_types": policy["candidate_pool_requirements"]["exclude_types"],
            "require_openalex_resolved": policy["source_constraints"]["require_openalex_resolved"],
            "reject_duplicate_cluster_overflow": policy["selection_rules"]["no_duplicate_cluster_overflow"],
        },
        "candidates": [candidate_to_packet_entry(row) for row in packet_rows],
        "instructions": {
            "reviewer_goal": (
                "Select the final benchmark seeds for the next cycle. "
                "You may only reference candidate_id values listed in this packet. "
                "Do not invent new DOIs. "
                "If the candidate pool is insufficient, return status=needs_expansion."
            )
        },
    }

    out_dir.mkdir(parents=True, exist_ok=True)

    packet_yaml_path = out_dir / "seed_selection_review_packet.yaml"
    packet_manifest_path = out_dir / "packet_manifest.json"
    packet_summary_path = out_dir / "packet_summary.md"
    candidate_snapshot_path = out_dir / "candidate_snapshot.csv"

    write_yaml(packet_yaml_path, packet)
    write_candidate_snapshot_csv(candidate_snapshot_path, packet_rows)

    manifest = {
        "packet_id": args.packet_id,
        "candidate_pool_csv": str(candidate_pool_csv),
        "policy_path": str(policy_path),
        "source_snapshot_id": args.source_snapshot_id,
        "candidate_pool_id": args.candidate_pool_id,
        "total_input_rows": len(rows),
        "eligible_row_count": len(eligible_rows),
        "packet_candidate_count": len(packet_rows),
        "max_candidates_per_tag": args.max_candidates_per_tag,
        "per_tag_packet_counts": per_tag_packet_counts,
        "dropped_reason_counts": dict(sorted(dropped_reason_counts.items())),
        "output_paths": {
            "seed_selection_review_packet_yaml": str(packet_yaml_path),
            "packet_manifest_json": str(packet_manifest_path),
            "packet_summary_md": str(packet_summary_path),
            "candidate_snapshot_csv": str(candidate_snapshot_path),
        },
    }

    write_json(packet_manifest_path, manifest)
    write_summary_md(packet_summary_path, args.packet_id, manifest)

    print("Seed review packet built successfully.")
    print(f"packet_id: {args.packet_id}")
    print(f"candidate_pool_csv: {candidate_pool_csv}")
    print(f"eligible_row_count: {len(eligible_rows)}")
    print(f"packet_candidate_count: {len(packet_rows)}")
    print(f"output_dir: {out_dir}")


if __name__ == "__main__":
    try:
        main()
    except PacketBuildError as exc:
        raise SystemExit(f"ERROR: {exc}")
