#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
TAG_RE = re.compile(r"^[a-z0-9_]+$")
CANDIDATE_ID_RE = re.compile(r"^C[0-9]{4,}$")
DOI_RE = re.compile(r"^10\..+/.+$")
VALID_ROLES = {"anchor", "boundary", "sentinel"}


class SeedCycleMaterializationError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize a frozen seed set, benchmark preset, launch profile, and cycle manifest from an accepted verifier reply."
    )
    parser.add_argument(
        "--reply-dir",
        required=True,
        help="Path to runs/seed_review_replies/<reply_id>",
    )
    parser.add_argument(
        "--packet-yaml",
        required=True,
        help="Path to the seed_selection_review_packet.yaml used for the reply.",
    )
    parser.add_argument(
        "--seed-set-id",
        required=True,
        help="Seed set identifier, e.g. seed_set_example",
    )
    parser.add_argument(
        "--cycle-id",
        required=True,
        help="Cycle identifier, e.g. seed_cycle_example",
    )
    parser.add_argument(
        "--benchmark-preset-id",
        required=True,
        help="Benchmark preset identifier to create.",
    )
    parser.add_argument(
        "--launch-profile-id",
        required=True,
        help="Launch profile identifier to create.",
    )
    parser.add_argument(
        "--accepted-baseline-id",
        required=True,
        help="Accepted baseline identifier, e.g. baseline_001",
    )
    parser.add_argument(
        "--eval-preset-id",
        required=True,
        help="Evaluation preset identifier, e.g. eval_preset_001",
    )
    parser.add_argument(
        "--created-at",
        required=True,
        help="Cycle creation timestamp in ISO-8601 format.",
    )
    parser.add_argument(
        "--description",
        required=True,
        help="Human-readable description for the materialized benchmark cycle.",
    )
    return parser.parse_args()


def ensure_nonempty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SeedCycleMaterializationError(f"{field_name} must be a non-empty string")
    return value.strip()


def ensure_dict(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise SeedCycleMaterializationError(f"{field_name} must be an object")
    return value


def ensure_list(value: Any, field_name: str) -> List[Any]:
    if not isinstance(value, list):
        raise SeedCycleMaterializationError(f"{field_name} must be a list")
    return value


def ensure_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SeedCycleMaterializationError(f"{field_name} must be an integer")
    return value


def ensure_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise SeedCycleMaterializationError(f"{field_name} must be a boolean")
    return value


def ensure_float_01(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SeedCycleMaterializationError(f"{field_name} must be a number in [0,1]")
    result = float(value)
    if not (0.0 <= result <= 1.0):
        raise SeedCycleMaterializationError(f"{field_name} must be in [0,1]")
    return result


def validate_id(value: str, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    if not ID_RE.match(result):
        raise SeedCycleMaterializationError(
            f"{field_name} must match ^[A-Za-z0-9._-]+$: {result}"
        )
    return result


def validate_iso_datetime(value: str, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    normalized = result.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SeedCycleMaterializationError(
            f"{field_name} must be a valid ISO-8601 datetime: {result}"
        ) from exc
    return result


def validate_tag(value: Any, field_name: str) -> str:
    tag = ensure_nonempty_str(value, field_name)
    if not TAG_RE.match(tag):
        raise SeedCycleMaterializationError(
            f"{field_name} must match ^[a-z0-9_]+$: {tag}"
        )
    return tag


def validate_candidate_id(value: Any, field_name: str) -> str:
    candidate_id = ensure_nonempty_str(value, field_name)
    if not CANDIDATE_ID_RE.match(candidate_id):
        raise SeedCycleMaterializationError(
            f"{field_name} must match ^C[0-9]{{4,}}$: {candidate_id}"
        )
    return candidate_id


def validate_doi(value: Any, field_name: str) -> str:
    doi = ensure_nonempty_str(value, field_name)
    if not DOI_RE.match(doi):
        raise SeedCycleMaterializationError(
            f"{field_name} must look like a DOI: {doi}"
        )
    return doi


def validate_role(value: Any, field_name: str) -> str:
    role = ensure_nonempty_str(value, field_name)
    if role not in VALID_ROLES:
        raise SeedCycleMaterializationError(
            f"{field_name} must be one of {sorted(VALID_ROLES)}"
        )
    return role


def load_yaml_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise SeedCycleMaterializationError(f"Expected YAML object in {path}")
    return data


def load_json_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise SeedCycleMaterializationError(f"Expected JSON object in {path}")
    return data


def load_json_array(path: Path) -> List[Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise SeedCycleMaterializationError(f"Expected JSON array in {path}")
    return data


def ensure_output_path_available(path: Path) -> None:
    if path.exists():
        raise SeedCycleMaterializationError(f"Refusing to overwrite existing path: {path}")


def load_packet(path: Path) -> Dict[str, Any]:
    data = load_yaml_object(path)

    packet_type = ensure_nonempty_str(data.get("packet_type"), "packet_type")
    if packet_type != "seed_selection_review_packet":
        raise SeedCycleMaterializationError(f"Unexpected packet_type: {packet_type}")

    schema_version = ensure_int(data.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise SeedCycleMaterializationError(f"Unsupported packet schema_version: {schema_version}")

    packet_id = ensure_nonempty_str(data.get("packet_id"), "packet_id")
    validate_iso_datetime(data.get("created_at"), "created_at")

    source_raw = ensure_dict(data.get("source"), "source")
    source = {
        "source_system": ensure_nonempty_str(source_raw.get("source_system"), "source.source_system"),
        "source_snapshot_id": ensure_nonempty_str(
            source_raw.get("source_snapshot_id"),
            "source.source_snapshot_id",
        ),
        "candidate_pool_id": ensure_nonempty_str(
            source_raw.get("candidate_pool_id"),
            "source.candidate_pool_id",
        ),
    }

    tag_targets_raw = ensure_list(data.get("tag_targets"), "tag_targets")
    tag_targets: Dict[str, Dict[str, int]] = {}
    for index, item in enumerate(tag_targets_raw):
        entry = ensure_dict(item, f"tag_targets[{index}]")
        tag = validate_tag(entry.get("tag"), f"tag_targets[{index}].tag")
        min_count = ensure_int(entry.get("min"), f"tag_targets[{index}].min")
        max_count = ensure_int(entry.get("max"), f"tag_targets[{index}].max")
        if min_count < 0 or max_count < min_count:
            raise SeedCycleMaterializationError(
                f"tag_targets[{index}] must satisfy 0 <= min <= max"
            )
        tag_targets[tag] = {"min": min_count, "max": max_count}

    candidates_raw = ensure_list(data.get("candidates"), "candidates")
    if not candidates_raw:
        raise SeedCycleMaterializationError("Packet candidates may not be empty")

    candidate_by_id: Dict[str, Dict[str, Any]] = {}
    for index, item in enumerate(candidates_raw):
        entry = ensure_dict(item, f"candidates[{index}]")
        candidate_id = validate_candidate_id(
            entry.get("candidate_id"),
            f"candidates[{index}].candidate_id",
        )
        doi = validate_doi(entry.get("doi"), f"candidates[{index}].doi")
        title = ensure_nonempty_str(entry.get("title"), f"candidates[{index}].title")
        proposed_tag = validate_tag(
            entry.get("proposed_tag"),
            f"candidates[{index}].proposed_tag",
        )
        secondary_tag_hints_raw = ensure_list(
            entry.get("secondary_tag_hints"),
            f"candidates[{index}].secondary_tag_hints",
        )
        secondary_tag_hints = [
            validate_tag(value, f"candidates[{index}].secondary_tag_hints item")
            for value in secondary_tag_hints_raw
        ]
        year = ensure_int(entry.get("year"), f"candidates[{index}].year")
        type_value = ensure_nonempty_str(entry.get("type"), f"candidates[{index}].type")
        openalex_resolved = ensure_bool(
            entry.get("openalex_resolved"),
            f"candidates[{index}].openalex_resolved",
        )
        citation_count = ensure_int(
            entry.get("citation_count"),
            f"candidates[{index}].citation_count",
        )
        referenced_works_count = ensure_int(
            entry.get("referenced_works_count"),
            f"candidates[{index}].referenced_works_count",
        )
        graph_boundary_score = ensure_float_01(
            entry.get("graph_boundary_score"),
            f"candidates[{index}].graph_boundary_score",
        )
        graph_centrality_score = ensure_float_01(
            entry.get("graph_centrality_score"),
            f"candidates[{index}].graph_centrality_score",
        )
        duplicate_cluster_id = ensure_nonempty_str(
            entry.get("duplicate_cluster_id"),
            f"candidates[{index}].duplicate_cluster_id",
        )

        if candidate_id in candidate_by_id:
            raise SeedCycleMaterializationError(f"Duplicate candidate_id in packet: {candidate_id}")

        candidate_by_id[candidate_id] = {
            "candidate_id": candidate_id,
            "doi": doi,
            "title": title,
            "proposed_tag": proposed_tag,
            "secondary_tag_hints": secondary_tag_hints,
            "year": year,
            "type": type_value,
            "openalex_resolved": openalex_resolved,
            "citation_count": citation_count,
            "referenced_works_count": referenced_works_count,
            "graph_boundary_score": graph_boundary_score,
            "graph_centrality_score": graph_centrality_score,
            "duplicate_cluster_id": duplicate_cluster_id,
        }

    return {
        "packet_id": packet_id,
        "packet_path": str(path),
        "source": source,
        "tag_targets": tag_targets,
        "candidate_by_id": candidate_by_id,
    }


def load_ingest_report(reply_dir: Path) -> Dict[str, Any]:
    ingest_report_path = reply_dir / "ingest_report.json"
    if not ingest_report_path.exists():
        raise SeedCycleMaterializationError(
            f"Missing ingest_report.json in reply dir: {ingest_report_path}"
        )

    data = load_json_object(ingest_report_path)

    validation_status = ensure_nonempty_str(
        data.get("validation_status"),
        "ingest_report.validation_status",
    )
    if validation_status != "accepted":
        raise SeedCycleMaterializationError(
            f"Ingest report is not accepted: {validation_status}"
        )

    reply_status = ensure_nonempty_str(
        data.get("reply_status"),
        "ingest_report.reply_status",
    )
    if reply_status != "ready_for_cycle":
        raise SeedCycleMaterializationError(
            f"Reply status must be ready_for_cycle for materialization: {reply_status}"
        )

    packet_id = ensure_nonempty_str(data.get("packet_id"), "ingest_report.packet_id")
    reply_id = ensure_nonempty_str(data.get("reply_id"), "ingest_report.reply_id")
    reviewer_id = ensure_nonempty_str(data.get("reviewer_id"), "ingest_report.reviewer_id")
    completed_at = validate_iso_datetime(
        data.get("completed_at"),
        "ingest_report.completed_at",
    )
    selected_count = ensure_int(data.get("selected_count"), "ingest_report.selected_count")
    rejected_count = ensure_int(data.get("rejected_count"), "ingest_report.rejected_count")
    expansion_request_count = ensure_int(
        data.get("expansion_request_count"),
        "ingest_report.expansion_request_count",
    )

    per_tag_counts = ensure_dict(
        data.get("per_tag_selected_counts"),
        "ingest_report.per_tag_selected_counts",
    )
    per_role_counts = ensure_dict(
        data.get("per_role_selected_counts"),
        "ingest_report.per_role_selected_counts",
    )
    summary = ensure_dict(data.get("summary"), "ingest_report.summary")

    return {
        "ingest_report_path": str(ingest_report_path),
        "packet_id": packet_id,
        "reply_id": reply_id,
        "reviewer_id": reviewer_id,
        "completed_at": completed_at,
        "validation_status": validation_status,
        "reply_status": reply_status,
        "selected_count": selected_count,
        "rejected_count": rejected_count,
        "expansion_request_count": expansion_request_count,
        "per_tag_selected_counts": {
            ensure_nonempty_str(k, "ingest_report.per_tag_selected_counts key"): ensure_int(v, "ingest_report.per_tag_selected_counts value")
            for k, v in per_tag_counts.items()
        },
        "per_role_selected_counts": {
            ensure_nonempty_str(k, "ingest_report.per_role_selected_counts key"): ensure_int(v, "ingest_report.per_role_selected_counts value")
            for k, v in per_role_counts.items()
        },
        "summary": summary,
    }


def load_selected_candidates(reply_dir: Path, packet: Dict[str, Any], ingest_report: Dict[str, Any]) -> List[Dict[str, Any]]:
    selected_path = reply_dir / "selected_candidates.json"
    if not selected_path.exists():
        raise SeedCycleMaterializationError(
            f"Missing selected_candidates.json in reply dir: {selected_path}"
        )

    raw_items = load_json_array(selected_path)
    if len(raw_items) != ingest_report["selected_count"]:
        raise SeedCycleMaterializationError(
            f"selected_candidates.json count={len(raw_items)} does not match ingest_report.selected_count={ingest_report['selected_count']}"
        )

    seen_candidate_ids = set()
    seen_dois = set()
    duplicate_cluster_counts: Dict[str, int] = {}
    per_tag_counts: Dict[str, int] = {}
    per_role_counts: Dict[str, int] = {}
    selected_items: List[Dict[str, Any]] = []

    for index, item in enumerate(raw_items):
        entry = ensure_dict(item, f"selected_candidates[{index}]")
        candidate_id = validate_candidate_id(
            entry.get("candidate_id"),
            f"selected_candidates[{index}].candidate_id",
        )
        if candidate_id in seen_candidate_ids:
            raise SeedCycleMaterializationError(
                f"Duplicate candidate_id in selected_candidates.json: {candidate_id}"
            )
        if candidate_id not in packet["candidate_by_id"]:
            raise SeedCycleMaterializationError(
                f"selected_candidates[{index}].candidate_id is not present in packet: {candidate_id}"
            )
        seen_candidate_ids.add(candidate_id)

        doi = validate_doi(entry.get("doi"), f"selected_candidates[{index}].doi")
        if doi in seen_dois:
            raise SeedCycleMaterializationError(
                f"Duplicate DOI in selected_candidates.json: {doi}"
            )
        seen_dois.add(doi)

        title = ensure_nonempty_str(entry.get("title"), f"selected_candidates[{index}].title")
        proposed_tag = validate_tag(
            entry.get("proposed_tag"),
            f"selected_candidates[{index}].proposed_tag",
        )
        secondary_tag_hints_raw = ensure_list(
            entry.get("secondary_tag_hints"),
            f"selected_candidates[{index}].secondary_tag_hints",
        )
        secondary_tag_hints = [
            validate_tag(value, f"selected_candidates[{index}].secondary_tag_hints item")
            for value in secondary_tag_hints_raw
        ]
        year = ensure_int(entry.get("year"), f"selected_candidates[{index}].year")
        type_value = ensure_nonempty_str(entry.get("type"), f"selected_candidates[{index}].type")
        openalex_resolved = ensure_bool(
            entry.get("openalex_resolved"),
            f"selected_candidates[{index}].openalex_resolved",
        )
        citation_count = ensure_int(
            entry.get("citation_count"),
            f"selected_candidates[{index}].citation_count",
        )
        referenced_works_count = ensure_int(
            entry.get("referenced_works_count"),
            f"selected_candidates[{index}].referenced_works_count",
        )
        graph_boundary_score = ensure_float_01(
            entry.get("graph_boundary_score"),
            f"selected_candidates[{index}].graph_boundary_score",
        )
        graph_centrality_score = ensure_float_01(
            entry.get("graph_centrality_score"),
            f"selected_candidates[{index}].graph_centrality_score",
        )
        duplicate_cluster_id = ensure_nonempty_str(
            entry.get("duplicate_cluster_id"),
            f"selected_candidates[{index}].duplicate_cluster_id",
        )
        final_tag = validate_tag(
            entry.get("final_tag"),
            f"selected_candidates[{index}].final_tag",
        )
        if final_tag not in packet["tag_targets"]:
            raise SeedCycleMaterializationError(
                f"selected_candidates[{index}].final_tag is not present in packet tag_targets: {final_tag}"
            )
        final_role = validate_role(
            entry.get("final_role"),
            f"selected_candidates[{index}].final_role",
        )
        confidence = ensure_float_01(
            entry.get("confidence"),
            f"selected_candidates[{index}].confidence",
        )
        reason_codes_raw = ensure_list(
            entry.get("reason_codes"),
            f"selected_candidates[{index}].reason_codes",
        )
        if not reason_codes_raw:
            raise SeedCycleMaterializationError(
                f"selected_candidates[{index}].reason_codes must not be empty"
            )
        reason_codes = [
            ensure_nonempty_str(value, f"selected_candidates[{index}].reason_codes item")
            for value in reason_codes_raw
        ]

        packet_meta = packet["candidate_by_id"][candidate_id]
        if doi != packet_meta["doi"]:
            raise SeedCycleMaterializationError(
                f"DOI mismatch for candidate_id {candidate_id}: selected={doi} packet={packet_meta['doi']}"
            )

        duplicate_cluster_counts[duplicate_cluster_id] = duplicate_cluster_counts.get(duplicate_cluster_id, 0) + 1
        per_tag_counts[final_tag] = per_tag_counts.get(final_tag, 0) + 1
        per_role_counts[final_role] = per_role_counts.get(final_role, 0) + 1

        selected_items.append(
            {
                "candidate_id": candidate_id,
                "doi": doi,
                "title": title,
                "proposed_tag": proposed_tag,
                "secondary_tag_hints": secondary_tag_hints,
                "year": year,
                "type": type_value,
                "openalex_resolved": openalex_resolved,
                "citation_count": citation_count,
                "referenced_works_count": referenced_works_count,
                "graph_boundary_score": graph_boundary_score,
                "graph_centrality_score": graph_centrality_score,
                "duplicate_cluster_id": duplicate_cluster_id,
                "final_tag": final_tag,
                "final_role": final_role,
                "confidence": confidence,
                "reason_codes": reason_codes,
            }
        )

    overflow_clusters = {k: v for k, v in duplicate_cluster_counts.items() if v > 1}
    if overflow_clusters:
        raise SeedCycleMaterializationError(
            f"Duplicate cluster overflow detected in selected candidates: {overflow_clusters}"
        )

    if dict(sorted(per_tag_counts.items())) != dict(sorted(ingest_report["per_tag_selected_counts"].items())):
        raise SeedCycleMaterializationError(
            "Per-tag selected counts do not match ingest_report"
        )

    if dict(sorted(per_role_counts.items())) != dict(sorted(ingest_report["per_role_selected_counts"].items())):
        raise SeedCycleMaterializationError(
            "Per-role selected counts do not match ingest_report"
        )

    return selected_items


def sort_selected_candidates(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item["final_tag"],
            item["doi"],
            item["candidate_id"],
        ),
    )


def load_eval_preset(eval_preset_id: str) -> Dict[str, Any]:
    eval_preset_path = Path("configs/presets/evals") / f"{eval_preset_id}.json"
    if not eval_preset_path.exists():
        raise SeedCycleMaterializationError(f"Eval preset does not exist: {eval_preset_path}")

    data = load_json_object(eval_preset_path)

    file_eval_preset_id = ensure_nonempty_str(data.get("eval_preset_id"), "eval_preset.eval_preset_id")
    if file_eval_preset_id != eval_preset_id:
        raise SeedCycleMaterializationError(
            f"Eval preset id mismatch: file says {file_eval_preset_id}, expected {eval_preset_id}"
        )

    required_fields = [
        "max_references",
        "max_related",
        "max_hard_negatives",
        "top_k",
        "label_source",
        "refresh",
    ]
    for field_name in required_fields:
        if field_name not in data:
            raise SeedCycleMaterializationError(f"Eval preset missing field: {field_name}")

    return {
        "eval_preset_id": file_eval_preset_id,
        "eval_preset_path": str(eval_preset_path),
        "max_references": ensure_int(data["max_references"], "eval_preset.max_references"),
        "max_related": ensure_int(data["max_related"], "eval_preset.max_related"),
        "max_hard_negatives": ensure_int(
            data["max_hard_negatives"],
            "eval_preset.max_hard_negatives",
        ),
        "top_k": ensure_int(data["top_k"], "eval_preset.top_k"),
        "label_source": ensure_nonempty_str(data["label_source"], "eval_preset.label_source"),
        "refresh": ensure_bool(data["refresh"], "eval_preset.refresh"),
    }


def write_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_seeds_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["doi", "tag"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_cycle_summary_md(path: Path, context: Dict[str, Any]) -> None:
    lines = [
        f"# Seed Cycle Summary: {context['cycle_id']}",
        "",
        f"- seed_set_id: `{context['seed_set_id']}`",
        f"- benchmark_preset_id: `{context['benchmark_preset_id']}`",
        f"- launch_profile_id: `{context['launch_profile_id']}`",
        f"- accepted_baseline_id: `{context['accepted_baseline_id']}`",
        f"- eval_preset_id: `{context['eval_preset_id']}`",
        f"- selected_count: `{context['selected_count']}`",
        "",
        "## Per-tag counts",
        "",
    ]

    for tag, count in context["per_tag_counts"].items():
        lines.append(f"- `{tag}`: `{count}`")

    lines.extend(
        [
            "",
            "## Per-role counts",
            "",
        ]
    )

    for role, count in context["per_role_counts"].items():
        lines.append(f"- `{role}`: `{count}`")

    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()

    seed_set_id = validate_id(args.seed_set_id, "seed_set_id")
    cycle_id = validate_id(args.cycle_id, "cycle_id")
    benchmark_preset_id = validate_id(args.benchmark_preset_id, "benchmark_preset_id")
    launch_profile_id = validate_id(args.launch_profile_id, "launch_profile_id")
    accepted_baseline_id = validate_id(args.accepted_baseline_id, "accepted_baseline_id")
    eval_preset_id = validate_id(args.eval_preset_id, "eval_preset_id")
    created_at = validate_iso_datetime(args.created_at, "created_at")
    description = ensure_nonempty_str(args.description, "description")

    reply_dir = Path(args.reply_dir)
    packet_path = Path(args.packet_yaml)

    if not reply_dir.exists():
        raise SeedCycleMaterializationError(f"Reply dir does not exist: {reply_dir}")
    if not packet_path.exists():
        raise SeedCycleMaterializationError(f"Packet YAML does not exist: {packet_path}")

    packet = load_packet(packet_path)
    ingest_report = load_ingest_report(reply_dir)

    if ingest_report["packet_id"] != packet["packet_id"]:
        raise SeedCycleMaterializationError(
            f"Packet mismatch: ingest_report packet_id={ingest_report['packet_id']} packet packet_id={packet['packet_id']}"
        )

    selected_items = load_selected_candidates(reply_dir, packet, ingest_report)
    selected_items_sorted = sort_selected_candidates(selected_items)

    seeds_rows = [
        {
            "doi": item["doi"],
            "tag": item["final_tag"],
        }
        for item in selected_items_sorted
    ]

    per_tag_counts: Dict[str, int] = {}
    per_role_counts: Dict[str, int] = {}
    for item in selected_items_sorted:
        per_tag_counts[item["final_tag"]] = per_tag_counts.get(item["final_tag"], 0) + 1
        per_role_counts[item["final_role"]] = per_role_counts.get(item["final_role"], 0) + 1

    seed_set_dir = Path("runs/seed_sets") / seed_set_id
    cycle_dir = Path("runs/seed_cycles") / cycle_id
    runtime_seeds_csv = Path("data/benchmarks") / f"{seed_set_id}.csv"
    benchmark_preset_path = Path("configs/presets/benchmarks") / f"{benchmark_preset_id}.json"
    launch_profile_path = Path("configs/presets/launch_profiles") / f"{launch_profile_id}.json"
    canonical_seeds_csv = seed_set_dir / "seeds.csv"
    selected_candidates_copy = seed_set_dir / "selected_candidates.json"
    seed_set_manifest_path = seed_set_dir / "seed_set_manifest.json"
    cycle_context_path = cycle_dir / "cycle_context.json"
    cycle_summary_path = cycle_dir / "cycle_summary.md"

    for output_path in [
        seed_set_dir,
        cycle_dir,
        runtime_seeds_csv,
        benchmark_preset_path,
        launch_profile_path,
    ]:
        ensure_output_path_available(output_path)

    accepted_baseline_dir = Path("runs/accepted_baselines") / accepted_baseline_id
    accepted_theory_snapshot = accepted_baseline_dir / "accepted_theory_snapshot.yaml"
    if not accepted_baseline_dir.exists():
        raise SeedCycleMaterializationError(
            f"Accepted baseline dir does not exist: {accepted_baseline_dir}"
        )
    if not accepted_theory_snapshot.exists():
        raise SeedCycleMaterializationError(
            f"Accepted theory snapshot does not exist: {accepted_theory_snapshot}"
        )

    eval_preset = load_eval_preset(eval_preset_id)

    seed_set_dir.mkdir(parents=True, exist_ok=False)
    cycle_dir.mkdir(parents=True, exist_ok=False)
    runtime_seeds_csv.parent.mkdir(parents=True, exist_ok=True)
    benchmark_preset_path.parent.mkdir(parents=True, exist_ok=True)
    launch_profile_path.parent.mkdir(parents=True, exist_ok=True)

    write_seeds_csv(canonical_seeds_csv, seeds_rows)
    write_seeds_csv(runtime_seeds_csv, seeds_rows)
    write_json(selected_candidates_copy, selected_items_sorted)

    final_tags = sorted(per_tag_counts.keys())
    common_tags = sorted(set(final_tags + ["seed_cycle_materialized"]))

    benchmark_preset = {
        "benchmark_preset_id": benchmark_preset_id,
        "seeds_csv": str(runtime_seeds_csv),
        "created_at": created_at,
        "description": description,
        "tags": common_tags,
    }
    write_json(benchmark_preset_path, benchmark_preset)

    launch_profile = {
        "launch_profile_id": launch_profile_id,
        "created_at": created_at,
        "accepted_baseline_id": accepted_baseline_id,
        "accepted_baseline_dir": str(accepted_baseline_dir),
        "accepted_theory_snapshot": str(accepted_theory_snapshot),
        "benchmark_preset_id": benchmark_preset_id,
        "seeds_csv": str(runtime_seeds_csv),
        "eval_preset_id": eval_preset["eval_preset_id"],
        "max_references": eval_preset["max_references"],
        "max_related": eval_preset["max_related"],
        "max_hard_negatives": eval_preset["max_hard_negatives"],
        "top_k": eval_preset["top_k"],
        "label_source": eval_preset["label_source"],
        "refresh": eval_preset["refresh"],
        "description": description,
        "tags": common_tags,
    }
    write_json(launch_profile_path, launch_profile)

    seed_set_manifest = {
        "seed_set_id": seed_set_id,
        "created_at": created_at,
        "packet_id": packet["packet_id"],
        "reply_id": ingest_report["reply_id"],
        "reviewer_id": ingest_report["reviewer_id"],
        "accepted_baseline_id": accepted_baseline_id,
        "eval_preset_id": eval_preset["eval_preset_id"],
        "selected_count": len(selected_items_sorted),
        "per_tag_counts": dict(sorted(per_tag_counts.items())),
        "per_role_counts": dict(sorted(per_role_counts.items())),
        "input_paths": {
            "packet_yaml": str(packet_path),
            "reply_dir": str(reply_dir),
            "ingest_report_json": ingest_report["ingest_report_path"],
        },
        "output_paths": {
            "canonical_seeds_csv": str(canonical_seeds_csv),
            "runtime_seeds_csv": str(runtime_seeds_csv),
            "selected_candidates_json": str(selected_candidates_copy),
            "seed_set_manifest_json": str(seed_set_manifest_path),
        },
    }
    write_json(seed_set_manifest_path, seed_set_manifest)

    cycle_context = {
        "cycle_id": cycle_id,
        "created_at": created_at,
        "description": description,
        "seed_set_id": seed_set_id,
        "packet_id": packet["packet_id"],
        "reply_id": ingest_report["reply_id"],
        "reviewer_id": ingest_report["reviewer_id"],
        "source_system": packet["source"]["source_system"],
        "source_snapshot_id": packet["source"]["source_snapshot_id"],
        "candidate_pool_id": packet["source"]["candidate_pool_id"],
        "accepted_baseline_id": accepted_baseline_id,
        "accepted_baseline_dir": str(accepted_baseline_dir),
        "accepted_theory_snapshot": str(accepted_theory_snapshot),
        "eval_preset_id": eval_preset["eval_preset_id"],
        "eval_preset_path": eval_preset["eval_preset_path"],
        "benchmark_preset_id": benchmark_preset_id,
        "benchmark_preset_path": str(benchmark_preset_path),
        "launch_profile_id": launch_profile_id,
        "launch_profile_path": str(launch_profile_path),
        "selected_count": len(selected_items_sorted),
        "per_tag_counts": dict(sorted(per_tag_counts.items())),
        "per_role_counts": dict(sorted(per_role_counts.items())),
        "input_paths": {
            "packet_yaml": str(packet_path),
            "reply_dir": str(reply_dir),
            "ingest_report_json": ingest_report["ingest_report_path"],
        },
        "output_paths": {
            "seed_set_manifest_json": str(seed_set_manifest_path),
            "cycle_context_json": str(cycle_context_path),
            "cycle_summary_md": str(cycle_summary_path),
            "benchmark_preset_json": str(benchmark_preset_path),
            "launch_profile_json": str(launch_profile_path),
            "runtime_seeds_csv": str(runtime_seeds_csv),
        },
    }
    write_json(cycle_context_path, cycle_context)
    write_cycle_summary_md(cycle_summary_path, cycle_context)

    print("Seed cycle materialized successfully.")
    print(f"seed_set_id: {seed_set_id}")
    print(f"cycle_id: {cycle_id}")
    print(f"benchmark_preset_id: {benchmark_preset_id}")
    print(f"launch_profile_id: {launch_profile_id}")
    print(f"selected_count: {len(selected_items_sorted)}")
    print(f"runtime_seeds_csv: {runtime_seeds_csv}")
    print(f"benchmark_preset_json: {benchmark_preset_path}")
    print(f"launch_profile_json: {launch_profile_path}")
    print(f"cycle_context_json: {cycle_context_path}")


if __name__ == "__main__":
    try:
        main()
    except SeedCycleMaterializationError as exc:
        raise SystemExit(f"ERROR: {exc}")
