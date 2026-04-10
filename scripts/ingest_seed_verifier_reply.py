#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

TAG_RE = re.compile(r"^[a-z0-9_]+$")
CANDIDATE_ID_RE = re.compile(r"^C[0-9]{4,}$")
DOI_RE = re.compile(r"^10\..+/.+$")
VALID_ROLES = {"anchor", "boundary", "sentinel"}


class ReplyIngestError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and ingest an external seed-selection verifier reply."
    )
    parser.add_argument(
        "--packet-yaml",
        required=True,
        help="Path to seed_selection_review_packet.yaml",
    )
    parser.add_argument(
        "--verifier-reply-yaml",
        required=True,
        help="Path to the external verifier reply YAML.",
    )
    parser.add_argument(
        "--policy",
        default="configs/presets/seed_policies/seed_selection_policy_v1.yaml",
        help="Path to the seed-selection policy YAML.",
    )
    parser.add_argument(
        "--reply-id",
        required=True,
        help="Logical reply identifier, e.g. seed_reply_001",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for reply-side artifacts.",
    )
    return parser.parse_args()


def validate_iso_datetime(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplyIngestError(f"{field_name} must be a non-empty ISO-8601 string")
    normalized = value.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ReplyIngestError(f"Invalid ISO-8601 datetime for {field_name}: {value}") from exc
    return value


def ensure_nonempty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplyIngestError(f"{field_name} must be a non-empty string")
    return value.strip()


def ensure_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ReplyIngestError(f"{field_name} must be a boolean")
    return value


def ensure_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReplyIngestError(f"{field_name} must be an integer")
    return value


def ensure_float_01(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReplyIngestError(f"{field_name} must be a number in [0,1]")
    result = float(value)
    if not (0.0 <= result <= 1.0):
        raise ReplyIngestError(f"{field_name} must be in [0,1]")
    return result


def ensure_list(value: Any, field_name: str) -> List[Any]:
    if not isinstance(value, list):
        raise ReplyIngestError(f"{field_name} must be a list")
    return value


def ensure_dict(value: Any, field_name: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ReplyIngestError(f"{field_name} must be an object")
    return value


def validate_tag(value: Any, field_name: str) -> str:
    tag = ensure_nonempty_str(value, field_name)
    if not TAG_RE.match(tag):
        raise ReplyIngestError(f"{field_name} must match ^[a-z0-9_]+$: {tag}")
    return tag


def validate_candidate_id(value: Any, field_name: str) -> str:
    candidate_id = ensure_nonempty_str(value, field_name)
    if not CANDIDATE_ID_RE.match(candidate_id):
        raise ReplyIngestError(f"{field_name} must match ^C[0-9]{{4,}}$: {candidate_id}")
    return candidate_id


def validate_doi(value: Any, field_name: str) -> str:
    doi = ensure_nonempty_str(value, field_name)
    if not DOI_RE.match(doi):
        raise ReplyIngestError(f"{field_name} must look like a DOI: {doi}")
    return doi


def validate_role(value: Any, field_name: str) -> str:
    role = ensure_nonempty_str(value, field_name)
    if role not in VALID_ROLES:
        raise ReplyIngestError(f"{field_name} must be one of {sorted(VALID_ROLES)}")
    return role


def load_yaml_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ReplyIngestError(f"Expected YAML object in {path}")
    return data


def load_policy(path: Path) -> Dict[str, Any]:
    data = load_yaml_object(path)

    reason_catalog = ensure_dict(data.get("reason_code_catalog"), "reason_code_catalog")
    keep_reason_codes = {
        ensure_nonempty_str(item, "reason_code_catalog.keep item")
        for item in ensure_list(reason_catalog.get("keep"), "reason_code_catalog.keep")
    }
    reject_reason_codes = {
        ensure_nonempty_str(item, "reason_code_catalog.reject item")
        for item in ensure_list(reason_catalog.get("reject"), "reason_code_catalog.reject")
    }

    reply_status_catalog = {
        ensure_nonempty_str(item, "reply_status_catalog item")
        for item in ensure_list(data.get("reply_status_catalog"), "reply_status_catalog")
    }

    selection_rules = ensure_dict(data.get("selection_rules"), "selection_rules")
    require_selected_from_packet_only = ensure_bool(
        selection_rules.get("require_selected_from_packet_only"),
        "selection_rules.require_selected_from_packet_only",
    )
    no_duplicate_cluster_overflow = ensure_bool(
        selection_rules.get("no_duplicate_cluster_overflow"),
        "selection_rules.no_duplicate_cluster_overflow",
    )

    return {
        "keep_reason_codes": keep_reason_codes,
        "reject_reason_codes": reject_reason_codes,
        "reply_status_catalog": reply_status_catalog,
        "require_selected_from_packet_only": require_selected_from_packet_only,
        "no_duplicate_cluster_overflow": no_duplicate_cluster_overflow,
        "policy_path": str(path),
    }


def load_packet(path: Path) -> Dict[str, Any]:
    data = load_yaml_object(path)

    packet_type = ensure_nonempty_str(data.get("packet_type"), "packet_type")
    if packet_type != "seed_selection_review_packet":
        raise ReplyIngestError(f"Unexpected packet_type: {packet_type}")

    schema_version = ensure_int(data.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise ReplyIngestError(f"Unsupported packet schema_version: {schema_version}")

    packet_id = ensure_nonempty_str(data.get("packet_id"), "packet_id")
    validate_iso_datetime(data.get("created_at"), "created_at")

    selection_policy = ensure_dict(data.get("selection_policy"), "selection_policy")
    final_target_total = ensure_int(
        selection_policy.get("final_target_total"),
        "selection_policy.final_target_total",
    )
    min_total = ensure_int(selection_policy.get("min_total"), "selection_policy.min_total")
    max_total = ensure_int(selection_policy.get("max_total"), "selection_policy.max_total")
    allow_only_candidate_ids = ensure_bool(
        selection_policy.get("allow_only_candidate_ids"),
        "selection_policy.allow_only_candidate_ids",
    )
    allow_freeform_doi_additions = ensure_bool(
        selection_policy.get("allow_freeform_doi_additions"),
        "selection_policy.allow_freeform_doi_additions",
    )
    if min_total < 0 or max_total < min_total:
        raise ReplyIngestError("selection_policy must satisfy 0 <= min_total <= max_total")

    tag_targets_raw = ensure_list(data.get("tag_targets"), "tag_targets")
    tag_targets: Dict[str, Dict[str, int]] = {}
    for index, item in enumerate(tag_targets_raw):
        entry = ensure_dict(item, f"tag_targets[{index}]")
        tag = validate_tag(entry.get("tag"), f"tag_targets[{index}].tag")
        min_count = ensure_int(entry.get("min"), f"tag_targets[{index}].min")
        max_count = ensure_int(entry.get("max"), f"tag_targets[{index}].max")
        if min_count < 0 or max_count < min_count:
            raise ReplyIngestError(
                f"tag_targets[{index}] must satisfy 0 <= min <= max"
            )
        if tag in tag_targets:
            raise ReplyIngestError(f"Duplicate tag target in packet: {tag}")
        tag_targets[tag] = {"min": min_count, "max": max_count}

    role_targets_raw = ensure_dict(data.get("role_targets"), "role_targets")
    role_targets = {
        "anchor_min": ensure_int(role_targets_raw.get("anchor_min"), "role_targets.anchor_min"),
        "boundary_min": ensure_int(role_targets_raw.get("boundary_min"), "role_targets.boundary_min"),
        "sentinel_min": ensure_int(role_targets_raw.get("sentinel_min"), "role_targets.sentinel_min"),
    }

    hard_rules_raw = ensure_dict(data.get("hard_rules"), "hard_rules")
    hard_rules = {
        "require_openalex_resolved": ensure_bool(
            hard_rules_raw.get("require_openalex_resolved"),
            "hard_rules.require_openalex_resolved",
        ),
        "reject_duplicate_cluster_overflow": ensure_bool(
            hard_rules_raw.get("reject_duplicate_cluster_overflow"),
            "hard_rules.reject_duplicate_cluster_overflow",
        ),
    }

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

    candidates_raw = ensure_list(data.get("candidates"), "candidates")
    if not candidates_raw:
        raise ReplyIngestError("Packet candidates list may not be empty")

    candidate_by_id: Dict[str, Dict[str, Any]] = {}
    doi_to_candidate: Dict[str, str] = {}

    for index, item in enumerate(candidates_raw):
        entry = ensure_dict(item, f"candidates[{index}]")
        candidate_id = validate_candidate_id(entry.get("candidate_id"), f"candidates[{index}].candidate_id")
        doi = validate_doi(entry.get("doi"), f"candidates[{index}].doi")
        title = ensure_nonempty_str(entry.get("title"), f"candidates[{index}].title")
        proposed_tag = validate_tag(entry.get("proposed_tag"), f"candidates[{index}].proposed_tag")
        if proposed_tag not in tag_targets:
            raise ReplyIngestError(
                f"candidates[{index}].proposed_tag is not present in tag_targets: {proposed_tag}"
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
            raise ReplyIngestError(f"Duplicate packet candidate_id: {candidate_id}")
        if doi in doi_to_candidate:
            raise ReplyIngestError(f"Duplicate packet DOI: {doi}")

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
        doi_to_candidate[doi] = candidate_id

    return {
        "packet_id": packet_id,
        "packet_path": str(path),
        "selection_policy": {
            "final_target_total": final_target_total,
            "min_total": min_total,
            "max_total": max_total,
            "allow_only_candidate_ids": allow_only_candidate_ids,
            "allow_freeform_doi_additions": allow_freeform_doi_additions,
        },
        "tag_targets": tag_targets,
        "role_targets": role_targets,
        "hard_rules": hard_rules,
        "source": source,
        "candidate_by_id": candidate_by_id,
        "doi_to_candidate": doi_to_candidate,
    }


def validate_reason_codes(
    raw_codes: Any,
    field_name: str,
    allowed_codes: set[str],
) -> List[str]:
    codes_raw = ensure_list(raw_codes, field_name)
    if not codes_raw:
        raise ReplyIngestError(f"{field_name} must not be empty")
    codes: List[str] = []
    seen = set()
    for index, value in enumerate(codes_raw):
        code = ensure_nonempty_str(value, f"{field_name}[{index}]")
        if not TAG_RE.match(code):
            raise ReplyIngestError(f"{field_name}[{index}] must match ^[a-z0-9_]+$: {code}")
        if code not in allowed_codes:
            raise ReplyIngestError(f"{field_name}[{index}] is not allowed by policy: {code}")
        if code in seen:
            raise ReplyIngestError(f"{field_name} contains duplicate code: {code}")
        seen.add(code)
        codes.append(code)
    return codes


def validate_reply(
    reply_path: Path,
    policy: Dict[str, Any],
    packet: Dict[str, Any],
) -> Dict[str, Any]:
    data = load_yaml_object(reply_path)

    reply_type = ensure_nonempty_str(data.get("reply_type"), "reply_type")
    if reply_type != "seed_selection_verifier_reply":
        raise ReplyIngestError(f"Unexpected reply_type: {reply_type}")

    schema_version = ensure_int(data.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise ReplyIngestError(f"Unsupported reply schema_version: {schema_version}")

    packet_id = ensure_nonempty_str(data.get("packet_id"), "packet_id")
    if packet_id != packet["packet_id"]:
        raise ReplyIngestError(
            f"Reply packet_id {packet_id} does not match packet packet_id {packet['packet_id']}"
        )

    reviewer_id = ensure_nonempty_str(data.get("reviewer_id"), "reviewer_id")
    completed_at = validate_iso_datetime(data.get("completed_at"), "completed_at")
    status = ensure_nonempty_str(data.get("status"), "status")
    if status not in policy["reply_status_catalog"]:
        raise ReplyIngestError(f"Reply status is not allowed by policy: {status}")

    selected_raw = ensure_list(data.get("selected_seeds"), "selected_seeds")
    rejected_raw = ensure_list(data.get("rejected_candidates"), "rejected_candidates")
    expansion_raw = ensure_list(data.get("expansion_requests"), "expansion_requests")
    summary_raw = ensure_dict(data.get("summary"), "summary")

    seen_candidate_ids = set()
    selected_candidates: List[Dict[str, Any]] = []
    rejected_candidates: List[Dict[str, Any]] = []
    expansion_requests: List[Dict[str, Any]] = []
    per_tag_counts: Counter[str] = Counter()
    per_role_counts: Counter[str] = Counter()
    duplicate_cluster_counts: Counter[str] = Counter()

    for index, item in enumerate(selected_raw):
        entry = ensure_dict(item, f"selected_seeds[{index}]")
        candidate_id = validate_candidate_id(
            entry.get("candidate_id"),
            f"selected_seeds[{index}].candidate_id",
        )
        if candidate_id in seen_candidate_ids:
            raise ReplyIngestError(f"Duplicate candidate_id in reply: {candidate_id}")
        if candidate_id not in packet["candidate_by_id"]:
            raise ReplyIngestError(
                f"selected_seeds[{index}].candidate_id is not present in packet: {candidate_id}"
            )
        seen_candidate_ids.add(candidate_id)

        final_tag = validate_tag(entry.get("final_tag"), f"selected_seeds[{index}].final_tag")
        if final_tag not in packet["tag_targets"]:
            raise ReplyIngestError(
                f"selected_seeds[{index}].final_tag is not in packet tag_targets: {final_tag}"
            )

        final_role = validate_role(entry.get("final_role"), f"selected_seeds[{index}].final_role")
        confidence = ensure_float_01(entry.get("confidence"), f"selected_seeds[{index}].confidence")
        reason_codes = validate_reason_codes(
            entry.get("reason_codes"),
            f"selected_seeds[{index}].reason_codes",
            policy["keep_reason_codes"],
        )

        candidate_meta = packet["candidate_by_id"][candidate_id]
        merged = dict(candidate_meta)
        merged.update(
            {
                "final_tag": final_tag,
                "final_role": final_role,
                "confidence": confidence,
                "reason_codes": reason_codes,
            }
        )
        selected_candidates.append(merged)

        per_tag_counts[final_tag] += 1
        per_role_counts[final_role] += 1
        duplicate_cluster_counts[candidate_meta["duplicate_cluster_id"]] += 1

    for index, item in enumerate(rejected_raw):
        entry = ensure_dict(item, f"rejected_candidates[{index}]")
        candidate_id = validate_candidate_id(
            entry.get("candidate_id"),
            f"rejected_candidates[{index}].candidate_id",
        )
        if candidate_id in seen_candidate_ids:
            raise ReplyIngestError(f"Duplicate candidate_id in reply: {candidate_id}")
        if candidate_id not in packet["candidate_by_id"]:
            raise ReplyIngestError(
                f"rejected_candidates[{index}].candidate_id is not present in packet: {candidate_id}"
            )
        seen_candidate_ids.add(candidate_id)

        confidence = ensure_float_01(
            entry.get("confidence"),
            f"rejected_candidates[{index}].confidence",
        )
        reason_codes = validate_reason_codes(
            entry.get("reason_codes"),
            f"rejected_candidates[{index}].reason_codes",
            policy["reject_reason_codes"],
        )

        candidate_meta = packet["candidate_by_id"][candidate_id]
        merged = dict(candidate_meta)
        merged.update(
            {
                "confidence": confidence,
                "reason_codes": reason_codes,
            }
        )
        rejected_candidates.append(merged)

    for index, item in enumerate(expansion_raw):
        entry = ensure_dict(item, f"expansion_requests[{index}]")
        tag = validate_tag(entry.get("tag"), f"expansion_requests[{index}].tag")
        if tag not in packet["tag_targets"]:
            raise ReplyIngestError(
                f"expansion_requests[{index}].tag is not in packet tag_targets: {tag}"
            )
        needed_count = ensure_int(
            entry.get("needed_count"),
            f"expansion_requests[{index}].needed_count",
        )
        if needed_count < 1:
            raise ReplyIngestError(
                f"expansion_requests[{index}].needed_count must be >= 1"
            )
        desired_roles_raw = ensure_list(
            entry.get("desired_roles"),
            f"expansion_requests[{index}].desired_roles",
        )
        if not desired_roles_raw:
            raise ReplyIngestError(
                f"expansion_requests[{index}].desired_roles must not be empty"
            )
        desired_roles: List[str] = []
        seen_roles = set()
        for role_index, role_value in enumerate(desired_roles_raw):
            role = validate_role(
                role_value,
                f"expansion_requests[{index}].desired_roles[{role_index}]",
            )
            if role in seen_roles:
                raise ReplyIngestError(
                    f"expansion_requests[{index}].desired_roles contains duplicate role: {role}"
                )
            seen_roles.add(role)
            desired_roles.append(role)

        reason = ensure_nonempty_str(entry.get("reason"), f"expansion_requests[{index}].reason")
        expansion_requests.append(
            {
                "tag": tag,
                "needed_count": needed_count,
                "desired_roles": desired_roles,
                "reason": reason,
            }
        )

    accepted_count = ensure_int(summary_raw.get("accepted_count"), "summary.accepted_count")
    rejected_count = ensure_int(summary_raw.get("rejected_count"), "summary.rejected_count")
    notes = ensure_nonempty_str(summary_raw.get("notes"), "summary.notes")

    if accepted_count != len(selected_candidates):
        raise ReplyIngestError(
            f"summary.accepted_count={accepted_count} does not match selected_seeds count={len(selected_candidates)}"
        )
    if rejected_count != len(rejected_candidates):
        raise ReplyIngestError(
            f"summary.rejected_count={rejected_count} does not match rejected_candidates count={len(rejected_candidates)}"
        )

    if policy["require_selected_from_packet_only"] and len(selected_candidates) == 0 and status == "ready_for_cycle":
        raise ReplyIngestError("status=ready_for_cycle requires at least one selected seed")

    if packet["hard_rules"]["reject_duplicate_cluster_overflow"] or policy["no_duplicate_cluster_overflow"]:
        overflow_clusters = {
            cluster_id: count
            for cluster_id, count in duplicate_cluster_counts.items()
            if count > 1
        }
        if overflow_clusters:
            raise ReplyIngestError(
                f"Duplicate cluster overflow in selected seeds: {overflow_clusters}"
            )

    min_total = packet["selection_policy"]["min_total"]
    max_total = packet["selection_policy"]["max_total"]
    role_targets = packet["role_targets"]

    if status == "ready_for_cycle":
        if expansion_requests:
            raise ReplyIngestError(
                "status=ready_for_cycle requires expansion_requests to be empty"
            )
        if not (min_total <= len(selected_candidates) <= max_total):
            raise ReplyIngestError(
                f"selected_seeds count {len(selected_candidates)} is outside [{min_total}, {max_total}]"
            )

        for tag, limits in packet["tag_targets"].items():
            count = per_tag_counts.get(tag, 0)
            if count < limits["min"] or count > limits["max"]:
                raise ReplyIngestError(
                    f"Selected count for tag '{tag}' is {count}, outside [{limits['min']}, {limits['max']}]"
                )

        if per_role_counts.get("anchor", 0) < role_targets["anchor_min"]:
            raise ReplyIngestError("Selected seeds do not satisfy role_targets.anchor_min")
        if per_role_counts.get("boundary", 0) < role_targets["boundary_min"]:
            raise ReplyIngestError("Selected seeds do not satisfy role_targets.boundary_min")
        if per_role_counts.get("sentinel", 0) < role_targets["sentinel_min"]:
            raise ReplyIngestError("Selected seeds do not satisfy role_targets.sentinel_min")

    elif status == "needs_expansion":
        if not expansion_requests:
            raise ReplyIngestError(
                "status=needs_expansion requires at least one expansion request"
            )
        if len(selected_candidates) > max_total:
            raise ReplyIngestError(
                f"selected_seeds count {len(selected_candidates)} exceeds max_total={max_total}"
            )

    elif status == "reject_packet":
        if selected_candidates:
            raise ReplyIngestError(
                "status=reject_packet requires selected_seeds to be empty"
            )

    return {
        "reply_path": str(reply_path),
        "packet_id": packet_id,
        "reviewer_id": reviewer_id,
        "completed_at": completed_at,
        "status": status,
        "selected_candidates": selected_candidates,
        "rejected_candidates": rejected_candidates,
        "expansion_requests": expansion_requests,
        "summary": {
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "notes": notes,
        },
        "per_tag_counts": dict(sorted(per_tag_counts.items())),
        "per_role_counts": dict(sorted(per_role_counts.items())),
    }


def write_json(path: Path, data: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_summary_md(path: Path, report: Dict[str, Any]) -> None:
    lines = [
        f"# Seed Verifier Reply Ingest Summary: {report['reply_id']}",
        "",
        f"- packet_id: `{report['packet_id']}`",
        f"- verifier_reply_yaml: `{report['verifier_reply_yaml']}`",
        f"- packet_yaml: `{report['packet_yaml']}`",
        f"- policy_path: `{report['policy_path']}`",
        f"- validation_status: `{report['validation_status']}`",
        f"- reply_status: `{report['reply_status']}`",
        f"- selected_count: `{report['selected_count']}`",
        f"- rejected_count: `{report['rejected_count']}`",
        f"- expansion_request_count: `{report['expansion_request_count']}`",
        "",
        "## Per-tag selected counts",
        "",
    ]

    if report["per_tag_selected_counts"]:
        for tag, count in report["per_tag_selected_counts"].items():
            lines.append(f"- `{tag}`: `{count}`")
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Per-role selected counts",
            "",
        ]
    )

    if report["per_role_selected_counts"]:
        for role, count in report["per_role_selected_counts"].items():
            lines.append(f"- `{role}`: `{count}`")
    else:
        lines.append("- none")

    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()

    packet_path = Path(args.packet_yaml)
    reply_path = Path(args.verifier_reply_yaml)
    policy_path = Path(args.policy)
    out_dir = Path(args.out_dir)

    if not packet_path.exists():
        raise ReplyIngestError(f"Packet YAML does not exist: {packet_path}")
    if not reply_path.exists():
        raise ReplyIngestError(f"Verifier reply YAML does not exist: {reply_path}")
    if not policy_path.exists():
        raise ReplyIngestError(f"Policy YAML does not exist: {policy_path}")

    policy = load_policy(policy_path)
    packet = load_packet(packet_path)
    reply = validate_reply(reply_path, policy, packet)

    out_dir.mkdir(parents=True, exist_ok=True)

    raw_reply_copy = out_dir / "raw_verifier_reply.yaml"
    selected_json = out_dir / "selected_candidates.json"
    rejected_json = out_dir / "rejected_candidates.json"
    expansion_json = out_dir / "expansion_requests.json"
    ingest_report_json = out_dir / "ingest_report.json"
    ingest_summary_md = out_dir / "ingest_summary.md"

    shutil.copyfile(reply_path, raw_reply_copy)
    write_json(selected_json, reply["selected_candidates"])
    write_json(rejected_json, reply["rejected_candidates"])
    write_json(expansion_json, reply["expansion_requests"])

    ingest_report = {
        "reply_id": args.reply_id,
        "packet_id": packet["packet_id"],
        "packet_yaml": str(packet_path),
        "verifier_reply_yaml": str(reply_path),
        "policy_path": str(policy_path),
        "reviewer_id": reply["reviewer_id"],
        "completed_at": reply["completed_at"],
        "reply_status": reply["status"],
        "validation_status": "accepted",
        "selected_count": len(reply["selected_candidates"]),
        "rejected_count": len(reply["rejected_candidates"]),
        "expansion_request_count": len(reply["expansion_requests"]),
        "per_tag_selected_counts": reply["per_tag_counts"],
        "per_role_selected_counts": reply["per_role_counts"],
        "summary": reply["summary"],
        "output_paths": {
            "raw_verifier_reply_yaml": str(raw_reply_copy),
            "selected_candidates_json": str(selected_json),
            "rejected_candidates_json": str(rejected_json),
            "expansion_requests_json": str(expansion_json),
            "ingest_report_json": str(ingest_report_json),
            "ingest_summary_md": str(ingest_summary_md),
        },
    }

    write_json(ingest_report_json, ingest_report)
    write_summary_md(ingest_summary_md, ingest_report)

    print("Seed verifier reply ingested successfully.")
    print(f"reply_id: {args.reply_id}")
    print(f"packet_id: {packet['packet_id']}")
    print(f"reply_status: {reply['status']}")
    print(f"selected_count: {len(reply['selected_candidates'])}")
    print(f"rejected_count: {len(reply['rejected_candidates'])}")
    print(f"expansion_request_count: {len(reply['expansion_requests'])}")
    print(f"output_dir: {out_dir}")


if __name__ == "__main__":
    try:
        main()
    except ReplyIngestError as exc:
        raise SystemExit(f"ERROR: {exc}")
