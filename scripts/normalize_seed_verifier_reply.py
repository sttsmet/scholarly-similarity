#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import yaml


TOP_LEVEL_KEYS = [
    "reply_type",
    "schema_version",
    "packet_id",
    "reviewer_id",
    "completed_at",
    "status",
    "selected_seeds",
    "rejected_candidates",
    "expansion_requests",
    "summary",
]

TOP_LEVEL_KEY_RE = re.compile(
    r"^(reply_type|schema_version|packet_id|reviewer_id|completed_at|status|selected_seeds|rejected_candidates|expansion_requests|summary):"
)
CANDIDATE_ID_RE = re.compile(r"^C[0-9]{4,}$")
VALID_ROLES = {"anchor", "boundary", "sentinel"}


class NormalizeReplyError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize and safely repair an external AI seed verifier reply before strict ingest."
    )
    parser.add_argument("--raw-reply", required=True, help="Path to raw external verifier reply.")
    parser.add_argument(
        "--normalized-reply",
        required=True,
        help="Path to write normalized canonical YAML reply.",
    )
    parser.add_argument(
        "--report-json",
        required=True,
        help="Path to write normalization report JSON.",
    )
    return parser.parse_args()


def ensure_nonempty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise NormalizeReplyError(f"{field_name} must be a non-empty string")
    return value.strip()


def normalize_candidate_id(value: Any, field_name: str) -> str:
    candidate_id = ensure_nonempty_str(value, field_name)
    if not CANDIDATE_ID_RE.match(candidate_id):
        raise NormalizeReplyError(
            f"{field_name} must match ^C[0-9]{{4,}}$: {candidate_id}"
        )
    return candidate_id


def normalize_float_01(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise NormalizeReplyError(f"{field_name} must be numeric in [0,1]")
    if not isinstance(value, (int, float)):
        raise NormalizeReplyError(f"{field_name} must be numeric in [0,1]")
    result = float(value)
    if not (0.0 <= result <= 1.0):
        raise NormalizeReplyError(f"{field_name} must be in [0,1]")
    return result


def normalize_role(value: Any, field_name: str) -> str:
    role = ensure_nonempty_str(value, field_name)
    if role not in VALID_ROLES:
        raise NormalizeReplyError(
            f"{field_name} must be one of {sorted(VALID_ROLES)}"
        )
    return role


def normalize_reason_codes(value: Any, field_name: str) -> List[str]:
    if not isinstance(value, list) or not value:
        raise NormalizeReplyError(f"{field_name} must be a non-empty list")
    result: List[str] = []
    seen = set()
    for index, item in enumerate(value):
        code = ensure_nonempty_str(item, f"{field_name}[{index}]")
        if code in seen:
            raise NormalizeReplyError(f"{field_name} contains duplicate code: {code}")
        seen.add(code)
        result.append(code)
    return result


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_yaml(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(data, handle, sort_keys=False, allow_unicode=True)


def read_text(path: Path) -> str:
    with path.open("r", encoding="utf-8") as handle:
        return handle.read()


def strip_markdown_fences(text: str, repairs: List[str]) -> str:
    fenced = re.search(r"```(?:yaml|yml)?\s*(.*?)```", text, re.IGNORECASE | re.DOTALL)
    if fenced:
        repairs.append("stripped_markdown_fences")
        return fenced.group(1).strip() + "\n"

    lines = text.splitlines()
    filtered = [line for line in lines if not line.strip().startswith("```")]
    if len(filtered) != len(lines):
        repairs.append("removed_fence_marker_lines")
    return "\n".join(filtered).strip() + "\n"


def trim_prefix_before_yaml(text: str, repairs: List[str]) -> str:
    lines = text.splitlines()
    start_index = None
    for i, line in enumerate(lines):
        if TOP_LEVEL_KEY_RE.match(line.strip()):
            start_index = i
            break
    if start_index is None:
        raise NormalizeReplyError("Could not find YAML start in raw reply")
    if start_index > 0:
        repairs.append("trimmed_prefix_before_yaml")
    return "\n".join(lines[start_index:]).strip() + "\n"


def strip_trailing_eof_marker(text: str, repairs: List[str]) -> str:
    lines = text.splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    if lines and lines[-1].strip() == "EOF":
        lines.pop()
        repairs.append("stripped_trailing_eof_marker")
    return "\n".join(lines) + "\n"


def detect_flat_candidate_blocks(text: str) -> bool:
    for section in ("selected_seeds", "rejected_candidates"):
        pattern = re.compile(
            rf"(?ms)^{section}:\s*\n(?:\s*\n)*candidate_id\s*:"
        )
        if pattern.search(text):
            return True
    return False


def repair_list_sections(text: str, repairs: List[str]) -> str:
    lines = text.splitlines()
    out: List[str] = []
    current_section: str | None = None
    changed = False

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        if not stripped:
            out.append("")
            continue

        if TOP_LEVEL_KEY_RE.match(stripped) and not line.startswith(" "):
            key = stripped.split(":", 1)[0]
            if key in {"selected_seeds", "rejected_candidates"}:
                if stripped.endswith("[]"):
                    current_section = None
                else:
                    current_section = key
                out.append(stripped)
                continue

            current_section = None
            out.append(stripped)
            continue

        if current_section in {"selected_seeds", "rejected_candidates"}:
            if not line.startswith(" ") and stripped.startswith("candidate_id:"):
                out.append(f"  - {stripped}")
                changed = True
                continue

            if line.startswith("  - ") or line.startswith("    "):
                out.append(line)
                continue

            out.append(f"    {stripped}")
            changed = True
            continue

        out.append(line)

    if changed:
        repairs.append("repaired_flat_candidate_list_sections")
    return "\n".join(out).strip() + "\n"


def repair_summary_section(text: str, repairs: List[str]) -> str:
    lines = text.splitlines()
    out: List[str] = []
    in_summary = False
    notes_block_active = False
    changed = False

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        if not line.startswith(" ") and TOP_LEVEL_KEY_RE.match(stripped):
            key = stripped.split(":", 1)[0]
            if key == "summary" and stripped == "summary:":
                in_summary = True
                notes_block_active = False
                out.append(line)
                continue

            if in_summary:
                in_summary = False
                notes_block_active = False
            out.append(line)
            continue

        if not in_summary:
            out.append(line)
            continue

        if not stripped:
            out.append("")
            continue

        if not line.startswith(" "):
            if re.match(r"^(accepted_count|rejected_count|notes):", stripped):
                out.append(f"  {stripped}")
                changed = True
                notes_value = stripped.split(":", 1)[1].strip()
                notes_block_active = stripped.startswith("notes:") and (
                    notes_value.startswith(">") or notes_value.startswith("|")
                )
                continue

            if notes_block_active:
                out.append(f"    {stripped}")
                changed = True
                continue

            out.append(line)
            continue

        out.append(line)
        summary_child = stripped
        if re.match(r"^(accepted_count|rejected_count|notes):", summary_child):
            notes_value = summary_child.split(":", 1)[1].strip()
            notes_block_active = summary_child.startswith("notes:") and (
                notes_value.startswith(">") or notes_value.startswith("|")
            )

    if changed:
        repairs.append("repaired_summary_section_indentation")
    return "\n".join(out) + "\n"


def parse_yaml_document(text: str) -> Dict[str, Any]:
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise NormalizeReplyError("Normalized YAML must parse to an object")
    return data


def is_placeholder_rejected_duplicate(item: Dict[str, Any]) -> bool:
    reason_codes = item.get("reason_codes")
    confidence = item.get("confidence")
    if not isinstance(reason_codes, list) or len(reason_codes) == 0:
        return True
    if confidence in (None, "", 0, 0.0):
        return True
    return False


def canonicalize_reply(
    data: Dict[str, Any],
    repairs: List[str],
) -> tuple[Dict[str, Any], int, int, int]:
    reply_type = ensure_nonempty_str(data.get("reply_type"), "reply_type")
    schema_version = data.get("schema_version")
    if schema_version != 1:
        raise NormalizeReplyError("schema_version must be 1")
    packet_id = ensure_nonempty_str(data.get("packet_id"), "packet_id")
    reviewer_id = ensure_nonempty_str(data.get("reviewer_id"), "reviewer_id")
    completed_at = ensure_nonempty_str(data.get("completed_at"), "completed_at")
    status = ensure_nonempty_str(data.get("status"), "status")

    selected_raw = data.get("selected_seeds")
    if selected_raw is None:
        selected_raw = []
        repairs.append("filled_missing_selected_seeds")
    if not isinstance(selected_raw, list):
        raise NormalizeReplyError("selected_seeds must be a list")

    rejected_raw = data.get("rejected_candidates")
    if rejected_raw is None:
        rejected_raw = []
        repairs.append("filled_missing_rejected_candidates")
    if not isinstance(rejected_raw, list):
        raise NormalizeReplyError("rejected_candidates must be a list")

    expansion_raw = data.get("expansion_requests")
    if expansion_raw is None:
        expansion_raw = []
        repairs.append("filled_missing_expansion_requests")
    if not isinstance(expansion_raw, list):
        raise NormalizeReplyError("expansion_requests must be a list")

    summary_raw = data.get("summary")
    if summary_raw is None:
        summary_raw = {}
        repairs.append("filled_missing_summary")
    if not isinstance(summary_raw, dict):
        raise NormalizeReplyError("summary must be an object")

    normalized_selected: List[Dict[str, Any]] = []
    normalized_rejected: List[Dict[str, Any]] = []
    seen_selected = set()
    seen_rejected = set()
    duplicate_conflicts_found = 0

    for index, item in enumerate(selected_raw):
        if not isinstance(item, dict):
            raise NormalizeReplyError(f"selected_seeds[{index}] must be an object")

        candidate_id = normalize_candidate_id(
            item.get("candidate_id"),
            f"selected_seeds[{index}].candidate_id",
        )
        if candidate_id in seen_selected:
            raise NormalizeReplyError(
                f"Duplicate selected candidate_id: {candidate_id}"
            )
        seen_selected.add(candidate_id)

        final_tag = ensure_nonempty_str(
            item.get("final_tag"),
            f"selected_seeds[{index}].final_tag",
        )
        final_role = normalize_role(
            item.get("final_role"),
            f"selected_seeds[{index}].final_role",
        )
        confidence = normalize_float_01(
            item.get("confidence"),
            f"selected_seeds[{index}].confidence",
        )
        reason_codes = normalize_reason_codes(
            item.get("reason_codes"),
            f"selected_seeds[{index}].reason_codes",
        )

        normalized_selected.append(
            {
                "candidate_id": candidate_id,
                "final_tag": final_tag,
                "final_role": final_role,
                "confidence": confidence,
                "reason_codes": reason_codes,
            }
        )

    for index, item in enumerate(rejected_raw):
        if not isinstance(item, dict):
            raise NormalizeReplyError(f"rejected_candidates[{index}] must be an object")

        candidate_id = normalize_candidate_id(
            item.get("candidate_id"),
            f"rejected_candidates[{index}].candidate_id",
        )

        if candidate_id in seen_selected:
            duplicate_conflicts_found += 1
            if is_placeholder_rejected_duplicate(item):
                repairs.append(f"dropped_placeholder_rejected_duplicate:{candidate_id}")
                continue
            raise NormalizeReplyError(
                f"Ambiguous duplicate candidate_id appears in both selected and rejected: {candidate_id}"
            )

        if candidate_id in seen_rejected:
            raise NormalizeReplyError(
                f"Duplicate rejected candidate_id: {candidate_id}"
            )
        seen_rejected.add(candidate_id)

        confidence = normalize_float_01(
            item.get("confidence"),
            f"rejected_candidates[{index}].confidence",
        )
        reason_codes = normalize_reason_codes(
            item.get("reason_codes"),
            f"rejected_candidates[{index}].reason_codes",
        )

        normalized_rejected.append(
            {
                "candidate_id": candidate_id,
                "confidence": confidence,
                "reason_codes": reason_codes,
            }
        )

    notes = summary_raw.get("notes", "")
    if notes is None:
        notes = ""
    if not isinstance(notes, str):
        notes = str(notes)
        repairs.append("coerced_summary_notes_to_string")

    accepted_count = len(normalized_selected)
    rejected_count = len(normalized_rejected)

    if summary_raw.get("accepted_count") != accepted_count:
        repairs.append("recomputed_summary_accepted_count")
    if summary_raw.get("rejected_count") != rejected_count:
        repairs.append("recomputed_summary_rejected_count")

    normalized = {
        "reply_type": reply_type,
        "schema_version": 1,
        "packet_id": packet_id,
        "reviewer_id": reviewer_id,
        "completed_at": completed_at,
        "status": status,
        "selected_seeds": normalized_selected,
        "rejected_candidates": normalized_rejected,
        "expansion_requests": expansion_raw,
        "summary": {
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "notes": notes,
        },
    }

    return normalized, accepted_count, rejected_count, duplicate_conflicts_found


def main() -> None:
    args = parse_args()

    raw_reply_path = Path(args.raw_reply)
    normalized_reply_path = Path(args.normalized_reply)
    report_json_path = Path(args.report_json)

    if not raw_reply_path.exists():
        raise SystemExit(f"ERROR: raw reply does not exist: {raw_reply_path}")

    repairs_applied: List[str] = []
    normalization_status = "failed"
    parser_mode_used = "direct_yaml"
    selected_count = 0
    rejected_count = 0
    duplicate_conflicts_found = 0
    error_message = None

    try:
        raw_text = read_text(raw_reply_path)
        prepared_text = strip_markdown_fences(raw_text, repairs_applied)
        prepared_text = trim_prefix_before_yaml(prepared_text, repairs_applied)
        prepared_text = strip_trailing_eof_marker(prepared_text, repairs_applied)

        if detect_flat_candidate_blocks(prepared_text):
            prepared_text = repair_list_sections(prepared_text, repairs_applied)
            parser_mode_used = "repaired_yaml"
        prepared_text = repair_summary_section(prepared_text, repairs_applied)
        try:
            data = parse_yaml_document(prepared_text)
        except Exception:
            if parser_mode_used != "repaired_yaml":
                prepared_text = repair_list_sections(prepared_text, repairs_applied)
                parser_mode_used = "repaired_yaml"
                prepared_text = repair_summary_section(prepared_text, repairs_applied)
                data = parse_yaml_document(prepared_text)
            else:
                raise

        normalized, selected_count, rejected_count, duplicate_conflicts_found = canonicalize_reply(
            data,
            repairs_applied,
        )
        normalization_status = "repaired" if repairs_applied else "pass_through"

        write_yaml(normalized_reply_path, normalized)

        report = {
            "input_path": str(raw_reply_path),
            "output_path": str(normalized_reply_path),
            "normalization_status": normalization_status,
            "repairs_applied": repairs_applied,
            "selected_count": selected_count,
            "rejected_count": rejected_count,
            "duplicate_conflicts_found": duplicate_conflicts_found,
            "parser_mode_used": parser_mode_used,
            "error_message": None,
        }
        write_json(report_json_path, report)

        print(f"normalization_status: {normalization_status}")
        print(f"selected_count: {selected_count}")
        print(f"rejected_count: {rejected_count}")
        print(f"normalized_reply: {normalized_reply_path}")
        print(f"report_json: {report_json_path}")

    except Exception as exc:
        error_message = str(exc)
        report = {
            "input_path": str(raw_reply_path),
            "output_path": str(normalized_reply_path),
            "normalization_status": "failed",
            "repairs_applied": repairs_applied,
            "selected_count": selected_count,
            "rejected_count": rejected_count,
            "duplicate_conflicts_found": duplicate_conflicts_found,
            "parser_mode_used": parser_mode_used,
            "error_message": error_message,
        }
        write_json(report_json_path, report)
        raise SystemExit(f"ERROR: {error_message}")


if __name__ == "__main__":
    main()
