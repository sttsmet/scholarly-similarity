#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class SeedReplyInboxError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process an inbox seed-review bundle into reply artifacts and a materialized seed cycle."
    )
    parser.add_argument(
        "--inbox-dir",
        required=True,
        help="Path to workspace_inbox/seed_review_replies/<request_id>",
    )
    parser.add_argument(
        "--policy",
        default="configs/presets/seed_policies/seed_selection_policy_v1.yaml",
        help="Path to the seed-selection policy YAML.",
    )
    return parser.parse_args()


def ensure_nonempty_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SeedReplyInboxError(f"{field_name} must be a non-empty string")
    return value.strip()


def ensure_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise SeedReplyInboxError(f"{field_name} must be a boolean")
    return value


def ensure_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SeedReplyInboxError(f"{field_name} must be an integer")
    return value


def validate_id(value: Any, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    if not ID_RE.match(result):
        raise SeedReplyInboxError(
            f"{field_name} must match ^[A-Za-z0-9._-]+$: {result}"
        )
    return result


def validate_iso_datetime(value: Any, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    normalized = result.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SeedReplyInboxError(
            f"{field_name} must be a valid ISO-8601 datetime: {result}"
        ) from exc
    return result


def load_yaml_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise SeedReplyInboxError(f"Expected YAML object in {path}")
    return data


def write_json(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def validate_request(path: Path) -> Dict[str, Any]:
    data = load_yaml_object(path)

    request_type = ensure_nonempty_str(data.get("request_type"), "request_type")
    if request_type != "seed_cycle_materialization_request":
        raise SeedReplyInboxError(
            f"Unexpected request_type: {request_type}"
        )

    schema_version = ensure_int(data.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise SeedReplyInboxError(
            f"Unsupported request schema_version: {schema_version}"
        )

    request_id = validate_id(data.get("request_id"), "request_id")
    created_at = validate_iso_datetime(data.get("created_at"), "created_at")

    packet_yaml = Path(ensure_nonempty_str(data.get("packet_yaml"), "packet_yaml"))
    if not packet_yaml.exists():
        raise SeedReplyInboxError(f"packet_yaml does not exist: {packet_yaml}")

    reply_id = validate_id(data.get("reply_id"), "reply_id")
    seed_set_id = validate_id(data.get("seed_set_id"), "seed_set_id")
    cycle_id = validate_id(data.get("cycle_id"), "cycle_id")
    benchmark_preset_id = validate_id(
        data.get("benchmark_preset_id"),
        "benchmark_preset_id",
    )
    launch_profile_id = validate_id(
        data.get("launch_profile_id"),
        "launch_profile_id",
    )
    accepted_baseline_id = validate_id(
        data.get("accepted_baseline_id"),
        "accepted_baseline_id",
    )
    eval_preset_id = validate_id(data.get("eval_preset_id"), "eval_preset_id")
    description = ensure_nonempty_str(data.get("description"), "description")
    materialize_only = ensure_bool(data.get("materialize_only"), "materialize_only")

    if not materialize_only:
        raise SeedReplyInboxError(
            "This processor currently supports only materialize_only: true"
        )

    return {
        "request_id": request_id,
        "created_at": created_at,
        "packet_yaml": str(packet_yaml),
        "reply_id": reply_id,
        "seed_set_id": seed_set_id,
        "cycle_id": cycle_id,
        "benchmark_preset_id": benchmark_preset_id,
        "launch_profile_id": launch_profile_id,
        "accepted_baseline_id": accepted_baseline_id,
        "eval_preset_id": eval_preset_id,
        "description": description,
        "materialize_only": materialize_only,
    }


def run_command(cmd: list[str], label: str) -> Dict[str, Any]:
    proc = subprocess.run(cmd, capture_output=True, text=True)
    result = {
        "label": label,
        "command": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
    if proc.returncode != 0:
        raise SeedReplyInboxError(
            f"{label} failed with returncode={proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    return result


def main() -> None:
    args = parse_args()

    inbox_dir = Path(args.inbox_dir)
    if not inbox_dir.exists():
        raise SeedReplyInboxError(f"Inbox dir does not exist: {inbox_dir}")

    request_yaml = inbox_dir / "seed_cycle_materialization_request.yaml"
    verifier_reply_yaml = inbox_dir / "verifier_reply.yaml"
    process_report_json = inbox_dir / "process_report.json"
    normalized_reply_yaml = inbox_dir / "verifier_reply.normalized.yaml"
    normalization_report_json = inbox_dir / "verifier_reply.normalization_report.json"

    if not request_yaml.exists():
        raise SeedReplyInboxError(f"Missing request YAML: {request_yaml}")
    if not verifier_reply_yaml.exists():
        raise SeedReplyInboxError(f"Missing verifier reply YAML: {verifier_reply_yaml}")
    if process_report_json.exists():
        raise SeedReplyInboxError(
            f"Refusing to overwrite existing process report: {process_report_json}"
        )
    if normalized_reply_yaml.exists():
        raise SeedReplyInboxError(
            f"Refusing to overwrite existing normalized reply YAML: {normalized_reply_yaml}"
        )
    if normalization_report_json.exists():
        raise SeedReplyInboxError(
            f"Refusing to overwrite existing normalization report JSON: {normalization_report_json}"
        )

    request = validate_request(request_yaml)

    reply_dir = Path("runs/seed_review_replies") / request["reply_id"]
    cycle_dir = Path("runs/seed_cycles") / request["cycle_id"]

    if reply_dir.exists():
        raise SeedReplyInboxError(
            f"Reply dir already exists, refusing to overwrite: {reply_dir}"
        )
    if cycle_dir.exists():
        raise SeedReplyInboxError(
            f"Cycle dir already exists, refusing to overwrite: {cycle_dir}"
        )

    normalize_cmd = [
        sys.executable,
        "scripts/normalize_seed_verifier_reply.py",
        "--raw-reply",
        str(verifier_reply_yaml),
        "--normalized-reply",
        str(normalized_reply_yaml),
        "--report-json",
        str(normalization_report_json),
    ]
    normalize_result = run_command(normalize_cmd, "normalize_seed_verifier_reply")

    ingest_cmd = [
        sys.executable,
        "scripts/ingest_seed_verifier_reply.py",
        "--packet-yaml",
        request["packet_yaml"],
        "--verifier-reply-yaml",
        str(normalized_reply_yaml),
        "--policy",
        args.policy,
        "--reply-id",
        request["reply_id"],
        "--out-dir",
        str(reply_dir),
    ]
    ingest_result = run_command(ingest_cmd, "ingest_seed_verifier_reply")

    materialize_cmd = [
        sys.executable,
        "scripts/materialize_seed_cycle.py",
        "--reply-dir",
        str(reply_dir),
        "--packet-yaml",
        request["packet_yaml"],
        "--seed-set-id",
        request["seed_set_id"],
        "--cycle-id",
        request["cycle_id"],
        "--benchmark-preset-id",
        request["benchmark_preset_id"],
        "--launch-profile-id",
        request["launch_profile_id"],
        "--accepted-baseline-id",
        request["accepted_baseline_id"],
        "--eval-preset-id",
        request["eval_preset_id"],
        "--created-at",
        request["created_at"],
        "--description",
        request["description"],
    ]
    materialize_result = run_command(materialize_cmd, "materialize_seed_cycle")

    if not cycle_dir.exists():
        raise SeedReplyInboxError(
            f"Expected cycle dir to exist after materialization: {cycle_dir}"
        )

    cycle_request_copy = cycle_dir / "source_inbox_request.yaml"
    cycle_reply_copy = cycle_dir / "source_inbox_verifier_reply.yaml"
    cycle_normalized_reply_yaml = cycle_dir / "source_inbox_verifier_reply_normalized.yaml"
    cycle_normalization_report_json = cycle_dir / "source_inbox_verifier_reply_normalization_report.json"
    cycle_process_report_json = cycle_dir / "inbox_process_report.json"

    shutil.copyfile(request_yaml, cycle_request_copy)
    shutil.copyfile(verifier_reply_yaml, cycle_reply_copy)
    shutil.copyfile(normalized_reply_yaml, cycle_normalized_reply_yaml)
    shutil.copyfile(normalization_report_json, cycle_normalization_report_json)

    process_report = {
        "request_id": request["request_id"],
        "created_at": request["created_at"],
        "inbox_dir": str(inbox_dir),
        "packet_yaml": request["packet_yaml"],
        "reply_id": request["reply_id"],
        "seed_set_id": request["seed_set_id"],
        "cycle_id": request["cycle_id"],
        "benchmark_preset_id": request["benchmark_preset_id"],
        "launch_profile_id": request["launch_profile_id"],
        "accepted_baseline_id": request["accepted_baseline_id"],
        "eval_preset_id": request["eval_preset_id"],
        "description": request["description"],
        "materialize_only": request["materialize_only"],
        "steps": {
            "normalize_seed_verifier_reply": normalize_result,
            "ingest_seed_verifier_reply": ingest_result,
            "materialize_seed_cycle": materialize_result,
        },
        "output_paths": {
            "reply_dir": str(reply_dir),
            "cycle_dir": str(cycle_dir),
            "normalized_reply_yaml": str(normalized_reply_yaml),
            "normalization_report_json": str(normalization_report_json),
            "cycle_request_copy_yaml": str(cycle_request_copy),
            "cycle_verifier_reply_copy_yaml": str(cycle_reply_copy),
            "cycle_normalized_reply_yaml": str(cycle_normalized_reply_yaml),
            "cycle_normalization_report_json": str(cycle_normalization_report_json),
            "process_report_json": str(process_report_json),
            "cycle_inbox_process_report_json": str(cycle_process_report_json),
        },
    }

    write_json(process_report_json, process_report)
    write_json(cycle_process_report_json, process_report)

    print("Seed reply inbox processed successfully.")
    print(f"request_id: {request['request_id']}")
    print(f"reply_id: {request['reply_id']}")
    print(f"seed_set_id: {request['seed_set_id']}")
    print(f"cycle_id: {request['cycle_id']}")
    print(f"benchmark_preset_id: {request['benchmark_preset_id']}")
    print(f"launch_profile_id: {request['launch_profile_id']}")
    print(f"reply_dir: {reply_dir}")
    print(f"cycle_dir: {cycle_dir}")
    print(f"normalized_reply_yaml: {normalized_reply_yaml}")
    print(f"normalization_report_json: {normalization_report_json}")
    print(f"process_report_json: {process_report_json}")


if __name__ == "__main__":
    try:
        main()
    except SeedReplyInboxError as exc:
        raise SystemExit(f"ERROR: {exc}")
