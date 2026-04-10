#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class SeedReplyAutorunError(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process an inbox seed-review bundle, materialize a seed cycle, and automatically run a batch."
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
        raise SeedReplyAutorunError(f"{field_name} must be a non-empty string")
    return value.strip()


def ensure_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise SeedReplyAutorunError(f"{field_name} must be a boolean")
    return value


def ensure_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SeedReplyAutorunError(f"{field_name} must be an integer")
    return value


def validate_id(value: Any, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    if not ID_RE.match(result):
        raise SeedReplyAutorunError(
            f"{field_name} must match ^[A-Za-z0-9._-]+$: {result}"
        )
    return result


def validate_iso_datetime(value: Any, field_name: str) -> str:
    result = ensure_nonempty_str(value, field_name)
    normalized = result.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SeedReplyAutorunError(
            f"{field_name} must be a valid ISO-8601 datetime: {result}"
        ) from exc
    return result


def load_yaml_object(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise SeedReplyAutorunError(f"Expected YAML object in {path}")
    return data


def write_json(path: Path, data: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def validate_request(path: Path) -> Dict[str, Any]:
    data = load_yaml_object(path)

    request_type = ensure_nonempty_str(data.get("request_type"), "request_type")
    if request_type != "seed_cycle_materialization_request":
        raise SeedReplyAutorunError(
            f"Unexpected request_type: {request_type}"
        )

    schema_version = ensure_int(data.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise SeedReplyAutorunError(
            f"Unsupported request schema_version: {schema_version}"
        )

    request_id = validate_id(data.get("request_id"), "request_id")
    created_at = validate_iso_datetime(data.get("created_at"), "created_at")

    packet_yaml = Path(ensure_nonempty_str(data.get("packet_yaml"), "packet_yaml"))
    if not packet_yaml.exists():
        raise SeedReplyAutorunError(f"packet_yaml does not exist: {packet_yaml}")

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
        raise SeedReplyAutorunError(
            "This autorun wrapper expects materialize_only: true so it can reuse the existing inbox materializer"
        )

    autorun_batch = ensure_bool(data.get("autorun_batch"), "autorun_batch")
    if not autorun_batch:
        raise SeedReplyAutorunError(
            "This wrapper requires autorun_batch: true"
        )

    batch_id = validate_id(data.get("batch_id"), "batch_id")

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
        "autorun_batch": autorun_batch,
        "batch_id": batch_id,
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
        raise SeedReplyAutorunError(
            f"{label} failed with returncode={proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\n"
            f"STDERR:\n{proc.stderr}"
        )
    return result


def main() -> None:
    args = parse_args()

    inbox_dir = Path(args.inbox_dir)
    if not inbox_dir.exists():
        raise SeedReplyAutorunError(f"Inbox dir does not exist: {inbox_dir}")

    request_yaml = inbox_dir / "seed_cycle_materialization_request.yaml"
    verifier_reply_yaml = inbox_dir / "verifier_reply.yaml"
    autorun_report_json = inbox_dir / "autorun_process_report.json"

    if not request_yaml.exists():
        raise SeedReplyAutorunError(f"Missing request YAML: {request_yaml}")
    if not verifier_reply_yaml.exists():
        raise SeedReplyAutorunError(f"Missing verifier reply YAML: {verifier_reply_yaml}")
    if autorun_report_json.exists():
        raise SeedReplyAutorunError(
            f"Refusing to overwrite existing autorun report: {autorun_report_json}"
        )

    request = validate_request(request_yaml)

    reply_dir = Path("runs/seed_review_replies") / request["reply_id"]
    cycle_dir = Path("runs/seed_cycles") / request["cycle_id"]
    batch_dir = Path("runs/batches") / request["batch_id"]

    if batch_dir.exists():
        raise SeedReplyAutorunError(
            f"Batch dir already exists, refusing to overwrite: {batch_dir}"
        )

    process_inbox_cmd = [
        sys.executable,
        "scripts/process_seed_reply_inbox.py",
        "--inbox-dir",
        str(inbox_dir),
        "--policy",
        args.policy,
    ]
    process_inbox_result = run_command(process_inbox_cmd, "process_seed_reply_inbox")

    run_batch_cmd = [
        sys.executable,
        "scripts/run_launch_profile_batch.py",
        "--launch-profile-id",
        request["launch_profile_id"],
        "--batch-id",
        request["batch_id"],
    ]
    run_batch_result = run_command(run_batch_cmd, "run_launch_profile_batch")

    if not cycle_dir.exists():
        raise SeedReplyAutorunError(
            f"Expected cycle dir to exist after inbox processing: {cycle_dir}"
        )
    if not reply_dir.exists():
        raise SeedReplyAutorunError(
            f"Expected reply dir to exist after inbox processing: {reply_dir}"
        )
    if not batch_dir.exists():
        raise SeedReplyAutorunError(
            f"Expected batch dir to exist after autorun: {batch_dir}"
        )

    aggregate_summary_json = batch_dir / "aggregate_summary.json"
    run_context_json = batch_dir / "run_context.json"
    if not aggregate_summary_json.exists():
        raise SeedReplyAutorunError(
            f"Missing aggregate_summary.json after autorun: {aggregate_summary_json}"
        )
    if not run_context_json.exists():
        raise SeedReplyAutorunError(
            f"Missing run_context.json after autorun: {run_context_json}"
        )

    cycle_autorun_report_json = cycle_dir / "autorun_process_report.json"

    autorun_report = {
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
        "autorun_batch": request["autorun_batch"],
        "batch_id": request["batch_id"],
        "steps": {
            "process_seed_reply_inbox": process_inbox_result,
            "run_launch_profile_batch": run_batch_result,
        },
        "output_paths": {
            "reply_dir": str(reply_dir),
            "cycle_dir": str(cycle_dir),
            "batch_dir": str(batch_dir),
            "aggregate_summary_json": str(aggregate_summary_json),
            "run_context_json": str(run_context_json),
            "autorun_process_report_json": str(autorun_report_json),
            "cycle_autorun_process_report_json": str(cycle_autorun_report_json),
        },
    }

    write_json(autorun_report_json, autorun_report)
    write_json(cycle_autorun_report_json, autorun_report)

    print("Seed reply inbox autorun processed successfully.")
    print(f"request_id: {request['request_id']}")
    print(f"reply_id: {request['reply_id']}")
    print(f"seed_set_id: {request['seed_set_id']}")
    print(f"cycle_id: {request['cycle_id']}")
    print(f"launch_profile_id: {request['launch_profile_id']}")
    print(f"batch_id: {request['batch_id']}")
    print(f"reply_dir: {reply_dir}")
    print(f"cycle_dir: {cycle_dir}")
    print(f"batch_dir: {batch_dir}")
    print(f"autorun_report_json: {autorun_report_json}")


if __name__ == "__main__":
    try:
        main()
    except SeedReplyAutorunError as exc:
        raise SystemExit(f"ERROR: {exc}")
