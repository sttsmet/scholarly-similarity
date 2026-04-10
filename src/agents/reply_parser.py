from __future__ import annotations

import json
from typing import Any

import yaml


def parse_structured_reply(reply_text: str) -> dict[str, Any]:
    """Parse a plain YAML/JSON reply or a fenced YAML/JSON block into a mapping."""

    stripped = _strip_fences(reply_text)
    try:
        payload = yaml.safe_load(stripped)
    except yaml.YAMLError as exc:
        raise ValueError(f"Failed to parse structured reply: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Expected a structured mapping reply")
    return payload


def parse_json_reply(reply_text: str) -> dict[str, Any]:
    """Backward-compatible JSON-only parser for legacy callers."""

    stripped = _strip_fences(reply_text)
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse JSON reply: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object reply")
    return payload


def _strip_fences(reply_text: str) -> str:
    stripped = reply_text.strip()
    if not stripped.startswith("```"):
        return stripped

    parts = stripped.split("```")
    if len(parts) < 3:
        return stripped
    fenced_block = parts[1].strip()
    if "\n" not in fenced_block:
        return fenced_block

    first_line, remainder = fenced_block.split("\n", 1)
    language = first_line.strip().lower()
    if language in {"json", "yaml", "yml"}:
        return remainder.strip()
    return fenced_block
