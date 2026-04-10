from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from src.agents.reply_parser import parse_structured_reply
from src.agents.revision_validator import validate_generator_reply_payload
from src.config import REPO_ROOT


TEMPLATE_MARKER = "TEMPLATE ONLY - not an actual generator reply"
REQUIRED_PACKET_FILENAMES = (
    "review_packet_manifest.json",
    "allowed_revision_paths.json",
    "baseline_theory_snapshot.yaml",
)


class ReviewPacketLoadError(ValueError):
    """Raised when a review packet directory cannot be loaded."""


@dataclass(frozen=True, slots=True)
class BaselineScalarLeaf:
    path: str
    value: Any
    scalar_type: str
    is_numeric: bool


@dataclass(frozen=True, slots=True)
class ReviewPacketBundle:
    packet_dir: Path
    manifest: dict[str, Any]
    allowed_revision_paths: list[str]
    baseline_snapshot_path: Path
    baseline_theory_payload: dict[str, Any]
    baseline_scalar_leaves: dict[str, BaselineScalarLeaf]
    template_path: Path | None


@dataclass(frozen=True, slots=True)
class ReplyPreviewResult:
    state: str
    reply_path: Path
    packet_id: str | None
    comparison_id: str | None
    selected_metric: str | None
    proposed_change_count: int
    valid_change_count: int
    invalid_change_count: int
    errors: list[str]
    warnings: list[str]
    diff_rows: list[dict[str, Any]]
    grouped_summary: list[dict[str, Any]]


def load_review_packet_bundle(packet_dir: str | Path) -> ReviewPacketBundle:
    resolved_packet_dir = _resolve_input_path(packet_dir)
    if not resolved_packet_dir.exists():
        raise ReviewPacketLoadError(f"Review packet directory does not exist: {resolved_packet_dir}")
    if not resolved_packet_dir.is_dir():
        raise ReviewPacketLoadError(f"Review packet path is not a directory: {resolved_packet_dir}")

    required_paths = {name: resolved_packet_dir / name for name in REQUIRED_PACKET_FILENAMES}
    missing_paths = [path.name for path in required_paths.values() if not path.exists()]
    if missing_paths:
        raise ReviewPacketLoadError(
            "Review packet is missing required artifact file(s): " + ", ".join(missing_paths)
        )

    manifest = _load_json_object(required_paths["review_packet_manifest.json"])
    allowed_payload = _load_json_value(required_paths["allowed_revision_paths.json"])
    allowed_paths = _extract_allowed_revision_paths(allowed_payload)

    baseline_snapshot_path = required_paths["baseline_theory_snapshot.yaml"]
    baseline_theory_payload = _load_yaml_object(baseline_snapshot_path)
    baseline_scalar_leaves = flatten_baseline_scalar_leaves(baseline_theory_payload)
    template_path = resolved_packet_dir / "candidate_reply_TEMPLATE.yaml"

    return ReviewPacketBundle(
        packet_dir=resolved_packet_dir,
        manifest=manifest,
        allowed_revision_paths=allowed_paths,
        baseline_snapshot_path=baseline_snapshot_path,
        baseline_theory_payload=baseline_theory_payload,
        baseline_scalar_leaves=baseline_scalar_leaves,
        template_path=template_path if template_path.exists() else None,
    )


def flatten_baseline_scalar_leaves(
    theory_payload: dict[str, Any],
) -> dict[str, BaselineScalarLeaf]:
    flattened: dict[str, BaselineScalarLeaf] = {}
    _collect_scalar_leaves("", theory_payload, flattened)
    return dict(sorted(flattened.items()))


def preview_candidate_reply(
    *,
    packet_bundle: ReviewPacketBundle,
    reply_path: str | Path,
    candidate_revision_id: str | None = None,
) -> ReplyPreviewResult:
    resolved_reply_path = _resolve_input_path(reply_path, relative_to=packet_bundle.packet_dir)
    if not resolved_reply_path.exists():
        raise ReviewPacketLoadError(f"Candidate reply file does not exist: {resolved_reply_path}")
    if not resolved_reply_path.is_file():
        raise ReviewPacketLoadError(f"Candidate reply path is not a file: {resolved_reply_path}")

    reply_text = resolved_reply_path.read_text(encoding="utf-8")
    template_message = detect_template_only_reply(
        packet_bundle=packet_bundle,
        reply_path=resolved_reply_path,
        reply_text=reply_text,
    )
    if template_message is not None:
        return ReplyPreviewResult(
            state="template_only",
            reply_path=resolved_reply_path,
            packet_id=_optional_str(packet_bundle.manifest.get("packet_id")),
            comparison_id=_optional_str(packet_bundle.manifest.get("comparison_id")),
            selected_metric=_optional_str(packet_bundle.manifest.get("selected_packet_metric")),
            proposed_change_count=0,
            valid_change_count=0,
            invalid_change_count=0,
            errors=[template_message],
            warnings=[],
            diff_rows=[],
            grouped_summary=[],
        )

    try:
        payload = parse_structured_reply(reply_text)
    except ValueError as exc:
        return ReplyPreviewResult(
            state="invalid",
            reply_path=resolved_reply_path,
            packet_id=_optional_str(packet_bundle.manifest.get("packet_id")),
            comparison_id=_optional_str(packet_bundle.manifest.get("comparison_id")),
            selected_metric=_optional_str(packet_bundle.manifest.get("selected_packet_metric")),
            proposed_change_count=0,
            valid_change_count=0,
            invalid_change_count=0,
            errors=[str(exc)],
            warnings=[],
            diff_rows=[],
            grouped_summary=[],
        )

    errors: list[str] = []
    warnings: list[str] = []
    payload_packet_id = _optional_str(payload.get("packet_id"))
    payload_comparison_id = _optional_str(payload.get("comparison_id"))
    manifest_packet_id = _optional_str(packet_bundle.manifest.get("packet_id"))
    manifest_comparison_id = _optional_str(packet_bundle.manifest.get("comparison_id"))

    if payload_packet_id is not None and manifest_packet_id is not None and payload_packet_id != manifest_packet_id:
        errors.append(
            f"Reply packet_id mismatch: reply has '{payload_packet_id}', packet has '{manifest_packet_id}'."
        )
    if (
        payload_comparison_id is not None
        and manifest_comparison_id is not None
        and payload_comparison_id != manifest_comparison_id
    ):
        errors.append(
            "Reply comparison_id mismatch: "
            f"reply has '{payload_comparison_id}', packet has '{manifest_comparison_id}'."
        )

    validated_reply = None
    try:
        validated_reply = validate_generator_reply_payload(
            payload=payload,
            theory_payload=packet_bundle.baseline_theory_payload,
            candidate_revision_id=candidate_revision_id,
        )
    except (ValidationError, ValueError) as exc:
        errors.extend(_format_validation_exception(exc))

    if validated_reply is not None:
        diff_rows = build_validated_reply_diff_rows(
            validated_reply=validated_reply,
            packet_bundle=packet_bundle,
        )
    else:
        raw_changes = payload.get("changes")
        if raw_changes is None:
            raw_changes = []
        if not isinstance(raw_changes, list):
            errors.append("Reply field 'changes' must be a list.")
            raw_changes = []
        elif not raw_changes:
            errors.append("Generator reply must include at least one change.")

        diff_rows = build_reply_diff_rows(
            raw_changes=raw_changes,
            packet_bundle=packet_bundle,
        )
    valid_change_count = sum(1 for row in diff_rows if row["status"] == "valid")
    invalid_change_count = sum(1 for row in diff_rows if row["status"] == "invalid")

    if validated_reply is not None and invalid_change_count == 0 and not errors:
        state = "valid"
    else:
        state = "invalid"

    if not diff_rows and state == "invalid" and not warnings:
        warnings.append("No diff rows could be previewed from the candidate reply.")

    return ReplyPreviewResult(
        state=state,
        reply_path=resolved_reply_path,
        packet_id=manifest_packet_id,
        comparison_id=manifest_comparison_id,
        selected_metric=_optional_str(packet_bundle.manifest.get("selected_packet_metric")),
        proposed_change_count=len(diff_rows),
        valid_change_count=valid_change_count,
        invalid_change_count=invalid_change_count,
        errors=_dedupe_strings(errors),
        warnings=_dedupe_strings(warnings),
        diff_rows=diff_rows,
        grouped_summary=group_reply_diff_rows(diff_rows),
    )


def detect_template_only_reply(
    *,
    packet_bundle: ReviewPacketBundle,
    reply_path: Path,
    reply_text: str,
) -> str | None:
    if packet_bundle.template_path is not None and _same_path(reply_path, packet_bundle.template_path):
        return "The selected file is the packet template and cannot be treated as an actual reply."
    if TEMPLATE_MARKER in reply_text:
        return "This file is marked as template-only and cannot be treated as an actual reply."
    return None


def build_reply_diff_rows(
    *,
    raw_changes: list[Any],
    packet_bundle: ReviewPacketBundle,
) -> list[dict[str, Any]]:
    seen_paths: set[str] = set()
    diff_rows: list[dict[str, Any]] = []

    for index, raw_change in enumerate(raw_changes, start=1):
        row = {
            "change_index": index,
            "path": None,
            "baseline_value": None,
            "proposed_value": None,
            "scalar_type": None,
            "numeric_delta": None,
            "status": "invalid",
            "note": None,
        }
        row_errors: list[str] = []

        if not isinstance(raw_change, dict):
            row_errors.append("Change entry must be a mapping with 'path' and 'value'.")
            row["note"] = "; ".join(row_errors)
            diff_rows.append(row)
            continue

        path = _optional_str(raw_change.get("path"))
        proposed_value = raw_change.get("value")
        row["path"] = path
        row["proposed_value"] = proposed_value

        if path is None:
            row_errors.append("Change path must not be empty.")
            row["note"] = "; ".join(row_errors)
            diff_rows.append(row)
            continue

        baseline_leaf = packet_bundle.baseline_scalar_leaves.get(path)
        if baseline_leaf is not None:
            row["baseline_value"] = baseline_leaf.value
            row["scalar_type"] = baseline_leaf.scalar_type

        if path in seen_paths:
            row_errors.append(f"Duplicate change path: {path}")
        seen_paths.add(path)

        if path not in packet_bundle.allowed_revision_paths:
            row_errors.append(f"Path is not allowed by allowed_revision_paths.json: {path}")

        if baseline_leaf is None:
            row_errors.append(f"Path does not exist in baseline_theory_snapshot.yaml: {path}")
        elif not baseline_leaf.is_numeric:
            row_errors.append(
                f"Baseline leaf type is {baseline_leaf.scalar_type}; "
                "the current generator reply schema only supports numeric scalar changes."
            )

        if isinstance(proposed_value, dict) or isinstance(proposed_value, list):
            row_errors.append("Proposed value must be a scalar and not a collection.")
        elif isinstance(proposed_value, bool):
            row_errors.append("Proposed value must be numeric under the current generator reply schema, not boolean.")
        elif not isinstance(proposed_value, (int, float)):
            row_errors.append("Proposed value must be numeric under the current generator reply schema.")

        if not row_errors:
            try:
                normalized_change = validate_generator_reply_payload(
                    payload=_synthetic_generator_payload(path=path, value=proposed_value),
                    theory_payload=packet_bundle.baseline_theory_payload,
                ).changes[0]
                row["proposed_value"] = normalized_change.value
                if baseline_leaf is not None and baseline_leaf.is_numeric:
                    row["numeric_delta"] = float(normalized_change.value) - float(baseline_leaf.value)
                row["status"] = "valid"
            except (ValidationError, ValueError) as exc:
                row_errors.extend(_format_validation_exception(exc))

        if row["status"] != "valid":
            row["status"] = "invalid"
        row["note"] = "; ".join(_dedupe_strings(row_errors)) if row_errors else None
        diff_rows.append(row)

    return diff_rows


def build_validated_reply_diff_rows(
    *,
    validated_reply: Any,
    packet_bundle: ReviewPacketBundle,
) -> list[dict[str, Any]]:
    return build_reply_diff_rows(
        raw_changes=[
            {
                "path": change.path,
                "value": change.value,
            }
            for change in validated_reply.changes
        ],
        packet_bundle=packet_bundle,
    )


def group_reply_diff_rows(diff_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in diff_rows:
        path = _optional_str(row.get("path")) or "unknown"
        prefix = path.split(".", 1)[0] if "." in path else path
        bucket = grouped.setdefault(
            prefix,
            {
                "prefix": prefix,
                "change_count": 0,
                "valid_count": 0,
                "invalid_count": 0,
            },
        )
        bucket["change_count"] += 1
        if row.get("status") == "valid":
            bucket["valid_count"] += 1
        else:
            bucket["invalid_count"] += 1
    return [grouped[key] for key in sorted(grouped)]


def _collect_scalar_leaves(
    prefix: str,
    value: Any,
    flattened: dict[str, BaselineScalarLeaf],
) -> None:
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            child_prefix = f"{prefix}.{child_key}" if prefix else child_key
            _collect_scalar_leaves(child_prefix, child_value, flattened)
        return

    if _is_scalar(value):
        flattened[prefix] = BaselineScalarLeaf(
            path=prefix,
            value=value,
            scalar_type=_scalar_type_name(value),
            is_numeric=_is_numeric_scalar(value),
        )


def _synthetic_generator_payload(*, path: str, value: Any) -> dict[str, Any]:
    return {
        "summary": "Preview candidate reply.",
        "expected_effect": "Preview only.",
        "risks": ["Preview only."],
        "changes": [
            {
                "path": path,
                "value": value,
            }
        ],
    }


def _extract_allowed_revision_paths(payload: Any) -> list[str]:
    if isinstance(payload, list):
        allowed_paths = payload
    elif isinstance(payload, dict):
        allowed_paths = payload.get("allowed_scalar_paths")
    else:
        raise ReviewPacketLoadError("allowed_revision_paths.json must be a JSON array or object.")

    if not isinstance(allowed_paths, list):
        raise ReviewPacketLoadError("allowed_revision_paths.json must expose an 'allowed_scalar_paths' list.")

    cleaned_paths = []
    for item in allowed_paths:
        text = _optional_str(item)
        if text is not None:
            cleaned_paths.append(text)
    return sorted(dict.fromkeys(cleaned_paths))


def _load_json_object(path: Path) -> dict[str, Any]:
    payload = _load_json_value(path)
    if not isinstance(payload, dict):
        raise ReviewPacketLoadError(f"Invalid {path.name}: expected a JSON object.")
    return payload


def _load_json_value(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ReviewPacketLoadError(f"Could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ReviewPacketLoadError(
            f"Malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc


def _load_yaml_object(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ReviewPacketLoadError(f"Could not read {path.name}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ReviewPacketLoadError(f"Malformed YAML in {path.name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReviewPacketLoadError(f"Invalid {path.name}: expected a YAML mapping.")
    return payload


def _format_validation_exception(exc: Exception) -> list[str]:
    if isinstance(exc, ValidationError):
        messages: list[str] = []
        for error in exc.errors():
            location = ".".join(str(item) for item in error.get("loc", ()))
            message = error.get("msg", str(exc))
            messages.append(f"{location}: {message}" if location else message)
        return messages or [str(exc)]
    return [str(exc)]


def _resolve_input_path(value: str | Path, *, relative_to: Path | None = None) -> Path:
    raw_value = str(value).strip()
    if not raw_value:
        raise ReviewPacketLoadError("Path must not be empty.")
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    if relative_to is not None:
        relative_candidate = (relative_to / candidate).resolve()
        if relative_candidate.exists():
            return relative_candidate
    if candidate.exists():
        return candidate.resolve()
    return (REPO_ROOT / candidate).resolve()


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _is_numeric_scalar(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _scalar_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    return type(value).__name__


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return left == right


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            deduped.append(value)
    return deduped
