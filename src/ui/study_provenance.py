from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT


STUDY_SOURCE_FIELD_NAMES = (
    "source_type",
    "source_study_id",
    "source_study_dir",
    "source_reference_batch_id",
    "source_candidate_batch_id",
    "source_candidate_decision",
    "source_suggested_decision",
    "source_selected_metric",
)


@dataclass(frozen=True, slots=True)
class StudySourceContextAssessment:
    source_type: str | None
    source_study_id: str | None
    source_study_dir: str | None
    source_reference_batch_id: str | None
    source_candidate_batch_id: str | None
    source_candidate_decision: str | None
    source_suggested_decision: str | None
    source_selected_metric: str | None
    active: bool
    stale: bool
    message: str | None


def build_study_source_session_payload(
    *,
    study_id: str,
    study_dir: str | Path,
    reference_batch_id: str,
    candidate_batch_id: str,
    candidate_decision: str | None = None,
    suggested_decision: str | None = None,
    selected_metric: str | None = None,
) -> dict[str, Any]:
    normalized_study_id = _required_text(study_id, label="study_id")
    normalized_reference_batch_id = _required_text(reference_batch_id, label="reference_batch_id")
    normalized_candidate_batch_id = _required_text(candidate_batch_id, label="candidate_batch_id")
    normalized_study_dir = _serialize_path(study_dir)
    if normalized_study_dir is None:
        raise ValueError("study_dir is required.")

    return {
        "source_type": "cohort_study",
        "source_study_id": normalized_study_id,
        "source_study_dir": normalized_study_dir,
        "source_reference_batch_id": normalized_reference_batch_id,
        "source_candidate_batch_id": normalized_candidate_batch_id,
        "source_candidate_decision": _optional_str(candidate_decision),
        "source_suggested_decision": _optional_str(suggested_decision),
        "source_selected_metric": _optional_str(selected_metric),
    }


def evaluate_study_source_context(
    payload: dict[str, Any] | None,
    *,
    primary_batch_id: str | None,
    secondary_batch_id: str | None,
) -> StudySourceContextAssessment:
    if not isinstance(payload, dict):
        return StudySourceContextAssessment(
            source_type=None,
            source_study_id=None,
            source_study_dir=None,
            source_reference_batch_id=None,
            source_candidate_batch_id=None,
            source_candidate_decision=None,
            source_suggested_decision=None,
            source_selected_metric=None,
            active=False,
            stale=False,
            message=None,
        )

    assessment = StudySourceContextAssessment(
        source_type=_optional_str(payload.get("source_type")),
        source_study_id=_optional_str(payload.get("source_study_id")),
        source_study_dir=_optional_str(payload.get("source_study_dir")),
        source_reference_batch_id=_optional_str(payload.get("source_reference_batch_id")),
        source_candidate_batch_id=_optional_str(payload.get("source_candidate_batch_id")),
        source_candidate_decision=_optional_str(payload.get("source_candidate_decision")),
        source_suggested_decision=_optional_str(payload.get("source_suggested_decision")),
        source_selected_metric=_optional_str(payload.get("source_selected_metric")),
        active=False,
        stale=False,
        message=None,
    )
    if assessment.source_type != "cohort_study":
        return _replace_assessment(
            assessment,
            stale=True,
            message="Unsupported study-source context is present and will be ignored.",
        )
    if assessment.source_study_id is None:
        return _replace_assessment(
            assessment,
            stale=True,
            message="Study-source context is missing a study_id and will be ignored.",
        )
    if assessment.source_reference_batch_id is None or assessment.source_candidate_batch_id is None:
        return _replace_assessment(
            assessment,
            stale=True,
            message="Study-source context is missing the saved reference/candidate batch ids.",
        )

    current_primary_batch_id = _optional_str(primary_batch_id)
    current_secondary_batch_id = _optional_str(secondary_batch_id)
    if (
        current_primary_batch_id == assessment.source_reference_batch_id
        and current_secondary_batch_id == assessment.source_candidate_batch_id
    ):
        return _replace_assessment(
            assessment,
            active=True,
            stale=False,
            message="Active cohort-study source context matches the current comparison pair.",
        )

    return _replace_assessment(
        assessment,
        active=False,
        stale=True,
        message="Study-source context is stale and does not match the current comparison pair.",
    )


def build_study_source_artifact_fields(
    assessment: StudySourceContextAssessment | None,
) -> dict[str, Any]:
    if assessment is None or not assessment.active:
        return {}
    return {
        "source_type": assessment.source_type,
        "source_study_id": assessment.source_study_id,
        "source_study_dir": assessment.source_study_dir,
        "source_reference_batch_id": assessment.source_reference_batch_id,
        "source_candidate_batch_id": assessment.source_candidate_batch_id,
        "source_candidate_decision": assessment.source_candidate_decision,
        "source_suggested_decision": assessment.source_suggested_decision,
        "source_selected_metric": assessment.source_selected_metric,
        "source_context_active": True,
    }


def extract_study_source_fields(
    *payloads: dict[str, Any] | None,
    require_active: bool = False,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    normalized_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    for field_name in STUDY_SOURCE_FIELD_NAMES:
        value = _first_defined_field(normalized_payloads, field_name)
        normalized = _optional_str(value)
        if normalized is not None:
            merged[field_name] = normalized

    if merged.get("source_type") != "cohort_study":
        return {}
    if _optional_str(merged.get("source_study_id")) is None:
        return {}

    if require_active:
        source_context_active = _first_defined_field(normalized_payloads, "source_context_active")
        if source_context_active is False:
            return {}
    return merged


def extract_saved_study_source_fields(
    comparison_manifest: dict[str, Any] | None,
    decision_record: dict[str, Any] | None,
) -> dict[str, Any]:
    return extract_study_source_fields(
        decision_record,
        comparison_manifest,
        require_active=True,
    )


def load_saved_study_source_from_comparison_dir(
    comparison_dir: str | Path,
) -> dict[str, Any]:
    resolved_dir = _resolve_dir(comparison_dir)
    comparison_manifest = _safe_load_json_object(resolved_dir / "comparison_manifest.json")
    decision_record = _safe_load_json_object(resolved_dir / "decision_record.json")
    return extract_saved_study_source_fields(comparison_manifest, decision_record)


def build_review_packet_study_source_fields(
    saved_study_source: dict[str, Any] | None,
) -> dict[str, Any]:
    return extract_study_source_fields(saved_study_source)


def build_evidence_summary_study_source_block(
    saved_study_source: dict[str, Any] | None,
) -> dict[str, Any] | None:
    study_source = build_review_packet_study_source_fields(saved_study_source)
    return study_source or None


def load_study_source_from_json_file(path: str | Path) -> dict[str, Any]:
    payload = _safe_load_json_object(_resolve_path(path))
    return extract_study_source_fields(payload)


def _replace_assessment(
    assessment: StudySourceContextAssessment,
    *,
    active: bool | None = None,
    stale: bool | None = None,
    message: str | None = None,
) -> StudySourceContextAssessment:
    return StudySourceContextAssessment(
        source_type=assessment.source_type,
        source_study_id=assessment.source_study_id,
        source_study_dir=assessment.source_study_dir,
        source_reference_batch_id=assessment.source_reference_batch_id,
        source_candidate_batch_id=assessment.source_candidate_batch_id,
        source_candidate_decision=assessment.source_candidate_decision,
        source_suggested_decision=assessment.source_suggested_decision,
        source_selected_metric=assessment.source_selected_metric,
        active=assessment.active if active is None else active,
        stale=assessment.stale if stale is None else stale,
        message=assessment.message if message is None else message,
    )


def _required_text(value: object, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise ValueError(f"{label} is required.")
    return normalized


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: str | Path) -> str | None:
    text = _optional_str(value)
    if text is None:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _resolve_path(value: str | Path) -> Path:
    text = _optional_str(value)
    if text is None:
        raise ValueError("path is required.")
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    return candidate


def _resolve_dir(value: str | Path) -> Path:
    return _resolve_path(value)


def _safe_load_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _first_defined_field(payloads: list[dict[str, Any]], field_name: str) -> Any:
    for payload in payloads:
        if field_name in payload:
            value = payload.get(field_name)
            if value is not None:
                return value
    return None
