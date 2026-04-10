from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ui.study_provenance import (
    build_evidence_summary_study_source_block,
    build_review_packet_study_source_fields,
    build_study_source_artifact_fields,
    build_study_source_session_payload,
    evaluate_study_source_context,
    extract_study_source_fields,
    extract_saved_study_source_fields,
    load_study_source_from_json_file,
    load_saved_study_source_from_comparison_dir,
)


def test_build_study_source_session_payload_normalizes_core_fields() -> None:
    payload = build_study_source_session_payload(
        study_id=" study_001 ",
        study_dir=Path("runs") / "cohort_studies" / "study_001",
        reference_batch_id=" batch_005 ",
        candidate_batch_id=" batch_010 ",
        candidate_decision=" shortlist ",
        suggested_decision=" review ",
        selected_metric=" ndcg_at_k ",
    )

    assert payload["source_type"] == "cohort_study"
    assert payload["source_study_id"] == "study_001"
    assert payload["source_reference_batch_id"] == "batch_005"
    assert payload["source_candidate_batch_id"] == "batch_010"
    assert payload["source_candidate_decision"] == "shortlist"
    assert payload["source_suggested_decision"] == "review"
    assert payload["source_selected_metric"] == "ndcg_at_k"


def test_evaluate_study_source_context_marks_matching_pair_active() -> None:
    payload = build_study_source_session_payload(
        study_id="study_001",
        study_dir=Path("runs") / "cohort_studies" / "study_001",
        reference_batch_id="batch_005",
        candidate_batch_id="batch_010",
        candidate_decision="shortlist",
        selected_metric="ndcg_at_k",
    )

    assessment = evaluate_study_source_context(
        payload,
        primary_batch_id="batch_005",
        secondary_batch_id="batch_010",
    )

    assert assessment.active is True
    assert assessment.stale is False
    assert assessment.source_study_id == "study_001"


def test_evaluate_study_source_context_marks_changed_pair_stale() -> None:
    payload = build_study_source_session_payload(
        study_id="study_001",
        study_dir=Path("runs") / "cohort_studies" / "study_001",
        reference_batch_id="batch_005",
        candidate_batch_id="batch_010",
        candidate_decision="shortlist",
    )

    assessment = evaluate_study_source_context(
        payload,
        primary_batch_id="batch_005",
        secondary_batch_id="batch_011",
    )

    assert assessment.active is False
    assert assessment.stale is True
    assert "stale" in str(assessment.message).lower()


def test_build_study_source_artifact_fields_only_emits_active_context() -> None:
    active_assessment = evaluate_study_source_context(
        build_study_source_session_payload(
            study_id="study_001",
            study_dir=Path("runs") / "cohort_studies" / "study_001",
            reference_batch_id="batch_005",
            candidate_batch_id="batch_010",
            candidate_decision="shortlist",
            suggested_decision="review",
            selected_metric="ndcg_at_k",
        ),
        primary_batch_id="batch_005",
        secondary_batch_id="batch_010",
    )
    stale_assessment = evaluate_study_source_context(
        build_study_source_session_payload(
            study_id="study_001",
            study_dir=Path("runs") / "cohort_studies" / "study_001",
            reference_batch_id="batch_005",
            candidate_batch_id="batch_010",
        ),
        primary_batch_id="batch_999",
        secondary_batch_id="batch_010",
    )

    active_fields = build_study_source_artifact_fields(active_assessment)
    stale_fields = build_study_source_artifact_fields(stale_assessment)

    assert active_fields["source_context_active"] is True
    assert active_fields["source_candidate_decision"] == "shortlist"
    assert stale_fields == {}


def test_build_study_source_session_payload_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError):
        build_study_source_session_payload(
            study_id="",
            study_dir="runs/cohort_studies/study_001",
            reference_batch_id="batch_005",
            candidate_batch_id="batch_010",
        )


def test_extract_saved_study_source_fields_merges_manifest_and_decision_record() -> None:
    extracted = extract_saved_study_source_fields(
        {
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_study_dir": "runs/cohort_studies/study_001",
            "source_reference_batch_id": "batch_005",
            "source_candidate_batch_id": "batch_010",
        },
        {
            "source_candidate_decision": "shortlist",
            "source_suggested_decision": "review",
            "source_selected_metric": "ndcg_at_k",
            "source_context_active": True,
        },
    )

    assert extracted["source_study_id"] == "study_001"
    assert extracted["source_candidate_decision"] == "shortlist"
    assert extracted["source_selected_metric"] == "ndcg_at_k"


def test_extract_saved_study_source_fields_noops_when_absent_or_inactive() -> None:
    assert extract_saved_study_source_fields({}, {}) == {}
    assert extract_saved_study_source_fields(
        {"source_type": "cohort_study", "source_study_id": "study_001"},
        {"source_context_active": False},
    ) == {}


def test_load_saved_study_source_from_comparison_dir_handles_malformed_json(tmp_path: Path) -> None:
    comparison_dir = tmp_path / "comparison_001"
    comparison_dir.mkdir(parents=True)
    (comparison_dir / "comparison_manifest.json").write_text("{bad json", encoding="utf-8")
    (comparison_dir / "decision_record.json").write_text(
        json.dumps(
            {
                "source_type": "cohort_study",
                "source_study_id": "study_001",
                "source_candidate_decision": "shortlist",
                "source_context_active": True,
            }
        ),
        encoding="utf-8",
    )

    extracted = load_saved_study_source_from_comparison_dir(comparison_dir)

    assert extracted["source_study_id"] == "study_001"
    assert extracted["source_candidate_decision"] == "shortlist"


def test_build_review_packet_study_source_helpers_return_compact_payloads() -> None:
    source_payload = {
        "source_type": "cohort_study",
        "source_study_id": "study_001",
        "source_study_dir": "runs/cohort_studies/study_001",
        "source_reference_batch_id": "batch_005",
        "source_candidate_batch_id": "batch_010",
        "source_candidate_decision": "shortlist",
        "source_suggested_decision": "review",
        "source_selected_metric": "ndcg_at_k",
        "source_context_active": True,
    }

    manifest_fields = build_review_packet_study_source_fields(source_payload)
    evidence_block = build_evidence_summary_study_source_block(source_payload)

    assert manifest_fields["source_study_id"] == "study_001"
    assert "source_context_active" not in manifest_fields
    assert evidence_block is not None
    assert evidence_block["source_candidate_decision"] == "shortlist"


def test_extract_study_source_fields_supports_single_saved_upstream_payload() -> None:
    extracted = extract_study_source_fields(
        {
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_study_dir": "runs/cohort_studies/study_001",
            "source_candidate_decision": "shortlist",
            "source_selected_metric": "ndcg_at_k",
        }
    )

    assert extracted["source_type"] == "cohort_study"
    assert extracted["source_study_id"] == "study_001"
    assert extracted["source_candidate_decision"] == "shortlist"


def test_load_study_source_from_json_file_handles_absent_and_present_payloads(tmp_path: Path) -> None:
    manifest_path = tmp_path / "candidate_apply_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "source_type": "cohort_study",
                "source_study_id": "study_001",
                "source_study_dir": "runs/cohort_studies/study_001",
                "source_reference_batch_id": "batch_005",
                "source_candidate_batch_id": "batch_010",
            }
        ),
        encoding="utf-8",
    )

    extracted = load_study_source_from_json_file(manifest_path)
    missing = load_study_source_from_json_file(tmp_path / "missing.json")

    assert extracted["source_study_id"] == "study_001"
    assert missing == {}
