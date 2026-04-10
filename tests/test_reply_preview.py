from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.ui.reply_preview import (
    ReplyPreviewResult,
    flatten_baseline_scalar_leaves,
    load_review_packet_bundle,
    preview_candidate_reply,
)


def _write_packet_dir(tmp_path: Path) -> Path:
    packet_dir = tmp_path / "runs" / "comparisons" / "comparison_001" / "review_packets" / "packet_001"
    packet_dir.mkdir(parents=True)
    (packet_dir / "review_packet_manifest.json").write_text(
        json.dumps(
            {
                "packet_id": "packet_001",
                "comparison_id": "comparison_001",
                "selected_packet_metric": "ndcg_at_k",
                "primary_batch": {"batch_id": "batch_005"},
                "secondary_batch": {"batch_id": "batch_006"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (packet_dir / "allowed_revision_paths.json").write_text(
        json.dumps(
            {
                "allowed_scalar_paths": [
                    "sim_weights.bibliographic_coupling",
                    "sim_weights.direct_citation",
                    "sim_weights.topical",
                    "sim_weights.temporal",
                    "sim_weights.semantic",
                    "sim_parameters.temporal_tau",
                    "confidence_parameters.maturity_tau",
                    "explanation.style",
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (packet_dir / "baseline_theory_snapshot.yaml").write_text(
        "\n".join(
            [
                "sim_weights:",
                "  bibliographic_coupling: 0.35",
                "  direct_citation: 0.25",
                "  topical: 0.20",
                "  temporal: 0.1",
                "  semantic: 0.10",
                "sim_parameters:",
                "  temporal_tau: 5.0",
                "confidence_parameters:",
                "  maturity_tau: 8.0",
                "explanation:",
                "  style: compact",
                "  enabled: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (packet_dir / "candidate_reply_TEMPLATE.yaml").write_text(
        "\n".join(
            [
                "# TEMPLATE ONLY - not an actual generator reply",
                "packet_id: packet_001",
                "comparison_id: comparison_001",
                "baseline_theory_config: baseline_theory_snapshot.yaml",
                "proposed_changes: []",
                "rationale: \"\"",
                "notes: \"TEMPLATE ONLY - not an actual generator reply\"",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return packet_dir


def test_flatten_baseline_scalar_leaves_includes_all_scalar_types() -> None:
    flattened = flatten_baseline_scalar_leaves(
        {
            "sim_weights": {"temporal": 0.1},
            "explanation": {"style": "compact", "enabled": True, "notes": None},
        }
    )

    assert list(flattened) == [
        "explanation.enabled",
        "explanation.notes",
        "explanation.style",
        "sim_weights.temporal",
    ]
    assert flattened["explanation.style"].scalar_type == "string"
    assert flattened["explanation.enabled"].scalar_type == "bool"
    assert flattened["sim_weights.temporal"].is_numeric is True


def test_load_review_packet_bundle_reads_required_artifacts(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)

    bundle = load_review_packet_bundle(packet_dir)

    assert bundle.packet_dir == packet_dir
    assert bundle.manifest["packet_id"] == "packet_001"
    assert bundle.allowed_revision_paths == [
        "confidence_parameters.maturity_tau",
        "explanation.style",
        "sim_parameters.temporal_tau",
        "sim_weights.bibliographic_coupling",
        "sim_weights.direct_citation",
        "sim_weights.semantic",
        "sim_weights.temporal",
        "sim_weights.topical",
    ]
    assert bundle.template_path == packet_dir / "candidate_reply_TEMPLATE.yaml"
    assert bundle.baseline_scalar_leaves["explanation.style"].value == "compact"


def test_preview_candidate_reply_rejects_template_file_and_marker(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)
    bundle = load_review_packet_bundle(packet_dir)

    template_result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=packet_dir / "candidate_reply_TEMPLATE.yaml",
    )

    copied_template_path = packet_dir / "copied_template.yaml"
    copied_template_path.write_text(
        (packet_dir / "candidate_reply_TEMPLATE.yaml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    copied_result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=copied_template_path,
    )

    assert template_result.state == "template_only"
    assert copied_result.state == "template_only"
    assert "template" in template_result.errors[0].lower()


def test_preview_candidate_reply_validates_existing_generator_reply_schema(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)
    bundle = load_review_packet_bundle(packet_dir)
    reply_path = packet_dir / "generator_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Increase temporal signal modestly.",
                "expected_effect": "Promote closer lineage matches.",
                "risks": ["May over-favor recent work."],
                "changes": [
                    {"path": "sim_weights.temporal", "value": 0.2},
                    {"path": "confidence_parameters.maturity_tau", "value": 9.0},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=reply_path,
    )

    assert isinstance(result, ReplyPreviewResult)
    assert result.state == "valid"
    assert result.valid_change_count == 2
    assert result.invalid_change_count == 0
    assert result.diff_rows[0]["path"] == "sim_weights.temporal"
    assert result.diff_rows[0]["numeric_delta"] == 0.1


def test_preview_candidate_reply_rejects_duplicate_and_disallowed_paths(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)
    bundle = load_review_packet_bundle(packet_dir)
    reply_path = packet_dir / "bad_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Bad reply.",
                "expected_effect": "Unknown.",
                "risks": ["Invalid changes."],
                "changes": [
                    {"path": "sim_weights.temporal", "value": 0.3},
                    {"path": "sim_weights.temporal", "value": 0.4},
                    {"path": "candidate_pool.max_candidates", "value": 10},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=reply_path,
    )

    assert result.state == "invalid"
    assert result.valid_change_count == 1
    assert result.invalid_change_count == 2
    assert any("Duplicate change path" in (row.get("note") or "") for row in result.diff_rows)
    assert any("not allowed" in (row.get("note") or "") for row in result.diff_rows)


def test_preview_candidate_reply_rejects_string_change_under_current_numeric_schema(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)
    bundle = load_review_packet_bundle(packet_dir)
    reply_path = packet_dir / "string_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Try changing explanation style.",
                "expected_effect": "None.",
                "risks": ["Schema mismatch."],
                "changes": [
                    {"path": "explanation.style", "value": "expanded"},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=reply_path,
    )

    assert result.state == "invalid"
    assert result.invalid_change_count == 1
    assert "numeric scalar changes" in (result.diff_rows[0]["note"] or "")


def test_preview_candidate_reply_validates_constrained_revision_schema(tmp_path: Path) -> None:
    packet_dir = _write_packet_dir(tmp_path)
    bundle = load_review_packet_bundle(packet_dir)
    reply_path = packet_dir / "generator_reply_constrained.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "generator_round_id": "gen_round_001",
                "mode": "constrained_lineage_reweight_round1",
                "baseline_reference": "baseline_001",
                "revisions": [
                    {
                        "candidate_revision_id": "rev_001_legal_semantic_trim",
                        "priority": 1,
                        "type": "reweight",
                        "target": {
                            "sim_weights": {
                                "bibliographic_coupling": 0.35,
                                "direct_citation": 0.25,
                                "topical": 0.18,
                                "temporal": 0.12,
                                "semantic": 0.10,
                            },
                            "sim_parameters": {
                                "temporal_tau": 6.0,
                            },
                        },
                        "legality_check": {
                            "weights_sum": 1.0,
                            "non_negative": True,
                            "allowed_keys_only": True,
                            "local_change_only": True,
                        },
                        "hypothesis": "Small topical-to-temporal rebalance.",
                        "why_now": "Exercise constrained preview support.",
                        "expected_effect": {
                            "silver_global": "neutral",
                            "strong_lineage": "non_regression",
                            "ambiguous_middle": "neutral",
                            "hard_negative_or_distractor": "neutral",
                            "independent_benchmark": "veto_only_if_prototype",
                        },
                        "main_risk": "May slightly over-favor recency.",
                        "reject_if": ["global silver primary ranking metric drops by more than 0.03"],
                        "verifier_tests": ["invariant_check"],
                    }
                ],
                "summary": "Single constrained revision.",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = preview_candidate_reply(
        packet_bundle=bundle,
        reply_path=reply_path,
    )

    assert isinstance(result, ReplyPreviewResult)
    assert result.state == "valid"
    assert result.valid_change_count == 3
    assert [row["path"] for row in result.diff_rows] == [
        "sim_weights.topical",
        "sim_weights.temporal",
        "sim_parameters.temporal_tau",
    ]
