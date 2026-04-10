from __future__ import annotations

from src.agents.revision_validator import (
    GeneratorChangeModel,
    GeneratorReplyModel,
    apply_generator_changes,
    validate_generator_reply_payload,
)


def _baseline_theory() -> dict[str, object]:
    return {
        "sim_weights": {
            "bibliographic_coupling": 0.35,
            "direct_citation": 0.25,
            "topical": 0.20,
            "temporal": 0.10,
            "semantic": 0.10,
            "graph_path": 0.0,
        },
        "sim_parameters": {
            "temporal_tau": 5.0,
            "graph_path": {
                "max_bridge_nodes": 40,
                "allowed_path_lengths": [2, 3],
                "related_edge_weight": 0.50,
                "path_length_decay": 0.60,
                "saturation_kappa": 1.00,
            },
        },
        "confidence_factors": {
            "coverage": 0.4,
            "support": 0.35,
            "maturity": 0.25,
        },
        "confidence_parameters": {
            "observation_year": 2026,
            "support_eta": 3.0,
            "maturity_tau": 8.0,
        },
        "explanation": {
            "top_k_features": 3,
            "include_raw_scores": True,
            "include_notes": True,
        },
    }


def test_apply_generator_changes_replaces_full_sim_weights_block_exactly() -> None:
    target_weights = {
        "bibliographic_coupling": 0.40,
        "direct_citation": 0.30,
        "topical": 0.18,
        "temporal": 0.07,
        "semantic": 0.05,
    }
    validated_reply = GeneratorReplyModel(
        summary="Replace the constrained weight simplex.",
        expected_effect="Exercise exact block replacement.",
        risks=["Test only."],
        changes=[GeneratorChangeModel(path="sim_weights.temporal", value=0.07)],
        candidate_revision_id="rev_001_legal_semantic_trim",
        generator_round_id="gen_round_001",
        mode="constrained_lineage_reweight_round1",
        target={"sim_weights": target_weights},
        reply_format="constrained",
    )

    updated = apply_generator_changes(
        theory_payload=_baseline_theory(),
        validated_reply=validated_reply,
    )

    assert updated["sim_weights"] == target_weights


def test_validate_generator_reply_payload_preserves_candidate_revision_id() -> None:
    payload = {
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
                    }
                },
                "legality_check": {
                    "weights_sum": 1.0,
                    "non_negative": True,
                    "allowed_keys_only": True,
                    "local_change_only": True,
                },
                "hypothesis": "Small topical-to-temporal rebalance.",
                "why_now": "Exercise constrained reply normalization.",
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
    }

    validated = validate_generator_reply_payload(
        payload=payload,
        theory_payload=_baseline_theory(),
    )

    assert validated.reply_format == "constrained"
    assert validated.candidate_revision_id == "rev_001_legal_semantic_trim"
    assert [change.path for change in validated.changes] == [
        "sim_weights.topical",
        "sim_weights.temporal",
    ]


def test_validate_generator_reply_payload_accepts_graph_path_a1_mode() -> None:
    payload = {
        "generator_round_id": "gen_round_graph_a1",
        "mode": "constrained_lineage_graph_path_a1",
        "baseline_reference": "baseline_001",
        "revisions": [
            {
                "candidate_revision_id": "rev_graph_001",
                "priority": 1,
                "type": "reweight",
                "target": {
                    "sim_weights": {
                        "bibliographic_coupling": 0.30,
                        "direct_citation": 0.25,
                        "topical": 0.15,
                        "temporal": 0.10,
                        "semantic": 0.10,
                        "graph_path": 0.10,
                    },
                    "sim_parameters": {
                        "graph_path": {
                            "related_edge_weight": 0.25,
                            "path_length_decay": 0.80,
                        }
                    },
                },
                "legality_check": {
                    "weights_sum": 1.0,
                    "non_negative": True,
                    "allowed_keys_only": True,
                    "local_change_only": True,
                },
                "hypothesis": "Add conservative bridge-path support.",
                "why_now": "Open the A1 graph surface.",
                "expected_effect": {
                    "silver_global": "neutral",
                    "strong_lineage": "non_regression",
                    "ambiguous_middle": "improve",
                    "hard_negative_or_distractor": "protect",
                    "independent_benchmark": "supportive_only",
                },
                "main_risk": "May over-credit weak related chains.",
                "reject_if": ["hard-negative intrusion rises"],
                "verifier_tests": ["invariant_check"],
            }
        ],
        "summary": "Single A1 graph revision.",
    }

    validated = validate_generator_reply_payload(
        payload=payload,
        theory_payload=_baseline_theory(),
    )

    assert validated.reply_format == "constrained"
    assert validated.mode == "constrained_lineage_graph_path_a1"
    assert [change.path for change in validated.changes] == [
        "sim_weights.bibliographic_coupling",
        "sim_weights.topical",
        "sim_weights.graph_path",
        "sim_parameters.graph_path.path_length_decay",
        "sim_parameters.graph_path.related_edge_weight",
    ]


def test_validate_generator_reply_payload_rejects_temporal_tau_in_graph_path_a1_mode() -> None:
    payload = {
        "generator_round_id": "gen_round_graph_a1",
        "mode": "constrained_lineage_graph_path_a1",
        "revisions": [
            {
                "candidate_revision_id": "rev_graph_bad",
                "priority": 1,
                "type": "reweight",
                "target": {
                    "sim_weights": {
                        "bibliographic_coupling": 0.30,
                        "direct_citation": 0.25,
                        "topical": 0.15,
                        "temporal": 0.10,
                        "semantic": 0.10,
                        "graph_path": 0.10,
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
                "hypothesis": "Illegal mixed surface.",
                "why_now": "Should be rejected.",
                "expected_effect": {
                    "silver_global": "neutral",
                    "strong_lineage": "non_regression",
                    "ambiguous_middle": "neutral",
                    "hard_negative_or_distractor": "protect",
                    "independent_benchmark": "supportive_only",
                },
                "main_risk": "Illegal surface.",
                "reject_if": ["any invariant fails"],
                "verifier_tests": ["invariant_check"],
            }
        ],
        "summary": "Illegal A1 graph revision.",
    }

    try:
        validate_generator_reply_payload(
            payload=payload,
            theory_payload=_baseline_theory(),
        )
    except ValueError as exc:
        assert "does not allow temporal_tau changes" in str(exc)
    else:
        raise AssertionError("Expected graph-path A1 mode to reject temporal_tau changes")
