from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import src.ui.candidate_apply_run as candidate_apply_run
from src.ui.batch_runner import BatchRunOutcome, BatchRunSummary
from src.ui.candidate_apply_run import (
    CandidateApplyRunError,
    build_applied_changes_rows,
    build_batch_run_result_payload,
    build_candidate_apply_run_request,
    derive_parity_batch_request,
    packet_primary_compatibility_errors,
    run_candidate_apply_and_batch,
    validate_candidate_reply_for_apply,
)
from src.ui.reply_preview import load_review_packet_bundle


def _write_review_packet(tmp_path: Path, *, include_study_source: bool = False) -> Path:
    packet_dir = tmp_path / "runs" / "comparisons" / "comparison_001" / "review_packets" / "packet_001"
    packet_dir.mkdir(parents=True)
    (packet_dir / "review_packet_manifest.json").write_text(
        json.dumps(
            {
                "packet_id": "packet_001",
                "comparison_id": "comparison_001",
                "selected_packet_metric": "ndcg_at_k",
                "primary_batch": {
                    "batch_id": "batch_005",
                    "batch_dir": str(tmp_path / "runs" / "batches" / "batch_005"),
                    "theory_config": "configs/theory_v001.yaml",
                },
                **(
                    {
                        "source_type": "cohort_study",
                        "source_study_id": "study_001",
                        "source_study_dir": "runs/cohort_studies/study_001",
                        "source_reference_batch_id": "batch_005",
                        "source_candidate_batch_id": "batch_010",
                        "source_candidate_decision": "shortlist",
                        "source_suggested_decision": "review",
                        "source_selected_metric": "ndcg_at_k",
                    }
                    if include_study_source
                    else {}
                ),
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
                ]
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    baseline_payload = {
        "version": "theory_v001",
        "aspect": {
            "source": "openalex",
            "scope": "lineage",
            "note": "test packet",
        },
        "candidate_pool": {
            "max_candidates": 100,
            "include_related_works": True,
            "include_references": True,
            "include_citations": False,
            "dedupe_key": "openalex_id",
        },
        "sim_weights": {
            "bibliographic_coupling": 0.4,
            "direct_citation": 0.3,
            "topical": 0.2,
            "temporal": 0.1,
            "semantic": 0.0,
        },
        "sim_parameters": {
            "temporal_tau": 8.0,
        },
        "confidence_factors": {
            "coverage": 0.5,
            "support": 0.3,
            "maturity": 0.2,
        },
        "confidence_parameters": {
            "observation_year": 2024,
            "support_eta": 1.5,
            "maturity_tau": 8.0,
        },
        "explanation": {
            "top_k_features": 3,
            "include_raw_scores": True,
            "include_notes": True,
        },
    }
    (packet_dir / "baseline_theory_snapshot.yaml").write_text(
        yaml.safe_dump(baseline_payload, sort_keys=False),
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


def _write_valid_reply(packet_dir: Path) -> Path:
    reply_path = packet_dir / "candidate_reply.yaml"
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
                                "bibliographic_coupling": 0.4,
                                "direct_citation": 0.3,
                                "topical": 0.1,
                                "temporal": 0.2,
                                "semantic": 0.0,
                            },
                            "sim_parameters": {
                                "temporal_tau": 7.0,
                            },
                        },
                        "legality_check": {
                            "weights_sum": 1.0,
                            "non_negative": True,
                            "allowed_keys_only": True,
                            "local_change_only": True,
                        },
                        "hypothesis": "Trim topical continuity while preserving the lineage backbone.",
                        "why_now": "Probe the conservative constrained-round replacement path.",
                        "expected_effect": {
                            "silver_global": "neutral",
                            "strong_lineage": "non_regression",
                            "ambiguous_middle": "improve",
                            "hard_negative_or_distractor": "neutral",
                            "independent_benchmark": "veto_only_if_prototype",
                        },
                        "main_risk": "May slightly underweight topical continuity.",
                        "reject_if": [
                            "strong_lineage primary ranking metric drops by more than 0.02",
                            "global silver primary ranking metric drops by more than 0.03",
                        ],
                        "verifier_tests": [
                            "invariant_check",
                            "silver_global_non_regression",
                        ],
                    }
                ],
                "summary": "Single constrained revision for candidate apply tests.",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return reply_path


def _write_illegal_reply(packet_dir: Path) -> Path:
    reply_path = packet_dir / "candidate_reply_illegal.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "generator_round_id": "gen_round_001",
                "mode": "constrained_lineage_reweight_round1",
                "baseline_reference": "baseline_001",
                "revisions": [
                    {
                        "candidate_revision_id": "rev_002_illegal",
                        "priority": 1,
                        "type": "reweight",
                        "target": {
                            "sim_weights": {
                                "bibliographic_coupling": 0.4,
                                "direct_citation": 0.3,
                                "topical": 0.2,
                                "temporal": 0.2,
                                "semantic": 0.0,
                            }
                        },
                        "legality_check": {
                            "weights_sum": 1.1,
                            "non_negative": True,
                            "allowed_keys_only": True,
                            "local_change_only": True,
                        },
                        "hypothesis": "Illegal test reply.",
                        "why_now": "Exercise pre-run legality rejection.",
                        "expected_effect": {
                            "silver_global": "small_risk",
                            "strong_lineage": "small_risk",
                            "ambiguous_middle": "neutral",
                            "hard_negative_or_distractor": "neutral",
                            "independent_benchmark": "veto_only_if_prototype",
                        },
                        "main_risk": "Illegal simplex.",
                        "reject_if": ["global silver primary ranking metric drops by more than 0.03"],
                        "verifier_tests": ["invariant_check"],
                    }
                ],
                "summary": "Illegal constrained revision for pre-run rejection tests.",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return reply_path


def _write_valid_legacy_reply(packet_dir: Path) -> Path:
    reply_path = packet_dir / "candidate_reply_legacy.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Increase temporal weight slightly.",
                "expected_effect": "Improve lineage ordering on edge cases.",
                "risks": ["May over-emphasize recency if pushed too far."],
                "changes": [
                    {"path": "sim_weights.temporal", "value": 0.2},
                    {"path": "sim_weights.topical", "value": 0.1},
                    {"path": "confidence_parameters.maturity_tau", "value": 9.0},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return reply_path


def _primary_bundle(tmp_path: Path) -> SimpleNamespace:
    batch_dir = tmp_path / "runs" / "batches" / "batch_005"
    batch_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = batch_dir / "benchmark_labels_snapshot.csv"
    snapshot_path.write_text(
        "\n".join(
            [
                "seed_openalex_id,candidate_openalex_id,label",
                "https://openalex.org/WSEED,https://openalex.org/WCAND1,2",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return SimpleNamespace(
        batch_dir=batch_dir,
        manifest=SimpleNamespace(
            batch_id="batch_005",
            batch_dir=str(batch_dir),
            theory_config="configs/theory_v001.yaml",
            seeds_csv="data/benchmarks/seeds.csv",
            options=SimpleNamespace(
                max_references=10,
                max_related=10,
                max_hard_negatives=10,
                top_k=10,
                label_source="benchmark",
                evaluation_mode="independent_benchmark",
                metric_scope="local_corpus_ranking",
                benchmark_labels_path="data/benchmarks/benchmark_labels_independent_smoke.csv",
                benchmark_labels_snapshot_path=str(snapshot_path),
                benchmark_dataset_id="benchmark_dataset_001",
                benchmark_labels_sha256="labels_sha256_001",
                evidence_tier="independent_benchmark",
                refresh=False,
            ),
        ),
    )


def test_build_candidate_apply_run_request_normalizes_fields() -> None:
    request = build_candidate_apply_run_request(
        candidate_id=" candidate_001 ",
        output_batch_id=" batch_007 ",
        reviewer=" Alice ",
        notes=" Try the candidate. ",
    )

    assert request.candidate_id == "candidate_001"
    assert request.output_batch_id == "batch_007"
    assert request.reviewer == "Alice"
    assert request.notes == "Try the candidate."


def test_build_candidate_apply_run_request_rejects_invalid_ids() -> None:
    with pytest.raises(CandidateApplyRunError):
        build_candidate_apply_run_request(
            candidate_id=" ",
            output_batch_id="batch_007",
            reviewer="",
            notes="",
        )
    with pytest.raises(CandidateApplyRunError):
        build_candidate_apply_run_request(
            candidate_id="candidate_001",
            output_batch_id="nested/path",
            reviewer="",
            notes="",
        )


def test_packet_primary_compatibility_errors_detect_mismatch(tmp_path: Path) -> None:
    packet_dir = _write_review_packet(tmp_path)
    packet_bundle = load_review_packet_bundle(packet_dir)
    primary_bundle = _primary_bundle(tmp_path)
    primary_bundle.manifest.batch_id = "batch_other"

    errors = packet_primary_compatibility_errors(packet_bundle, primary_bundle)

    assert errors
    assert "mismatch" in errors[0].lower()


def test_derive_parity_batch_request_reuses_primary_manifest_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    theory_path = tmp_path / "candidate_theory.yaml"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    theory_path.write_text("version: theory_v001\n", encoding="utf-8")

    request = derive_parity_batch_request(
        primary_bundle=_primary_bundle(tmp_path),
        candidate_theory_snapshot_path=theory_path,
        output_batch_id="batch_007",
        runtime_loader=lambda: SimpleNamespace(runs_dir="runs"),
    )

    assert request.seeds_csv_path == seeds_path.resolve()
    assert request.theory_config_path == theory_path
    assert request.max_references == 10
    assert request.max_related == 10
    assert request.max_hard_negatives == 10
    assert request.top_k == 10
    assert request.label_source == "benchmark"
    assert request.evaluation_mode == "independent_benchmark"
    assert request.benchmark_labels_path is not None
    assert request.benchmark_labels_path.name == "benchmark_labels_snapshot.csv"
    assert request.benchmark_dataset_id == "benchmark_dataset_001"
    assert request.benchmark_labels_sha256 == "labels_sha256_001"
    assert request.batch_dir.parent == tmp_path / "runs" / "batches"
    assert request.batch_id.startswith("batch_007__")
    assert request.batch_dir.name == request.batch_id


def test_validate_candidate_reply_for_apply_and_change_rows_preserve_baseline(tmp_path: Path) -> None:
    packet_dir = _write_review_packet(tmp_path)
    reply_path = _write_valid_reply(packet_dir)
    packet_bundle = load_review_packet_bundle(packet_dir)
    baseline_before = copy.deepcopy(packet_bundle.baseline_theory_payload)

    validated_reply, resolved_reply_path = validate_candidate_reply_for_apply(
        packet_bundle=packet_bundle,
        reply_path=reply_path,
    )
    rows = build_applied_changes_rows(
        packet_bundle=packet_bundle,
        validated_reply=validated_reply,
    )

    assert resolved_reply_path == reply_path
    assert validated_reply.candidate_revision_id == "rev_001_legal_semantic_trim"
    assert [row["path"] for row in rows] == [
        "sim_weights.topical",
        "sim_weights.temporal",
        "sim_parameters.temporal_tau",
    ]
    assert rows[0]["numeric_delta"] == pytest.approx(-0.1)
    assert rows[1]["numeric_delta"] == pytest.approx(0.1)
    assert rows[2]["numeric_delta"] == pytest.approx(-1.0)
    assert packet_bundle.baseline_theory_payload == baseline_before


def test_run_candidate_apply_and_batch_writes_expected_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    packet_dir = _write_review_packet(tmp_path)
    reply_path = _write_valid_reply(packet_dir)
    packet_bundle = load_review_packet_bundle(packet_dir)
    primary_bundle = _primary_bundle(tmp_path)
    baseline_snapshot_text = packet_bundle.baseline_snapshot_path.read_text(encoding="utf-8")
    request = build_candidate_apply_run_request(
        candidate_id="candidate_001",
        output_batch_id="batch_007",
        reviewer="Alice",
        notes="Apply and rerun.",
    )

    def fake_batch_runner(batch_request, previous_bundle=None):
        loaded_bundle = SimpleNamespace(
            batch_dir=batch_request.batch_dir,
            manifest=SimpleNamespace(batch_id=batch_request.batch_id),
        )
        summary = BatchRunSummary(
            initial_doi_context=batch_request.initial_doi_context,
            batch_id=batch_request.batch_id,
            batch_dir=str(batch_request.batch_dir),
            theory_config=str(batch_request.theory_config_path),
            seeds_csv=str(batch_request.seeds_csv_path),
            seed_count=5,
            completed_seed_count=3,
            failed_seed_count=2,
            status="completed",
            output_paths={
                "aggregate_summary_json": str(batch_request.batch_dir / "aggregate_summary.json"),
            },
        )
        return BatchRunOutcome(
            success=True,
            request=batch_request,
            loaded_bundle=loaded_bundle,
            summary=summary,
            error_message=None,
            partial_bundle=None,
        )

    result = run_candidate_apply_and_batch(
        request=request,
        packet_bundle=packet_bundle,
        reply_path=reply_path,
        primary_bundle=primary_bundle,
        selected_metric="ndcg_at_k",
        batch_runner=fake_batch_runner,
    )

    assert result.status == "completed"
    assert result.candidate_dir == packet_dir / "candidate_runs" / "candidate_001"
    assert result.candidate_revision_id == "rev_001_legal_semantic_trim"
    assert result.output_batch_dir.parent == tmp_path / "runs" / "batches"
    assert result.output_batch_id.startswith("batch_007__")
    assert result.manifest_path.exists()
    assert result.copied_reply_path.exists()
    assert result.candidate_theory_snapshot_path.exists()
    assert result.applied_changes_path.exists()
    assert result.batch_run_request_path.exists()
    assert result.batch_run_result_path.exists()

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    batch_request_payload = json.loads(result.batch_run_request_path.read_text(encoding="utf-8"))
    batch_result_payload = json.loads(result.batch_run_result_path.read_text(encoding="utf-8"))
    applied_change_rows = [
        json.loads(line)
        for line in result.applied_changes_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    candidate_theory_payload = yaml.safe_load(result.candidate_theory_snapshot_path.read_text(encoding="utf-8"))

    assert manifest_payload["status"] == "completed"
    assert manifest_payload["candidate_revision_id"] == "rev_001_legal_semantic_trim"
    assert manifest_payload["requested_output_batch_id"] == "batch_007"
    assert manifest_payload["reply_yaml_path"].endswith("candidate_reply.yaml")
    assert manifest_payload["copied_reply_yaml"].endswith("candidate_reply.yaml")
    assert manifest_payload["evaluation_mode"] == "independent_benchmark"
    assert manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert manifest_payload["promotion_eligible"] is True
    assert batch_request_payload["requested_output_batch_id"] == "batch_007"
    assert batch_request_payload["batch_id"].startswith("batch_007__")
    assert batch_request_payload["seeds_csv"] == str(Path("data") / "benchmarks" / "seeds.csv")
    assert batch_request_payload["evaluation_mode"] == "independent_benchmark"
    assert batch_request_payload["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert batch_result_payload["status"] == "completed"
    assert batch_result_payload["completed_seed_count"] == 3
    assert batch_result_payload["promotion_eligible"] is True
    assert [row["path"] for row in applied_change_rows] == [
        "sim_weights.topical",
        "sim_weights.temporal",
        "sim_parameters.temporal_tau",
    ]
    assert candidate_theory_payload["sim_weights"] == {
        "bibliographic_coupling": pytest.approx(0.4),
        "direct_citation": pytest.approx(0.3),
        "topical": pytest.approx(0.1),
        "temporal": pytest.approx(0.2),
        "semantic": pytest.approx(0.0),
    }
    assert candidate_theory_payload["sim_parameters"]["temporal_tau"] == pytest.approx(7.0)
    assert packet_bundle.baseline_snapshot_path.read_text(encoding="utf-8") == baseline_snapshot_text


def test_run_candidate_apply_and_batch_propagates_saved_packet_study_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    packet_dir = _write_review_packet(tmp_path, include_study_source=True)
    reply_path = _write_valid_reply(packet_dir)
    packet_bundle = load_review_packet_bundle(packet_dir)
    request = build_candidate_apply_run_request(
        candidate_id="candidate_002",
        output_batch_id="batch_011",
        reviewer="Alice",
        notes="Apply and rerun.",
    )

    def fake_batch_runner(batch_request, previous_bundle=None):
        loaded_bundle = SimpleNamespace(
            batch_dir=batch_request.batch_dir,
            manifest=SimpleNamespace(batch_id=batch_request.batch_id),
        )
        summary = BatchRunSummary(
            initial_doi_context=batch_request.initial_doi_context,
            batch_id=batch_request.batch_id,
            batch_dir=str(batch_request.batch_dir),
            theory_config=str(batch_request.theory_config_path),
            seeds_csv=str(batch_request.seeds_csv_path),
            seed_count=5,
            completed_seed_count=3,
            failed_seed_count=2,
            status="completed",
            output_paths={},
        )
        return BatchRunOutcome(
            success=True,
            request=batch_request,
            loaded_bundle=loaded_bundle,
            summary=summary,
            error_message=None,
            partial_bundle=None,
        )

    result = run_candidate_apply_and_batch(
        request=request,
        packet_bundle=packet_bundle,
        reply_path=reply_path,
        primary_bundle=_primary_bundle(tmp_path),
        selected_metric="ndcg_at_k",
        batch_runner=fake_batch_runner,
    )

    manifest_payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    batch_request_payload = json.loads(result.batch_run_request_path.read_text(encoding="utf-8"))

    assert manifest_payload["source_study_id"] == "study_001"
    assert manifest_payload["source_candidate_decision"] == "shortlist"
    assert manifest_payload["candidate_revision_id"] == "rev_001_legal_semantic_trim"
    assert batch_request_payload["source_study_id"] == "study_001"
    assert batch_request_payload["source_selected_metric"] == "ndcg_at_k"


def test_run_candidate_apply_and_batch_refuses_existing_candidate_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    packet_dir = _write_review_packet(tmp_path)
    reply_path = _write_valid_reply(packet_dir)
    packet_bundle = load_review_packet_bundle(packet_dir)
    (packet_dir / "candidate_runs" / "candidate_001").mkdir(parents=True)

    with pytest.raises(CandidateApplyRunError):
        run_candidate_apply_and_batch(
            request=build_candidate_apply_run_request(
                candidate_id="candidate_001",
                output_batch_id="batch_007",
                reviewer="",
                notes="",
            ),
            packet_bundle=packet_bundle,
            reply_path=reply_path,
            primary_bundle=_primary_bundle(tmp_path),
            batch_runner=lambda *args, **kwargs: None,
        )


def test_derive_parity_batch_request_mints_fresh_namespace_every_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")
    theory_path = tmp_path / "candidate_theory.yaml"
    theory_path.write_text("version: theory_v001\n", encoding="utf-8")

    first = derive_parity_batch_request(
        primary_bundle=_primary_bundle(tmp_path),
        candidate_theory_snapshot_path=theory_path,
        output_batch_id="batch_007",
        runtime_loader=lambda: SimpleNamespace(runs_dir="runs"),
    )
    second = derive_parity_batch_request(
        primary_bundle=_primary_bundle(tmp_path),
        candidate_theory_snapshot_path=theory_path,
        output_batch_id="batch_007",
        runtime_loader=lambda: SimpleNamespace(runs_dir="runs"),
    )

    assert first.batch_id != second.batch_id
    assert first.batch_dir != second.batch_dir
    assert first.batch_id.startswith("batch_007__")
    assert second.batch_id.startswith("batch_007__")


def test_run_candidate_apply_and_batch_rejects_illegal_candidate_before_batch_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(candidate_apply_run, "REPO_ROOT", tmp_path)
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi\n10.1038/nphys1170\n", encoding="utf-8")

    packet_dir = _write_review_packet(tmp_path)
    reply_path = _write_illegal_reply(packet_dir)
    packet_bundle = load_review_packet_bundle(packet_dir)
    batch_runner_called = False

    def fake_batch_runner(*args, **kwargs):
        nonlocal batch_runner_called
        batch_runner_called = True
        raise AssertionError("batch runner should not execute for an illegal candidate")

    with pytest.raises(CandidateApplyRunError):
        run_candidate_apply_and_batch(
            request=build_candidate_apply_run_request(
                candidate_id="candidate_illegal",
                output_batch_id="batch_008",
                reviewer="",
                notes="",
            ),
            packet_bundle=packet_bundle,
            reply_path=reply_path,
            primary_bundle=_primary_bundle(tmp_path),
            batch_runner=fake_batch_runner,
        )

    assert batch_runner_called is False
    candidate_dir = packet_dir / "candidate_runs" / "candidate_illegal"
    manifest_payload = json.loads((candidate_dir / "candidate_apply_manifest.json").read_text(encoding="utf-8"))
    batch_result_payload = json.loads((candidate_dir / "batch_run_result.json").read_text(encoding="utf-8"))

    assert manifest_payload["status"] == "failed"
    assert manifest_payload["error_message"] is not None
    assert "sum to exactly 1.0" in manifest_payload["error_message"]
    assert batch_result_payload["status"] == "failed"
    assert "sum to exactly 1.0" in (batch_result_payload["error_message"] or "")


def test_build_batch_run_result_payload_reports_failure_error() -> None:
    request = SimpleNamespace(
        batch_id="batch_007",
        batch_dir=Path("runs") / "batches" / "batch_007",
    )

    payload = build_batch_run_result_payload(
        request=request,
        error=RuntimeError("batch failed"),
    )

    assert payload["status"] == "failed"
    assert payload["error_type"] == "RuntimeError"
    assert payload["error_message"] == "batch failed"
