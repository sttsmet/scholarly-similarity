from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.agents.packet_builder import (
    apply_generator_reply,
    build_generator_packet,
    build_verifier_packet,
    record_verifier_reply,
)
from src.config import DEFAULT_THEORY_PATH, load_theory_config
from src.eval.benchmark import generate_silver_labels, run_theory_eval


def _make_record(
    *,
    openalex_id: str,
    title: str,
    publication_year: int | None = 2020,
    referenced_works: list[str] | None = None,
    related_works: list[str] | None = None,
    primary_topic: str | None = "Physics",
    topics: list[str] | None = None,
    abstract_text: str | None = "Quantum measurement experiment",
    candidate_origins: list[str] | None = None,
) -> dict[str, object]:
    return {
        "openalex_id": openalex_id,
        "doi": None,
        "title": title,
        "publication_year": publication_year,
        "cited_by_count": 10,
        "referenced_works": referenced_works or [],
        "related_works": related_works or [],
        "primary_topic": primary_topic,
        "topics": topics or [],
        "abstract_text": abstract_text,
        "candidate_origins": candidate_origins or [],
        "source": "openalex",
    }


def _write_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "runs" / "agent_packet_run"
    run_dir.mkdir(parents=True, exist_ok=True)

    seed = _make_record(
        openalex_id="https://openalex.org/WSEED",
        title="Seed paper",
        publication_year=2018,
        referenced_works=["https://openalex.org/WCAND1"],
        related_works=["https://openalex.org/WCAND2"],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol for coupled systems",
    )
    candidate_one = _make_record(
        openalex_id="https://openalex.org/WCAND1",
        title="Reference match",
        publication_year=2019,
        referenced_works=["https://openalex.org/W1", "https://openalex.org/W2"],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol with related systems",
        candidate_origins=["seed_reference", "direct_neighbor"],
    )
    candidate_two = _make_record(
        openalex_id="https://openalex.org/WCAND2",
        title="Related match",
        publication_year=2017,
        referenced_works=["https://openalex.org/W2"],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Measurement protocol in a related system",
        candidate_origins=["seed_related"],
    )
    candidate_three = _make_record(
        openalex_id="https://openalex.org/WCAND3",
        title="Hard negative",
        publication_year=2018,
        referenced_works=["https://openalex.org/W9"],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="A different but nearby topic",
        candidate_origins=["hard_negative"],
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two, candidate_three):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate_one["openalex_id"],
                    "edge_type": "seed_references",
                }
            )
        )
        handle.write("\n")
        handle.write(
            json.dumps(
                {
                    "src": seed["openalex_id"],
                    "dst": candidate_two["openalex_id"],
                    "edge_type": "seed_related",
                }
            )
        )
        handle.write("\n")

    return run_dir


def _write_theory_copy(tmp_path: Path, filename: str = "theory_agent.yaml") -> Path:
    destination = tmp_path / filename
    destination.write_text(DEFAULT_THEORY_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    return destination


def _write_modified_theory(tmp_path: Path, filename: str = "theory_candidate.yaml") -> Path:
    destination = tmp_path / filename
    payload = yaml.safe_load(DEFAULT_THEORY_PATH.read_text(encoding="utf-8"))
    payload["sim_weights"]["temporal"] = 0.25
    payload["sim_weights"]["semantic"] = 0.05
    destination.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return destination


def _prepare_experiments(tmp_path: Path) -> tuple[Path, Path, Path]:
    run_dir = _write_run(tmp_path)
    baseline_theory_path = _write_theory_copy(tmp_path)
    candidate_theory_path = _write_modified_theory(tmp_path)

    generate_silver_labels(run_dir=run_dir)
    run_theory_eval(
        run_dir=run_dir,
        theory_config_path=baseline_theory_path,
        theory=load_theory_config(baseline_theory_path),
        experiment_id="baseline",
        label_source="silver",
        top_k=3,
    )
    run_theory_eval(
        run_dir=run_dir,
        theory_config_path=candidate_theory_path,
        theory=load_theory_config(candidate_theory_path),
        experiment_id="candidate_a",
        label_source="silver",
        top_k=3,
    )
    return run_dir, baseline_theory_path, candidate_theory_path


def test_build_generator_packet_writes_expected_files(tmp_path: Path) -> None:
    run_dir, _, _ = _prepare_experiments(tmp_path)

    result = build_generator_packet(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        packet_id="packet_a",
    )

    context_path = Path(result.output_paths.generator_context_json)
    packet_path = Path(result.output_paths.generator_packet_md)
    template_path = Path(result.output_paths.generator_reply_template_yaml)

    assert packet_path.exists()
    assert template_path.exists()
    context = json.loads(context_path.read_text(encoding="utf-8"))
    allowed_paths = {item["path"] for item in context["allowed_theory_change_surface"]}
    assert context["baseline_experiment_id"] == "baseline"
    assert "sim_weights.bibliographic_coupling" in allowed_paths
    assert "confidence_parameters.maturity_tau" in allowed_paths
    assert "explanation.top_k_features" in allowed_paths
    assert "## Reply Schema" in packet_path.read_text(encoding="utf-8")


def test_apply_generator_reply_materializes_candidate_theory(tmp_path: Path) -> None:
    run_dir, _, _ = _prepare_experiments(tmp_path)
    build_generator_packet(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        packet_id="packet_b",
    )

    reply_path = tmp_path / "generator_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Increase temporal influence slightly.",
                "expected_effect": "Promote closer lineage neighbors by publication year.",
                "risks": ["Could over-penalize older foundational work."],
                "changes": [
                    {"path": "sim_weights.temporal", "value": 0.2},
                    {"path": "sim_weights.topical", "value": 0.15},
                    {"path": "sim_weights.semantic", "value": 0.05},
                    {"path": "confidence_parameters.maturity_tau", "value": 10.0},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = apply_generator_reply(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        packet_id="packet_b",
        reply_path=reply_path,
        candidate_id="cand_alpha",
    )

    candidate_theory = yaml.safe_load(Path(result.output_paths.candidate_theory_yaml).read_text(encoding="utf-8"))
    validated_reply = json.loads(
        Path(result.output_paths.generator_reply_validated_json).read_text(encoding="utf-8")
    )

    assert candidate_theory["sim_weights"]["temporal"] == 0.2
    assert candidate_theory["sim_weights"]["topical"] == 0.15
    assert candidate_theory["sim_weights"]["semantic"] == 0.05
    assert candidate_theory["confidence_parameters"]["maturity_tau"] == 10.0
    assert validated_reply["changes"][0]["path"] == "sim_weights.temporal"
    assert Path(result.output_paths.candidate_manifest_json).exists()


def test_apply_generator_reply_rejects_unknown_change_path(tmp_path: Path) -> None:
    run_dir, _, _ = _prepare_experiments(tmp_path)
    build_generator_packet(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        packet_id="packet_c",
    )
    reply_path = tmp_path / "bad_generator_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "summary": "Bad change",
                "expected_effect": "None",
                "risks": ["Invalid"],
                "changes": [
                    {"path": "candidate_pool.max_candidates", "value": 10},
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    try:
        apply_generator_reply(
            run_dir=run_dir,
            baseline_experiment_id="baseline",
            packet_id="packet_c",
            reply_path=reply_path,
            candidate_id="cand_bad",
        )
    except ValueError as exc:
        assert "not allowed" in str(exc)
    else:
        raise AssertionError("Expected generator reply validation to reject disallowed path")


def test_build_verifier_packet_writes_comparison_context(tmp_path: Path) -> None:
    run_dir, _, _ = _prepare_experiments(tmp_path)

    result = build_verifier_packet(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        candidate_experiment_id="candidate_a",
        packet_id="packet_d",
    )

    context = json.loads(Path(result.output_paths.verifier_context_json).read_text(encoding="utf-8"))
    packet_text = Path(result.output_paths.verifier_packet_md).read_text(encoding="utf-8")

    assert context["baseline_experiment_id"] == "baseline"
    assert context["candidate_experiment_id"] == "candidate_a"
    assert isinstance(context["metric_deltas"], dict)
    assert context["movement_diagnostics"] is not None
    assert "movement_diagnostic_note" in context["movement_diagnostics"]
    assert "## Rank Movement Diagnostics" in packet_text
    assert "## Metric Deltas" in packet_text
    assert Path(result.output_paths.verifier_reply_template_yaml).exists()


def test_record_verifier_reply_writes_validated_reply_and_decision(tmp_path: Path) -> None:
    run_dir, _, _ = _prepare_experiments(tmp_path)
    build_verifier_packet(
        run_dir=run_dir,
        baseline_experiment_id="baseline",
        candidate_experiment_id="candidate_a",
        packet_id="packet_e",
    )

    reply_path = tmp_path / "verifier_reply.yaml"
    reply_path.write_text(
        yaml.safe_dump(
            {
                "pass": True,
                "score": 0.75,
                "issues": ["Temporal gain is modest but positive."],
                "next_change": "Try a smaller direct citation adjustment next.",
                "notes": "Looks safe for another revision round.",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = record_verifier_reply(
        run_dir=run_dir,
        packet_id="packet_e",
        reply_path=reply_path,
    )

    decision_payload = json.loads(Path(result.output_paths.decision_json).read_text(encoding="utf-8"))
    assert result.verifier_pass is True
    assert result.verifier_score == 0.75
    assert decision_payload["verdict"]["pass"] is True
    assert Path(result.output_paths.verifier_reply_validated_json).exists()
