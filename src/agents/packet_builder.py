from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict

from src.agents.reply_parser import parse_structured_reply
from src.agents.revision_validator import (
    GeneratorReplyModel,
    VerifierReplyModel,
    allowed_theory_change_specs,
    apply_generator_changes,
    validate_applied_candidate_theory,
    validate_generator_reply_payload,
    validate_verifier_reply_payload,
)
from src.config import TheoryConfig
from src.ui.comparison import (
    comparison_metric_summary,
    movement_diagnostics_payload,
    paired_metric_rows,
)


TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
AGENT_LOOP_DIRNAME = "agent_loops"
EXPERIMENT_DIRNAME = "experiments"
CASE_LIMIT = 3

GENERATOR_REPLY_SCHEMA = {
    "summary": "Short summary of the proposed scalar theory revision.",
    "expected_effect": "Expected effect on lineage-oriented ranking/evaluation.",
    "risks": [
        "Short bullet strings describing risks or tradeoffs.",
    ],
    "changes": [
        {
            "path": "sim_weights.temporal",
            "value": 0.15,
        }
    ],
}

VERIFIER_REPLY_SCHEMA = {
    "pass": False,
    "score": 0.0,
    "issues": [
        "Short issue strings about the candidate experiment.",
    ],
    "next_change": "Concrete next scalar revision to try.",
    "notes": "Optional additional notes.",
}


class GeneratorPacketOutputPaths(BaseModel):
    """Files written for one generator packet."""

    model_config = ConfigDict(extra="forbid")

    packet_dir: str
    generator_packet_md: str
    generator_reply_template_yaml: str
    generator_context_json: str


class GeneratorPacketBuildResult(BaseModel):
    """Structured result returned by generator packet creation."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    packet_id: str
    baseline_experiment_id: str
    output_paths: GeneratorPacketOutputPaths


class CandidateMaterializationPaths(BaseModel):
    """Files written when a validated generator reply is applied."""

    model_config = ConfigDict(extra="forbid")

    candidate_dir: str
    candidate_theory_yaml: str
    candidate_manifest_json: str
    generator_reply_validated_json: str


class GeneratorReplyApplicationResult(BaseModel):
    """Structured result returned after materializing a candidate theory."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    packet_id: str
    candidate_id: str
    candidate_revision_id: str | None = None
    baseline_experiment_id: str
    changed_paths: list[str]
    output_paths: CandidateMaterializationPaths


class VerifierPacketOutputPaths(BaseModel):
    """Files written for one verifier packet."""

    model_config = ConfigDict(extra="forbid")

    packet_dir: str
    verifier_packet_md: str
    verifier_reply_template_yaml: str
    verifier_context_json: str


class VerifierPacketBuildResult(BaseModel):
    """Structured result returned by verifier packet creation."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    packet_id: str
    baseline_experiment_id: str
    candidate_experiment_id: str
    output_paths: VerifierPacketOutputPaths


class VerifierReplyRecordPaths(BaseModel):
    """Files written when a verifier reply is validated and recorded."""

    model_config = ConfigDict(extra="forbid")

    verifier_reply_validated_json: str
    decision_json: str


class VerifierReplyRecordResult(BaseModel):
    """Structured result returned after recording a verifier reply."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    packet_id: str
    verifier_pass: bool
    verifier_score: float
    output_paths: VerifierReplyRecordPaths


def build_generator_packet(
    *,
    run_dir: Path,
    baseline_experiment_id: str,
    packet_id: str,
) -> GeneratorPacketBuildResult:
    """Write a generator packet from an existing baseline experiment."""

    packet_dir = _packet_dir(run_dir, packet_id)
    baseline_dir = _experiment_dir(run_dir, baseline_experiment_id)
    baseline_theory_path = baseline_dir / "theory_snapshot.yaml"
    baseline_ranking_summary_path = baseline_dir / "ranking_summary.json"
    baseline_evaluation_summary_path = baseline_dir / "evaluation_summary.json"
    baseline_cases_path = baseline_dir / "evaluation_cases.json"

    baseline_theory = _load_theory_snapshot(baseline_theory_path)
    baseline_ranking_summary = _load_json_object(baseline_ranking_summary_path)
    baseline_evaluation_summary = _load_json_object(baseline_evaluation_summary_path)
    baseline_cases = _load_optional_json_object(baseline_cases_path)
    allowed_change_specs = allowed_theory_change_specs(baseline_theory.model_dump(mode="json"))

    context = {
        "objective": (
            "Propose one small auditable theory revision that may improve lineage-oriented "
            "ranking quality relative to the baseline experiment."
        ),
        "aspect": baseline_theory.aspect.model_dump(mode="json"),
        "baseline_experiment_id": baseline_experiment_id,
        "baseline_paths": {
            "theory_snapshot_yaml": str(baseline_theory_path),
            "ranking_summary_json": str(baseline_ranking_summary_path),
            "evaluation_summary_json": str(baseline_evaluation_summary_path),
            "evaluation_cases_json": str(baseline_cases_path),
        },
        "baseline_metrics": baseline_evaluation_summary.get("metrics", {}),
        "evaluation_context": _evaluation_context_payload(baseline_evaluation_summary),
        "score_ranges": baseline_ranking_summary.get("score_ranges", {}),
        "selected_diagnostic_cases": _select_diagnostic_cases(baseline_cases),
        "hard_constraints": _generator_hard_constraints(),
        "allowed_theory_change_surface": [
            {
                "path": path,
                "current_value": spec["current_value"],
                "value_type": spec["value_type"],
            }
            for path, spec in allowed_change_specs.items()
        ],
        "reply_schema": GENERATOR_REPLY_SCHEMA,
    }

    generator_packet_path = packet_dir / "generator_packet.md"
    generator_reply_template_path = packet_dir / "generator_reply_template.yaml"
    generator_context_path = packet_dir / "generator_context.json"

    generator_context_path.write_text(json.dumps(context, indent=2, sort_keys=True), encoding="utf-8")
    generator_reply_template_path.write_text(
        yaml.safe_dump(GENERATOR_REPLY_SCHEMA, sort_keys=False),
        encoding="utf-8",
    )
    generator_packet_path.write_text(
        _render_generator_packet(
            template_text=load_template("generator_packet.md"),
            context=context,
        ),
        encoding="utf-8",
    )

    return GeneratorPacketBuildResult(
        run_dir=str(run_dir),
        packet_id=packet_id,
        baseline_experiment_id=baseline_experiment_id,
        output_paths=GeneratorPacketOutputPaths(
            packet_dir=str(packet_dir),
            generator_packet_md=str(generator_packet_path),
            generator_reply_template_yaml=str(generator_reply_template_path),
            generator_context_json=str(generator_context_path),
        ),
    )


def apply_generator_reply(
    *,
    run_dir: Path,
    baseline_experiment_id: str,
    packet_id: str,
    reply_path: Path,
    candidate_id: str,
) -> GeneratorReplyApplicationResult:
    """Validate a generator reply and materialize a candidate theory YAML."""

    packet_dir = _packet_dir(run_dir, packet_id)
    baseline_dir = _experiment_dir(run_dir, baseline_experiment_id)
    baseline_theory_path = baseline_dir / "theory_snapshot.yaml"
    generator_context_path = packet_dir / "generator_context.json"

    _require_file(generator_context_path)
    baseline_theory_payload = _load_yaml_object(baseline_theory_path)
    context = _load_json_object(generator_context_path)
    context_baseline_id = str(context.get("baseline_experiment_id", "")).strip()
    if context_baseline_id and context_baseline_id != baseline_experiment_id:
        raise ValueError(
            "Generator packet baseline experiment mismatch: "
            f"context has '{context_baseline_id}', command used '{baseline_experiment_id}'"
        )

    validated_reply = validate_generator_reply_payload(
        payload=parse_structured_reply(reply_path.read_text(encoding="utf-8")),
        theory_payload=baseline_theory_payload,
    )
    updated_payload = apply_generator_changes(
        theory_payload=baseline_theory_payload,
        validated_reply=validated_reply,
    )
    validate_applied_candidate_theory(
        baseline_payload=baseline_theory_payload,
        updated_payload=updated_payload,
        validated_reply=validated_reply,
    )
    validated_theory = TheoryConfig.model_validate(updated_payload)

    safe_candidate_id = _validate_artifact_id(candidate_id, "candidate_id")
    candidate_dir = packet_dir / safe_candidate_id
    candidate_dir.mkdir(parents=True, exist_ok=True)

    validated_reply_path = candidate_dir / "generator_reply_validated.json"
    candidate_theory_path = candidate_dir / "candidate_theory.yaml"
    candidate_manifest_path = candidate_dir / "candidate_manifest.json"

    validated_reply_path.write_text(
        json.dumps(validated_reply.model_dump(by_alias=True), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    candidate_theory_path.write_text(
        yaml.safe_dump(
            validated_theory.model_dump(mode="json", exclude_defaults=True),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    candidate_manifest_path.write_text(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "packet_id": packet_id,
                "baseline_experiment_id": baseline_experiment_id,
                "candidate_id": safe_candidate_id,
                "candidate_revision_id": validated_reply.candidate_revision_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "baseline_theory_snapshot": str(baseline_theory_path),
                "reply_path": str(reply_path),
                "changed_paths": [change.path for change in validated_reply.changes],
                "output_paths": {
                    "candidate_theory_yaml": str(candidate_theory_path),
                    "candidate_manifest_json": str(candidate_manifest_path),
                    "generator_reply_validated_json": str(validated_reply_path),
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    return GeneratorReplyApplicationResult(
        run_dir=str(run_dir),
        packet_id=packet_id,
        candidate_id=safe_candidate_id,
        candidate_revision_id=validated_reply.candidate_revision_id,
        baseline_experiment_id=baseline_experiment_id,
        changed_paths=[change.path for change in validated_reply.changes],
        output_paths=CandidateMaterializationPaths(
            candidate_dir=str(candidate_dir),
            candidate_theory_yaml=str(candidate_theory_path),
            candidate_manifest_json=str(candidate_manifest_path),
            generator_reply_validated_json=str(validated_reply_path),
        ),
    )


def build_verifier_packet(
    *,
    run_dir: Path,
    baseline_experiment_id: str,
    candidate_experiment_id: str,
    packet_id: str,
) -> VerifierPacketBuildResult:
    """Write a verifier packet comparing baseline and candidate experiments."""

    packet_dir = _packet_dir(run_dir, packet_id)
    baseline_dir = _experiment_dir(run_dir, baseline_experiment_id)
    candidate_dir = _experiment_dir(run_dir, candidate_experiment_id)

    baseline_eval = _load_json_object(baseline_dir / "evaluation_summary.json")
    candidate_eval = _load_json_object(candidate_dir / "evaluation_summary.json")
    baseline_cases = _load_optional_json_object(baseline_dir / "evaluation_cases.json")
    candidate_cases = _load_optional_json_object(candidate_dir / "evaluation_cases.json")

    context = {
        "baseline_experiment_id": baseline_experiment_id,
        "candidate_experiment_id": candidate_experiment_id,
        "baseline_paths": {
            "experiment_dir": str(baseline_dir),
            "evaluation_summary_json": str(baseline_dir / "evaluation_summary.json"),
            "evaluation_cases_json": str(baseline_dir / "evaluation_cases.json"),
        },
        "candidate_paths": {
            "experiment_dir": str(candidate_dir),
            "evaluation_summary_json": str(candidate_dir / "evaluation_summary.json"),
            "evaluation_cases_json": str(candidate_dir / "evaluation_cases.json"),
        },
        "baseline_metrics": baseline_eval.get("metrics", {}),
        "candidate_metrics": candidate_eval.get("metrics", {}),
        "baseline_evaluation_context": _evaluation_context_payload(baseline_eval),
        "candidate_evaluation_context": _evaluation_context_payload(candidate_eval),
        "shared_evidence_context": _shared_evidence_context(baseline_eval, candidate_eval),
        "metric_deltas": _metric_deltas(
            baseline_eval.get("metrics", {}),
            candidate_eval.get("metrics", {}),
        ),
        "movement_diagnostics": _verifier_movement_diagnostics(
            baseline_dir=baseline_dir,
            candidate_dir=candidate_dir,
            baseline_eval=baseline_eval,
            candidate_eval=candidate_eval,
        ),
        "comparison_cases": {
            "top_false_positives_before": _case_list(baseline_cases, "top_false_positives"),
            "top_false_positives_after": _case_list(candidate_cases, "top_false_positives"),
            "top_strong_relevants_before": _case_list(baseline_cases, "top_strong_relevants"),
            "top_strong_relevants_after": _case_list(candidate_cases, "top_strong_relevants"),
        },
        "hard_constraints": _verifier_hard_constraints(),
        "reply_schema": VERIFIER_REPLY_SCHEMA,
    }

    verifier_packet_path = packet_dir / "verifier_packet.md"
    verifier_reply_template_path = packet_dir / "verifier_reply_template.yaml"
    verifier_context_path = packet_dir / "verifier_context.json"

    verifier_context_path.write_text(json.dumps(context, indent=2, sort_keys=True), encoding="utf-8")
    verifier_reply_template_path.write_text(
        yaml.safe_dump(VERIFIER_REPLY_SCHEMA, sort_keys=False),
        encoding="utf-8",
    )
    verifier_packet_path.write_text(
        _render_verifier_packet(
            template_text=load_template("verifier_packet.md"),
            context=context,
        ),
        encoding="utf-8",
    )

    return VerifierPacketBuildResult(
        run_dir=str(run_dir),
        packet_id=packet_id,
        baseline_experiment_id=baseline_experiment_id,
        candidate_experiment_id=candidate_experiment_id,
        output_paths=VerifierPacketOutputPaths(
            packet_dir=str(packet_dir),
            verifier_packet_md=str(verifier_packet_path),
            verifier_reply_template_yaml=str(verifier_reply_template_path),
            verifier_context_json=str(verifier_context_path),
        ),
    )


def record_verifier_reply(
    *,
    run_dir: Path,
    packet_id: str,
    reply_path: Path,
) -> VerifierReplyRecordResult:
    """Validate and record a structured verifier reply."""

    packet_dir = _packet_dir(run_dir, packet_id)
    verifier_context_path = packet_dir / "verifier_context.json"
    _require_file(verifier_context_path)
    context = _load_json_object(verifier_context_path)

    validated_reply = validate_verifier_reply_payload(
        payload=parse_structured_reply(reply_path.read_text(encoding="utf-8"))
    )

    validated_reply_path = packet_dir / "verifier_reply_validated.json"
    decision_path = packet_dir / "decision.json"
    validated_reply_path.write_text(
        json.dumps(validated_reply.model_dump(by_alias=True), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    decision_path.write_text(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "packet_id": packet_id,
                "baseline_experiment_id": context.get("baseline_experiment_id"),
                "candidate_experiment_id": context.get("candidate_experiment_id"),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "reply_path": str(reply_path),
                "verdict": validated_reply.model_dump(by_alias=True),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    return VerifierReplyRecordResult(
        run_dir=str(run_dir),
        packet_id=packet_id,
        verifier_pass=validated_reply.pass_verdict,
        verifier_score=validated_reply.score,
        output_paths=VerifierReplyRecordPaths(
            verifier_reply_validated_json=str(validated_reply_path),
            decision_json=str(decision_path),
        ),
    )


def load_template(template_name: str) -> str:
    """Load a packet markdown template from the repo-level agents template directory."""

    return (TEMPLATE_DIR / template_name).read_text(encoding="utf-8")


def write_packet(path: str | Path, packet: dict[str, Any]) -> Path:
    """Compatibility helper for writing JSON packet payloads to disk."""

    packet_path = Path(path)
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(json.dumps(packet, indent=2, sort_keys=True), encoding="utf-8")
    return packet_path


def _render_generator_packet(*, template_text: str, context: dict[str, object]) -> str:
    allowed_paths = context["allowed_theory_change_surface"]
    allowed_path_lines = "\n".join(
        f"- `{item['path']}` (current={item['current_value']}, type={item['value_type']})"
        for item in allowed_paths
    )
    diagnostics = json.dumps(context["selected_diagnostic_cases"], indent=2, sort_keys=True)
    metrics = json.dumps(context["baseline_metrics"], indent=2, sort_keys=True)
    evaluation_context = json.dumps(context["evaluation_context"], indent=2, sort_keys=True)
    score_ranges = json.dumps(context["score_ranges"], indent=2, sort_keys=True)
    reply_schema = yaml.safe_dump(context["reply_schema"], sort_keys=False).rstrip()
    hard_constraints = "\n".join(f"- {item}" for item in context["hard_constraints"])

    return "\n".join(
        [
            template_text.rstrip(),
            "",
            "## Objective",
            str(context["objective"]),
            "",
            "## Aspect",
            json.dumps(context["aspect"], indent=2, sort_keys=True),
            "",
            "## Baseline",
            f"- Experiment id: `{context['baseline_experiment_id']}`",
            f"- Theory snapshot: `{context['baseline_paths']['theory_snapshot_yaml']}`",
            "",
            "## Baseline Metrics",
            "```json",
            metrics,
            "```",
            "",
            "## Evaluation Context",
            "```json",
            evaluation_context,
            "```",
            "",
            "## Score Ranges",
            "```json",
            score_ranges,
            "```",
            "",
            "## Diagnostic Cases",
            "```json",
            diagnostics,
            "```",
            "",
            "## Hard Constraints",
            hard_constraints,
            "",
            "## Allowed Theory Change Surface",
            allowed_path_lines,
            "",
            "## Reply Schema",
            "```yaml",
            reply_schema,
            "```",
        ]
    )


def _render_verifier_packet(*, template_text: str, context: dict[str, object]) -> str:
    baseline_metrics = json.dumps(context["baseline_metrics"], indent=2, sort_keys=True)
    candidate_metrics = json.dumps(context["candidate_metrics"], indent=2, sort_keys=True)
    baseline_evaluation_context = json.dumps(
        context["baseline_evaluation_context"],
        indent=2,
        sort_keys=True,
    )
    candidate_evaluation_context = json.dumps(
        context["candidate_evaluation_context"],
        indent=2,
        sort_keys=True,
    )
    shared_evidence_context = json.dumps(context["shared_evidence_context"], indent=2, sort_keys=True)
    metric_deltas = json.dumps(context["metric_deltas"], indent=2, sort_keys=True)
    movement_diagnostics = json.dumps(context["movement_diagnostics"], indent=2, sort_keys=True)
    comparison_cases = json.dumps(context["comparison_cases"], indent=2, sort_keys=True)
    reply_schema = yaml.safe_dump(context["reply_schema"], sort_keys=False).rstrip()
    hard_constraints = "\n".join(f"- {item}" for item in context["hard_constraints"])

    return "\n".join(
        [
            template_text.rstrip(),
            "",
            "## Baseline Experiment",
            f"`{context['baseline_experiment_id']}`",
            "",
            "## Candidate Experiment",
            f"`{context['candidate_experiment_id']}`",
            "",
            "## Baseline Metrics",
            "```json",
            baseline_metrics,
            "```",
            "",
            "## Baseline Evaluation Context",
            "```json",
            baseline_evaluation_context,
            "```",
            "",
            "## Candidate Metrics",
            "```json",
            candidate_metrics,
            "```",
            "",
            "## Candidate Evaluation Context",
            "```json",
            candidate_evaluation_context,
            "```",
            "",
            "## Shared Evidence Context",
            "```json",
            shared_evidence_context,
            "```",
            "",
            "## Metric Deltas",
            "```json",
            metric_deltas,
            "```",
            "",
            "## Rank Movement Diagnostics",
            "```json",
            movement_diagnostics,
            "```",
            "",
            "## Comparison Cases",
            "```json",
            comparison_cases,
            "```",
            "",
            "## Hard Constraints",
            hard_constraints,
            "",
            "## Reply Schema",
            "```yaml",
            reply_schema,
            "```",
        ]
    )


def _generator_hard_constraints() -> list[str]:
    return [
        "Return structured YAML only.",
        "Change only the allowed scalar dotted paths already present in the theory config.",
        "Do not add new top-level sections or rename keys.",
        "Do not propose repo code changes, data changes, or manual label changes.",
        "Do not change the scoring formula structure or silver-label policy in this packet.",
        "Keep the proposal local, deterministic, and auditable.",
    ]


def _verifier_hard_constraints() -> list[str]:
    return [
        "Assess only the observed experiment outputs and metric deltas.",
        "Do not invent external evidence or call external services.",
        "Return structured YAML only.",
        "Do not apply deterministic accept/reject automation in this reply.",
        "Keep the verdict auditable and tied to the provided experiment artifacts.",
    ]


def _select_diagnostic_cases(cases_payload: dict[str, Any] | None) -> dict[str, object]:
    if not isinstance(cases_payload, dict):
        return {}
    return {
        key: value[:CASE_LIMIT]
        for key, value in cases_payload.items()
        if isinstance(value, list)
    }


def _case_list(cases_payload: dict[str, Any] | None, key: str) -> list[object]:
    if not isinstance(cases_payload, dict):
        return []
    value = cases_payload.get(key)
    if not isinstance(value, list):
        return []
    return value[:CASE_LIMIT]


def _metric_deltas(
    baseline_metrics: object,
    candidate_metrics: object,
    prefix: str = "",
) -> dict[str, float]:
    deltas: dict[str, float] = {}
    if not isinstance(baseline_metrics, dict) or not isinstance(candidate_metrics, dict):
        return deltas

    shared_keys = sorted(set(baseline_metrics) & set(candidate_metrics))
    for key in shared_keys:
        dotted_key = f"{prefix}.{key}" if prefix else key
        baseline_value = baseline_metrics[key]
        candidate_value = candidate_metrics[key]
        if isinstance(baseline_value, dict) and isinstance(candidate_value, dict):
            deltas.update(_metric_deltas(baseline_value, candidate_value, prefix=dotted_key))
            continue
        if _is_numeric(baseline_value) and _is_numeric(candidate_value):
            deltas[dotted_key] = round(float(candidate_value) - float(baseline_value), 6)
    return deltas


def _verifier_movement_diagnostics(
    *,
    baseline_dir: Path,
    candidate_dir: Path,
    baseline_eval: dict[str, Any],
    candidate_eval: dict[str, Any],
) -> dict[str, Any] | None:
    selected_metric = _default_verifier_metric(
        baseline_eval.get("metrics"),
        candidate_eval.get("metrics"),
    )
    if selected_metric is None:
        return None
    aligned_row = {
        "doi": "__verifier_packet__",
        "primary_status": "completed",
        "secondary_status": "completed",
        "primary_run_dir": str(baseline_dir.parent.parent),
        "secondary_run_dir": str(candidate_dir.parent.parent),
        "primary_experiment_id": baseline_dir.name,
        "secondary_experiment_id": candidate_dir.name,
        "primary_evaluation_summary_json": str(baseline_dir / "evaluation_summary.json"),
        "secondary_evaluation_summary_json": str(candidate_dir / "evaluation_summary.json"),
        f"primary_{selected_metric}": _numeric_metric_value(baseline_eval.get("metrics", {}), selected_metric),
        f"secondary_{selected_metric}": _numeric_metric_value(candidate_eval.get("metrics", {}), selected_metric),
    }
    paired_rows = paired_metric_rows(
        [aligned_row],
        metric_name=selected_metric,
        status_mode="all common seeds with metric available",
    )
    if not paired_rows:
        return None
    summary = comparison_metric_summary(paired_rows)
    return movement_diagnostics_payload(summary.movement_diagnostics)


def _default_verifier_metric(
    baseline_metrics: object,
    candidate_metrics: object,
) -> str | None:
    if not isinstance(baseline_metrics, dict) or not isinstance(candidate_metrics, dict):
        return None
    for metric_name in ("ndcg_at_k", "precision_at_k", "recall_at_k"):
        baseline_value = _numeric_metric_value(baseline_metrics, metric_name)
        candidate_value = _numeric_metric_value(candidate_metrics, metric_name)
        if baseline_value is not None and candidate_value is not None:
            return metric_name
    return None


def _numeric_metric_value(metrics: object, key: str) -> float | None:
    if not isinstance(metrics, dict):
        return None
    value = metrics.get(key)
    if _is_numeric(value):
        return float(value)
    return None


def _evaluation_context_payload(summary_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "evaluation_mode": summary_payload.get("evaluation_mode"),
        "evidence_tier": summary_payload.get("evidence_tier") or summary_payload.get("evaluation_mode"),
        "metric_scope": summary_payload.get("metric_scope"),
        "benchmark_dataset_id": summary_payload.get("benchmark_dataset_id"),
        "benchmark_labels_sha256": summary_payload.get("benchmark_labels_sha256"),
        "benchmark_labels_snapshot_path": summary_payload.get("benchmark_labels_snapshot_path"),
        "promotion_eligible": summary_payload.get("promotion_eligible"),
    }


def _shared_evidence_context(
    baseline_eval: dict[str, Any],
    candidate_eval: dict[str, Any],
) -> dict[str, Any]:
    baseline_context = _evaluation_context_payload(baseline_eval)
    candidate_context = _evaluation_context_payload(candidate_eval)
    return {
        "evaluation_mode": _shared_context_value(
            baseline_context.get("evaluation_mode"),
            candidate_context.get("evaluation_mode"),
        ),
        "evidence_tier": _shared_context_value(
            baseline_context.get("evidence_tier"),
            candidate_context.get("evidence_tier"),
        ),
        "metric_scope": _shared_context_value(
            baseline_context.get("metric_scope"),
            candidate_context.get("metric_scope"),
        ),
        "benchmark_dataset_id": _shared_context_value(
            baseline_context.get("benchmark_dataset_id"),
            candidate_context.get("benchmark_dataset_id"),
        ),
        "benchmark_labels_sha256": _shared_context_value(
            baseline_context.get("benchmark_labels_sha256"),
            candidate_context.get("benchmark_labels_sha256"),
        ),
    }


def _shared_context_value(primary_value: Any, secondary_value: Any) -> Any:
    if primary_value is None or secondary_value is None:
        return None
    if primary_value != secondary_value:
        return None
    return primary_value


def _packet_dir(run_dir: Path, packet_id: str) -> Path:
    safe_packet_id = _validate_artifact_id(packet_id, "packet_id")
    packet_dir = run_dir / AGENT_LOOP_DIRNAME / safe_packet_id
    packet_dir.mkdir(parents=True, exist_ok=True)
    return packet_dir


def _experiment_dir(run_dir: Path, experiment_id: str) -> Path:
    safe_experiment_id = _validate_artifact_id(experiment_id, "experiment_id")
    experiment_dir = run_dir / EXPERIMENT_DIRNAME / safe_experiment_id
    if not experiment_dir.exists():
        raise FileNotFoundError(experiment_dir)
    return experiment_dir


def _validate_artifact_id(value: str, field_name: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if any(char not in allowed for char in text):
        raise ValueError(f"{field_name} may contain only letters, digits, '.', '_' and '-'")
    return text


def _require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(path)


def _load_json_object(path: Path) -> dict[str, Any]:
    _require_file(path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def _load_optional_json_object(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return payload


def _load_yaml_object(path: Path) -> dict[str, Any]:
    _require_file(path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return payload


def _load_theory_snapshot(path: Path) -> TheoryConfig:
    return TheoryConfig.model_validate(_load_yaml_object(path))


def _is_numeric(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
