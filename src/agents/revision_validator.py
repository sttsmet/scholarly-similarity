from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from math import isclose
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


ALLOWED_ROOTS = (
    "sim_weights",
    "confidence_factors",
    "sim_parameters",
    "confidence_parameters",
    "explanation",
)
CONSTRAINED_ROUND_MODE = "constrained_lineage_reweight_round1"
GRAPH_PATH_A1_ROUND_MODE = "constrained_lineage_graph_path_a1"
SUPPORTED_CONSTRAINED_ROUND_MODES = (
    CONSTRAINED_ROUND_MODE,
    GRAPH_PATH_A1_ROUND_MODE,
)
CONSTRAINED_SIM_WEIGHT_KEYS = (
    "bibliographic_coupling",
    "direct_citation",
    "topical",
    "temporal",
    "semantic",
)
GRAPH_PATH_SIM_WEIGHT_KEY = "graph_path"
ALL_SIM_WEIGHT_KEYS = (*CONSTRAINED_SIM_WEIGHT_KEYS, GRAPH_PATH_SIM_WEIGHT_KEY)
CONSTRAINED_ALLOWED_PATHS = {
    *(f"sim_weights.{key}" for key in CONSTRAINED_SIM_WEIGHT_KEYS),
    "sim_parameters.temporal_tau",
}
GRAPH_PATH_A1_ALLOWED_PATHS = {
    *(f"sim_weights.{key}" for key in ALL_SIM_WEIGHT_KEYS),
    "sim_parameters.graph_path.related_edge_weight",
    "sim_parameters.graph_path.path_length_decay",
    "sim_parameters.graph_path.saturation_kappa",
    "sim_parameters.graph_path.max_bridge_nodes",
}
MODE_ALLOWED_PATHS = {
    CONSTRAINED_ROUND_MODE: CONSTRAINED_ALLOWED_PATHS,
    GRAPH_PATH_A1_ROUND_MODE: GRAPH_PATH_A1_ALLOWED_PATHS,
}
CONSTRAINED_ALLOWED_TAU_VALUES = {4.0, 5.0, 6.0, 7.0}
GRAPH_PATH_ALLOWED_RHO_VALUES = {0.25, 0.50}
GRAPH_PATH_ALLOWED_DECAY_VALUES = {0.40, 0.60, 0.80}
GRAPH_PATH_ALLOWED_KAPPA_VALUES = {0.50, 1.00, 2.00}
GRAPH_PATH_ALLOWED_MAX_BRIDGE_VALUES = {20, 40, 60}
SIM_WEIGHT_SUM_TARGET = 1.0
SIM_WEIGHT_SUM_TOLERANCE = 1e-9


class GeneratorChangeModel(BaseModel):
    """One scalar dotted-path update proposed by the generator."""

    model_config = ConfigDict(extra="forbid")

    path: str
    value: float | int

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Generator change path must not be empty")
        return text

    @field_validator("value")
    @classmethod
    def _validate_value(cls, value: float | int) -> float | int:
        if isinstance(value, bool):
            raise ValueError("Generator change value must be numeric, not boolean")
        return value


class GeneratorReplyModel(BaseModel):
    """Validated structured generator reply or selected constrained revision."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    expected_effect: str
    risks: list[str]
    changes: list[GeneratorChangeModel] = Field(default_factory=list)
    candidate_revision_id: str | None = None
    generator_round_id: str | None = None
    mode: str | None = None
    target: dict[str, Any] | None = None
    reply_format: Literal["legacy", "constrained"] = "legacy"

    @field_validator("summary", "expected_effect")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Generator reply text fields must not be empty")
        return text

    @field_validator("risks")
    @classmethod
    def _validate_risks(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("Generator reply must include at least one non-empty risk")
        return cleaned


class VerifierReplyModel(BaseModel):
    """Validated structured verifier reply."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    pass_verdict: bool = Field(alias="pass")
    score: float = Field(ge=0.0, le=1.0)
    issues: list[str]
    next_change: str
    notes: str | None = None

    @field_validator("issues")
    @classmethod
    def _validate_issues(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("Verifier reply must include at least one non-empty issue")
        return cleaned

    @field_validator("next_change")
    @classmethod
    def _validate_next_change(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Verifier reply next_change must not be empty")
        return text

    @field_validator("notes")
    @classmethod
    def _validate_notes(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = value.strip()
        return text or None


class ConstrainedTargetSimWeightsModel(BaseModel):
    """Full constrained-round replacement block for similarity weights."""

    model_config = ConfigDict(extra="forbid")

    bibliographic_coupling: float = Field(ge=0.0)
    direct_citation: float = Field(ge=0.0)
    topical: float = Field(ge=0.0)
    temporal: float = Field(ge=0.0)
    semantic: float = Field(ge=0.0)
    graph_path: float = Field(default=0.0, ge=0.0)

    @model_validator(mode="after")
    def _validate_sum(self) -> ConstrainedTargetSimWeightsModel:
        total = sum(float(getattr(self, key)) for key in ALL_SIM_WEIGHT_KEYS)
        if not isclose(total, SIM_WEIGHT_SUM_TARGET, abs_tol=SIM_WEIGHT_SUM_TOLERANCE):
            raise ValueError(
                "sim_weights must sum to exactly 1.0 for constrained lineage reweight revisions"
            )
        return self


class ConstrainedTargetGraphPathParametersModel(BaseModel):
    """Allowed graph-path parameter updates for A1 graph-surface rounds."""

    model_config = ConfigDict(extra="forbid")

    related_edge_weight: float | None = None
    path_length_decay: float | None = None
    saturation_kappa: float | None = None
    max_bridge_nodes: int | None = None

    @field_validator("related_edge_weight")
    @classmethod
    def _validate_related_edge_weight(cls, value: float | None) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if numeric not in GRAPH_PATH_ALLOWED_RHO_VALUES:
            allowed = ", ".join(str(item) for item in sorted(GRAPH_PATH_ALLOWED_RHO_VALUES))
            raise ValueError(f"related_edge_weight must be one of: {allowed}")
        return numeric

    @field_validator("path_length_decay")
    @classmethod
    def _validate_path_length_decay(cls, value: float | None) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if numeric not in GRAPH_PATH_ALLOWED_DECAY_VALUES:
            allowed = ", ".join(str(item) for item in sorted(GRAPH_PATH_ALLOWED_DECAY_VALUES))
            raise ValueError(f"path_length_decay must be one of: {allowed}")
        return numeric

    @field_validator("saturation_kappa")
    @classmethod
    def _validate_saturation_kappa(cls, value: float | None) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if numeric not in GRAPH_PATH_ALLOWED_KAPPA_VALUES:
            allowed = ", ".join(str(item) for item in sorted(GRAPH_PATH_ALLOWED_KAPPA_VALUES))
            raise ValueError(f"saturation_kappa must be one of: {allowed}")
        return numeric

    @field_validator("max_bridge_nodes")
    @classmethod
    def _validate_max_bridge_nodes(cls, value: int | None) -> int | None:
        if value is None:
            return None
        numeric = int(value)
        if numeric not in GRAPH_PATH_ALLOWED_MAX_BRIDGE_VALUES:
            allowed = ", ".join(str(item) for item in sorted(GRAPH_PATH_ALLOWED_MAX_BRIDGE_VALUES))
            raise ValueError(f"max_bridge_nodes must be one of: {allowed}")
        return numeric

    @model_validator(mode="after")
    def _require_graph_path_field(self) -> ConstrainedTargetGraphPathParametersModel:
        if (
            self.related_edge_weight is None
            and self.path_length_decay is None
            and self.saturation_kappa is None
            and self.max_bridge_nodes is None
        ):
            raise ValueError("graph_path target parameters must include at least one field")
        return self


class ConstrainedTargetSimParametersModel(BaseModel):
    """Allowed constrained-round sim parameter update surface."""

    model_config = ConfigDict(extra="forbid")

    temporal_tau: float | None = None
    graph_path: ConstrainedTargetGraphPathParametersModel | None = None

    @field_validator("temporal_tau")
    @classmethod
    def _validate_temporal_tau(cls, value: float | None) -> float | None:
        if value is None:
            return None
        numeric = float(value)
        if numeric not in CONSTRAINED_ALLOWED_TAU_VALUES:
            allowed = ", ".join(str(item) for item in sorted(CONSTRAINED_ALLOWED_TAU_VALUES))
            raise ValueError(f"temporal_tau must be one of: {allowed}")
        return numeric

    @model_validator(mode="after")
    def _require_parameter_target(self) -> ConstrainedTargetSimParametersModel:
        if self.temporal_tau is None and self.graph_path is None:
            raise ValueError("sim_parameters target must include temporal_tau or graph_path")
        return self


class ConstrainedRevisionTargetModel(BaseModel):
    """Selected constrained candidate target payload."""

    model_config = ConfigDict(extra="forbid")

    sim_weights: ConstrainedTargetSimWeightsModel | None = None
    sim_parameters: ConstrainedTargetSimParametersModel | None = None

    @model_validator(mode="after")
    def _require_target(self) -> ConstrainedRevisionTargetModel:
        if self.sim_weights is None and self.sim_parameters is None:
            raise ValueError("Constrained revision target must include sim_weights or sim_parameters")
        return self


class ConstrainedLegalityCheckModel(BaseModel):
    """Generator-provided legality metadata recorded for auditability."""

    model_config = ConfigDict(extra="forbid")

    weights_sum: float
    non_negative: bool
    allowed_keys_only: bool
    local_change_only: bool


class ConstrainedExpectedEffectModel(BaseModel):
    """Expected-effect metadata carried through from the generator reply."""

    model_config = ConfigDict(extra="forbid")

    silver_global: str
    strong_lineage: str
    ambiguous_middle: str
    hard_negative_or_distractor: str
    independent_benchmark: str


class ConstrainedRevisionModel(BaseModel):
    """One constrained lineage-round revision candidate."""

    model_config = ConfigDict(extra="forbid")

    candidate_revision_id: str
    priority: int
    type: str
    target: ConstrainedRevisionTargetModel
    legality_check: ConstrainedLegalityCheckModel
    hypothesis: str
    why_now: str
    expected_effect: ConstrainedExpectedEffectModel
    main_risk: str
    reject_if: list[str]
    verifier_tests: list[str]

    @field_validator(
        "candidate_revision_id",
        "type",
        "hypothesis",
        "why_now",
        "main_risk",
    )
    @classmethod
    def _validate_nonempty_text(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Constrained revision text fields must not be empty")
        return text

    @field_validator("reject_if", "verifier_tests")
    @classmethod
    def _validate_nonempty_list(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("Constrained revision list fields must not be empty")
        return cleaned


class ConstrainedGeneratorReplyPayload(BaseModel):
    """Top-level constrained round generator reply."""

    model_config = ConfigDict(extra="forbid")

    generator_round_id: str
    mode: str
    baseline_reference: str | None = None
    revisions: list[ConstrainedRevisionModel] = Field(min_length=1)
    summary: str

    @field_validator("generator_round_id", "mode", "summary")
    @classmethod
    def _validate_nonempty_text(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Constrained generator reply text fields must not be empty")
        return text


def validate_required_fields(payload: Mapping[str, object], required_fields: Iterable[str]) -> list[str]:
    """Return validation errors for missing required top-level fields."""

    errors: list[str] = []
    for field_name in required_fields:
        if field_name not in payload:
            errors.append(f"Missing required field: {field_name}")
    return errors


def allowed_theory_change_specs(theory_payload: Mapping[str, object]) -> dict[str, dict[str, object]]:
    """Return allowed dotted scalar leaf paths for v0 generator edits."""

    specs: dict[str, dict[str, object]] = {}
    for root in ALLOWED_ROOTS:
        value = theory_payload.get(root)
        if isinstance(value, Mapping):
            _collect_allowed_paths(root, value, specs)
    return dict(sorted(specs.items()))


def validate_generator_reply_payload(
    *,
    payload: Mapping[str, object],
    theory_payload: Mapping[str, object],
    candidate_revision_id: str | None = None,
) -> GeneratorReplyModel:
    """Validate a generator reply against the supported theory change surfaces."""

    if "revisions" in payload or payload.get("mode") in SUPPORTED_CONSTRAINED_ROUND_MODES:
        return _validate_constrained_generator_reply_payload(
            payload=payload,
            theory_payload=theory_payload,
            candidate_revision_id=candidate_revision_id,
        )

    return _validate_legacy_generator_reply_payload(
        payload=payload,
        theory_payload=theory_payload,
    )


def apply_generator_changes(
    *,
    theory_payload: Mapping[str, object],
    validated_reply: GeneratorReplyModel,
) -> dict[str, Any]:
    """Apply validated generator changes to a copy of the theory payload."""

    updated = deepcopy(dict(theory_payload))

    target = validated_reply.target
    if isinstance(target, Mapping):
        sim_weights = target.get("sim_weights")
        if isinstance(sim_weights, Mapping):
            updated["sim_weights"] = deepcopy(dict(sim_weights))

        sim_parameters = target.get("sim_parameters")
        if isinstance(sim_parameters, Mapping):
            current_sim_parameters = updated.get("sim_parameters")
            if not isinstance(current_sim_parameters, dict):
                raise ValueError("Unknown theory path: sim_parameters")
            merged_sim_parameters = deepcopy(current_sim_parameters)
            merged_sim_parameters.update(dict(sim_parameters))
            updated["sim_parameters"] = merged_sim_parameters

    for change in validated_reply.changes:
        _set_dotted_value(updated, change.path, change.value)
    return updated


def validate_applied_candidate_theory(
    *,
    baseline_payload: Mapping[str, object],
    updated_payload: Mapping[str, object],
    validated_reply: GeneratorReplyModel,
) -> None:
    """Validate pre-run legality for an applied candidate theory snapshot."""

    sim_weights = updated_payload.get("sim_weights")
    if not isinstance(sim_weights, Mapping):
        raise ValueError("Candidate theory is missing sim_weights")

    expected_weight_keys = set(CONSTRAINED_SIM_WEIGHT_KEYS)
    optional_weight_keys = {GRAPH_PATH_SIM_WEIGHT_KEY}
    missing_weight_keys = [key for key in expected_weight_keys if key not in sim_weights]
    if missing_weight_keys:
        raise ValueError(
            "Candidate theory sim_weights is missing required keys: "
            + ", ".join(missing_weight_keys)
        )

    unexpected_weight_keys = sorted(
        str(key)
        for key in sim_weights.keys()
        if str(key) not in expected_weight_keys | optional_weight_keys
    )
    if unexpected_weight_keys:
        raise ValueError(
            "Candidate theory sim_weights contains unexpected keys: "
            + ", ".join(unexpected_weight_keys)
        )

    total = 0.0
    for key in ALL_SIM_WEIGHT_KEYS:
        value = sim_weights.get(key, 0.0)
        if not _is_numeric_scalar(value):
            raise ValueError(f"Candidate theory sim_weights.{key} must be numeric")
        numeric_value = float(value)
        if numeric_value < 0.0:
            raise ValueError(f"Candidate theory sim_weights.{key} must be non-negative")
        total += numeric_value
    if not isclose(total, SIM_WEIGHT_SUM_TARGET, abs_tol=SIM_WEIGHT_SUM_TOLERANCE):
        raise ValueError(
            f"Candidate theory sim_weights must sum to 1.0 before batch execution (got {total:.12g})"
        )

    if validated_reply.reply_format != "constrained":
        return

    changed_paths = diff_scalar_paths(
        baseline_payload=baseline_payload,
        updated_payload=updated_payload,
        roots=("sim_weights", "sim_parameters"),
    )
    allowed_paths = MODE_ALLOWED_PATHS.get(validated_reply.mode or CONSTRAINED_ROUND_MODE, CONSTRAINED_ALLOWED_PATHS)
    disallowed_paths = sorted(path for path in changed_paths if path not in allowed_paths)
    if disallowed_paths:
        raise ValueError(
            "Candidate theory changed paths outside the constrained round surface: "
            + ", ".join(disallowed_paths)
        )


def diff_scalar_paths(
    *,
    baseline_payload: Mapping[str, object],
    updated_payload: Mapping[str, object],
    roots: Sequence[str] = ("sim_weights", "sim_parameters"),
) -> list[str]:
    """Return dotted scalar paths that changed between two theory payloads."""

    baseline_specs = allowed_theory_change_specs(baseline_payload)
    updated_specs = allowed_theory_change_specs(updated_payload)
    changed_paths: list[str] = []
    for path in sorted(set(baseline_specs) | set(updated_specs)):
        if not any(path == root or path.startswith(f"{root}.") for root in roots):
            continue
        baseline_value = baseline_specs.get(path, {}).get("current_value")
        updated_value = updated_specs.get(path, {}).get("current_value")
        if baseline_value != updated_value:
            changed_paths.append(path)
    return changed_paths


def validate_verifier_reply_payload(*, payload: Mapping[str, object]) -> VerifierReplyModel:
    """Validate a structured verifier reply payload."""

    return VerifierReplyModel.model_validate(dict(payload))


def _validate_legacy_generator_reply_payload(
    *,
    payload: Mapping[str, object],
    theory_payload: Mapping[str, object],
) -> GeneratorReplyModel:
    legacy_reply = _LegacyGeneratorReplyModel.model_validate(dict(payload))
    allowed_specs = allowed_theory_change_specs(theory_payload)
    seen_paths: set[str] = set()
    normalized_changes: list[GeneratorChangeModel] = []

    for change in legacy_reply.changes:
        if change.path in seen_paths:
            raise ValueError(f"Duplicate generator change path: {change.path}")
        seen_paths.add(change.path)

        spec = allowed_specs.get(change.path)
        if spec is None:
            raise ValueError(f"Generator change path is not allowed: {change.path}")

        coerced_value = _coerce_numeric_value(
            path=change.path,
            new_value=change.value,
            current_value=spec["current_value"],
        )
        normalized_changes.append(GeneratorChangeModel(path=change.path, value=coerced_value))

    return GeneratorReplyModel(
        summary=legacy_reply.summary,
        expected_effect=legacy_reply.expected_effect,
        risks=legacy_reply.risks,
        changes=normalized_changes,
        reply_format="legacy",
    )


def _validate_constrained_generator_reply_payload(
    *,
    payload: Mapping[str, object],
    theory_payload: Mapping[str, object],
    candidate_revision_id: str | None,
) -> GeneratorReplyModel:
    constrained_reply = ConstrainedGeneratorReplyPayload.model_validate(dict(payload))
    if constrained_reply.mode not in SUPPORTED_CONSTRAINED_ROUND_MODES:
        raise ValueError(
            f"Unsupported constrained generator reply mode: {constrained_reply.mode}"
        )

    selected_revision = _select_constrained_revision(
        revisions=constrained_reply.revisions,
        candidate_revision_id=candidate_revision_id,
    )
    _validate_constrained_target_mode(
        target=selected_revision.target,
        mode=constrained_reply.mode,
    )
    normalized_target = selected_revision.target.model_dump(
        mode="json",
        exclude_none=True,
        exclude_defaults=True,
    )
    normalized_changes = _constrained_target_changes(
        baseline_payload=theory_payload,
        target=normalized_target,
        mode=constrained_reply.mode,
    )
    if not normalized_changes:
        raise ValueError(
            f"Selected constrained candidate '{selected_revision.candidate_revision_id}' does not change any allowed fields"
        )

    return GeneratorReplyModel(
        summary=selected_revision.hypothesis,
        expected_effect=selected_revision.why_now,
        risks=[selected_revision.main_risk],
        changes=normalized_changes,
        candidate_revision_id=selected_revision.candidate_revision_id,
        generator_round_id=constrained_reply.generator_round_id,
        mode=constrained_reply.mode,
        target=normalized_target,
        reply_format="constrained",
    )


def _select_constrained_revision(
    *,
    revisions: list[ConstrainedRevisionModel],
    candidate_revision_id: str | None,
) -> ConstrainedRevisionModel:
    if candidate_revision_id is not None:
        normalized_id = candidate_revision_id.strip()
        for revision in revisions:
            if revision.candidate_revision_id == normalized_id:
                return revision
        raise ValueError(
            f"Constrained generator reply does not include candidate_revision_id '{normalized_id}'"
        )

    if len(revisions) == 1:
        return revisions[0]

    raise ValueError(
        "Constrained generator reply contains multiple revisions; "
        "candidate_revision_id is required to select one."
    )


def _constrained_target_changes(
    *,
    baseline_payload: Mapping[str, object],
    target: Mapping[str, object],
    mode: str,
) -> list[GeneratorChangeModel]:
    baseline_sim_weights = baseline_payload.get("sim_weights")
    if not isinstance(baseline_sim_weights, Mapping):
        raise ValueError("Baseline theory is missing sim_weights for constrained revision validation")

    normalized_changes: list[GeneratorChangeModel] = []
    changed_weight_paths: list[str] = []

    sim_weights = target.get("sim_weights")
    if isinstance(sim_weights, Mapping):
        sim_weight_keys = set(str(key) for key in sim_weights.keys())
        allowed_weight_key_sets = [
            set(CONSTRAINED_SIM_WEIGHT_KEYS),
            set(ALL_SIM_WEIGHT_KEYS),
        ]
        if sim_weight_keys not in allowed_weight_key_sets:
            raise ValueError(
                "Constrained sim_weights target must include either the five baseline weights or all six graph-surface weights"
            )
        ordered_weight_keys = [key for key in ALL_SIM_WEIGHT_KEYS if key in sim_weight_keys]
        for key in ordered_weight_keys:
            baseline_value = baseline_sim_weights.get(key, 0.0)
            if not _is_numeric_scalar(baseline_value):
                raise ValueError(f"Baseline theory sim_weights.{key} must be numeric")
            new_value = float(sim_weights[key])
            if not isclose(float(baseline_value), new_value, abs_tol=SIM_WEIGHT_SUM_TOLERANCE):
                path = f"sim_weights.{key}"
                normalized_changes.append(GeneratorChangeModel(path=path, value=new_value))
                changed_weight_paths.append(path)

    if len(changed_weight_paths) > 3:
        raise ValueError(
            "Constrained lineage reweight revisions may change at most 3 similarity weights"
        )

    sim_parameters = target.get("sim_parameters")
    if isinstance(sim_parameters, Mapping) and "temporal_tau" in sim_parameters:
        baseline_sim_parameters = baseline_payload.get("sim_parameters")
        if not isinstance(baseline_sim_parameters, Mapping):
            raise ValueError("Baseline theory is missing sim_parameters for constrained revision validation")
        baseline_tau = baseline_sim_parameters.get("temporal_tau")
        if not _is_numeric_scalar(baseline_tau):
            raise ValueError("Baseline theory sim_parameters.temporal_tau must be numeric")
        new_tau = float(sim_parameters["temporal_tau"])
        if not isclose(float(baseline_tau), new_tau, abs_tol=SIM_WEIGHT_SUM_TOLERANCE):
            normalized_changes.append(
                GeneratorChangeModel(path="sim_parameters.temporal_tau", value=new_tau)
            )

    if isinstance(sim_parameters, Mapping) and isinstance(sim_parameters.get("graph_path"), Mapping):
        baseline_sim_parameters = baseline_payload.get("sim_parameters")
        if not isinstance(baseline_sim_parameters, Mapping):
            raise ValueError("Baseline theory is missing sim_parameters for constrained revision validation")
        baseline_graph_path = baseline_sim_parameters.get("graph_path")
        baseline_graph_path = baseline_graph_path if isinstance(baseline_graph_path, Mapping) else {}
        for key, new_value_raw in sorted(sim_parameters["graph_path"].items()):
            baseline_value = baseline_graph_path.get(key)
            if baseline_value is None:
                raise ValueError(f"Baseline theory sim_parameters.graph_path.{key} is missing")
            if not _is_numeric_scalar(baseline_value):
                raise ValueError(f"Baseline theory sim_parameters.graph_path.{key} must be numeric")
            new_value = float(new_value_raw) if not isinstance(new_value_raw, int) else int(new_value_raw)
            if float(baseline_value) != float(new_value):
                normalized_changes.append(
                    GeneratorChangeModel(
                        path=f"sim_parameters.graph_path.{key}",
                        value=new_value,
                    )
                )

    allowed_paths = MODE_ALLOWED_PATHS.get(mode, CONSTRAINED_ALLOWED_PATHS)
    disallowed_changes = sorted(
        change.path for change in normalized_changes if change.path not in allowed_paths
    )
    if disallowed_changes:
        raise ValueError(
            "Constrained revision target changes fields outside the selected mode surface: "
            + ", ".join(disallowed_changes)
        )

    return normalized_changes


def _validate_constrained_target_mode(
    *,
    target: ConstrainedRevisionTargetModel,
    mode: str,
) -> None:
    sim_parameters = target.sim_parameters
    if mode == CONSTRAINED_ROUND_MODE:
        if sim_parameters is not None and sim_parameters.graph_path is not None:
            raise ValueError(
                "constrained_lineage_reweight_round1 does not allow graph_path parameter changes"
            )
        return

    if mode == GRAPH_PATH_A1_ROUND_MODE:
        if sim_parameters is not None and sim_parameters.temporal_tau is not None:
            raise ValueError(
                "constrained_lineage_graph_path_a1 does not allow temporal_tau changes"
            )
        return


def _collect_allowed_paths(
    prefix: str,
    payload: Mapping[str, object],
    specs: dict[str, dict[str, object]],
) -> None:
    for key, value in payload.items():
        path = f"{prefix}.{key}"
        if isinstance(value, Mapping):
            _collect_allowed_paths(path, value, specs)
            continue
        if _is_numeric_scalar(value):
            specs[path] = {
                "current_value": value,
                "value_type": "int" if isinstance(value, int) else "float",
            }


def _coerce_numeric_value(*, path: str, new_value: float | int, current_value: object) -> float | int:
    if not _is_numeric_scalar(current_value):
        raise ValueError(f"Generator change path is not numeric: {path}")
    if isinstance(new_value, bool):
        raise ValueError(f"Generator change value must be numeric: {path}")

    if isinstance(current_value, int):
        if isinstance(new_value, float) and not new_value.is_integer():
            raise ValueError(f"Generator change for {path} must be an integer value")
        return int(new_value)
    return float(new_value)


def _set_dotted_value(payload: dict[str, Any], dotted_path: str, value: float | int) -> None:
    keys = dotted_path.split(".")
    current: dict[str, Any] = payload
    for key in keys[:-1]:
        next_value = current.get(key)
        if not isinstance(next_value, dict):
            raise ValueError(f"Unknown theory path: {dotted_path}")
        current = next_value
    leaf_key = keys[-1]
    if leaf_key not in current:
        raise ValueError(f"Unknown theory path: {dotted_path}")
    current[leaf_key] = value


def _is_numeric_scalar(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


class _LegacyGeneratorReplyModel(BaseModel):
    """Validated legacy structured generator reply."""

    model_config = ConfigDict(extra="forbid")

    summary: str
    expected_effect: str
    risks: list[str]
    changes: list[GeneratorChangeModel] = Field(min_length=1)

    @field_validator("summary", "expected_effect")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("Generator reply text fields must not be empty")
        return text

    @field_validator("risks")
    @classmethod
    def _validate_risks(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("Generator reply must include at least one non-empty risk")
        return cleaned
