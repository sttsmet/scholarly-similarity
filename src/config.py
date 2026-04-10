from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "configs"
DEFAULT_THEORY_PATH = CONFIG_DIR / "theory_v001.yaml"
DEFAULT_RUNTIME_PATH = CONFIG_DIR / "runtime.yaml"
DEFAULT_EVALUATION_PATH = CONFIG_DIR / "evaluation.yaml"


class StrictBaseModel(BaseModel):
    """Base model that rejects unexpected keys in config files."""

    model_config = ConfigDict(extra="forbid")


class AspectConfig(StrictBaseModel):
    source: str
    scope: str
    note: str


class CandidatePoolConfig(StrictBaseModel):
    max_candidates: int = Field(ge=1, le=500)
    include_related_works: bool
    include_references: bool
    include_citations: bool
    dedupe_key: str


class SimWeightsConfig(StrictBaseModel):
    bibliographic_coupling: float = Field(ge=0.0)
    direct_citation: float = Field(ge=0.0)
    topical: float = Field(ge=0.0)
    temporal: float = Field(ge=0.0)
    semantic: float = Field(ge=0.0)
    graph_path: float = Field(default=0.0, ge=0.0)


class GraphPathParametersConfig(StrictBaseModel):
    max_bridge_nodes: int = Field(default=40, ge=1, le=500)
    allowed_path_lengths: list[int] = Field(default_factory=lambda: [2, 3])
    related_edge_weight: float = Field(default=0.5, ge=0.0, le=1.0)
    path_length_decay: float = Field(default=0.6, ge=0.0, le=1.0)
    saturation_kappa: float = Field(default=1.0, gt=0.0)

    @field_validator("allowed_path_lengths")
    @classmethod
    def _validate_allowed_path_lengths(cls, value: list[int]) -> list[int]:
        normalized = [int(item) for item in value]
        if sorted(dict.fromkeys(normalized)) != [2, 3]:
            raise ValueError("graph_path.allowed_path_lengths must be exactly [2, 3]")
        return normalized


class SimParametersConfig(StrictBaseModel):
    temporal_tau: float = Field(gt=0.0)
    graph_path: GraphPathParametersConfig = Field(default_factory=GraphPathParametersConfig)


class ConfidenceFactorsConfig(StrictBaseModel):
    coverage: float = Field(ge=0.0)
    support: float = Field(ge=0.0)
    maturity: float = Field(ge=0.0)


class ConfidenceParametersConfig(StrictBaseModel):
    observation_year: int = Field(ge=1900, le=3000)
    support_eta: float = Field(gt=0.0)
    maturity_tau: float = Field(gt=0.0)


class ExplanationConfig(StrictBaseModel):
    top_k_features: int = Field(ge=1, le=10)
    include_raw_scores: bool
    include_notes: bool


class TheoryConfig(StrictBaseModel):
    version: str
    aspect: AspectConfig
    candidate_pool: CandidatePoolConfig
    sim_weights: SimWeightsConfig
    sim_parameters: SimParametersConfig
    confidence_factors: ConfidenceFactorsConfig
    confidence_parameters: ConfidenceParametersConfig
    explanation: ExplanationConfig


class RuntimeConfig(StrictBaseModel):
    app_name: str
    openalex_base_url: str
    use_network: bool
    cache_dir: str
    runs_dir: str
    request_timeout_seconds: float = Field(gt=0.0)


class PerturbationConfig(StrictBaseModel):
    enabled: bool
    zero_weight_features: list[str] = Field(default_factory=list)


class GeneratorVerifierConfig(StrictBaseModel):
    packet_dir: str


class EvaluationConfig(StrictBaseModel):
    benchmark_path: str
    metrics: list[str] = Field(default_factory=list)
    perturbation: PerturbationConfig
    generator_verifier: GeneratorVerifierConfig


class ConfigBundle(StrictBaseModel):
    theory: TheoryConfig
    runtime: RuntimeConfig
    evaluation: EvaluationConfig


def load_theory_config(path: str | Path = DEFAULT_THEORY_PATH) -> TheoryConfig:
    return TheoryConfig.model_validate(_load_yaml_dict(Path(path)))


def load_runtime_config(path: str | Path = DEFAULT_RUNTIME_PATH) -> RuntimeConfig:
    return RuntimeConfig.model_validate(_load_yaml_dict(Path(path)))


def load_evaluation_config(path: str | Path = DEFAULT_EVALUATION_PATH) -> EvaluationConfig:
    return EvaluationConfig.model_validate(_load_yaml_dict(Path(path)))


def load_all_configs(
    theory_path: str | Path = DEFAULT_THEORY_PATH,
    runtime_path: str | Path = DEFAULT_RUNTIME_PATH,
    evaluation_path: str | Path = DEFAULT_EVALUATION_PATH,
) -> ConfigBundle:
    return ConfigBundle(
        theory=load_theory_config(theory_path),
        runtime=load_runtime_config(runtime_path),
        evaluation=load_evaluation_config(evaluation_path),
    )


def _load_yaml_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a mapping in {path}")
    return payload
