from __future__ import annotations

from src.config import TheoryConfig


def zero_weight_feature(theory: TheoryConfig, feature_name: str) -> TheoryConfig:
    """Return a copy of theory config with one similarity feature weight set to zero."""

    payload = theory.model_dump(mode="python")
    payload["sim_weights"][feature_name] = 0.0
    return TheoryConfig.model_validate(payload)

