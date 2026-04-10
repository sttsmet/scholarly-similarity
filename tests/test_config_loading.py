from __future__ import annotations

from src.config import load_all_configs


def test_required_configs_load() -> None:
    configs = load_all_configs()
    assert configs.theory.aspect.source == "openalex"
    assert configs.theory.candidate_pool.max_candidates > 0
    assert configs.theory.sim_parameters.temporal_tau > 0.0
    assert configs.theory.sim_parameters.graph_path.allowed_path_lengths == [2, 3]
    assert configs.theory.sim_parameters.graph_path.max_bridge_nodes > 0
    assert configs.theory.confidence_factors.coverage > 0.0
    assert configs.theory.confidence_parameters.observation_year >= 2026
    assert configs.runtime.cache_dir == "data/cache"
    assert configs.runtime.use_network is False
    assert configs.runtime.openalex_base_url == "https://api.openalex.org"
    assert "reciprocal_rank" in configs.evaluation.metrics
