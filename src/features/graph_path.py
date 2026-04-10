from __future__ import annotations

from src.config import TheoryConfig
from src.graph.bridge_graph import (
    BridgeGraphContext,
    GraphPathResult,
    build_bridge_graph_context,
    compute_graph_path,
)
from src.ingest.doi_resolver import NormalizedOpenAlexRecord


def analyze(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    theory: TheoryConfig,
    *,
    context: BridgeGraphContext | None = None,
) -> GraphPathResult:
    """Return bounded short-path evidence for one seed-candidate pair."""

    graph_context = context or build_bridge_graph_context(
        seed=seed,
        candidates=[candidate],
        parameters=theory.sim_parameters.graph_path,
    )
    return compute_graph_path(
        context=graph_context,
        candidate_openalex_id=candidate.openalex_id,
    )


def score(
    seed: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    theory: TheoryConfig,
    context: BridgeGraphContext | None = None,
) -> float:
    """Return a deterministic [0,1] short-path bridge score."""

    return analyze(seed, candidate, theory, context=context).score
