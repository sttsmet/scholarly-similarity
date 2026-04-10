from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import exp

from src.config import GraphPathParametersConfig
from src.ingest.doi_resolver import NormalizedOpenAlexRecord
from src.ingest.openalex_client import normalize_openalex_work_id


CITATION_EDGE_TYPE = "citation"
RELATED_EDGE_TYPE = "related"
CITATION_EDGE_WEIGHT = 1.0


@dataclass(frozen=True, slots=True)
class BridgeEdge:
    neighbor_id: str
    edge_type: str
    weight: float


@dataclass(frozen=True, slots=True)
class GraphPath:
    node_ids: tuple[str, ...]
    edge_types: tuple[str, ...]
    weight_product: float
    contribution: float


@dataclass(frozen=True, slots=True)
class GraphPathResult:
    score: float
    raw_path_mass: float
    supporting_path_count: int
    dominant_motif: str | None
    motif_weight_by_type: dict[str, float]
    paths: tuple[GraphPath, ...]

    @property
    def explanation_note(self) -> str | None:
        if self.supporting_path_count <= 0:
            return "paths=0"
        motif = self.dominant_motif or "mixed"
        return f"paths={self.supporting_path_count}; motif={motif}"


@dataclass(frozen=True, slots=True)
class BridgeGraphContext:
    seed_id: str
    candidate_ids: tuple[str, ...]
    bridge_node_ids: tuple[str, ...]
    adjacency: dict[str, tuple[BridgeEdge, ...]]
    parameters: GraphPathParametersConfig


def build_bridge_graph_context(
    *,
    seed: NormalizedOpenAlexRecord,
    candidates: list[NormalizedOpenAlexRecord],
    parameters: GraphPathParametersConfig,
) -> BridgeGraphContext:
    local_records = [seed, *candidates]
    local_node_ids = {
        _canonicalize_openalex_id(record.openalex_id)
        for record in local_records
    }
    bridge_node_ids = tuple(
        _select_bridge_node_ids(
            records=local_records,
            excluded_ids=local_node_ids,
            max_bridge_nodes=parameters.max_bridge_nodes,
        )
    )
    node_ids = set(local_node_ids) | set(bridge_node_ids)
    adjacency = _build_typed_adjacency(
        records=local_records,
        allowed_node_ids=node_ids,
        related_edge_weight=parameters.related_edge_weight,
    )
    return BridgeGraphContext(
        seed_id=_canonicalize_openalex_id(seed.openalex_id),
        candidate_ids=tuple(
            _canonicalize_openalex_id(record.openalex_id)
            for record in candidates
        ),
        bridge_node_ids=bridge_node_ids,
        adjacency=adjacency,
        parameters=parameters,
    )


def compute_graph_path(
    *,
    context: BridgeGraphContext,
    candidate_openalex_id: str,
) -> GraphPathResult:
    candidate_id = _canonicalize_openalex_id(candidate_openalex_id)
    if candidate_id == context.seed_id:
        return GraphPathResult(
            score=0.0,
            raw_path_mass=0.0,
            supporting_path_count=0,
            dominant_motif=None,
            motif_weight_by_type={},
            paths=(),
        )

    path_list: list[GraphPath] = []
    for path_length in context.parameters.allowed_path_lengths:
        path_list.extend(
            _enumerate_paths_of_length(
                context=context,
                target_id=candidate_id,
                path_length=path_length,
            )
        )

    raw_path_mass = sum(path.contribution for path in path_list)
    normalized_score = 1.0 - exp(-raw_path_mass / context.parameters.saturation_kappa)
    motif_weights = _motif_weight_by_type(path_list)
    dominant_motif = _dominant_motif(motif_weights)
    return GraphPathResult(
        score=round(normalized_score, 6),
        raw_path_mass=round(raw_path_mass, 6),
        supporting_path_count=len(path_list),
        dominant_motif=dominant_motif,
        motif_weight_by_type={
            key: round(value, 6)
            for key, value in sorted(motif_weights.items())
        },
        paths=tuple(path_list),
    )


def _select_bridge_node_ids(
    *,
    records: list[NormalizedOpenAlexRecord],
    excluded_ids: set[str],
    max_bridge_nodes: int,
) -> list[str]:
    bridge_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"citation_count": 0, "related_count": 0}
    )
    for record in records:
        for openalex_id in record.referenced_works:
            canonical_id = _canonicalize_openalex_id(openalex_id)
            if canonical_id in excluded_ids:
                continue
            bridge_stats[canonical_id]["citation_count"] += 1
        for openalex_id in record.related_works:
            canonical_id = _canonicalize_openalex_id(openalex_id)
            if canonical_id in excluded_ids:
                continue
            bridge_stats[canonical_id]["related_count"] += 1

    ranked_bridge_ids = sorted(
        bridge_stats,
        key=lambda node_id: (
            -(bridge_stats[node_id]["citation_count"] + bridge_stats[node_id]["related_count"]),
            -bridge_stats[node_id]["citation_count"],
            -bridge_stats[node_id]["related_count"],
            node_id,
        ),
    )
    return ranked_bridge_ids[:max_bridge_nodes]


def _build_typed_adjacency(
    *,
    records: list[NormalizedOpenAlexRecord],
    allowed_node_ids: set[str],
    related_edge_weight: float,
) -> dict[str, tuple[BridgeEdge, ...]]:
    adjacency_lists: dict[str, list[BridgeEdge]] = defaultdict(list)
    seen_edges: set[tuple[str, str, str]] = set()

    for record in records:
        src_id = _canonicalize_openalex_id(record.openalex_id)
        for openalex_id in record.referenced_works:
            target_id = _canonicalize_openalex_id(openalex_id)
            if target_id not in allowed_node_ids:
                continue
            _add_undirected_edge(
                adjacency_lists=adjacency_lists,
                seen_edges=seen_edges,
                src_id=src_id,
                dst_id=target_id,
                edge_type=CITATION_EDGE_TYPE,
                weight=CITATION_EDGE_WEIGHT,
            )
        for openalex_id in record.related_works:
            target_id = _canonicalize_openalex_id(openalex_id)
            if target_id not in allowed_node_ids:
                continue
            _add_undirected_edge(
                adjacency_lists=adjacency_lists,
                seen_edges=seen_edges,
                src_id=src_id,
                dst_id=target_id,
                edge_type=RELATED_EDGE_TYPE,
                weight=related_edge_weight,
            )

    return {
        node_id: tuple(
            sorted(
                edges,
                key=lambda edge: (
                    edge.neighbor_id,
                    edge.edge_type,
                    edge.weight,
                ),
            )
        )
        for node_id, edges in adjacency_lists.items()
    }


def _add_undirected_edge(
    *,
    adjacency_lists: dict[str, list[BridgeEdge]],
    seen_edges: set[tuple[str, str, str]],
    src_id: str,
    dst_id: str,
    edge_type: str,
    weight: float,
) -> None:
    if src_id == dst_id:
        return
    dedupe_key = (min(src_id, dst_id), max(src_id, dst_id), edge_type)
    if dedupe_key in seen_edges:
        return
    seen_edges.add(dedupe_key)
    adjacency_lists[src_id].append(BridgeEdge(neighbor_id=dst_id, edge_type=edge_type, weight=weight))
    adjacency_lists[dst_id].append(BridgeEdge(neighbor_id=src_id, edge_type=edge_type, weight=weight))


def _enumerate_paths_of_length(
    *,
    context: BridgeGraphContext,
    target_id: str,
    path_length: int,
) -> list[GraphPath]:
    collected_paths: list[GraphPath] = []

    def _walk(
        current_id: str,
        *,
        remaining_edges: int,
        visited: set[str],
        node_path: list[str],
        edge_types: list[str],
        weight_product: float,
    ) -> None:
        if remaining_edges == 0:
            if current_id == target_id:
                contribution = weight_product * (
                    context.parameters.path_length_decay ** max(path_length - 2, 0)
                )
                collected_paths.append(
                    GraphPath(
                        node_ids=tuple(node_path),
                        edge_types=tuple(edge_types),
                        weight_product=round(weight_product, 6),
                        contribution=round(contribution, 6),
                    )
                )
            return

        for edge in context.adjacency.get(current_id, ()):
            if edge.neighbor_id in visited:
                continue
            node_path.append(edge.neighbor_id)
            edge_types.append(edge.edge_type)
            visited.add(edge.neighbor_id)
            _walk(
                edge.neighbor_id,
                remaining_edges=remaining_edges - 1,
                visited=visited,
                node_path=node_path,
                edge_types=edge_types,
                weight_product=weight_product * edge.weight,
            )
            visited.remove(edge.neighbor_id)
            edge_types.pop()
            node_path.pop()

    _walk(
        context.seed_id,
        remaining_edges=path_length,
        visited={context.seed_id},
        node_path=[context.seed_id],
        edge_types=[],
        weight_product=1.0,
    )
    return collected_paths


def _motif_weight_by_type(paths: list[GraphPath]) -> dict[str, float]:
    motif_weights: dict[str, float] = defaultdict(float)
    for path in paths:
        motif_weights[_motif_label(path.edge_types)] += path.contribution
    return dict(motif_weights)


def _motif_label(edge_types: tuple[str, ...]) -> str:
    if edge_types and all(edge_type == CITATION_EDGE_TYPE for edge_type in edge_types):
        return "citation"
    if edge_types and all(edge_type == RELATED_EDGE_TYPE for edge_type in edge_types):
        return "related"
    return "mixed"


def _dominant_motif(motif_weights: dict[str, float]) -> str | None:
    if not motif_weights:
        return None
    return sorted(
        motif_weights.items(),
        key=lambda item: (-item[1], item[0]),
    )[0][0]


def _canonicalize_openalex_id(openalex_id: str) -> str:
    stripped = openalex_id.strip().rstrip("/") or openalex_id
    try:
        return normalize_openalex_work_id(stripped)
    except ValueError:
        return stripped
