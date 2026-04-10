from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from math import log2


def precision_at_k(ranked_ids: Sequence[str], relevant_ids: set[str], k: int) -> float:
    if k <= 0:
        return 0.0
    top_k = ranked_ids[:k]
    if not top_k:
        return 0.0
    hits = sum(1 for item in top_k if item in relevant_ids)
    return hits / len(top_k)


def reciprocal_rank(ranked_ids: Sequence[str], relevant_ids: set[str]) -> float:
    for index, item in enumerate(ranked_ids, start=1):
        if item in relevant_ids:
            return 1.0 / index
    return 0.0


def mean_reciprocal_rank(rankings: Iterable[Sequence[str]], relevance_sets: Iterable[set[str]]) -> float:
    values = [
        reciprocal_rank(ranked_ids, relevant_ids)
        for ranked_ids, relevant_ids in zip(rankings, relevance_sets, strict=False)
    ]
    return sum(values) / len(values) if values else 0.0


def precision_from_labels(labels: Sequence[int], *, relevant_threshold: int = 1) -> float:
    """Return binary precision over an already-selected judged ranking window."""

    if not labels:
        return 0.0
    hits = sum(1 for label in labels if label >= relevant_threshold)
    return hits / len(labels)


def recall_from_labels(
    window_labels: Sequence[int],
    all_labels: Sequence[int],
    *,
    relevant_threshold: int = 1,
) -> float:
    """Return binary recall over judged rows, safely handling zero relevant items."""

    total_relevant = sum(1 for label in all_labels if label >= relevant_threshold)
    if total_relevant == 0:
        return 0.0
    hits = sum(1 for label in window_labels if label >= relevant_threshold)
    return hits / total_relevant


def dcg_from_labels(labels: Sequence[int]) -> float:
    """Return DCG for graded labels in ranked order."""

    total = 0.0
    for index, label in enumerate(labels, start=1):
        total += label / log2(index + 1)
    return total


def ndcg_from_labels(labels: Sequence[int]) -> float:
    """Return nDCG for graded labels in ranked order, or 0.0 if the ideal DCG is zero."""

    if not labels:
        return 0.0
    dcg = dcg_from_labels(labels)
    ideal_dcg = dcg_from_labels(sorted(labels, reverse=True))
    if ideal_dcg == 0.0:
        return 0.0
    return dcg / ideal_dcg


def mean_value_by_label(
    scored_rows: Sequence[tuple[int, float]],
    *,
    labels: Sequence[int] = (0, 1, 2),
) -> dict[str, float | None]:
    """Return a stable label-keyed mean mapping, using None when no rows exist."""

    result: dict[str, float | None] = {}
    for label in labels:
        values = [value for row_label, value in scored_rows if row_label == label]
        result[str(label)] = (sum(values) / len(values)) if values else None
    return result


def brier_score_from_probabilities(targets: Sequence[int], probabilities: Sequence[float]) -> float:
    """Return the mean squared probability error, or 0.0 when no judged rows exist."""

    if not targets or not probabilities:
        return 0.0
    if len(targets) != len(probabilities):
        raise ValueError("Targets and probabilities must have the same length")

    total = 0.0
    for target, probability in zip(targets, probabilities, strict=False):
        total += (probability - target) ** 2
    return total / len(targets)


def expected_calibration_error(
    targets: Sequence[int],
    probabilities: Sequence[float],
    *,
    bin_count: int = 10,
) -> float:
    """Return a fixed-bin expected calibration error, or 0.0 when no judged rows exist."""

    if not targets or not probabilities:
        return 0.0
    if len(targets) != len(probabilities):
        raise ValueError("Targets and probabilities must have the same length")
    if bin_count <= 0:
        raise ValueError("bin_count must be positive")

    buckets: list[list[tuple[int, float]]] = [[] for _ in range(bin_count)]
    for target, probability in zip(targets, probabilities, strict=False):
        bounded_probability = min(max(probability, 0.0), 1.0)
        bin_index = min(int(bounded_probability * bin_count), bin_count - 1)
        buckets[bin_index].append((target, bounded_probability))

    total_count = len(targets)
    error = 0.0
    for bucket in buckets:
        if not bucket:
            continue
        bucket_accuracy = sum(target for target, _ in bucket) / len(bucket)
        bucket_confidence = sum(probability for _, probability in bucket) / len(bucket)
        error += abs(bucket_accuracy - bucket_confidence) * (len(bucket) / total_count)
    return error


def jaccard_similarity(items_a: Sequence[str], items_b: Sequence[str]) -> float:
    """Return Jaccard similarity for two item collections."""

    set_a = {item for item in items_a if item}
    set_b = {item for item in items_b if item}
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def count_position_changes(
    ranked_a: Sequence[str],
    ranked_b: Sequence[str],
    *,
    items: Sequence[str] | None = None,
) -> int:
    """Count shared items whose rank position differs between two rankings."""

    if items is None:
        compared_items = sorted(set(ranked_a) & set(ranked_b))
    else:
        compared_items = [item for item in items if item in ranked_a and item in ranked_b]
    if not compared_items:
        return 0

    positions_a = {item: index for index, item in enumerate(ranked_a)}
    positions_b = {item: index for index, item in enumerate(ranked_b)}
    return sum(
        1
        for item in compared_items
        if positions_a.get(item) is not None
        and positions_b.get(item) is not None
        and positions_a[item] != positions_b[item]
    )


def count_pair_order_reversals(
    ranks_a: Mapping[str, int],
    ranks_b: Mapping[str, int],
    *,
    labels_by_item: Mapping[str, int] | None = None,
) -> tuple[int, int, int]:
    """Count pairwise order reversals, optionally split by label agreement."""

    shared_items = sorted(set(ranks_a) & set(ranks_b))
    if len(shared_items) < 2:
        return 0, 0, 0

    total = 0
    cross_label = 0
    same_label = 0
    for left_index, left_item in enumerate(shared_items):
        left_rank_a = ranks_a[left_item]
        left_rank_b = ranks_b[left_item]
        left_label = labels_by_item.get(left_item) if labels_by_item is not None else None
        for right_item in shared_items[left_index + 1 :]:
            right_rank_a = ranks_a[right_item]
            right_rank_b = ranks_b[right_item]
            if (left_rank_a - right_rank_a) * (left_rank_b - right_rank_b) >= 0:
                continue
            total += 1
            if labels_by_item is None:
                continue
            right_label = labels_by_item.get(right_item)
            if left_label is None or right_label is None:
                continue
            if left_label == right_label:
                same_label += 1
            else:
                cross_label += 1
    return total, cross_label, same_label


def pairwise_label_order_stats(
    ranks: Mapping[str, int],
    labels_by_item: Mapping[str, int],
    *,
    items: Sequence[str] | None = None,
) -> dict[str, float | int | None]:
    """Summarize directional label-order quality for judged items with different labels."""

    if items is None:
        compared_items = sorted(set(ranks) & set(labels_by_item))
    else:
        compared_items = [
            item for item in items if item in ranks and item in labels_by_item
        ]
    if len(compared_items) < 2:
        return {
            "concordant_pair_count": 0,
            "discordant_pair_count": 0,
            "tied_pair_count": 0,
            "weighted_concordant_pair_count": 0.0,
            "weighted_discordant_pair_count": 0.0,
            "weighted_tied_pair_count": 0.0,
            "pairwise_label_order_accuracy": None,
            "weighted_pairwise_label_order_accuracy": None,
        }

    concordant = 0
    discordant = 0
    tied = 0
    weighted_concordant = 0.0
    weighted_discordant = 0.0
    weighted_tied = 0.0

    for left_index, left_item in enumerate(compared_items):
        left_label = labels_by_item[left_item]
        left_rank = ranks[left_item]
        for right_item in compared_items[left_index + 1 :]:
            right_label = labels_by_item[right_item]
            if left_label == right_label:
                continue
            right_rank = ranks[right_item]
            weight = float(abs(left_label - right_label))
            if left_rank == right_rank:
                tied += 1
                weighted_tied += weight
                continue

            if left_label > right_label:
                higher_label_rank = left_rank
                lower_label_rank = right_rank
            else:
                higher_label_rank = right_rank
                lower_label_rank = left_rank

            if higher_label_rank < lower_label_rank:
                concordant += 1
                weighted_concordant += weight
            else:
                discordant += 1
                weighted_discordant += weight

    strict_pair_count = concordant + discordant
    weighted_strict_pair_count = weighted_concordant + weighted_discordant
    return {
        "concordant_pair_count": concordant,
        "discordant_pair_count": discordant,
        "tied_pair_count": tied,
        "weighted_concordant_pair_count": weighted_concordant,
        "weighted_discordant_pair_count": weighted_discordant,
        "weighted_tied_pair_count": weighted_tied,
        "pairwise_label_order_accuracy": (
            concordant / strict_pair_count if strict_pair_count > 0 else None
        ),
        "weighted_pairwise_label_order_accuracy": (
            weighted_concordant / weighted_strict_pair_count
            if weighted_strict_pair_count > 0
            else None
        ),
    }
