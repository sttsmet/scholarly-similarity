from __future__ import annotations

import csv
import hashlib
import json
import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copyfile
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from src.config import REPO_ROOT, TheoryConfig, load_runtime_config
from src.eval.metrics import (
    brier_score_from_probabilities,
    dcg_from_labels,
    expected_calibration_error,
    mean_value_by_label,
    ndcg_from_labels,
    precision_from_labels,
    recall_from_labels,
)
from src.graph.build_local_corpus import build_local_corpus, build_local_corpus_run_id
from src.ingest.cache_store import CacheStore
from src.ingest.doi_resolver import DOIResolver, NormalizedOpenAlexRecord, normalize_doi
from src.ingest.openalex_client import OpenAlexClient, normalize_openalex_work_id
from src.rank.ranker import RankingSummary, ScoreRanges, rank_local_corpus
from src.rank.scorer import ScoredCandidateRecord


LABEL_TEMPLATE_COLUMNS = [
    "seed_openalex_id",
    "seed_title",
    "candidate_openalex_id",
    "title",
    "publication_year",
    "label",
    "label_confidence",
    "aspect",
    "annotator_id",
    "notes",
    "adjudicated_label",
    "adjudication_notes",
]
ADJUDICATION_TEMPLATE_COLUMNS = [*LABEL_TEMPLATE_COLUMNS, "prior_labels"]
MANUAL_LABEL_REQUIRED_COLUMNS = (
    "seed_openalex_id",
    "candidate_openalex_id",
    "label",
)
ANNOTATION_LABEL_REQUIRED_COLUMNS = (
    "seed_openalex_id",
    "candidate_openalex_id",
    "label",
    "label_confidence",
    "aspect",
    "annotator_id",
    "notes",
    "adjudicated_label",
    "adjudication_notes",
)
BENCHMARK_LABEL_COLUMNS = [
    "seed_openalex_id",
    "candidate_openalex_id",
    "label",
    "label_confidence",
    "label_reason",
    "notes",
]
SILVER_LABEL_COLUMNS = [
    "seed_openalex_id",
    "candidate_openalex_id",
    "label",
    "label_confidence",
    "label_reason",
    "origin_flags",
]
CASE_LIMIT = 5
BATCH_DIRNAME = "batches"
DEFAULT_BATCH_MAX_REFERENCES = 10
DEFAULT_BATCH_MAX_RELATED = 10
DEFAULT_BATCH_MAX_HARD_NEGATIVES = 10
DEFAULT_BATCH_TOP_K = 10
AGGREGATE_METRIC_NAMES = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
    "brier_score",
    "expected_calibration_error",
)
BEST_WORST_LIMIT = 5
EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION = "silver_provenance_regression"
EVALUATION_MODE_INDEPENDENT_BENCHMARK = "independent_benchmark"
EVALUATION_MODE_VALUES = (
    EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
    EVALUATION_MODE_INDEPENDENT_BENCHMARK,
)
DEFAULT_EVALUATION_MODE = EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION
METRIC_SCOPE_LOCAL_CORPUS_RANKING = "local_corpus_ranking"
BENCHMARK_SCHEMA_VERSION_V1 = "benchmark_labels.v1"
ANNOTATION_SCHEMA_VERSION_V1 = "benchmark_annotation_rows.v1"
ANNOTATION_DEFAULT_ASPECT = "lineage"
ANNOTATION_ALLOWED_ASPECTS = (ANNOTATION_DEFAULT_ASPECT,)
BENCHMARK_DATASETS_DIRNAME = Path("data") / "benchmarks" / "datasets"
ANNOTATION_BATCHES_DIRNAME = Path("data") / "benchmarks" / "annotation_batches"
CONFLICT_ADJUDICATION_DIRNAME = Path("data") / "benchmarks" / "adjudication_batches"
BENCHMARK_DATASET_LABELS_FILENAME = "benchmark_labels.csv"
BENCHMARK_DATASET_MANIFEST_FILENAME = "benchmark_dataset_manifest.json"
BENCHMARK_DATASET_SOURCE_ANNOTATIONS_DIRNAME = "source_annotations"
ANNOTATION_BATCH_MANIFEST_FILENAME = "annotation_batch_manifest.json"
ANNOTATION_BATCH_ASSIGNMENTS_FILENAME = "pair_assignments.jsonl"
CONFLICT_ADJUDICATION_MANIFEST_FILENAME = "conflict_adjudication_manifest.json"
CONFLICT_ADJUDICATION_TEMPLATE_FILENAME = "conflict_adjudication_template.csv"
BENCHMARK_MATURITY_INSUFFICIENT = "insufficient"
BENCHMARK_MATURITY_PROTOTYPE = "prototype"
BENCHMARK_MATURITY_PILOT = "pilot"
BENCHMARK_MATURITY_PROMOTION_READY = "promotion_ready"
ANNOTATION_BATCH_SCHEMA_VERSION_V1 = "benchmark_annotation_batch.v1"
ADJUDICATION_TEMPLATE_SCHEMA_VERSION_V1 = "benchmark_adjudication_rows.v1"
LINEAGE_STRATUM_STRONG_LINEAGE = "strong_lineage"
LINEAGE_STRATUM_INDIRECT_LINEAGE = "indirect_lineage"
LINEAGE_STRATUM_AMBIGUOUS_MIDDLE = "ambiguous_middle"
LINEAGE_STRATUM_PROVENANCE_WEAK = "provenance_weak"
LINEAGE_STRATUM_HARD_NEGATIVE_OR_DISTRACTOR = "hard_negative_or_distractor"
LINEAGE_STRATUM_ORDER = (
    LINEAGE_STRATUM_STRONG_LINEAGE,
    LINEAGE_STRATUM_INDIRECT_LINEAGE,
    LINEAGE_STRATUM_AMBIGUOUS_MIDDLE,
    LINEAGE_STRATUM_PROVENANCE_WEAK,
    LINEAGE_STRATUM_HARD_NEGATIVE_OR_DISTRACTOR,
)
DEFAULT_LINEAGE_BATCH_TARGET_PAIR_COUNT = 50
DEFAULT_LINEAGE_BATCH_PER_SEED_QUOTA = 10
DEFAULT_LINEAGE_BATCH_OVERLAP_RATIO = 0.70
DEFAULT_LINEAGE_BATCH_ANNOTATOR_IDS = ("annotator_a", "annotator_b")
DEFAULT_LINEAGE_BATCH_STRATUM_QUOTAS = {
    LINEAGE_STRATUM_STRONG_LINEAGE: 10,
    LINEAGE_STRATUM_INDIRECT_LINEAGE: 10,
    LINEAGE_STRATUM_AMBIGUOUS_MIDDLE: 10,
    LINEAGE_STRATUM_PROVENANCE_WEAK: 10,
    LINEAGE_STRATUM_HARD_NEGATIVE_OR_DISTRACTOR: 10,
}


@dataclass(frozen=True, slots=True)
class BenchmarkSeed:
    """Single benchmark seed row loaded from CSV."""

    query_doi: str
    label: str


@dataclass(frozen=True, slots=True)
class AnnotationPoolRow:
    """Neutral candidate row used by stratified benchmark expansion exports."""

    seed_openalex_id: str
    seed_title: str
    candidate_openalex_id: str
    title: str
    publication_year: int | None
    run_id: str
    run_dir: str
    rank: int
    sim: float
    stratum: str
    candidate_origins: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvaluationLabelRow:
    """Generic label row joined to a ranked candidate by OpenAlex identifier."""

    seed_openalex_id: str
    candidate_openalex_id: str
    label: int | None
    label_confidence: float | None = None
    label_reason: str = ""
    origin_flags: tuple[str, ...] = ()
    notes: str = ""


class LabelTemplateExportResult(BaseModel):
    """Structured result returned by label template export."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    seed_openalex_id: str
    candidate_count: int
    exported_count: int
    top_k_used: int | None = None
    output_path: str


class BenchmarkPresetLabelTemplateExportResult(BaseModel):
    """Structured result returned by benchmark-preset annotation export."""

    model_config = ConfigDict(extra="forbid")

    benchmark_preset_id: str
    benchmark_preset_path: str
    seeds_csv_path: str
    seed_count: int
    run_count: int
    exported_count: int
    top_k_used: int | None = None
    output_path: str
    manifest_path: str


class MaterializedBenchmarkDatasetManifest(BaseModel):
    """Frozen manifest describing one materialized independent benchmark dataset."""

    model_config = ConfigDict(extra="forbid")

    benchmark_dataset_id: str
    benchmark_dataset_dir: str
    benchmark_labels_path: str
    benchmark_dataset_manifest_path: str
    benchmark_schema_version: str
    annotation_schema_version: str
    benchmark_labels_row_count: int
    aspect_scope: str
    labels_sha256: str
    created_at: str
    source_annotation_files: list[str]
    source_annotation_row_count: int
    seed_count: int
    per_seed_counts: dict[str, int]
    label_distribution: dict[str, int]
    annotator_ids: list[str]
    annotator_count: int
    multi_annotated_pair_count: int
    overlap_pair_count: int
    agreement_summary: dict[str, Any]
    conflict_pair_count: int
    adjudicated_conflict_count: int
    adjudication_complete: bool
    benchmark_maturity_tier: str
    promotion_ready: bool
    promotion_ineligibility_reasons: list[str]
    benchmark_preset_id: str | None = None
    benchmark_preset_path: str | None = None


class MaterializedBenchmarkDatasetResult(BaseModel):
    """Structured result returned by benchmark dataset materialization."""

    model_config = ConfigDict(extra="forbid")

    benchmark_dataset_id: str
    dataset_dir: str
    benchmark_labels_path: str
    benchmark_dataset_manifest_path: str
    benchmark_labels_sha256: str
    benchmark_labels_row_count: int
    benchmark_schema_version: str
    annotation_schema_version: str
    aspect_scope: str
    source_annotation_files: list[str]
    source_annotation_row_count: int
    seed_count: int
    per_seed_counts: dict[str, int]
    label_distribution: dict[str, int]
    annotator_ids: list[str]
    annotator_count: int
    multi_annotated_pair_count: int
    overlap_pair_count: int
    agreement_summary: dict[str, Any]
    conflict_pair_count: int
    adjudicated_conflict_count: int
    adjudication_complete: bool
    benchmark_maturity_tier: str
    promotion_ready: bool
    promotion_ineligibility_reasons: list[str]
    benchmark_preset_id: str | None = None
    benchmark_preset_path: str | None = None


class StratifiedAnnotationBatchOutputPaths(BaseModel):
    """Absolute output paths written by one stratified annotation batch export."""

    model_config = ConfigDict(extra="forbid")

    batch_manifest_json: str
    pair_assignments_jsonl: str
    annotator_csv_paths: dict[str, str]


class StratifiedAnnotationBatchManifest(BaseModel):
    """Frozen manifest describing one lineage benchmark expansion batch."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str
    batch_dir: str
    batch_manifest_path: str
    annotation_batch_schema_version: str
    annotation_schema_version: str
    benchmark_dataset_id: str
    aspect_scope: str
    source_benchmark_preset_id: str | None = None
    source_benchmark_preset_path: str | None = None
    source_run_ids: list[str]
    source_run_dirs: list[str]
    seed_ids: list[str]
    target_pair_count: int
    exported_pair_count: int
    overlap_ratio: float = Field(ge=0.0, le=1.0)
    planned_overlap_ratio: float = Field(ge=0.0, le=1.0)
    overlap_pair_count: int = Field(ge=0)
    annotator_ids: list[str]
    per_seed_quota: int = Field(ge=1)
    per_seed_quota_overrides: dict[str, int]
    per_stratum_quotas: dict[str, int]
    seed_pair_counts: dict[str, int]
    strata_counts: dict[str, int]
    created_at: str
    output_paths: StratifiedAnnotationBatchOutputPaths


class StratifiedAnnotationBatchExportResult(BaseModel):
    """Structured result returned by a stratified annotation batch export."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str
    batch_dir: str
    manifest_path: str
    benchmark_dataset_id: str
    aspect_scope: str
    source_run_ids: list[str]
    seed_ids: list[str]
    exported_pair_count: int
    overlap_pair_count: int
    planned_overlap_ratio: float = Field(ge=0.0, le=1.0)
    annotator_ids: list[str]
    strata_counts: dict[str, int]
    seed_pair_counts: dict[str, int]
    output_paths: StratifiedAnnotationBatchOutputPaths


class ConflictAdjudicationExportResult(BaseModel):
    """Structured result returned by conflict-only adjudication export."""

    model_config = ConfigDict(extra="forbid")

    adjudication_id: str
    adjudication_dir: str
    adjudication_manifest_path: str
    output_path: str
    benchmark_dataset_id: str | None = None
    aspect_scope: str
    conflict_pair_count: int
    source_annotation_files: list[str]


class SilverLabelOutputPaths(BaseModel):
    """Absolute output paths written by silver-label generation."""

    model_config = ConfigDict(extra="forbid")

    silver_labels_csv: str
    silver_labels_jsonl: str


class SilverLabelGenerationResult(BaseModel):
    """Structured result returned by silver-label generation."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    seed_openalex_id: str
    candidate_count: int
    judged_count: int
    output_paths: SilverLabelOutputPaths


class EvaluationOutputPaths(BaseModel):
    """Absolute output paths written by local ranking evaluation."""

    model_config = ConfigDict(extra="forbid")

    evaluation_summary_json: str
    judged_candidates_jsonl: str
    evaluation_cases_json: str
    mode_evaluation_summary_json: str | None = None
    mode_judged_candidates_jsonl: str | None = None
    mode_evaluation_cases_json: str | None = None


class LocalRankingEvaluationResult(BaseModel):
    """Structured result returned by local ranking evaluation."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    labels_path: str
    label_source: str
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE
    metric_scope: Literal[METRIC_SCOPE_LOCAL_CORPUS_RANKING] = METRIC_SCOPE_LOCAL_CORPUS_RANKING
    evidence_tier: str | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    benchmark_labels_snapshot_path: str | None = None
    benchmark_labels_row_count: int | None = None
    benchmark_schema_version: str | None = None
    seed_openalex_id: str
    top_k_used: int
    candidate_count: int
    judged_count: int
    judged_fraction: float = Field(ge=0.0, le=1.0)
    metrics: dict[str, object]
    provenance_slice_summaries: dict[str, Any] | None = None
    output_paths: EvaluationOutputPaths


class TheoryEvalOutputPaths(BaseModel):
    """Absolute paths written for one isolated theory experiment."""

    model_config = ConfigDict(extra="forbid")

    experiment_dir: str
    theory_snapshot_yaml: str
    scored_candidates_jsonl: str
    ranking_summary_json: str
    evaluation_summary_json: str
    judged_candidates_jsonl: str
    evaluation_cases_json: str
    mode_evaluation_summary_json: str | None = None
    mode_judged_candidates_jsonl: str | None = None
    mode_evaluation_cases_json: str | None = None
    experiment_manifest_json: str
    metrics_delta_json: str | None = None


class TheoryEvalExperimentResult(BaseModel):
    """Structured result returned by an isolated theory evaluation run."""

    model_config = ConfigDict(extra="forbid")

    run_dir: str
    experiment_id: str
    experiment_dir: str
    theory_config_path: str
    label_source: str
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE
    metric_scope: Literal[METRIC_SCOPE_LOCAL_CORPUS_RANKING] = METRIC_SCOPE_LOCAL_CORPUS_RANKING
    evidence_tier: str | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    benchmark_labels_snapshot_path: str | None = None
    benchmark_labels_row_count: int | None = None
    benchmark_schema_version: str | None = None
    top_k: int
    created_at: str
    seed_openalex_id: str
    candidate_count: int
    judged_count: int
    score_ranges: ScoreRanges
    metrics: dict[str, object]
    output_paths: TheoryEvalOutputPaths


class SeedBatchOptions(BaseModel):
    """Effective per-seed protocol used during a batch run."""

    model_config = ConfigDict(extra="forbid")

    max_references: int = Field(ge=0)
    max_related: int = Field(ge=0)
    max_hard_negatives: int = Field(ge=0)
    top_k: int = Field(ge=1)
    label_source: str = "silver"
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE
    benchmark_labels_path: str | None = None
    benchmark_labels_snapshot_path: str | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    benchmark_labels_row_count: int | None = None
    benchmark_schema_version: str | None = None
    benchmark_maturity_tier: str | None = None
    promotion_ready: bool | None = None
    promotion_ineligibility_reasons: list[str] = Field(default_factory=list)
    evidence_tier: str | None = None
    metric_scope: Literal[METRIC_SCOPE_LOCAL_CORPUS_RANKING] = METRIC_SCOPE_LOCAL_CORPUS_RANKING
    refresh: bool = False


class BatchOutputPaths(BaseModel):
    """Absolute output paths written for a batch directory."""

    model_config = ConfigDict(extra="forbid")

    batch_manifest_json: str
    seed_runs_jsonl: str
    aggregate_summary_json: str
    seed_table_jsonl: str
    worst_cases_json: str


class SeedBatchManifest(BaseModel):
    """Top-level manifest for one multi-seed batch run."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str
    batch_dir: str
    seeds_csv: str
    theory_config: str
    created_at: str
    completed_at: str | None = None
    status: Literal["running", "completed"]
    seed_count: int
    completed_seed_count: int
    failed_seed_count: int
    options: SeedBatchOptions
    output_paths: BatchOutputPaths


class SeedBatchRunRecord(BaseModel):
    """One per-seed execution record persisted to seed_runs.jsonl."""

    model_config = ConfigDict(extra="forbid")

    batch_index: int = Field(ge=1)
    doi: str
    status: Literal["completed", "failed"]
    started_at: str
    completed_at: str
    duration_seconds: float = Field(ge=0.0)
    run_id: str | None = None
    run_dir: str | None = None
    experiment_id: str
    theory_config: str
    reused_existing_run: bool = False
    corpus_manifest_json: str | None = None
    silver_labels_csv: str | None = None
    benchmark_labels_path: str | None = None
    benchmark_labels_snapshot_path: str | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    benchmark_labels_row_count: int | None = None
    benchmark_schema_version: str | None = None
    benchmark_maturity_tier: str | None = None
    promotion_ready: bool | None = None
    promotion_ineligibility_reasons: list[str] = Field(default_factory=list)
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE
    evidence_tier: str | None = None
    metric_scope: Literal[METRIC_SCOPE_LOCAL_CORPUS_RANKING] = METRIC_SCOPE_LOCAL_CORPUS_RANKING
    experiment_dir: str | None = None
    experiment_manifest_json: str | None = None
    evaluation_summary_json: str | None = None
    evaluation_cases_json: str | None = None
    mode_evaluation_summary_json: str | None = None
    mode_evaluation_cases_json: str | None = None
    mode_judged_candidates_jsonl: str | None = None
    seed_openalex_id: str | None = None
    candidate_count: int | None = None
    judged_count: int | None = None
    metrics: dict[str, Any] | None = None
    failed_stage: str | None = None
    error_type: str | None = None
    error_message: str | None = None


class AggregateMetricStats(BaseModel):
    """Aggregate statistics for one numeric evaluation metric."""

    model_config = ConfigDict(extra="forbid")

    count: int = Field(ge=0)
    mean: float | None = None
    median: float | None = None
    std: float | None = None
    spread: float | None = None
    min: float | None = None
    max: float | None = None


class AggregatedSeedSummary(BaseModel):
    """Compact per-seed summary used in aggregate outputs."""

    model_config = ConfigDict(extra="forbid")

    batch_index: int = Field(ge=1)
    doi: str
    run_dir: str
    experiment_id: str
    seed_openalex_id: str
    ranking_metric: str | None = None
    ranking_value: float | None = None
    precision_at_k: float | None = None
    recall_at_k: float | None = None
    ndcg_at_k: float | None = None
    brier_score: float | None = None
    expected_calibration_error: float | None = None
    evaluation_summary_json: str
    evaluation_cases_json: str | None = None


class BatchAggregateEvalResult(BaseModel):
    """Structured result returned by aggregate-batch-eval."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str
    batch_dir: str
    aggregated_at: str
    seed_count: int
    completed_seed_count: int
    failed_seed_count: int
    ranking_metric: str | None = None
    metric_aggregates: dict[str, AggregateMetricStats]
    best_seeds: list[AggregatedSeedSummary]
    worst_seeds: list[AggregatedSeedSummary]
    failed_seeds: list[dict[str, Any]]
    output_paths: BatchOutputPaths


def load_benchmark_seeds(path: str | Path) -> list[BenchmarkSeed]:
    benchmark_path = Path(path)
    if not benchmark_path.exists():
        raise FileNotFoundError(benchmark_path)

    with benchmark_path.open("r", encoding="utf-8", newline="") as handle:
        rows = [
            [_clean_csv_cell(cell) for cell in row]
            for row in csv.reader(handle)
            if row and any(_clean_csv_cell(cell) for cell in row)
        ]

    filtered_rows = [row for row in rows if not str(row[0]).strip().startswith("#")]
    if not filtered_rows:
        return []

    header_row = filtered_rows[0]
    doi_index = _detect_seed_doi_column(header_row)
    label_index = _detect_seed_label_column(header_row) if doi_index is not None else (1 if len(header_row) > 1 else None)
    data_rows = filtered_rows[1:] if doi_index is not None else filtered_rows

    seen_dois: set[str] = set()
    seeds: list[BenchmarkSeed] = []
    for row in data_rows:
        if not row or not any(cell.strip() for cell in row):
            continue
        raw_doi = row[doi_index].strip() if doi_index is not None and doi_index < len(row) else row[0].strip()
        if not raw_doi:
            continue
        normalized_doi = normalize_doi(raw_doi)
        if normalized_doi in seen_dois:
            continue
        seen_dois.add(normalized_doi)

        label = ""
        if label_index is not None and label_index < len(row):
            label = row[label_index].strip()
        seeds.append(BenchmarkSeed(query_doi=raw_doi, label=label))
    return seeds


def export_label_template(
    *,
    run_dir: Path,
    top_k: int | None,
    output_path: Path | None,
) -> LabelTemplateExportResult:
    """Export a CSV template for manual labeling from ranked local candidates."""

    ranking_summary = _load_ranking_summary(run_dir / "ranking_summary.json")
    seed_record = _load_record(run_dir / "seed_record.json")
    candidates = _load_scored_candidates(run_dir / "scored_candidates.jsonl")
    selected_candidates = candidates[:top_k] if top_k is not None else candidates
    selected_candidates = sorted(selected_candidates, key=_manual_label_sort_key)
    destination = output_path or (run_dir / "label_template.csv")
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LABEL_TEMPLATE_COLUMNS)
        writer.writeheader()
        for candidate in selected_candidates:
            writer.writerow(_annotation_template_row(seed_record, candidate))

    return LabelTemplateExportResult(
        run_dir=str(run_dir),
        seed_openalex_id=ranking_summary.seed_openalex_id,
        candidate_count=len(candidates),
        exported_count=len(selected_candidates),
        top_k_used=min(top_k, len(candidates)) if top_k is not None else None,
        output_path=str(destination),
    )


def export_benchmark_preset_label_template(
    *,
    benchmark_preset_path: str | Path,
    top_k: int | None,
    output_path: Path | None = None,
    runs_root: Path | None = None,
    max_references: int = DEFAULT_BATCH_MAX_REFERENCES,
    max_related: int = DEFAULT_BATCH_MAX_RELATED,
    max_hard_negatives: int = DEFAULT_BATCH_MAX_HARD_NEGATIVES,
) -> BenchmarkPresetLabelTemplateExportResult:
    """Export one blinded annotation CSV across every existing run referenced by a benchmark preset."""

    resolved_preset_path = _resolve_existing_file_path(
        benchmark_preset_path,
        label="Benchmark preset path",
    )
    preset_payload = json.loads(resolved_preset_path.read_text(encoding="utf-8"))
    if not isinstance(preset_payload, dict):
        raise ValueError(f"Benchmark preset must be a JSON object: {resolved_preset_path}")

    benchmark_preset_id = _optional_str(preset_payload.get("benchmark_preset_id")) or resolved_preset_path.stem
    seeds_csv_value = _optional_str(preset_payload.get("seeds_csv"))
    if seeds_csv_value is None:
        raise ValueError(f"Benchmark preset is missing seeds_csv: {resolved_preset_path}")
    seeds_csv_path = _resolve_existing_file_path(seeds_csv_value, label="Benchmark preset seeds_csv")
    seeds = load_benchmark_seeds(seeds_csv_path)
    if not seeds:
        raise ValueError(f"No seed DOIs found in {seeds_csv_path}")

    resolved_runs_root = _resolve_runs_root(runs_root)
    destination = output_path or (
        REPO_ROOT / "data" / "benchmarks" / f"{benchmark_preset_id}_annotation_template.csv"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = destination.with_name(f"{destination.stem}_manifest.json")

    exported_rows: list[dict[str, Any]] = []
    source_runs: list[dict[str, Any]] = []
    for seed_index, seed in enumerate(seeds, start=1):
        run_id = build_local_corpus_run_id(
            doi=seed.query_doi,
            max_references=max_references,
            max_related=max_related,
            max_hard_negatives=max_hard_negatives,
        )
        run_dir = resolved_runs_root / run_id
        seed_record = _load_record(run_dir / "seed_record.json")
        candidates = _load_scored_candidates(run_dir / "scored_candidates.jsonl")
        selected_candidates = candidates[:top_k] if top_k is not None else candidates
        selected_candidates = sorted(selected_candidates, key=_manual_label_sort_key)
        source_runs.append(
            {
                "seed_index": seed_index,
                "query_doi": seed.query_doi,
                "run_id": run_id,
                "run_dir": str(run_dir),
                "seed_openalex_id": seed_record.openalex_id,
                "candidate_count": len(candidates),
                "exported_count": len(selected_candidates),
            }
        )
        for candidate in selected_candidates:
            exported_rows.append(
                {
                    "_seed_index": seed_index,
                    **_annotation_template_row(seed_record, candidate),
                }
            )

    exported_rows.sort(
        key=lambda row: (
            int(row["_seed_index"]),
            str(row.get("title", "")).lower(),
            str(row.get("candidate_openalex_id", "")).lower(),
        )
    )

    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LABEL_TEMPLATE_COLUMNS)
        writer.writeheader()
        for row in exported_rows:
            payload = dict(row)
            payload.pop("_seed_index", None)
            writer.writerow(payload)

    _write_json(
        manifest_path,
        {
            "benchmark_preset_id": benchmark_preset_id,
            "benchmark_preset_path": str(resolved_preset_path),
            "seeds_csv_path": str(seeds_csv_path),
            "seed_count": len(seeds),
            "run_count": len(source_runs),
            "exported_count": len(exported_rows),
            "top_k_used": top_k,
            "annotation_schema_version": ANNOTATION_SCHEMA_VERSION_V1,
            "aspect_scope": ANNOTATION_DEFAULT_ASPECT,
            "candidate_ordering": "seed_csv_order_then_title_then_candidate_openalex_id",
            "source_runs": source_runs,
        },
    )

    return BenchmarkPresetLabelTemplateExportResult(
        benchmark_preset_id=benchmark_preset_id,
        benchmark_preset_path=str(resolved_preset_path),
        seeds_csv_path=str(seeds_csv_path),
        seed_count=len(seeds),
        run_count=len(source_runs),
        exported_count=len(exported_rows),
        top_k_used=top_k,
        output_path=str(destination),
        manifest_path=str(manifest_path),
    )


def export_stratified_lineage_annotation_batch_from_runs(
    *,
    batch_id: str,
    run_dirs: list[str | Path],
    benchmark_dataset_id: str,
    annotator_ids: list[str] | tuple[str, ...] = DEFAULT_LINEAGE_BATCH_ANNOTATOR_IDS,
    target_pair_count: int = DEFAULT_LINEAGE_BATCH_TARGET_PAIR_COUNT,
    per_seed_quota: int = DEFAULT_LINEAGE_BATCH_PER_SEED_QUOTA,
    seed_quota_overrides: dict[str, int] | None = None,
    stratum_quota_overrides: dict[str, int] | None = None,
    planned_overlap_ratio: float = DEFAULT_LINEAGE_BATCH_OVERLAP_RATIO,
    output_root: str | Path | None = None,
    source_benchmark_preset_id: str | None = None,
    source_benchmark_preset_path: str | Path | None = None,
) -> StratifiedAnnotationBatchExportResult:
    """Export blinded annotator batches from existing ranked runs using deterministic lineage strata."""

    normalized_batch_id = _normalize_directory_label(batch_id, label="Annotation batch id")
    normalized_benchmark_dataset_id = _normalize_directory_label(
        benchmark_dataset_id,
        label="Benchmark dataset id",
    )
    normalized_annotators = _normalize_annotator_ids(annotator_ids)
    normalized_target_pair_count = _positive_int(
        target_pair_count,
        label="target_pair_count",
    )
    normalized_per_seed_quota = _positive_int(per_seed_quota, label="per_seed_quota")
    normalized_overlap_ratio = _normalize_ratio(
        planned_overlap_ratio,
        label="planned_overlap_ratio",
    )
    normalized_seed_quota_overrides = _normalize_seed_quota_overrides(seed_quota_overrides)
    normalized_stratum_quotas = _normalize_stratum_quota_overrides(stratum_quota_overrides)
    resolved_run_dirs = _resolve_ranked_run_dirs(run_dirs)
    if not resolved_run_dirs:
        raise ValueError("At least one ranked run directory is required.")

    pool_rows = _collect_annotation_pool_rows(resolved_run_dirs)
    if not pool_rows:
        raise ValueError("No scored candidate pairs were found across the provided runs.")

    selection = _select_stratified_annotation_rows(
        pool_rows=pool_rows,
        target_pair_count=normalized_target_pair_count,
        per_seed_quota=normalized_per_seed_quota,
        seed_quota_overrides=normalized_seed_quota_overrides,
        per_stratum_quotas=normalized_stratum_quotas,
    )
    if not selection["selected_rows"]:
        raise ValueError("No candidate pairs matched the requested stratified export constraints.")

    batch_root = _resolve_annotation_batch_root(output_root)
    batch_dir = batch_root / normalized_batch_id
    if batch_dir.exists():
        raise FileExistsError(f"Annotation batch directory already exists: {batch_dir}")
    batch_dir.mkdir(parents=True, exist_ok=False)

    manifest_path = batch_dir / ANNOTATION_BATCH_MANIFEST_FILENAME
    assignments_path = batch_dir / ANNOTATION_BATCH_ASSIGNMENTS_FILENAME
    assigned_rows, overlap_pair_count, realized_overlap_ratio = _assign_annotation_rows(
        selection["selected_rows"],
        annotator_ids=normalized_annotators,
        planned_overlap_ratio=normalized_overlap_ratio,
    )

    annotator_output_paths: dict[str, str] = {}
    for annotator_id in normalized_annotators:
        annotator_path = batch_dir / f"{annotator_id}.csv"
        annotator_rows = sorted(
            assigned_rows.get(annotator_id, []),
            key=_annotation_export_row_sort_key,
        )
        _write_annotation_export_csv(annotator_path, annotator_rows, annotator_id=annotator_id)
        annotator_output_paths[annotator_id] = str(annotator_path)

    _write_annotation_pair_assignments(assignments_path, assigned_rows)
    manifest = StratifiedAnnotationBatchManifest(
        batch_id=normalized_batch_id,
        batch_dir=str(batch_dir),
        batch_manifest_path=str(manifest_path),
        annotation_batch_schema_version=ANNOTATION_BATCH_SCHEMA_VERSION_V1,
        annotation_schema_version=ANNOTATION_SCHEMA_VERSION_V1,
        benchmark_dataset_id=normalized_benchmark_dataset_id,
        aspect_scope=ANNOTATION_DEFAULT_ASPECT,
        source_benchmark_preset_id=_optional_str(source_benchmark_preset_id),
        source_benchmark_preset_path=(
            str(_resolve_existing_file_path(source_benchmark_preset_path, label="Benchmark preset path"))
            if source_benchmark_preset_path is not None
            else None
        ),
        source_run_ids=list(selection["source_run_ids"]),
        source_run_dirs=list(selection["source_run_dirs"]),
        seed_ids=list(selection["seed_ids"]),
        target_pair_count=normalized_target_pair_count,
        exported_pair_count=len(selection["selected_rows"]),
        overlap_ratio=realized_overlap_ratio,
        planned_overlap_ratio=normalized_overlap_ratio,
        overlap_pair_count=overlap_pair_count,
        annotator_ids=list(normalized_annotators),
        per_seed_quota=normalized_per_seed_quota,
        per_seed_quota_overrides=dict(normalized_seed_quota_overrides),
        per_stratum_quotas=dict(normalized_stratum_quotas),
        seed_pair_counts=dict(selection["seed_pair_counts"]),
        strata_counts=dict(selection["strata_counts"]),
        created_at=_utcnow(),
        output_paths=StratifiedAnnotationBatchOutputPaths(
            batch_manifest_json=str(manifest_path),
            pair_assignments_jsonl=str(assignments_path),
            annotator_csv_paths=dict(annotator_output_paths),
        ),
    )
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    return StratifiedAnnotationBatchExportResult(
        batch_id=normalized_batch_id,
        batch_dir=str(batch_dir),
        manifest_path=str(manifest_path),
        benchmark_dataset_id=normalized_benchmark_dataset_id,
        aspect_scope=ANNOTATION_DEFAULT_ASPECT,
        source_run_ids=list(selection["source_run_ids"]),
        seed_ids=list(selection["seed_ids"]),
        exported_pair_count=len(selection["selected_rows"]),
        overlap_pair_count=overlap_pair_count,
        planned_overlap_ratio=normalized_overlap_ratio,
        annotator_ids=list(normalized_annotators),
        strata_counts=dict(selection["strata_counts"]),
        seed_pair_counts=dict(selection["seed_pair_counts"]),
        output_paths=manifest.output_paths,
    )


def export_stratified_lineage_annotation_batch_from_benchmark_preset(
    *,
    batch_id: str,
    benchmark_preset_path: str | Path,
    benchmark_dataset_id: str,
    annotator_ids: list[str] | tuple[str, ...] = DEFAULT_LINEAGE_BATCH_ANNOTATOR_IDS,
    target_pair_count: int = DEFAULT_LINEAGE_BATCH_TARGET_PAIR_COUNT,
    per_seed_quota: int = DEFAULT_LINEAGE_BATCH_PER_SEED_QUOTA,
    seed_quota_overrides: dict[str, int] | None = None,
    stratum_quota_overrides: dict[str, int] | None = None,
    planned_overlap_ratio: float = DEFAULT_LINEAGE_BATCH_OVERLAP_RATIO,
    output_root: str | Path | None = None,
    runs_root: Path | None = None,
    max_references: int = DEFAULT_BATCH_MAX_REFERENCES,
    max_related: int = DEFAULT_BATCH_MAX_RELATED,
    max_hard_negatives: int = DEFAULT_BATCH_MAX_HARD_NEGATIVES,
) -> StratifiedAnnotationBatchExportResult:
    """Export one stratified lineage annotation batch from the existing runs referenced by a preset."""

    resolved_preset_path = _resolve_existing_file_path(
        benchmark_preset_path,
        label="Benchmark preset path",
    )
    preset_payload = json.loads(resolved_preset_path.read_text(encoding="utf-8"))
    if not isinstance(preset_payload, dict):
        raise ValueError(f"Benchmark preset must be a JSON object: {resolved_preset_path}")

    benchmark_preset_id = _optional_str(preset_payload.get("benchmark_preset_id")) or resolved_preset_path.stem
    resolved_run_dirs = _resolve_benchmark_preset_run_dirs(
        benchmark_preset_path=resolved_preset_path,
        runs_root=runs_root,
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
    )
    return export_stratified_lineage_annotation_batch_from_runs(
        batch_id=batch_id,
        run_dirs=[str(path) for path in resolved_run_dirs],
        benchmark_dataset_id=benchmark_dataset_id,
        annotator_ids=annotator_ids,
        target_pair_count=target_pair_count,
        per_seed_quota=per_seed_quota,
        seed_quota_overrides=seed_quota_overrides,
        stratum_quota_overrides=stratum_quota_overrides,
        planned_overlap_ratio=planned_overlap_ratio,
        output_root=output_root,
        source_benchmark_preset_id=benchmark_preset_id,
        source_benchmark_preset_path=resolved_preset_path,
    )


def export_lineage_conflict_adjudication_template(
    *,
    annotation_paths: list[str | Path],
    adjudication_id: str,
    adjudicator_id: str = "adjudicator",
    output_root: str | Path | None = None,
    benchmark_dataset_id: str | None = None,
    aspect_scope: str = ANNOTATION_DEFAULT_ASPECT,
) -> ConflictAdjudicationExportResult:
    """Export only unresolved annotation conflicts into a blinded adjudication template."""

    normalized_adjudication_id = _normalize_directory_label(
        adjudication_id,
        label="Adjudication batch id",
    )
    normalized_aspect_scope = _normalize_annotation_aspect(aspect_scope)
    normalized_adjudicator_id = _normalize_nonempty_annotator_id(adjudicator_id)
    resolved_annotation_paths = [
        _resolve_existing_file_path(path, label="Annotation file")
        for path in annotation_paths
    ]
    if not resolved_annotation_paths:
        raise ValueError("At least one annotation file is required for adjudication export.")

    conflict_rows = _collect_unresolved_conflict_rows(
        resolved_annotation_paths,
        aspect_scope=normalized_aspect_scope,
        adjudicator_id=normalized_adjudicator_id,
    )
    adjudication_root = _resolve_conflict_adjudication_root(output_root)
    adjudication_dir = adjudication_root / normalized_adjudication_id
    if adjudication_dir.exists():
        raise FileExistsError(f"Adjudication directory already exists: {adjudication_dir}")
    adjudication_dir.mkdir(parents=True, exist_ok=False)

    output_path = adjudication_dir / CONFLICT_ADJUDICATION_TEMPLATE_FILENAME
    manifest_path = adjudication_dir / CONFLICT_ADJUDICATION_MANIFEST_FILENAME
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ADJUDICATION_TEMPLATE_COLUMNS)
        writer.writeheader()
        for row in sorted(conflict_rows, key=_annotation_export_row_sort_key):
            writer.writerow(row)

    _write_json(
        manifest_path,
        {
            "adjudication_id": normalized_adjudication_id,
            "adjudication_dir": str(adjudication_dir),
            "adjudication_manifest_path": str(manifest_path),
            "output_path": str(output_path),
            "annotation_schema_version": ANNOTATION_SCHEMA_VERSION_V1,
            "adjudication_schema_version": ADJUDICATION_TEMPLATE_SCHEMA_VERSION_V1,
            "benchmark_dataset_id": _optional_str(benchmark_dataset_id),
            "aspect_scope": normalized_aspect_scope,
            "conflict_pair_count": len(conflict_rows),
            "source_annotation_files": [str(path) for path in resolved_annotation_paths],
            "created_at": _utcnow(),
        },
    )
    return ConflictAdjudicationExportResult(
        adjudication_id=normalized_adjudication_id,
        adjudication_dir=str(adjudication_dir),
        adjudication_manifest_path=str(manifest_path),
        output_path=str(output_path),
        benchmark_dataset_id=_optional_str(benchmark_dataset_id),
        aspect_scope=normalized_aspect_scope,
        conflict_pair_count=len(conflict_rows),
        source_annotation_files=[str(path) for path in resolved_annotation_paths],
    )


def materialize_independent_benchmark_dataset(
    *,
    annotation_paths: list[str | Path],
    benchmark_dataset_id: str,
    seeds_csv_path: str | Path,
    output_root: str | Path | None = None,
    aspect_scope: str = ANNOTATION_DEFAULT_ASPECT,
    benchmark_preset_id: str | None = None,
    benchmark_preset_base_dir: str | Path | None = None,
    benchmark_preset_description: str | None = None,
    benchmark_preset_tags: list[str] | None = None,
) -> MaterializedBenchmarkDatasetResult:
    """Validate, consolidate, and freeze one independent benchmark dataset bundle."""

    normalized_dataset_id = _normalize_directory_label(
        benchmark_dataset_id,
        label="Benchmark dataset id",
    )
    normalized_aspect_scope = _normalize_annotation_aspect(aspect_scope)
    resolved_seeds_csv_path = _resolve_existing_file_path(seeds_csv_path, label="Seeds CSV path")
    resolved_annotation_paths = [
        _resolve_existing_file_path(path, label="Annotation file")
        for path in annotation_paths
    ]
    if not resolved_annotation_paths:
        raise ValueError("At least one annotation file is required.")

    consolidated_rows, source_annotation_row_count, qa_summary = _consolidate_annotation_files(
        resolved_annotation_paths,
        aspect_scope=normalized_aspect_scope,
    )
    if not consolidated_rows:
        raise ValueError("No reviewed annotation rows were found across the provided files.")
    maturity_summary = _benchmark_maturity_metadata(
        rows=consolidated_rows,
        qa_summary=qa_summary,
    )

    dataset_root = _resolve_benchmark_dataset_root(output_root)
    dataset_dir = dataset_root / normalized_dataset_id
    if dataset_dir.exists():
        raise FileExistsError(f"Benchmark dataset directory already exists: {dataset_dir}")

    dataset_dir.mkdir(parents=True, exist_ok=False)
    source_annotations_dir = dataset_dir / BENCHMARK_DATASET_SOURCE_ANNOTATIONS_DIRNAME
    source_annotations_dir.mkdir(parents=True, exist_ok=False)
    labels_path = dataset_dir / BENCHMARK_DATASET_LABELS_FILENAME
    manifest_path = dataset_dir / BENCHMARK_DATASET_MANIFEST_FILENAME

    copied_annotation_paths = _copy_annotation_sources(
        resolved_annotation_paths,
        source_annotations_dir,
    )
    _write_benchmark_labels_csv(labels_path, consolidated_rows)
    labels_sha256 = _sha256_file(labels_path)
    normalized_preset_id = _optional_str(benchmark_preset_id)
    manifest = MaterializedBenchmarkDatasetManifest(
        benchmark_dataset_id=normalized_dataset_id,
        benchmark_dataset_dir=str(dataset_dir),
        benchmark_labels_path=str(labels_path),
        benchmark_dataset_manifest_path=str(manifest_path),
        benchmark_schema_version=BENCHMARK_SCHEMA_VERSION_V1,
        annotation_schema_version=ANNOTATION_SCHEMA_VERSION_V1,
        benchmark_labels_row_count=len(consolidated_rows),
        aspect_scope=normalized_aspect_scope,
        labels_sha256=labels_sha256,
        created_at=_utcnow(),
        source_annotation_files=[str(path) for path in copied_annotation_paths],
        source_annotation_row_count=source_annotation_row_count,
        seed_count=int(maturity_summary["seed_count"]),
        per_seed_counts=dict(maturity_summary["per_seed_counts"]),
        label_distribution=dict(maturity_summary["label_distribution"]),
        annotator_ids=list(qa_summary["annotator_ids"]),
        annotator_count=int(qa_summary["annotator_count"]),
        multi_annotated_pair_count=int(qa_summary["multi_annotated_pair_count"]),
        overlap_pair_count=int(qa_summary["overlap_pair_count"]),
        agreement_summary=dict(qa_summary["agreement_summary"]),
        conflict_pair_count=int(qa_summary["conflict_pair_count"]),
        adjudicated_conflict_count=int(qa_summary["adjudicated_conflict_count"]),
        adjudication_complete=bool(qa_summary["adjudication_complete"]),
        benchmark_maturity_tier=str(maturity_summary["benchmark_maturity_tier"]),
        promotion_ready=bool(maturity_summary["promotion_ready"]),
        promotion_ineligibility_reasons=list(maturity_summary["promotion_ineligibility_reasons"]),
        benchmark_preset_id=normalized_preset_id,
        benchmark_preset_path=None,
    )
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    benchmark_preset_path: Path | None = None
    if normalized_preset_id is not None:
        benchmark_preset_path = _save_materialized_benchmark_preset(
            benchmark_preset_id=normalized_preset_id,
            seeds_csv_path=resolved_seeds_csv_path,
            labels_path=labels_path,
            labels_sha256=labels_sha256,
            benchmark_dataset_id=normalized_dataset_id,
            benchmark_labels_row_count=len(consolidated_rows),
            benchmark_schema_version=BENCHMARK_SCHEMA_VERSION_V1,
            aspect_scope=normalized_aspect_scope,
            benchmark_dataset_manifest_path=manifest_path,
            base_dir=benchmark_preset_base_dir,
            description=benchmark_preset_description,
            tags=benchmark_preset_tags or [],
        )
        manifest = manifest.model_copy(
            update={"benchmark_preset_path": str(benchmark_preset_path)}
        )
        _write_json(manifest_path, manifest.model_dump(mode="json"))

    return MaterializedBenchmarkDatasetResult(
        benchmark_dataset_id=normalized_dataset_id,
        dataset_dir=str(dataset_dir),
        benchmark_labels_path=str(labels_path),
        benchmark_dataset_manifest_path=str(manifest_path),
        benchmark_labels_sha256=labels_sha256,
        benchmark_labels_row_count=len(consolidated_rows),
        benchmark_schema_version=BENCHMARK_SCHEMA_VERSION_V1,
        annotation_schema_version=ANNOTATION_SCHEMA_VERSION_V1,
        aspect_scope=normalized_aspect_scope,
        source_annotation_files=[str(path) for path in copied_annotation_paths],
        source_annotation_row_count=source_annotation_row_count,
        seed_count=int(maturity_summary["seed_count"]),
        per_seed_counts=dict(maturity_summary["per_seed_counts"]),
        label_distribution=dict(maturity_summary["label_distribution"]),
        annotator_ids=list(qa_summary["annotator_ids"]),
        annotator_count=int(qa_summary["annotator_count"]),
        multi_annotated_pair_count=int(qa_summary["multi_annotated_pair_count"]),
        overlap_pair_count=int(qa_summary["overlap_pair_count"]),
        agreement_summary=dict(qa_summary["agreement_summary"]),
        conflict_pair_count=int(qa_summary["conflict_pair_count"]),
        adjudicated_conflict_count=int(qa_summary["adjudicated_conflict_count"]),
        adjudication_complete=bool(qa_summary["adjudication_complete"]),
        benchmark_maturity_tier=str(maturity_summary["benchmark_maturity_tier"]),
        promotion_ready=bool(maturity_summary["promotion_ready"]),
        promotion_ineligibility_reasons=list(maturity_summary["promotion_ineligibility_reasons"]),
        benchmark_preset_id=normalized_preset_id,
        benchmark_preset_path=str(benchmark_preset_path) if benchmark_preset_path is not None else None,
    )


def generate_silver_labels(*, run_dir: Path) -> SilverLabelGenerationResult:
    """Generate deterministic lineage-oriented silver labels from provenance only."""

    seed_record = _load_record(run_dir / "seed_record.json")
    paper_records = _load_papers(run_dir / "papers.jsonl")
    edge_origin_map = _load_edge_origin_map(run_dir / "edges.jsonl", seed_record.openalex_id)
    silver_rows = [
        _build_silver_label_row(seed_record, record, edge_origin_map)
        for record in paper_records
        if record.openalex_id != seed_record.openalex_id
    ]

    csv_path = run_dir / "silver_labels.csv"
    jsonl_path = run_dir / "silver_labels.jsonl"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SILVER_LABEL_COLUMNS)
        writer.writeheader()
        for row in silver_rows:
            writer.writerow(
                {
                    "seed_openalex_id": row.seed_openalex_id,
                    "candidate_openalex_id": row.candidate_openalex_id,
                    "label": "" if row.label is None else row.label,
                    "label_confidence": "" if row.label_confidence is None else f"{row.label_confidence:.3f}",
                    "label_reason": row.label_reason,
                    "origin_flags": "|".join(row.origin_flags),
                }
            )

    _write_jsonl(
        jsonl_path,
        [
            {
                "seed_openalex_id": row.seed_openalex_id,
                "candidate_openalex_id": row.candidate_openalex_id,
                "label": row.label,
                "label_confidence": row.label_confidence,
                "label_reason": row.label_reason,
                "origin_flags": list(row.origin_flags),
            }
            for row in silver_rows
        ],
    )

    judged_count = sum(1 for row in silver_rows if row.label is not None)
    return SilverLabelGenerationResult(
        run_dir=str(run_dir),
        seed_openalex_id=seed_record.openalex_id,
        candidate_count=len(silver_rows),
        judged_count=judged_count,
        output_paths=SilverLabelOutputPaths(
            silver_labels_csv=str(csv_path),
            silver_labels_jsonl=str(jsonl_path),
        ),
    )


def run_theory_eval(
    *,
    run_dir: Path,
    theory_config_path: Path,
    theory: TheoryConfig,
    experiment_id: str,
    label_source: str = "silver",
    top_k: int,
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE,
    benchmark_labels_path: Path | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    benchmark_labels_snapshot_path: Path | None = None,
    benchmark_labels_row_count: int | None = None,
    benchmark_schema_version: str | None = None,
) -> TheoryEvalExperimentResult:
    """Run isolated ranking and evaluation outputs for one theory revision."""
    normalized_mode = _normalize_evaluation_mode(
        evaluation_mode=evaluation_mode,
        label_source=label_source,
    )
    benchmark_source_labels_path: Path | None = None
    benchmark_effective_labels_path: Path | None = None
    normalized_benchmark_snapshot_path = (
        Path(benchmark_labels_snapshot_path)
        if benchmark_labels_snapshot_path is not None
        else None
    )
    if normalized_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK:
        if benchmark_labels_path is None and normalized_benchmark_snapshot_path is None:
            raise ValueError("independent_benchmark mode requires benchmark_labels_path")
        if benchmark_labels_path is not None:
            benchmark_source_labels_path = Path(benchmark_labels_path)
            if not benchmark_source_labels_path.exists():
                raise FileNotFoundError(benchmark_source_labels_path)
        if _optional_str(benchmark_dataset_id) is None:
            raise ValueError("independent_benchmark mode requires benchmark_dataset_id")
        effective_label_source = "benchmark"
    else:
        effective_label_source = "silver"

    safe_experiment_id = _validate_experiment_id(experiment_id)
    experiment_dir = run_dir / "experiments" / safe_experiment_id
    experiment_dir.mkdir(parents=True, exist_ok=True)

    if normalized_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK:
        if normalized_benchmark_snapshot_path is None:
            if benchmark_source_labels_path is None:
                raise ValueError("independent_benchmark mode requires benchmark labels source or snapshot path")
            normalized_benchmark_snapshot_path = experiment_dir / "benchmark_labels_snapshot.csv"
            snapshot_metadata = _freeze_benchmark_labels_snapshot(
                source_path=benchmark_source_labels_path,
                snapshot_path=normalized_benchmark_snapshot_path,
                sha256=benchmark_labels_sha256,
            )
            benchmark_labels_sha256 = snapshot_metadata["benchmark_labels_sha256"]
            benchmark_labels_row_count = snapshot_metadata["benchmark_labels_row_count"]
            benchmark_schema_version = snapshot_metadata["benchmark_schema_version"]
        else:
            if not normalized_benchmark_snapshot_path.exists():
                raise FileNotFoundError(normalized_benchmark_snapshot_path)
            snapshot_metadata = _benchmark_labels_snapshot_metadata(
                normalized_benchmark_snapshot_path,
                sha256=benchmark_labels_sha256,
            )
            benchmark_labels_sha256 = snapshot_metadata["benchmark_labels_sha256"]
            benchmark_labels_row_count = benchmark_labels_row_count or snapshot_metadata["benchmark_labels_row_count"]
            benchmark_schema_version = benchmark_schema_version or snapshot_metadata["benchmark_schema_version"]
        benchmark_effective_labels_path = normalized_benchmark_snapshot_path

    theory_snapshot_path = experiment_dir / "theory_snapshot.yaml"
    theory_snapshot_path.write_text(theory_config_path.read_text(encoding="utf-8"), encoding="utf-8")

    ranking_result = rank_local_corpus(
        run_dir=run_dir,
        theory=theory,
        top_k=top_k,
        output_dir=experiment_dir,
    )
    evaluation_result = evaluate_local_ranking(
        run_dir=experiment_dir,
        labels_path=(
            benchmark_effective_labels_path
            if normalized_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK
            else run_dir / "silver_labels.csv"
        ),
        top_k=top_k,
        label_source=effective_label_source,
        evaluation_mode=normalized_mode,
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=benchmark_labels_sha256,
        benchmark_labels_snapshot_path=normalized_benchmark_snapshot_path,
        benchmark_labels_row_count=benchmark_labels_row_count,
        benchmark_schema_version=benchmark_schema_version,
    )

    metrics_delta_path = _maybe_write_metrics_delta(
        run_dir=run_dir,
        experiment_id=safe_experiment_id,
        experiment_dir=experiment_dir,
        current_metrics=evaluation_result.metrics,
    )
    manifest_path = experiment_dir / "experiment_manifest.json"
    result = TheoryEvalExperimentResult(
        run_dir=str(run_dir),
        experiment_id=safe_experiment_id,
        experiment_dir=str(experiment_dir),
        theory_config_path=str(theory_config_path),
        label_source=effective_label_source,
        evaluation_mode=normalized_mode,
        metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
        evidence_tier=_evidence_tier_for_mode(normalized_mode),
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=benchmark_labels_sha256,
        benchmark_labels_snapshot_path=(
            str(normalized_benchmark_snapshot_path)
            if normalized_benchmark_snapshot_path is not None
            else None
        ),
        benchmark_labels_row_count=benchmark_labels_row_count,
        benchmark_schema_version=benchmark_schema_version,
        top_k=top_k,
        created_at=datetime.now(timezone.utc).isoformat(),
        seed_openalex_id=ranking_result.seed_openalex_id,
        candidate_count=ranking_result.candidate_count,
        judged_count=evaluation_result.judged_count,
        score_ranges=ranking_result.score_ranges,
        metrics=evaluation_result.metrics,
        output_paths=TheoryEvalOutputPaths(
            experiment_dir=str(experiment_dir),
            theory_snapshot_yaml=str(theory_snapshot_path),
            scored_candidates_jsonl=ranking_result.output_paths.scored_candidates_jsonl,
            ranking_summary_json=ranking_result.output_paths.ranking_summary_json,
            evaluation_summary_json=evaluation_result.output_paths.evaluation_summary_json,
            judged_candidates_jsonl=evaluation_result.output_paths.judged_candidates_jsonl,
            evaluation_cases_json=evaluation_result.output_paths.evaluation_cases_json,
            mode_evaluation_summary_json=evaluation_result.output_paths.mode_evaluation_summary_json,
            mode_judged_candidates_jsonl=evaluation_result.output_paths.mode_judged_candidates_jsonl,
            mode_evaluation_cases_json=evaluation_result.output_paths.mode_evaluation_cases_json,
            experiment_manifest_json=str(manifest_path),
            metrics_delta_json=str(metrics_delta_path) if metrics_delta_path is not None else None,
        ),
    )
    _write_json(manifest_path, result.model_dump(mode="json"))
    return result


def run_seed_batch(
    *,
    seeds_path: Path,
    theory_config_path: Path,
    theory: TheoryConfig,
    batch_id: str,
    max_references: int = DEFAULT_BATCH_MAX_REFERENCES,
    max_related: int = DEFAULT_BATCH_MAX_RELATED,
    max_hard_negatives: int = DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    top_k: int = DEFAULT_BATCH_TOP_K,
    label_source: str = "silver",
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] = DEFAULT_EVALUATION_MODE,
    benchmark_labels_path: Path | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    refresh: bool = False,
) -> SeedBatchManifest:
    """Run local corpus build, label loading, and theory eval across many DOI seeds."""
    normalized_mode = _normalize_evaluation_mode(
        evaluation_mode=evaluation_mode,
        label_source=label_source,
    )
    resolved_benchmark_labels_path: Path | None = None
    frozen_benchmark_labels_snapshot_path: Path | None = None
    normalized_benchmark_dataset_id = _optional_str(benchmark_dataset_id)
    normalized_benchmark_labels_sha256 = _optional_str(benchmark_labels_sha256)
    normalized_benchmark_labels_row_count: int | None = None
    normalized_benchmark_schema_version: str | None = None
    normalized_benchmark_maturity_tier: str | None = None
    normalized_promotion_ready: bool | None = None
    normalized_promotion_ineligibility_reasons: list[str] = []
    if normalized_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK:
        if benchmark_labels_path is None:
            raise ValueError("independent_benchmark mode requires benchmark_labels_path")
        resolved_benchmark_labels_path = Path(benchmark_labels_path)
        if not resolved_benchmark_labels_path.exists():
            raise FileNotFoundError(resolved_benchmark_labels_path)
        dataset_metadata = _load_materialized_benchmark_dataset_metadata(
            resolved_benchmark_labels_path
        )
        normalized_benchmark_maturity_tier = _optional_str(
            dataset_metadata.get("benchmark_maturity_tier")
        )
        dataset_promotion_ready = dataset_metadata.get("promotion_ready")
        if isinstance(dataset_promotion_ready, bool):
            normalized_promotion_ready = dataset_promotion_ready
        dataset_ineligibility_reasons = dataset_metadata.get("promotion_ineligibility_reasons")
        if isinstance(dataset_ineligibility_reasons, list):
            normalized_promotion_ineligibility_reasons = [
                reason
                for reason in (
                    _optional_str(item)
                    for item in dataset_ineligibility_reasons
                )
                if reason is not None
            ]
        if normalized_benchmark_dataset_id is None:
            raise ValueError("independent_benchmark mode requires benchmark_dataset_id")
        if normalized_benchmark_labels_sha256 is None:
            normalized_benchmark_labels_sha256 = _sha256_file(resolved_benchmark_labels_path)
        effective_label_source = "benchmark"
    else:
        effective_label_source = "silver"

    safe_batch_id = _validate_batch_id(batch_id)
    runtime = load_runtime_config()
    runs_root = REPO_ROOT / runtime.runs_dir
    batch_dir = runs_root / BATCH_DIRNAME / safe_batch_id
    if batch_dir.exists():
        raise FileExistsError(f"Batch directory already exists: {batch_dir}")

    seeds = load_benchmark_seeds(seeds_path)
    if not seeds:
        raise ValueError(f"No seed DOIs found in {seeds_path}")

    batch_dir.mkdir(parents=True, exist_ok=False)
    if normalized_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK and resolved_benchmark_labels_path is not None:
        frozen_benchmark_labels_snapshot_path = batch_dir / "benchmark_labels_snapshot.csv"
        snapshot_metadata = _freeze_benchmark_labels_snapshot(
            source_path=resolved_benchmark_labels_path,
            snapshot_path=frozen_benchmark_labels_snapshot_path,
            sha256=normalized_benchmark_labels_sha256,
        )
        normalized_benchmark_labels_sha256 = snapshot_metadata["benchmark_labels_sha256"]
        normalized_benchmark_labels_row_count = snapshot_metadata["benchmark_labels_row_count"]
        normalized_benchmark_schema_version = snapshot_metadata["benchmark_schema_version"]
    output_paths = _build_batch_output_paths(batch_dir)
    options = SeedBatchOptions(
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
        top_k=top_k,
        label_source=effective_label_source,
        evaluation_mode=normalized_mode,
        benchmark_labels_path=(
            str(resolved_benchmark_labels_path)
            if resolved_benchmark_labels_path is not None
            else None
        ),
        benchmark_labels_snapshot_path=(
            str(frozen_benchmark_labels_snapshot_path)
            if frozen_benchmark_labels_snapshot_path is not None
            else None
        ),
        benchmark_dataset_id=normalized_benchmark_dataset_id,
        benchmark_labels_sha256=normalized_benchmark_labels_sha256,
        benchmark_labels_row_count=normalized_benchmark_labels_row_count,
        benchmark_schema_version=normalized_benchmark_schema_version,
        benchmark_maturity_tier=normalized_benchmark_maturity_tier,
        promotion_ready=normalized_promotion_ready,
        promotion_ineligibility_reasons=list(normalized_promotion_ineligibility_reasons),
        evidence_tier=_evidence_tier_for_mode(normalized_mode),
        metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
        refresh=refresh,
    )
    created_at = _utcnow()
    manifest = SeedBatchManifest(
        batch_id=safe_batch_id,
        batch_dir=str(batch_dir),
        seeds_csv=str(seeds_path),
        theory_config=str(theory_config_path),
        created_at=created_at,
        status="running",
        seed_count=len(seeds),
        completed_seed_count=0,
        failed_seed_count=0,
        options=options,
        output_paths=output_paths,
    )
    _write_json(Path(output_paths.batch_manifest_json), manifest.model_dump(mode="json"))

    cache_store = CacheStore(REPO_ROOT / runtime.cache_dir)
    openalex_client = OpenAlexClient(
        base_url=runtime.openalex_base_url,
        timeout_seconds=runtime.request_timeout_seconds,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    records: list[SeedBatchRunRecord] = []
    for batch_index, seed in enumerate(seeds, start=1):
        started_at = _utcnow()
        start_time = datetime.now(timezone.utc)
        stage = "build-local-corpus"
        run_id: str | None = None
        run_dir: Path | None = None
        reused_existing_run = False
        corpus_manifest_json: str | None = None

        try:
            run_id = build_local_corpus_run_id(
                doi=seed.query_doi,
                max_references=max_references,
                max_related=max_related,
                max_hard_negatives=max_hard_negatives,
            )
            run_dir = runs_root / run_id
            corpus_manifest_path = run_dir / "manifest.json"
            corpus_manifest_json = str(corpus_manifest_path) if corpus_manifest_path.exists() else None

            if refresh or not _has_local_corpus_artifacts(run_dir):
                corpus_result = build_local_corpus(
                    doi=seed.query_doi,
                    max_references=max_references,
                    max_related=max_related,
                    max_hard_negatives=max_hard_negatives,
                    refresh=refresh,
                    runs_root=runs_root,
                    doi_resolver=doi_resolver,
                    openalex_client=openalex_client,
                    cache_store=cache_store,
                )
                run_id = corpus_result.run_id
                run_dir = Path(corpus_result.output_paths.run_dir)
                corpus_manifest_json = corpus_result.output_paths.manifest
            else:
                reused_existing_run = True

            silver_result: SilverLabelGenerationResult | None = None
            if normalized_mode == EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION:
                stage = "generate-silver-labels"
                silver_result = generate_silver_labels(run_dir=run_dir)

            stage = "run-theory-eval"
            experiment_dir = run_dir / "experiments" / safe_batch_id
            if experiment_dir.exists():
                raise FileExistsError(f"Experiment directory already exists: {experiment_dir}")

            theory_result = run_theory_eval(
                run_dir=run_dir,
                theory_config_path=theory_config_path,
                theory=theory,
                experiment_id=safe_batch_id,
                label_source=effective_label_source,
                top_k=top_k,
                evaluation_mode=normalized_mode,
                benchmark_labels_path=(
                    frozen_benchmark_labels_snapshot_path
                    if frozen_benchmark_labels_snapshot_path is not None
                    else resolved_benchmark_labels_path
                ),
                benchmark_dataset_id=normalized_benchmark_dataset_id,
                benchmark_labels_sha256=normalized_benchmark_labels_sha256,
                benchmark_labels_snapshot_path=frozen_benchmark_labels_snapshot_path,
                benchmark_labels_row_count=normalized_benchmark_labels_row_count,
                benchmark_schema_version=normalized_benchmark_schema_version,
            )

            completed_at = _utcnow()
            duration_seconds = round((datetime.now(timezone.utc) - start_time).total_seconds(), 6)
            records.append(
                SeedBatchRunRecord(
                    batch_index=batch_index,
                    doi=seed.query_doi,
                    status="completed",
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_seconds=duration_seconds,
                    run_id=run_id,
                    run_dir=str(run_dir),
                    experiment_id=safe_batch_id,
                    theory_config=str(theory_config_path),
                    reused_existing_run=reused_existing_run,
                    corpus_manifest_json=corpus_manifest_json,
                    silver_labels_csv=(
                        silver_result.output_paths.silver_labels_csv
                        if silver_result is not None
                        else None
                    ),
                    benchmark_labels_path=(
                        str(resolved_benchmark_labels_path)
                        if resolved_benchmark_labels_path is not None
                        else None
                    ),
                    benchmark_labels_snapshot_path=(
                        str(frozen_benchmark_labels_snapshot_path)
                        if frozen_benchmark_labels_snapshot_path is not None
                        else None
                    ),
                    benchmark_dataset_id=normalized_benchmark_dataset_id,
                    benchmark_labels_sha256=normalized_benchmark_labels_sha256,
                    benchmark_labels_row_count=normalized_benchmark_labels_row_count,
                    benchmark_schema_version=normalized_benchmark_schema_version,
                    benchmark_maturity_tier=normalized_benchmark_maturity_tier,
                    promotion_ready=normalized_promotion_ready,
                    promotion_ineligibility_reasons=list(
                        normalized_promotion_ineligibility_reasons
                    ),
                    evaluation_mode=normalized_mode,
                    evidence_tier=_evidence_tier_for_mode(normalized_mode),
                    metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
                    experiment_dir=theory_result.experiment_dir,
                    experiment_manifest_json=theory_result.output_paths.experiment_manifest_json,
                    evaluation_summary_json=theory_result.output_paths.evaluation_summary_json,
                    evaluation_cases_json=theory_result.output_paths.evaluation_cases_json,
                    mode_evaluation_summary_json=theory_result.output_paths.mode_evaluation_summary_json,
                    mode_evaluation_cases_json=theory_result.output_paths.mode_evaluation_cases_json,
                    mode_judged_candidates_jsonl=theory_result.output_paths.mode_judged_candidates_jsonl,
                    seed_openalex_id=theory_result.seed_openalex_id,
                    candidate_count=theory_result.candidate_count,
                    judged_count=theory_result.judged_count,
                    metrics=theory_result.metrics,
                )
            )
        except Exception as exc:
            completed_at = _utcnow()
            duration_seconds = round((datetime.now(timezone.utc) - start_time).total_seconds(), 6)
            records.append(
                SeedBatchRunRecord(
                    batch_index=batch_index,
                    doi=seed.query_doi,
                    status="failed",
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_seconds=duration_seconds,
                    run_id=run_id,
                    run_dir=str(run_dir) if run_dir is not None else None,
                    experiment_id=safe_batch_id,
                    theory_config=str(theory_config_path),
                    reused_existing_run=reused_existing_run,
                    corpus_manifest_json=corpus_manifest_json,
                    benchmark_labels_path=(
                        str(resolved_benchmark_labels_path)
                        if resolved_benchmark_labels_path is not None
                        else None
                    ),
                    benchmark_labels_snapshot_path=(
                        str(frozen_benchmark_labels_snapshot_path)
                        if frozen_benchmark_labels_snapshot_path is not None
                        else None
                    ),
                    benchmark_dataset_id=normalized_benchmark_dataset_id,
                    benchmark_labels_sha256=normalized_benchmark_labels_sha256,
                    benchmark_labels_row_count=normalized_benchmark_labels_row_count,
                    benchmark_schema_version=normalized_benchmark_schema_version,
                    benchmark_maturity_tier=normalized_benchmark_maturity_tier,
                    promotion_ready=normalized_promotion_ready,
                    promotion_ineligibility_reasons=list(
                        normalized_promotion_ineligibility_reasons
                    ),
                    evaluation_mode=normalized_mode,
                    evidence_tier=_evidence_tier_for_mode(normalized_mode),
                    metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
                    failed_stage=stage,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )

        _write_jsonl(
            Path(output_paths.seed_runs_jsonl),
            [record.model_dump(mode="json") for record in records],
        )
        manifest = manifest.model_copy(
            update={
                "completed_seed_count": sum(1 for record in records if record.status == "completed"),
                "failed_seed_count": sum(1 for record in records if record.status == "failed"),
            }
        )
        _write_json(Path(output_paths.batch_manifest_json), manifest.model_dump(mode="json"))

    aggregate_result = aggregate_batch_eval(batch_dir=batch_dir)
    manifest = manifest.model_copy(
        update={
            "completed_at": aggregate_result.aggregated_at,
            "status": "completed",
            "completed_seed_count": aggregate_result.completed_seed_count,
            "failed_seed_count": aggregate_result.failed_seed_count,
        }
    )
    _write_json(Path(output_paths.batch_manifest_json), manifest.model_dump(mode="json"))
    return manifest


def aggregate_batch_eval(*, batch_dir: Path) -> BatchAggregateEvalResult:
    """Aggregate seed-level evaluation outputs written by a prior batch run."""

    manifest_path = batch_dir / "batch_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)

    manifest = SeedBatchManifest.model_validate(json.loads(manifest_path.read_text(encoding="utf-8")))
    output_paths = manifest.output_paths
    seed_runs_path = Path(output_paths.seed_runs_jsonl)
    records = _load_seed_run_records(seed_runs_path)

    seed_table_rows: list[dict[str, Any]] = []
    completed_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []

    for record in records:
        base_row = {
            "batch_index": record.batch_index,
            "doi": record.doi,
            "run_dir": record.run_dir,
            "experiment_id": record.experiment_id,
            "theory_config": record.theory_config,
            "seed_openalex_id": record.seed_openalex_id,
            "evaluation_mode": record.evaluation_mode,
            "evidence_tier": record.evidence_tier,
            "metric_scope": record.metric_scope,
            "benchmark_labels_path": record.benchmark_labels_path,
            "benchmark_labels_snapshot_path": record.benchmark_labels_snapshot_path,
            "benchmark_dataset_id": record.benchmark_dataset_id,
            "benchmark_labels_sha256": record.benchmark_labels_sha256,
            "benchmark_labels_row_count": record.benchmark_labels_row_count,
            "benchmark_schema_version": record.benchmark_schema_version,
            "candidate_count": record.candidate_count,
            "judged_count": record.judged_count,
            "evaluation_summary_json": record.evaluation_summary_json,
            "evaluation_cases_json": record.evaluation_cases_json,
            "precision_at_k": None,
            "recall_at_k": None,
            "ndcg_at_k": None,
            "brier_score": None,
            "expected_calibration_error": None,
            "error_type": record.error_type,
            "error_message": record.error_message,
        }

        if record.status != "completed" or not record.evaluation_summary_json:
            failed_row = {
                **base_row,
                "status": "failed",
                "error_type": record.error_type or "SeedBatchError",
                "error_message": record.error_message or "Seed did not complete successfully.",
            }
            seed_table_rows.append(failed_row)
            failed_rows.append(failed_row)
            continue

        try:
            evaluation_payload = json.loads(Path(record.evaluation_summary_json).read_text(encoding="utf-8"))
            evaluation_result = LocalRankingEvaluationResult.model_validate(evaluation_payload)
            metrics = evaluation_result.metrics
            completed_row = {
                **base_row,
                "status": "completed",
                "seed_openalex_id": evaluation_result.seed_openalex_id,
                "evaluation_mode": evaluation_result.evaluation_mode,
                "evidence_tier": evaluation_result.evidence_tier,
                "metric_scope": evaluation_result.metric_scope,
                "benchmark_dataset_id": evaluation_result.benchmark_dataset_id,
                "benchmark_labels_sha256": evaluation_result.benchmark_labels_sha256,
                "benchmark_labels_snapshot_path": evaluation_result.benchmark_labels_snapshot_path,
                "benchmark_labels_row_count": evaluation_result.benchmark_labels_row_count,
                "benchmark_schema_version": evaluation_result.benchmark_schema_version,
                "candidate_count": evaluation_result.candidate_count,
                "judged_count": evaluation_result.judged_count,
                "precision_at_k": _metric_value(metrics, "precision_at_k"),
                "recall_at_k": _metric_value(metrics, "recall_at_k"),
                "ndcg_at_k": _metric_value(metrics, "ndcg_at_k"),
                "brier_score": _metric_value(metrics, "brier_score"),
                "expected_calibration_error": _metric_value(metrics, "expected_calibration_error"),
                "error_type": None,
                "error_message": None,
            }
            seed_table_rows.append(completed_row)
            completed_rows.append(completed_row)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            failed_row = {
                **base_row,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            }
            seed_table_rows.append(failed_row)
            failed_rows.append(failed_row)

    _write_jsonl(Path(output_paths.seed_table_jsonl), seed_table_rows)

    ranking_metric = _select_batch_ranking_metric(completed_rows)
    metric_aggregates = {
        metric_name: _aggregate_metric_stats([row.get(metric_name) for row in completed_rows])
        for metric_name in AGGREGATE_METRIC_NAMES
    }
    best_rows = _select_ranked_rows(completed_rows, ranking_metric, reverse=True)
    worst_rows = _select_ranked_rows(completed_rows, ranking_metric, reverse=False)
    best_seeds = [_build_aggregated_seed_summary(row, ranking_metric) for row in best_rows]
    worst_seeds = [_build_aggregated_seed_summary(row, ranking_metric) for row in worst_rows]

    worst_cases_payload = {
        "batch_id": manifest.batch_id,
        "batch_dir": manifest.batch_dir,
        "generated_at": _utcnow(),
        "ranking_metric": ranking_metric,
        "best_seeds": [
            _with_cases_payload(summary, _load_optional_json_file(summary.evaluation_cases_json))
            for summary in best_seeds
        ],
        "worst_seeds": [
            _with_cases_payload(summary, _load_optional_json_file(summary.evaluation_cases_json))
            for summary in worst_seeds
        ],
        "failed_seeds": failed_rows,
    }
    _write_json(Path(output_paths.worst_cases_json), worst_cases_payload)

    result = BatchAggregateEvalResult(
        batch_id=manifest.batch_id,
        batch_dir=manifest.batch_dir,
        aggregated_at=worst_cases_payload["generated_at"],
        seed_count=len(records),
        completed_seed_count=len(completed_rows),
        failed_seed_count=len(failed_rows),
        ranking_metric=ranking_metric,
        metric_aggregates=metric_aggregates,
        best_seeds=best_seeds,
        worst_seeds=worst_seeds,
        failed_seeds=failed_rows,
        output_paths=output_paths,
    )
    _write_json(Path(output_paths.aggregate_summary_json), result.model_dump(mode="json"))
    return result


def evaluate_local_ranking(
    *,
    run_dir: Path,
    labels_path: Path | None = None,
    top_k: int | None,
    label_source: str = "manual",
    evaluation_mode: Literal[
        EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
        EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    ] | None = None,
    benchmark_dataset_id: str | None = None,
    benchmark_labels_sha256: str | None = None,
    benchmark_labels_snapshot_path: Path | None = None,
    benchmark_labels_row_count: int | None = None,
    benchmark_schema_version: str | None = None,
) -> LocalRankingEvaluationResult:
    """Join labels to scored candidates and write local evaluation artifacts."""
    ranking_summary = _load_ranking_summary(run_dir / "ranking_summary.json")
    candidates = _load_scored_candidates(run_dir / "scored_candidates.jsonl")
    known_candidate_ids = {
        _normalize_join_openalex_id(candidate.openalex_id)
        for candidate in candidates
    }
    normalized_mode = _normalize_evaluation_mode(
        evaluation_mode=evaluation_mode,
        label_source=label_source,
    )
    normalized_snapshot_path = (
        Path(benchmark_labels_snapshot_path)
        if benchmark_labels_snapshot_path is not None
        else None
    )
    if normalized_mode == EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION:
        resolved_labels_path = labels_path or (run_dir / "silver_labels.csv")
        label_map, loaded_label_rows = _load_silver_labels(
            labels_path=resolved_labels_path,
            expected_seed_openalex_id=ranking_summary.seed_openalex_id,
            known_candidate_ids=known_candidate_ids,
        )
    else:
        if labels_path is None:
            raise ValueError("independent_benchmark evaluation requires labels_path")
        resolved_labels_path = labels_path
        if benchmark_labels_sha256 is None and resolved_labels_path.exists():
            benchmark_labels_sha256 = _sha256_file(resolved_labels_path)
        if normalized_snapshot_path is None:
            normalized_snapshot_path = resolved_labels_path
        if benchmark_labels_row_count is None or benchmark_schema_version is None:
            snapshot_metadata = _benchmark_labels_snapshot_metadata(
                normalized_snapshot_path,
                sha256=benchmark_labels_sha256,
            )
            benchmark_labels_row_count = benchmark_labels_row_count or snapshot_metadata["benchmark_labels_row_count"]
            benchmark_schema_version = benchmark_schema_version or snapshot_metadata["benchmark_schema_version"]
        if evaluation_mode is not None and _optional_str(benchmark_dataset_id) is None:
            raise ValueError("independent_benchmark evaluation requires benchmark_dataset_id")
        if label_source == "manual":
            label_map, loaded_label_rows = _load_manual_labels(
                labels_path=resolved_labels_path,
                expected_seed_openalex_id=ranking_summary.seed_openalex_id,
                known_candidate_ids=known_candidate_ids,
            )
        else:
            label_map, loaded_label_rows = _load_benchmark_labels(
                labels_path=resolved_labels_path,
                expected_seed_openalex_id=ranking_summary.seed_openalex_id,
                known_candidate_ids=known_candidate_ids,
            )

    if loaded_label_rows == 0:
        raise ValueError(f"No label rows found in {resolved_labels_path}")
    if not label_map:
        raise ValueError(
            f"No labels matched scored candidates after OpenAlex id normalization: {resolved_labels_path}"
        )

    joined_rows = [
        _join_candidate(
            candidate,
            label_map.get(_normalize_join_openalex_id(candidate.openalex_id)),
            ranking_summary.seed_openalex_id,
            label_source,
        )
        for candidate in candidates
    ]
    judged_rows = [row for row in joined_rows if row["label"] is not None]

    top_k_used = min(top_k or len(candidates), len(candidates))
    ranking_window = joined_rows[:top_k_used]
    judged_window = [row for row in ranking_window if row["label"] is not None]
    all_judged_labels = [int(row["label"]) for row in judged_rows]
    window_labels = [int(row["label"]) for row in judged_window]
    binary_targets = [1 if int(row["label"]) >= 1 else 0 for row in judged_window]
    window_confidences = [float(row["conf"]) for row in judged_window]

    candidate_count = len(candidates)
    judged_count = len(judged_rows)
    judged_fraction = (judged_count / candidate_count) if candidate_count else 0.0
    metrics = {
        "judged_count": judged_count,
        "judged_fraction": round(judged_fraction, 6),
        "precision_at_k": round(precision_from_labels(window_labels), 6),
        "recall_at_k": round(recall_from_labels(window_labels, all_judged_labels), 6),
        "dcg_at_k": round(dcg_from_labels(window_labels), 6),
        "ndcg_at_k": round(ndcg_from_labels(window_labels), 6),
        "brier_score": round(brier_score_from_probabilities(binary_targets, window_confidences), 6),
        "expected_calibration_error": round(
            expected_calibration_error(binary_targets, window_confidences),
            6,
        ),
        "mean_sim_by_label": _round_mean_mapping(
            mean_value_by_label([(int(row["label"]), float(row["sim"])) for row in judged_rows])
        ),
        "mean_conf_by_label": _round_mean_mapping(
            mean_value_by_label([(int(row["label"]), float(row["conf"])) for row in judged_rows])
        ),
    }
    provenance_slice_summaries = (
        _build_provenance_slice_summaries(joined_rows, ranking_window)
        if normalized_mode == EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION
        else None
    )
    summary_path, judged_candidates_path, cases_path = _legacy_evaluation_paths(run_dir)
    mode_summary_path, mode_judged_candidates_path, mode_cases_path = _mode_evaluation_paths(
        run_dir,
        normalized_mode,
    )

    cases_payload = {
        "evaluation_mode": normalized_mode,
        "metric_scope": METRIC_SCOPE_LOCAL_CORPUS_RANKING,
        "top_false_positives": [
            _case_payload(row) for row in ranking_window if row["label"] == 0
        ][:CASE_LIMIT],
        "top_strong_relevants": [
            _case_payload(row) for row in joined_rows if row["label"] == 2
        ][:CASE_LIMIT],
        "unlabeled_top_candidates": [
            _case_payload(row) for row in ranking_window if row["label"] is None
        ][:CASE_LIMIT],
        "top_ranked_hard_negatives": [
            _case_payload(row)
            for row in ranking_window
            if row["label"] == 0 and "hard_negative" in row["origin_flags"]
        ][:CASE_LIMIT],
    }
    if provenance_slice_summaries is not None:
        cases_payload["provenance_slice_summaries"] = provenance_slice_summaries

    _write_jsonl(judged_candidates_path, joined_rows)
    if mode_judged_candidates_path != judged_candidates_path:
        _write_jsonl(mode_judged_candidates_path, joined_rows)
    _write_json(cases_path, cases_payload)
    if mode_cases_path != cases_path:
        _write_json(mode_cases_path, cases_payload)

    result = LocalRankingEvaluationResult(
        run_dir=str(run_dir),
        labels_path=str(resolved_labels_path),
        label_source=label_source,
        evaluation_mode=normalized_mode,
        metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
        evidence_tier=_evidence_tier_for_mode(normalized_mode),
        benchmark_dataset_id=_optional_str(benchmark_dataset_id),
        benchmark_labels_sha256=_optional_str(benchmark_labels_sha256),
        benchmark_labels_snapshot_path=(
            str(normalized_snapshot_path)
            if normalized_snapshot_path is not None
            else None
        ),
        benchmark_labels_row_count=benchmark_labels_row_count,
        benchmark_schema_version=benchmark_schema_version,
        seed_openalex_id=ranking_summary.seed_openalex_id,
        top_k_used=top_k_used,
        candidate_count=candidate_count,
        judged_count=judged_count,
        judged_fraction=round(judged_fraction, 6),
        metrics=metrics,
        provenance_slice_summaries=provenance_slice_summaries,
        output_paths=EvaluationOutputPaths(
            evaluation_summary_json=str(summary_path),
            judged_candidates_jsonl=str(judged_candidates_path),
            evaluation_cases_json=str(cases_path),
            mode_evaluation_summary_json=str(mode_summary_path),
            mode_judged_candidates_jsonl=str(mode_judged_candidates_path),
            mode_evaluation_cases_json=str(mode_cases_path),
        ),
    )
    _write_json(summary_path, result.model_dump(mode="json"))
    if mode_summary_path != summary_path:
        _write_json(mode_summary_path, result.model_dump(mode="json"))
    return result


def _build_silver_label_row(
    seed_record: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    edge_origin_map: dict[str, set[str]],
) -> EvaluationLabelRow:
    origin_flags = _derive_origin_flags(seed_record, candidate, edge_origin_map)
    label, label_confidence, label_reason = _derive_silver_label(origin_flags)
    return EvaluationLabelRow(
        seed_openalex_id=seed_record.openalex_id,
        candidate_openalex_id=candidate.openalex_id,
        label=label,
        label_confidence=label_confidence,
        label_reason=label_reason,
        origin_flags=tuple(origin_flags),
    )


def _derive_origin_flags(
    seed_record: NormalizedOpenAlexRecord,
    candidate: NormalizedOpenAlexRecord,
    edge_origin_map: dict[str, set[str]],
) -> list[str]:
    origins = set(candidate.candidate_origins)
    if candidate.openalex_id in seed_record.referenced_works:
        origins.update({"seed_reference", "direct_neighbor"})
    if candidate.openalex_id in seed_record.related_works:
        origins.add("seed_related")
    if seed_record.openalex_id in candidate.referenced_works:
        origins.add("direct_neighbor")
    origins.update(edge_origin_map.get(candidate.openalex_id, set()))
    if "seed_reference" in origins:
        origins.add("direct_neighbor")
    return sorted(origins)


def _derive_silver_label(origin_flags: list[str]) -> tuple[int | None, float | None, str]:
    strong_lineage_flags = [
        flag for flag in ("seed_reference", "direct_neighbor") if flag in origin_flags
    ]
    if strong_lineage_flags:
        confidence = 0.95 if _positive_origin_count(origin_flags) > 1 else 0.85
        return 2, confidence, "lineage provenance: seed_reference/direct_neighbor"
    if "seed_related" in origin_flags:
        return 1, 0.65, "seed_related provenance"
    if "hard_negative" in origin_flags:
        return 0, 0.40, "hard_negative provenance"
    return None, None, "no silver provenance"


def _positive_origin_count(origin_flags: list[str]) -> int:
    return sum(1 for flag in origin_flags if flag in {"seed_related", "seed_reference", "direct_neighbor"})


def _detect_seed_doi_column(header_row: list[str]) -> int | None:
    normalized = [cell.strip().lower() for cell in header_row]
    for candidate in ("doi", "query_doi"):
        if candidate in normalized:
            return normalized.index(candidate)
    return None


def _detect_seed_label_column(header_row: list[str]) -> int | None:
    normalized = [cell.strip().lower() for cell in header_row]
    for candidate in ("label", "tag"):
        if candidate in normalized:
            return normalized.index(candidate)
    return None


def _build_batch_output_paths(batch_dir: Path) -> BatchOutputPaths:
    return BatchOutputPaths(
        batch_manifest_json=str(batch_dir / "batch_manifest.json"),
        seed_runs_jsonl=str(batch_dir / "seed_runs.jsonl"),
        aggregate_summary_json=str(batch_dir / "aggregate_summary.json"),
        seed_table_jsonl=str(batch_dir / "seed_table.jsonl"),
        worst_cases_json=str(batch_dir / "worst_cases.json"),
    )


def _has_local_corpus_artifacts(run_dir: Path) -> bool:
    required_paths = [
        run_dir / "seed_record.json",
        run_dir / "papers.jsonl",
        run_dir / "edges.jsonl",
    ]
    return all(path.exists() for path in required_paths)


def _load_seed_run_records(path: Path) -> list[SeedBatchRunRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    records: list[SeedBatchRunRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            records.append(SeedBatchRunRecord.model_validate(json.loads(line)))
    return records


def _metric_value(metrics: dict[str, object], metric_name: str) -> float | None:
    value = metrics.get(metric_name)
    if not _is_numeric_metric(value):
        return None
    return round(float(value), 6)


def _aggregate_metric_stats(values: list[object]) -> AggregateMetricStats:
    numeric_values = [float(value) for value in values if _is_numeric_metric(value)]
    if not numeric_values:
        return AggregateMetricStats(count=0)

    min_value = min(numeric_values)
    max_value = max(numeric_values)
    std_value = statistics.stdev(numeric_values) if len(numeric_values) >= 2 else None
    return AggregateMetricStats(
        count=len(numeric_values),
        mean=round(statistics.fmean(numeric_values), 6),
        median=round(float(statistics.median(numeric_values)), 6),
        std=round(float(std_value), 6) if std_value is not None else None,
        spread=round(max_value - min_value, 6),
        min=round(min_value, 6),
        max=round(max_value, 6),
    )


def _select_batch_ranking_metric(rows: list[dict[str, Any]]) -> str | None:
    for metric_name in ("ndcg_at_k", "precision_at_k"):
        if any(_is_numeric_metric(row.get(metric_name)) for row in rows):
            return metric_name
    return None


def _select_ranked_rows(
    rows: list[dict[str, Any]],
    ranking_metric: str | None,
    *,
    reverse: bool,
) -> list[dict[str, Any]]:
    if ranking_metric is None:
        return []

    ranked_rows = [
        row for row in rows
        if _is_numeric_metric(row.get(ranking_metric))
    ]
    ranked_rows.sort(
        key=lambda row: (
            float(row[ranking_metric]),
            -int(row["batch_index"]) if reverse else int(row["batch_index"]),
            str(row["doi"]),
        ),
        reverse=reverse,
    )
    return ranked_rows[:BEST_WORST_LIMIT]


def _build_aggregated_seed_summary(
    row: dict[str, Any],
    ranking_metric: str | None,
) -> AggregatedSeedSummary:
    return AggregatedSeedSummary(
        batch_index=int(row["batch_index"]),
        doi=str(row["doi"]),
        run_dir=str(row["run_dir"]),
        experiment_id=str(row["experiment_id"]),
        seed_openalex_id=str(row["seed_openalex_id"]),
        ranking_metric=ranking_metric,
        ranking_value=_metric_value(row, ranking_metric) if ranking_metric is not None else None,
        precision_at_k=_metric_value(row, "precision_at_k"),
        recall_at_k=_metric_value(row, "recall_at_k"),
        ndcg_at_k=_metric_value(row, "ndcg_at_k"),
        brier_score=_metric_value(row, "brier_score"),
        expected_calibration_error=_metric_value(row, "expected_calibration_error"),
        evaluation_summary_json=str(row["evaluation_summary_json"]),
        evaluation_cases_json=str(row["evaluation_cases_json"]) if row.get("evaluation_cases_json") else None,
    )


def _with_cases_payload(
    summary: AggregatedSeedSummary,
    cases_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = summary.model_dump(mode="json")
    payload["cases"] = cases_payload
    return payload


def _load_optional_json_file(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None

    candidate_path = Path(path)
    if not candidate_path.exists():
        return None

    payload = json.loads(candidate_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    return payload


def _annotation_template_row(
    seed_record: NormalizedOpenAlexRecord,
    candidate: ScoredCandidateRecord,
) -> dict[str, object]:
    return {
        "seed_openalex_id": seed_record.openalex_id,
        "seed_title": seed_record.title,
        "candidate_openalex_id": candidate.openalex_id,
        "title": candidate.title,
        "publication_year": candidate.publication_year,
        "label": "",
        "label_confidence": "",
        "aspect": ANNOTATION_DEFAULT_ASPECT,
        "annotator_id": "",
        "notes": "",
        "adjudicated_label": "",
        "adjudication_notes": "",
    }


def _annotation_pool_row_to_export_row(
    row: AnnotationPoolRow,
    *,
    annotator_id: str = "",
) -> dict[str, object]:
    return {
        "seed_openalex_id": row.seed_openalex_id,
        "seed_title": row.seed_title,
        "candidate_openalex_id": row.candidate_openalex_id,
        "title": row.title,
        "publication_year": row.publication_year,
        "label": "",
        "label_confidence": "",
        "aspect": ANNOTATION_DEFAULT_ASPECT,
        "annotator_id": annotator_id,
        "notes": "",
        "adjudicated_label": "",
        "adjudication_notes": "",
    }


def _resolve_existing_file_path(value: str | Path, *, label: str) -> Path:
    path = Path(str(value).strip()).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"{label} does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"{label} is not a file: {path}")
    return path


def _resolve_runs_root(value: Path | None) -> Path:
    if value is not None:
        candidate = Path(value).expanduser()
        return candidate if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()
    runtime = load_runtime_config()
    return (REPO_ROOT / runtime.runs_dir).resolve()


def _resolve_benchmark_dataset_root(value: str | Path | None) -> Path:
    if value is None:
        return (REPO_ROOT / BENCHMARK_DATASETS_DIRNAME).resolve()
    candidate = Path(str(value).strip()).expanduser()
    return candidate if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()


def _resolve_annotation_batch_root(value: str | Path | None) -> Path:
    if value is None:
        return (REPO_ROOT / ANNOTATION_BATCHES_DIRNAME).resolve()
    candidate = Path(str(value).strip()).expanduser()
    return candidate if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()


def _resolve_conflict_adjudication_root(value: str | Path | None) -> Path:
    if value is None:
        return (REPO_ROOT / CONFLICT_ADJUDICATION_DIRNAME).resolve()
    candidate = Path(str(value).strip()).expanduser()
    return candidate if candidate.is_absolute() else (REPO_ROOT / candidate).resolve()


def _normalize_directory_label(value: object, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise ValueError(f"{label} is required.")
    if normalized in {".", ".."} or Path(normalized).name != normalized:
        raise ValueError(f"{label} must be a single directory name without path separators.")
    return normalized


def _normalize_annotation_aspect(value: object) -> str:
    normalized = _optional_str(value) or ANNOTATION_DEFAULT_ASPECT
    if normalized not in ANNOTATION_ALLOWED_ASPECTS:
        supported = ", ".join(ANNOTATION_ALLOWED_ASPECTS)
        raise ValueError(f"aspect_scope must be one of: {supported}")
    return normalized


def _load_annotation_rows(
    path: Path,
    *,
    aspect_scope: str,
) -> tuple[list[dict[str, Any]], int]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing_columns = [
            column for column in ANNOTATION_LABEL_REQUIRED_COLUMNS if column not in fieldnames
        ]
        if missing_columns:
            raise ValueError(
                f"Annotation CSV missing required columns in {path}: {', '.join(missing_columns)}"
            )

        rows: list[dict[str, Any]] = []
        seen_source_keys: set[tuple[str, str, str]] = set()
        loaded_row_count = 0
        for row_number, row in enumerate(reader, start=2):
            if _row_is_blank(row):
                continue
            loaded_row_count += 1

            seed_text = _optional_str(row.get("seed_openalex_id"))
            candidate_text = _optional_str(row.get("candidate_openalex_id"))
            if seed_text is None:
                raise ValueError(f"Annotation CSV row {row_number} in {path} is missing seed_openalex_id")
            if candidate_text is None:
                raise ValueError(
                    f"Annotation CSV row {row_number} in {path} is missing candidate_openalex_id"
                )

            label_value = _parse_label(row.get("label", ""))
            adjudicated_label = _parse_label(row.get("adjudicated_label", ""))
            if label_value is None and adjudicated_label is None:
                continue

            annotator_id = _optional_str(row.get("annotator_id"))
            if annotator_id is None:
                raise ValueError(f"Annotation CSV row {row_number} in {path} is missing annotator_id")

            normalized_aspect = _normalize_annotation_aspect(row.get("aspect"))
            if normalized_aspect != aspect_scope:
                raise ValueError(
                    f"Annotation CSV row {row_number} in {path} uses unsupported aspect "
                    f"'{normalized_aspect}' for aspect_scope '{aspect_scope}'"
                )

            label_confidence = _parse_optional_float(row.get("label_confidence", ""))
            if label_confidence is not None and not 0.0 <= label_confidence <= 1.0:
                raise ValueError(
                    f"Annotation CSV row {row_number} in {path} has label_confidence outside [0, 1]"
                )

            seed_openalex_id = _canonical_openalex_id(seed_text)
            candidate_openalex_id = _canonical_openalex_id(candidate_text)
            source_key = (seed_openalex_id, candidate_openalex_id, annotator_id.lower())
            if source_key in seen_source_keys:
                raise ValueError(
                    "Duplicate annotation row for seed/candidate/annotator in "
                    f"{path}: {seed_openalex_id} -> {candidate_openalex_id} ({annotator_id})"
                )
            seen_source_keys.add(source_key)
            rows.append(
                {
                    "seed_openalex_id": seed_openalex_id,
                    "seed_title": _optional_str(row.get("seed_title")),
                    "candidate_openalex_id": candidate_openalex_id,
                    "title": _optional_str(row.get("title")),
                    "publication_year": _optional_str(row.get("publication_year")),
                    "label": label_value,
                    "label_confidence": label_confidence,
                    "aspect": normalized_aspect,
                    "annotator_id": annotator_id,
                    "notes": str(row.get("notes", "")).strip(),
                    "adjudicated_label": adjudicated_label,
                    "adjudication_notes": str(row.get("adjudication_notes", "")).strip(),
                    "source_path": str(path),
                    "source_row_number": row_number,
                }
            )
    return rows, loaded_row_count


def _consolidate_annotation_files(
    annotation_paths: list[Path],
    *,
    aspect_scope: str,
) -> tuple[list[EvaluationLabelRow], int, dict[str, Any]]:
    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    total_source_rows = 0
    for path in sorted(annotation_paths, key=lambda candidate: str(candidate)):
        annotation_rows, source_row_count = _load_annotation_rows(path, aspect_scope=aspect_scope)
        total_source_rows += source_row_count
        for row in annotation_rows:
            key = (row["seed_openalex_id"], row["candidate_openalex_id"])
            grouped_rows.setdefault(key, []).append(row)

    consolidated_rows: list[EvaluationLabelRow] = []
    annotator_ids: set[str] = set()
    overlap_pair_count = 0
    multi_annotated_pair_count = 0
    exact_match_pair_count = 0
    conflict_pair_count = 0
    adjudicated_conflict_count = 0
    for seed_candidate_key in sorted(grouped_rows):
        group_rows = sorted(
            grouped_rows[seed_candidate_key],
            key=lambda row: (
                str(row["source_path"]).lower(),
                int(row["source_row_number"]),
                str(row["annotator_id"]).lower(),
            ),
        )
        raw_labels = {
            int(row["label"])
            for row in group_rows
            if row["label"] is not None
        }
        adjudicated_labels = {
            int(row["adjudicated_label"])
            for row in group_rows
            if row["adjudicated_label"] is not None
        }
        normalized_annotators = {
            str(row["annotator_id"]).strip()
            for row in group_rows
            if _optional_str(row.get("annotator_id")) is not None
        }
        annotator_ids.update(normalized_annotators)
        if len(normalized_annotators) >= 2:
            overlap_pair_count += 1
            multi_annotated_pair_count += 1
            if raw_labels and len(raw_labels) == 1:
                exact_match_pair_count += 1
        if len(raw_labels) > 1:
            conflict_pair_count += 1
            if len(adjudicated_labels) == 1:
                adjudicated_conflict_count += 1
        if len(adjudicated_labels) > 1:
            raise ValueError(
                "Conflicting adjudicated_label values for "
                f"{seed_candidate_key[0]} -> {seed_candidate_key[1]}"
            )

        if adjudicated_labels:
            final_label = next(iter(adjudicated_labels))
        else:
            if not raw_labels:
                continue
            if len(raw_labels) > 1:
                raise ValueError(
                    "Conflicting label values for "
                    f"{seed_candidate_key[0]} -> {seed_candidate_key[1]}"
                )
            final_label = next(iter(raw_labels))

        supporting_confidences = [
            float(row["label_confidence"])
            for row in group_rows
            if row["label_confidence"] is not None
            and (
                row["adjudicated_label"] == final_label
                or (
                    row["adjudicated_label"] is None
                    and row["label"] == final_label
                )
            )
        ]
        label_confidence = (
            round(statistics.fmean(supporting_confidences), 6)
            if supporting_confidences
            else None
        )
        notes_parts: list[str] = []
        for row in group_rows:
            notes = _optional_str(row.get("notes"))
            if notes is not None:
                notes_parts.append(f"{row['annotator_id']}: {notes}")
            adjudication_notes = _optional_str(row.get("adjudication_notes"))
            if adjudication_notes is not None:
                notes_parts.append(f"adjudication {row['annotator_id']}: {adjudication_notes}")
        consolidated_rows.append(
            EvaluationLabelRow(
                seed_openalex_id=seed_candidate_key[0],
                candidate_openalex_id=seed_candidate_key[1],
                label=final_label,
                label_confidence=label_confidence,
                label_reason=f"independent_{aspect_scope}_human_annotation",
                notes=" | ".join(dict.fromkeys(notes_parts)),
            )
        )

    agreement_rate = (
        round(exact_match_pair_count / overlap_pair_count, 6)
        if overlap_pair_count > 0
        else None
    )
    agreement_summary = {
        "metric_name": "raw_pair_agreement",
        "overlap_pair_count": overlap_pair_count,
        "exact_match_pair_count": exact_match_pair_count,
        "disagreement_pair_count": max(overlap_pair_count - exact_match_pair_count, 0),
        "agreement_rate": agreement_rate,
    }
    return (
        consolidated_rows,
        total_source_rows,
        {
            "annotator_ids": sorted(annotator_ids, key=str.lower),
            "annotator_count": len(annotator_ids),
            "multi_annotated_pair_count": multi_annotated_pair_count,
            "overlap_pair_count": overlap_pair_count,
            "agreement_summary": agreement_summary,
            "conflict_pair_count": conflict_pair_count,
            "adjudicated_conflict_count": adjudicated_conflict_count,
            "adjudication_complete": conflict_pair_count == adjudicated_conflict_count,
        },
    )


def _copy_annotation_sources(
    source_paths: list[Path],
    destination_dir: Path,
) -> list[Path]:
    copied_paths: list[Path] = []
    used_names: set[str] = set()
    for index, source_path in enumerate(sorted(source_paths, key=lambda path: str(path)), start=1):
        destination_name = source_path.name
        if destination_name in used_names:
            destination_name = f"{index:02d}_{destination_name}"
        used_names.add(destination_name)
        destination_path = destination_dir / destination_name
        copyfile(source_path, destination_path)
        copied_paths.append(destination_path)
    return copied_paths


def _write_benchmark_labels_csv(
    path: Path,
    rows: list[EvaluationLabelRow],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=BENCHMARK_LABEL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "seed_openalex_id": row.seed_openalex_id,
                    "candidate_openalex_id": row.candidate_openalex_id,
                    "label": row.label,
                    "label_confidence": (
                        "" if row.label_confidence is None else f"{row.label_confidence:.6f}"
                    ),
                    "label_reason": row.label_reason,
                    "notes": row.notes,
                }
            )


def _benchmark_label_distribution(rows: list[EvaluationLabelRow]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for row in rows:
        if row.label is None:
            continue
        label_key = str(int(row.label))
        distribution[label_key] = distribution.get(label_key, 0) + 1
    return {
        label_key: distribution[label_key]
        for label_key in sorted(distribution, key=lambda value: int(value))
    }


def _benchmark_seed_counts(rows: list[EvaluationLabelRow]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.seed_openalex_id] = counts.get(row.seed_openalex_id, 0) + 1
    return {
        seed_openalex_id: counts[seed_openalex_id]
        for seed_openalex_id in sorted(counts)
    }


def _benchmark_maturity_metadata(
    *,
    rows: list[EvaluationLabelRow],
    qa_summary: dict[str, Any],
) -> dict[str, Any]:
    seed_counts = _benchmark_seed_counts(rows)
    label_distribution = _benchmark_label_distribution(rows)
    row_count = len(rows)
    seed_count = len(seed_counts)
    distinct_labels = set(label_distribution)
    adjudication_complete = bool(qa_summary.get("adjudication_complete", False))
    conflict_pair_count = int(qa_summary.get("conflict_pair_count") or 0)
    overlap_pair_count = int(qa_summary.get("overlap_pair_count") or 0)
    agreement_summary = dict(qa_summary.get("agreement_summary") or {})
    agreement_rate = agreement_summary.get("agreement_rate")
    if isinstance(agreement_rate, (int, float)):
        normalized_agreement_rate: float | None = float(agreement_rate)
    else:
        normalized_agreement_rate = None

    max_label_share = 0.0
    if row_count > 0 and label_distribution:
        max_label_share = max(count / row_count for count in label_distribution.values())

    prototype_reasons: list[str] = []
    if seed_count < 1:
        prototype_reasons.append("At least 1 labeled seed is required for prototype maturity.")
    if row_count < 3:
        prototype_reasons.append("At least 3 labeled pairs are required for prototype maturity.")
    if len(distinct_labels) < 2:
        prototype_reasons.append(
            "At least 2 distinct label values are required for prototype maturity."
        )
    if not adjudication_complete:
        prototype_reasons.append("All conflicting pairs must be resolved for prototype maturity.")
    prototype_met = not prototype_reasons

    pilot_reasons: list[str] = []
    if seed_count < 5:
        pilot_reasons.append("At least 5 labeled seeds are required for pilot maturity.")
    if row_count < 50:
        pilot_reasons.append("At least 50 labeled pairs are required for pilot maturity.")
    if max_label_share > 0.70:
        pilot_reasons.append("No label may exceed 70% of rows for pilot maturity.")
    if not adjudication_complete:
        pilot_reasons.append("All conflicting pairs must be adjudicated for pilot maturity.")
    if overlap_pair_count <= 0 or normalized_agreement_rate is None:
        pilot_reasons.append(
            "Raw agreement is required on overlapping doubly-labeled pairs for pilot maturity."
        )
    elif normalized_agreement_rate < 0.70:
        pilot_reasons.append("Raw agreement must be at least 0.70 for pilot maturity.")
    pilot_met = not pilot_reasons

    promotion_ready_reasons: list[str] = []
    if seed_count < 10:
        promotion_ready_reasons.append(
            "At least 10 labeled seeds are required for promotion-ready maturity."
        )
    if row_count < 150:
        promotion_ready_reasons.append(
            "At least 150 labeled pairs are required for promotion-ready maturity."
        )
    if {"0", "1", "2"} - distinct_labels:
        promotion_ready_reasons.append(
            "Labels 0, 1, and 2 must all be present for promotion-ready maturity."
        )
    if max_label_share > 0.60:
        promotion_ready_reasons.append(
            "No label may exceed 60% of rows for promotion-ready maturity."
        )
    if not adjudication_complete:
        promotion_ready_reasons.append(
            "All conflicting pairs must be adjudicated for promotion-ready maturity."
        )
    if overlap_pair_count <= 0 or normalized_agreement_rate is None:
        promotion_ready_reasons.append(
            "Raw agreement is required on overlapping doubly-labeled pairs for promotion-ready maturity."
        )
    elif normalized_agreement_rate < 0.80:
        promotion_ready_reasons.append(
            "Raw agreement must be at least 0.80 for promotion-ready maturity."
        )
    promotion_ready = not promotion_ready_reasons

    if promotion_ready:
        benchmark_maturity_tier = BENCHMARK_MATURITY_PROMOTION_READY
    elif pilot_met:
        benchmark_maturity_tier = BENCHMARK_MATURITY_PILOT
    elif prototype_met:
        benchmark_maturity_tier = BENCHMARK_MATURITY_PROTOTYPE
    else:
        benchmark_maturity_tier = BENCHMARK_MATURITY_INSUFFICIENT

    return {
        "seed_count": seed_count,
        "per_seed_counts": seed_counts,
        "label_distribution": label_distribution,
        "benchmark_maturity_tier": benchmark_maturity_tier,
        "promotion_ready": promotion_ready,
        "promotion_ineligibility_reasons": promotion_ready_reasons,
    }


def _resolve_ranked_run_dirs(run_dirs: list[str | Path]) -> list[Path]:
    resolved_dirs: list[Path] = []
    for value in run_dirs:
        candidate = Path(str(value).strip()).expanduser()
        if not candidate.is_absolute():
            candidate = (REPO_ROOT / candidate).resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"Ranked run directory does not exist: {candidate}")
        if not candidate.is_dir():
            raise ValueError(f"Ranked run path is not a directory: {candidate}")
        required_paths = (
            candidate / "seed_record.json",
            candidate / "scored_candidates.jsonl",
        )
        missing_paths = [str(path) for path in required_paths if not path.exists()]
        if missing_paths:
            raise FileNotFoundError(
                "Ranked run is missing required artifacts: " + ", ".join(missing_paths)
            )
        resolved_dirs.append(candidate)
    return resolved_dirs


def _resolve_benchmark_preset_run_dirs(
    *,
    benchmark_preset_path: Path,
    runs_root: Path | None,
    max_references: int,
    max_related: int,
    max_hard_negatives: int,
) -> list[Path]:
    preset_payload = json.loads(benchmark_preset_path.read_text(encoding="utf-8"))
    if not isinstance(preset_payload, dict):
        raise ValueError(f"Benchmark preset must be a JSON object: {benchmark_preset_path}")
    seeds_csv_value = _optional_str(preset_payload.get("seeds_csv"))
    if seeds_csv_value is None:
        raise ValueError(f"Benchmark preset is missing seeds_csv: {benchmark_preset_path}")
    seeds_csv_path = _resolve_existing_file_path(seeds_csv_value, label="Benchmark preset seeds_csv")
    seeds = load_benchmark_seeds(seeds_csv_path)
    if not seeds:
        raise ValueError(f"No seed DOIs found in {seeds_csv_path}")

    resolved_runs_root = _resolve_runs_root(runs_root)
    resolved_run_dirs: list[Path] = []
    for seed in seeds:
        run_id = build_local_corpus_run_id(
            doi=seed.query_doi,
            max_references=max_references,
            max_related=max_related,
            max_hard_negatives=max_hard_negatives,
        )
        resolved_run_dirs.append(resolved_runs_root / run_id)
    return _resolve_ranked_run_dirs([str(path) for path in resolved_run_dirs])


def _collect_annotation_pool_rows(run_dirs: list[Path]) -> list[AnnotationPoolRow]:
    pool_rows: list[AnnotationPoolRow] = []
    seen_keys: set[tuple[str, str]] = set()
    for run_dir in run_dirs:
        seed_record = _load_record(run_dir / "seed_record.json")
        for candidate in _load_scored_candidates(run_dir / "scored_candidates.jsonl"):
            pair_key = (seed_record.openalex_id, candidate.openalex_id)
            if pair_key in seen_keys:
                continue
            seen_keys.add(pair_key)
            pool_rows.append(
                AnnotationPoolRow(
                    seed_openalex_id=seed_record.openalex_id,
                    seed_title=seed_record.title,
                    candidate_openalex_id=candidate.openalex_id,
                    title=candidate.title,
                    publication_year=candidate.publication_year,
                    run_id=run_dir.name,
                    run_dir=str(run_dir),
                    rank=int(candidate.rank),
                    sim=float(candidate.sim),
                    stratum=_lineage_annotation_stratum(candidate),
                    candidate_origins=tuple(
                        sorted(
                            {
                                str(origin).strip()
                                for origin in candidate.candidate_origins
                                if _optional_str(origin) is not None
                            }
                        )
                    ),
                )
            )
    return pool_rows


def _lineage_annotation_stratum(candidate: ScoredCandidateRecord) -> str:
    origins = {
        str(origin).strip()
        for origin in candidate.candidate_origins
        if _optional_str(origin) is not None
    }
    if "seed_reference" in origins or "direct_neighbor" in origins:
        return LINEAGE_STRATUM_STRONG_LINEAGE
    if "hard_negative" in origins:
        return LINEAGE_STRATUM_HARD_NEGATIVE_OR_DISTRACTOR
    if "seed_related" in origins and candidate.rank <= 5:
        return LINEAGE_STRATUM_INDIRECT_LINEAGE
    if "seed_related" in origins:
        return LINEAGE_STRATUM_AMBIGUOUS_MIDDLE
    if candidate.rank <= 10:
        return LINEAGE_STRATUM_AMBIGUOUS_MIDDLE
    return LINEAGE_STRATUM_PROVENANCE_WEAK


def _select_stratified_annotation_rows(
    *,
    pool_rows: list[AnnotationPoolRow],
    target_pair_count: int,
    per_seed_quota: int,
    seed_quota_overrides: dict[str, int],
    per_stratum_quotas: dict[str, int],
) -> dict[str, Any]:
    seed_order = _ordered_unique([row.seed_openalex_id for row in pool_rows])
    run_order = _ordered_unique([row.run_id for row in pool_rows])
    run_dirs = _ordered_unique([row.run_dir for row in pool_rows])
    rows_by_seed_and_stratum: dict[str, dict[str, list[AnnotationPoolRow]]] = {
        seed_openalex_id: {stratum: [] for stratum in LINEAGE_STRATUM_ORDER}
        for seed_openalex_id in seed_order
    }
    for row in sorted(pool_rows, key=_annotation_pool_row_sort_key):
        rows_by_seed_and_stratum.setdefault(
            row.seed_openalex_id,
            {stratum: [] for stratum in LINEAGE_STRATUM_ORDER},
        )[row.stratum].append(row)

    selected_rows_by_key: dict[tuple[str, str], AnnotationPoolRow] = {}
    seed_pair_counts = {seed_openalex_id: 0 for seed_openalex_id in seed_order}
    strata_counts = {stratum: 0 for stratum in LINEAGE_STRATUM_ORDER}

    def seed_quota_for(seed_openalex_id: str) -> int:
        return seed_quota_overrides.get(seed_openalex_id, per_seed_quota)

    def select_row(row: AnnotationPoolRow) -> bool:
        pair_key = (row.seed_openalex_id, row.candidate_openalex_id)
        if pair_key in selected_rows_by_key:
            return False
        if seed_pair_counts[row.seed_openalex_id] >= seed_quota_for(row.seed_openalex_id):
            return False
        selected_rows_by_key[pair_key] = row
        seed_pair_counts[row.seed_openalex_id] += 1
        strata_counts[row.stratum] += 1
        return True

    def next_available_row(seed_openalex_id: str, strata: tuple[str, ...]) -> AnnotationPoolRow | None:
        if seed_pair_counts[seed_openalex_id] >= seed_quota_for(seed_openalex_id):
            return None
        for stratum in strata:
            for row in rows_by_seed_and_stratum.get(seed_openalex_id, {}).get(stratum, []):
                pair_key = (row.seed_openalex_id, row.candidate_openalex_id)
                if pair_key not in selected_rows_by_key:
                    return row
        return None

    for stratum in LINEAGE_STRATUM_ORDER:
        target_for_stratum = per_stratum_quotas.get(stratum, 0)
        if target_for_stratum <= 0:
            continue
        made_progress = True
        while (
            len(selected_rows_by_key) < target_pair_count
            and strata_counts[stratum] < target_for_stratum
            and made_progress
        ):
            made_progress = False
            for seed_openalex_id in seed_order:
                row = next_available_row(seed_openalex_id, (stratum,))
                if row is None:
                    continue
                if select_row(row):
                    made_progress = True
                if (
                    len(selected_rows_by_key) >= target_pair_count
                    or strata_counts[stratum] >= target_for_stratum
                ):
                    break

    made_progress = True
    while len(selected_rows_by_key) < target_pair_count and made_progress:
        made_progress = False
        for seed_openalex_id in seed_order:
            row = next_available_row(seed_openalex_id, LINEAGE_STRATUM_ORDER)
            if row is None:
                continue
            if select_row(row):
                made_progress = True
            if len(selected_rows_by_key) >= target_pair_count:
                break

    selected_rows = sorted(
        selected_rows_by_key.values(),
        key=_annotation_pool_row_sort_key,
    )
    return {
        "selected_rows": selected_rows,
        "seed_pair_counts": dict(seed_pair_counts),
        "strata_counts": dict(strata_counts),
        "seed_ids": list(seed_order),
        "source_run_ids": run_order,
        "source_run_dirs": run_dirs,
    }


def _assign_annotation_rows(
    selected_rows: list[AnnotationPoolRow],
    *,
    annotator_ids: list[str],
    planned_overlap_ratio: float,
) -> tuple[dict[str, list[AnnotationPoolRow]], int, float]:
    assigned_rows = {annotator_id: [] for annotator_id in annotator_ids}
    if not selected_rows:
        return assigned_rows, 0, 0.0
    if len(annotator_ids) == 1:
        assigned_rows[annotator_ids[0]] = list(selected_rows)
        return assigned_rows, 0, 0.0

    overlap_pair_count = min(
        len(selected_rows),
        int(round(len(selected_rows) * planned_overlap_ratio)),
    )
    ordered_for_assignment = sorted(selected_rows, key=_annotation_assignment_sort_key)
    shared_rows = ordered_for_assignment[:overlap_pair_count]
    exclusive_rows = ordered_for_assignment[overlap_pair_count:]
    for annotator_id in annotator_ids:
        assigned_rows[annotator_id].extend(shared_rows)
    for index, row in enumerate(exclusive_rows):
        annotator_id = annotator_ids[index % len(annotator_ids)]
        assigned_rows[annotator_id].append(row)

    realized_overlap_ratio = (
        round(overlap_pair_count / len(selected_rows), 6)
        if selected_rows
        else 0.0
    )
    return assigned_rows, overlap_pair_count, realized_overlap_ratio


def _write_annotation_export_csv(
    path: Path,
    rows: list[AnnotationPoolRow],
    *,
    annotator_id: str,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=LABEL_TEMPLATE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_annotation_pool_row_to_export_row(row, annotator_id=annotator_id))


def _write_annotation_pair_assignments(
    path: Path,
    assigned_rows: dict[str, list[AnnotationPoolRow]],
) -> None:
    pair_assignments: dict[tuple[str, str], dict[str, Any]] = {}
    for annotator_id, rows in assigned_rows.items():
        for row in rows:
            pair_key = (row.seed_openalex_id, row.candidate_openalex_id)
            payload = pair_assignments.setdefault(
                pair_key,
                {
                    "seed_openalex_id": row.seed_openalex_id,
                    "seed_title": row.seed_title,
                    "candidate_openalex_id": row.candidate_openalex_id,
                    "title": row.title,
                    "publication_year": row.publication_year,
                    "run_id": row.run_id,
                    "run_dir": row.run_dir,
                    "stratum": row.stratum,
                    "annotator_ids": [],
                },
            )
            payload["annotator_ids"].append(annotator_id)

    serialized_rows = sorted(
        pair_assignments.values(),
        key=lambda row: (
            str(row.get("seed_title", "")).lower(),
            str(row.get("title", "")).lower(),
            str(row.get("candidate_openalex_id", "")).lower(),
        ),
    )
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in serialized_rows:
            payload = dict(row)
            payload["annotator_ids"] = sorted(set(payload["annotator_ids"]), key=str.lower)
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def _collect_unresolved_conflict_rows(
    annotation_paths: list[Path],
    *,
    aspect_scope: str,
    adjudicator_id: str,
) -> list[dict[str, Any]]:
    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for path in sorted(annotation_paths, key=lambda candidate: str(candidate)):
        annotation_rows, _ = _load_annotation_rows(path, aspect_scope=aspect_scope)
        for row in annotation_rows:
            key = (row["seed_openalex_id"], row["candidate_openalex_id"])
            grouped_rows.setdefault(key, []).append(row)

    conflict_rows: list[dict[str, Any]] = []
    for seed_candidate_key in sorted(grouped_rows):
        group_rows = sorted(
            grouped_rows[seed_candidate_key],
            key=lambda row: (
                str(row.get("source_path", "")).lower(),
                int(row.get("source_row_number") or 0),
                str(row.get("annotator_id", "")).lower(),
            ),
        )
        raw_labels = {
            int(row["label"])
            for row in group_rows
            if row["label"] is not None
        }
        if len(raw_labels) <= 1:
            continue
        adjudicated_labels = {
            int(row["adjudicated_label"])
            for row in group_rows
            if row["adjudicated_label"] is not None
        }
        if len(adjudicated_labels) > 1:
            raise ValueError(
                "Conflicting adjudicated_label values for "
                f"{seed_candidate_key[0]} -> {seed_candidate_key[1]}"
            )
        if len(adjudicated_labels) == 1:
            continue

        prior_labels = [
            f"{row['annotator_id']}:{row['label']}"
            for row in group_rows
            if row["label"] is not None
        ]
        conflict_rows.append(
            {
                "seed_openalex_id": seed_candidate_key[0],
                "seed_title": _first_present_text(group_rows, "seed_title"),
                "candidate_openalex_id": seed_candidate_key[1],
                "title": _first_present_text(group_rows, "title"),
                "publication_year": _first_present_text(group_rows, "publication_year"),
                "label": "",
                "label_confidence": "",
                "aspect": aspect_scope,
                "annotator_id": adjudicator_id,
                "notes": "",
                "adjudicated_label": "",
                "adjudication_notes": "",
                "prior_labels": " | ".join(prior_labels),
            }
        )
    return conflict_rows


def _save_materialized_benchmark_preset(
    *,
    benchmark_preset_id: str,
    seeds_csv_path: Path,
    labels_path: Path,
    labels_sha256: str,
    benchmark_dataset_id: str,
    benchmark_labels_row_count: int,
    benchmark_schema_version: str,
    aspect_scope: str,
    benchmark_dataset_manifest_path: Path,
    base_dir: str | Path | None,
    description: str | None,
    tags: list[str],
) -> Path:
    from src.ui.preset_registry import (
        build_benchmark_preset_save_request,
        save_benchmark_preset,
    )

    request = build_benchmark_preset_save_request(
        preset_id=benchmark_preset_id,
        seeds_csv_path=seeds_csv_path,
        benchmark_labels_path=labels_path,
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=labels_sha256,
        benchmark_dataset_manifest_path=benchmark_dataset_manifest_path,
        benchmark_schema_version=benchmark_schema_version,
        benchmark_labels_row_count=benchmark_labels_row_count,
        aspect_scope=aspect_scope,
        description=description or "",
        tags_text=",".join(tags),
    )
    return save_benchmark_preset(request, base_dir=base_dir)


def _canonical_openalex_id(value: str) -> str:
    return f"https://openalex.org/{normalize_openalex_work_id(value)}"


def _load_ranking_summary(path: Path) -> RankingSummary:
    if not path.exists():
        raise FileNotFoundError(path)
    return RankingSummary.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _load_scored_candidates(path: Path) -> list[ScoredCandidateRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    candidates: list[ScoredCandidateRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            candidates.append(ScoredCandidateRecord.model_validate(json.loads(line)))
    candidates.sort(key=lambda candidate: candidate.rank)
    return candidates


def _load_record(path: Path) -> NormalizedOpenAlexRecord:
    if not path.exists():
        raise FileNotFoundError(path)
    return NormalizedOpenAlexRecord.model_validate(json.loads(path.read_text(encoding="utf-8")))


def _load_papers(path: Path) -> list[NormalizedOpenAlexRecord]:
    if not path.exists():
        raise FileNotFoundError(path)

    records: list[NormalizedOpenAlexRecord] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            records.append(NormalizedOpenAlexRecord.model_validate(json.loads(line)))
    return records


def _load_edge_origin_map(path: Path, seed_openalex_id: str) -> dict[str, set[str]]:
    if not path.exists():
        raise FileNotFoundError(path)

    origin_map: dict[str, set[str]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                continue
            src = payload.get("src")
            dst = payload.get("dst")
            edge_type = payload.get("edge_type")
            if src != seed_openalex_id or not isinstance(dst, str):
                continue
            if edge_type == "seed_references":
                origin_map.setdefault(dst, set()).update({"seed_reference", "direct_neighbor"})
            elif edge_type == "seed_related":
                origin_map.setdefault(dst, set()).add("seed_related")
    return origin_map


def _load_manual_labels(
    *,
    labels_path: Path,
    expected_seed_openalex_id: str,
    known_candidate_ids: set[str],
) -> tuple[dict[str, EvaluationLabelRow], int]:
    if not labels_path.exists():
        raise FileNotFoundError(labels_path)

    with labels_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing_columns = [
            column
            for column in MANUAL_LABEL_REQUIRED_COLUMNS
            if column not in fieldnames
        ]
        if missing_columns:
            raise ValueError(f"Labels CSV missing required columns: {', '.join(missing_columns)}")

        label_map: dict[str, EvaluationLabelRow] = {}
        loaded_row_count = 0
        expected_seed_join_id = _normalize_join_openalex_id(expected_seed_openalex_id)
        for row in reader:
            if _row_is_blank(row):
                continue
            loaded_row_count += 1

            candidate_openalex_id = str(row.get("candidate_openalex_id", "")).strip()
            if not candidate_openalex_id:
                raise ValueError("Labels CSV contains a row without candidate_openalex_id")
            candidate_join_id = _normalize_join_openalex_id(candidate_openalex_id)
            if candidate_join_id in label_map:
                raise ValueError(f"Duplicate label row for candidate_openalex_id: {candidate_openalex_id}")

            seed_openalex_id = str(row.get("seed_openalex_id", "")).strip()
            if seed_openalex_id and _normalize_join_openalex_id(seed_openalex_id) != expected_seed_join_id:
                raise ValueError(
                    f"Labels CSV seed_openalex_id mismatch for {candidate_openalex_id}: {seed_openalex_id}"
                )

            if candidate_join_id not in known_candidate_ids:
                continue

            label_map[candidate_join_id] = EvaluationLabelRow(
                seed_openalex_id=expected_seed_openalex_id,
                candidate_openalex_id=candidate_join_id,
                label=_parse_label(row.get("label", "")),
                notes=str(row.get("notes", "")).strip(),
            )
        return label_map, loaded_row_count


def _load_silver_labels(
    *,
    labels_path: Path,
    expected_seed_openalex_id: str,
    known_candidate_ids: set[str],
) -> tuple[dict[str, EvaluationLabelRow], int]:
    if not labels_path.exists():
        raise FileNotFoundError(labels_path)

    with labels_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing_columns = [column for column in SILVER_LABEL_COLUMNS if column not in fieldnames]
        if missing_columns:
            raise ValueError(f"Silver labels CSV missing required columns: {', '.join(missing_columns)}")

        label_map: dict[str, EvaluationLabelRow] = {}
        loaded_row_count = 0
        expected_seed_join_id = _normalize_join_openalex_id(expected_seed_openalex_id)
        for row in reader:
            if _row_is_blank(row):
                continue
            loaded_row_count += 1

            candidate_openalex_id = str(row.get("candidate_openalex_id", "")).strip()
            if not candidate_openalex_id:
                raise ValueError("Silver labels CSV contains a row without candidate_openalex_id")
            candidate_join_id = _normalize_join_openalex_id(candidate_openalex_id)
            if candidate_join_id in label_map:
                raise ValueError(f"Duplicate silver label row for candidate_openalex_id: {candidate_openalex_id}")

            seed_openalex_id = str(row.get("seed_openalex_id", "")).strip()
            if seed_openalex_id and _normalize_join_openalex_id(seed_openalex_id) != expected_seed_join_id:
                raise ValueError(
                    f"Silver labels CSV seed_openalex_id mismatch for {candidate_openalex_id}: {seed_openalex_id}"
                )

            if candidate_join_id not in known_candidate_ids:
                continue

            label_map[candidate_join_id] = EvaluationLabelRow(
                seed_openalex_id=expected_seed_openalex_id,
                candidate_openalex_id=candidate_join_id,
                label=_parse_label(row.get("label", "")),
                label_confidence=_parse_optional_float(row.get("label_confidence", "")),
                label_reason=str(row.get("label_reason", "")).strip(),
                origin_flags=tuple(_parse_origin_flags(row.get("origin_flags", ""))),
            )
        return label_map, loaded_row_count


def _load_benchmark_labels(
    *,
    labels_path: Path,
    expected_seed_openalex_id: str,
    known_candidate_ids: set[str],
) -> tuple[dict[str, EvaluationLabelRow], int]:
    if not labels_path.exists():
        raise FileNotFoundError(labels_path)

    with labels_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing_columns = [
            column
            for column in ("seed_openalex_id", "candidate_openalex_id", "label")
            if column not in fieldnames
        ]
        if missing_columns:
            raise ValueError(
                f"Benchmark labels CSV missing required columns: {', '.join(missing_columns)}"
            )

        label_map: dict[str, EvaluationLabelRow] = {}
        loaded_row_count = 0
        expected_seed_join_id = _normalize_join_openalex_id(expected_seed_openalex_id)
        for row in reader:
            if _row_is_blank(row):
                continue
            loaded_row_count += 1

            candidate_openalex_id = str(row.get("candidate_openalex_id", "")).strip()
            if not candidate_openalex_id:
                raise ValueError("Benchmark labels CSV contains a row without candidate_openalex_id")

            seed_openalex_id = str(row.get("seed_openalex_id", "")).strip()
            if seed_openalex_id and _normalize_join_openalex_id(seed_openalex_id) != expected_seed_join_id:
                continue

            candidate_join_id = _normalize_join_openalex_id(candidate_openalex_id)
            if candidate_join_id in label_map:
                raise ValueError(
                    f"Duplicate benchmark label row for candidate_openalex_id: {candidate_openalex_id}"
                )

            if candidate_join_id not in known_candidate_ids:
                continue

            label_map[candidate_join_id] = EvaluationLabelRow(
                seed_openalex_id=expected_seed_openalex_id,
                candidate_openalex_id=candidate_join_id,
                label=_parse_label(row.get("label", "")),
                label_confidence=_parse_optional_float(row.get("label_confidence", "")),
                label_reason=str(row.get("label_reason", "")).strip(),
                notes=str(row.get("notes", "")).strip(),
            )
        return label_map, loaded_row_count


def _freeze_benchmark_labels_snapshot(
    *,
    source_path: Path,
    snapshot_path: Path,
    sha256: str | None = None,
) -> dict[str, Any]:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    copyfile(source_path, snapshot_path)
    return _benchmark_labels_snapshot_metadata(snapshot_path, sha256=sha256)


def _load_materialized_benchmark_dataset_metadata(labels_path: Path) -> dict[str, Any]:
    manifest_path = labels_path.parent / BENCHMARK_DATASET_MANIFEST_FILENAME
    if not manifest_path.exists():
        return {}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    manifest_labels_path = _optional_str(payload.get("benchmark_labels_path"))
    if manifest_labels_path is not None:
        manifest_candidate = Path(manifest_labels_path).expanduser()
        if not manifest_candidate.is_absolute():
            manifest_candidate = (REPO_ROOT / manifest_candidate).resolve()
        try:
            if manifest_candidate.resolve() != labels_path.resolve():
                return {}
        except OSError:
            return {}
    return payload


def _benchmark_labels_snapshot_metadata(
    labels_path: Path,
    *,
    sha256: str | None = None,
) -> dict[str, Any]:
    if not labels_path.exists():
        raise FileNotFoundError(labels_path)
    with labels_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        missing_columns = [
            column
            for column in ("seed_openalex_id", "candidate_openalex_id", "label")
            if column not in fieldnames
        ]
        if missing_columns:
            raise ValueError(
                "Benchmark labels CSV missing required columns: "
                + ", ".join(missing_columns)
            )
        row_count = sum(1 for row in reader if not _row_is_blank(row))
    return {
        "benchmark_labels_snapshot_path": str(labels_path),
        "benchmark_labels_row_count": row_count,
        "benchmark_schema_version": BENCHMARK_SCHEMA_VERSION_V1,
        "benchmark_labels_sha256": sha256 or _sha256_file(labels_path),
    }


def _join_candidate(
    candidate: ScoredCandidateRecord,
    label_row: EvaluationLabelRow | None,
    seed_openalex_id: str,
    label_source: Literal["manual", "silver", "benchmark"],
) -> dict[str, object]:
    payload = candidate.model_dump(mode="json")
    payload["seed_openalex_id"] = seed_openalex_id
    payload["candidate_openalex_id"] = candidate.openalex_id
    payload["label_source"] = label_source
    payload["label"] = label_row.label if label_row is not None else None
    payload["label_confidence"] = label_row.label_confidence if label_row is not None else None
    payload["label_reason"] = label_row.label_reason if label_row is not None else ""
    payload["origin_flags"] = list(label_row.origin_flags) if label_row is not None else list(candidate.candidate_origins)
    payload["notes"] = label_row.notes if label_row is not None else ""
    return payload


def _case_payload(row: dict[str, object]) -> dict[str, object]:
    return {
        "rank": row.get("rank"),
        "candidate_openalex_id": row.get("candidate_openalex_id"),
        "title": row.get("title"),
        "sim": row.get("sim"),
        "conf": row.get("conf"),
        "label": row.get("label"),
        "label_reason": row.get("label_reason"),
        "origin_flags": row.get("origin_flags"),
    }


def _parse_label(value: object) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    if text not in {"0", "1", "2"}:
        raise ValueError(f"Invalid label value: {text}")
    return int(text)


def _parse_optional_float(value: object) -> float | None:
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def _parse_origin_flags(value: object) -> list[str]:
    text = str(value).strip()
    if not text:
        return []
    return [item for item in text.split("|") if item]


def _legacy_evaluation_paths(run_dir: Path) -> tuple[Path, Path, Path]:
    return (
        run_dir / "evaluation_summary.json",
        run_dir / "judged_candidates.jsonl",
        run_dir / "evaluation_cases.json",
    )


def _mode_evaluation_paths(run_dir: Path, evaluation_mode: str) -> tuple[Path, Path, Path]:
    prefix = evaluation_mode.strip()
    return (
        run_dir / f"{prefix}_summary.json",
        run_dir / f"{prefix}_judged_candidates.jsonl",
        run_dir / f"{prefix}_cases.json",
    )


def _normalize_evaluation_mode(
    *,
    evaluation_mode: str | None,
    label_source: str,
) -> Literal[
    EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION,
    EVALUATION_MODE_INDEPENDENT_BENCHMARK,
]:
    normalized_mode = _optional_str(evaluation_mode)
    if normalized_mode is None:
        return (
            EVALUATION_MODE_SILVER_PROVENANCE_REGRESSION
            if label_source == "silver"
            else EVALUATION_MODE_INDEPENDENT_BENCHMARK
        )
    if normalized_mode not in EVALUATION_MODE_VALUES:
        supported = ", ".join(EVALUATION_MODE_VALUES)
        raise ValueError(f"evaluation_mode must be one of: {supported}")
    return normalized_mode


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_provenance_slice_summaries(
    joined_rows: list[dict[str, object]],
    ranking_window: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    ranking_groups = {
        id(row): _provenance_group(row.get("origin_flags"))
        for row in ranking_window
    }
    summaries: dict[str, dict[str, object]] = {}
    for group_name in ("strong_lineage", "seed_related", "hard_negative", "other"):
        group_rows = [
            row
            for row in joined_rows
            if _provenance_group(row.get("origin_flags")) == group_name
        ]
        group_window = [row for row in ranking_window if ranking_groups.get(id(row)) == group_name]
        judged_group = [row for row in group_rows if row.get("label") is not None]
        label_counts = {"0": 0, "1": 0, "2": 0, "unlabeled": 0}
        for row in group_rows:
            label = row.get("label")
            if label is None:
                label_counts["unlabeled"] += 1
            else:
                label_counts[str(label)] += 1
        summaries[group_name] = {
            "candidate_count": len(group_rows),
            "judged_count": len(judged_group),
            "top_k_candidate_count": len(group_window),
            "top_k_judged_count": sum(1 for row in group_window if row.get("label") is not None),
            "label_counts": label_counts,
            "mean_sim": _mean_from_rows(group_rows, "sim"),
            "mean_conf": _mean_from_rows(group_rows, "conf"),
        }
    return summaries


def _provenance_group(origin_flags: object) -> str:
    values = set(str(item).strip() for item in (origin_flags or []) if str(item).strip())
    if "seed_reference" in values or "direct_neighbor" in values:
        return "strong_lineage"
    if "seed_related" in values:
        return "seed_related"
    if "hard_negative" in values:
        return "hard_negative"
    return "other"


def _mean_from_rows(rows: list[dict[str, object]], field_name: str) -> float | None:
    numeric_values = [
        float(value)
        for row in rows
        if (value := row.get(field_name)) is not None
    ]
    if not numeric_values:
        return None
    return round(statistics.fmean(numeric_values), 6)


def _positive_int(value: object, *, label: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer.") from exc
    if normalized <= 0:
        raise ValueError(f"{label} must be a positive integer.")
    return normalized


def _nonnegative_int(value: object, *, label: str) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer.") from exc
    if normalized < 0:
        raise ValueError(f"{label} must be a non-negative integer.")
    return normalized


def _normalize_ratio(value: object, *, label: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a float in [0, 1].") from exc
    if not 0.0 <= normalized <= 1.0:
        raise ValueError(f"{label} must be a float in [0, 1].")
    return round(normalized, 6)


def _normalize_nonempty_annotator_id(value: object) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise ValueError("Annotator id is required.")
    return normalized


def _normalize_annotator_ids(values: list[str] | tuple[str, ...]) -> list[str]:
    normalized_values: list[str] = []
    seen_values: set[str] = set()
    for value in values:
        normalized = _normalize_nonempty_annotator_id(value)
        key = normalized.lower()
        if key in seen_values:
            continue
        seen_values.add(key)
        normalized_values.append(normalized)
    if not normalized_values:
        raise ValueError("At least one annotator id is required.")
    return normalized_values


def _normalize_seed_quota_overrides(
    overrides: dict[str, int] | None,
) -> dict[str, int]:
    if not overrides:
        return {}
    normalized: dict[str, int] = {}
    for seed_openalex_id, quota in overrides.items():
        normalized[_canonical_openalex_id(str(seed_openalex_id))] = _positive_int(
            quota,
            label=f"seed_quota_overrides[{seed_openalex_id}]",
        )
    return normalized


def _normalize_stratum_quota_overrides(
    overrides: dict[str, int] | None,
) -> dict[str, int]:
    normalized = dict(DEFAULT_LINEAGE_BATCH_STRATUM_QUOTAS)
    if not overrides:
        return normalized
    unknown_strata = [
        stratum for stratum in overrides if stratum not in LINEAGE_STRATUM_ORDER
    ]
    if unknown_strata:
        raise ValueError(
            "Unknown lineage stratum quota override(s): " + ", ".join(sorted(unknown_strata))
        )
    for stratum in LINEAGE_STRATUM_ORDER:
        if stratum in overrides:
            normalized[stratum] = _nonnegative_int(
                overrides[stratum],
                label=f"stratum_quota_overrides[{stratum}]",
            )
    return normalized


def _ordered_unique(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def _annotation_pool_row_sort_key(row: AnnotationPoolRow) -> tuple[str, int, str, str]:
    return (
        row.seed_openalex_id.lower(),
        LINEAGE_STRATUM_ORDER.index(row.stratum),
        row.title.lower(),
        row.candidate_openalex_id.lower(),
    )


def _annotation_assignment_sort_key(row: AnnotationPoolRow) -> tuple[str, str, str, str]:
    return (
        row.seed_title.lower(),
        row.title.lower(),
        row.candidate_openalex_id.lower(),
        row.seed_openalex_id.lower(),
    )


def _annotation_export_row_sort_key(row: object) -> tuple[str, str, str]:
    if isinstance(row, AnnotationPoolRow):
        return (
            row.seed_title.lower(),
            row.title.lower(),
            row.candidate_openalex_id.lower(),
        )
    if isinstance(row, dict):
        return (
            str(row.get("seed_title", "")).lower(),
            str(row.get("title", "")).lower(),
            str(row.get("candidate_openalex_id", "")).lower(),
        )
    return ("", "", "")


def _first_present_text(rows: list[dict[str, Any]], field_name: str) -> str:
    for row in rows:
        value = _optional_str(row.get(field_name))
        if value is not None:
            return value
    return ""


def _manual_label_sort_key(candidate: ScoredCandidateRecord) -> tuple[str, str]:
    title = _optional_str(candidate.title)
    return (
        title.lower() if title is not None else "",
        candidate.openalex_id.lower(),
    )


def _evidence_tier_for_mode(evaluation_mode: str | None) -> str | None:
    normalized_mode = _optional_str(evaluation_mode)
    if normalized_mode in EVALUATION_MODE_VALUES:
        return normalized_mode
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_csv_cell(value: object) -> str:
    return str(value).strip().removeprefix("\ufeff")


def _normalize_join_openalex_id(value: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError("OpenAlex id must not be empty for evaluator join")
    return normalize_openalex_work_id(text)


def _validate_batch_id(value: str) -> str:
    return _validate_identifier(value, "batch_id")


def _validate_experiment_id(value: str) -> str:
    return _validate_identifier(value, "experiment_id")


def _validate_identifier(value: str, field_name: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
        raise ValueError(f"{field_name} may contain only letters, digits, '.', '_' and '-'")
    return text


def _maybe_write_metrics_delta(
    *,
    run_dir: Path,
    experiment_id: str,
    experiment_dir: Path,
    current_metrics: dict[str, object],
) -> Path | None:
    if experiment_id == "baseline":
        return None

    baseline_summary_path = run_dir / "experiments" / "baseline" / "evaluation_summary.json"
    if not baseline_summary_path.exists():
        return None

    try:
        baseline_payload = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    baseline_metrics = baseline_payload.get("metrics")
    if not isinstance(baseline_metrics, dict):
        return None

    delta_metrics: dict[str, float] = {}
    for metric_name, current_value in current_metrics.items():
        baseline_value = baseline_metrics.get(metric_name)
        if _is_numeric_metric(current_value) and _is_numeric_metric(baseline_value):
            delta_metrics[metric_name] = round(float(current_value) - float(baseline_value), 6)

    if not delta_metrics:
        return None

    metrics_delta_path = experiment_dir / "metrics_delta.json"
    _write_json(
        metrics_delta_path,
        {
            "baseline_experiment_id": "baseline",
            "current_experiment_id": experiment_id,
            "delta_metrics": delta_metrics,
        },
    )
    return metrics_delta_path


def _is_numeric_metric(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _row_is_blank(row: dict[str, object]) -> bool:
    return not any(str(value).strip() for value in row.values())


def _round_mean_mapping(value: dict[str, float | None]) -> dict[str, float | None]:
    return {
        key: (round(metric_value, 6) if metric_value is not None else None)
        for key, metric_value in value.items()
    }


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[object]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
