from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

from src.config import REPO_ROOT, TheoryConfig, load_runtime_config, load_theory_config
from src.eval.benchmark import (
    BATCH_DIRNAME,
    DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    DEFAULT_BATCH_MAX_REFERENCES,
    DEFAULT_BATCH_MAX_RELATED,
    DEFAULT_BATCH_TOP_K,
    DEFAULT_EVALUATION_MODE,
    EVALUATION_MODE_INDEPENDENT_BENCHMARK,
    EVALUATION_MODE_VALUES,
    METRIC_SCOPE_LOCAL_CORPUS_RANKING,
    SeedBatchManifest,
    run_seed_batch,
)
from src.ui.batch_loader import BatchLoadError, BatchUiBundle, load_batch_bundle


SUPPORTED_LABEL_SOURCES = ("silver", "benchmark")


class BatchRunValidationError(ValueError):
    """Raised when the UI batch run form contains invalid inputs."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("\n".join(errors))


@dataclass(frozen=True, slots=True)
class BatchRunRequest:
    """Validated batch run request built from the UI form."""

    initial_doi_context: str
    theory_config_path: Path
    seeds_csv_path: Path
    batch_id: str
    batch_dir: Path
    max_references: int = DEFAULT_BATCH_MAX_REFERENCES
    max_related: int = DEFAULT_BATCH_MAX_RELATED
    max_hard_negatives: int = DEFAULT_BATCH_MAX_HARD_NEGATIVES
    top_k: int = DEFAULT_BATCH_TOP_K
    label_source: Literal["silver", "benchmark"] = "silver"
    evaluation_mode: Literal[
        "silver_provenance_regression",
        "independent_benchmark",
    ] = DEFAULT_EVALUATION_MODE
    benchmark_labels_path: Path | None = None
    benchmark_dataset_id: str | None = None
    benchmark_labels_sha256: str | None = None
    metric_scope: Literal["local_corpus_ranking"] = METRIC_SCOPE_LOCAL_CORPUS_RANKING
    refresh: bool = False


@dataclass(frozen=True, slots=True)
class BatchRunSummary:
    """Compact summary shown in the UI after a run attempt."""

    initial_doi_context: str
    batch_id: str
    batch_dir: str
    theory_config: str
    seeds_csv: str
    seed_count: int | None = None
    completed_seed_count: int | None = None
    failed_seed_count: int | None = None
    status: str | None = None
    output_paths: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class BatchRunOutcome:
    """Result of invoking the backend batch service from the UI."""

    success: bool
    request: BatchRunRequest
    loaded_bundle: BatchUiBundle | None
    summary: BatchRunSummary | None
    error_message: str | None = None
    partial_bundle: BatchUiBundle | None = None


def build_batch_run_request(
    *,
    initial_doi_context: str,
    theory_config_path: str | Path,
    seeds_csv_path: str | Path,
    batch_id: str,
    max_references: object = DEFAULT_BATCH_MAX_REFERENCES,
    max_related: object = DEFAULT_BATCH_MAX_RELATED,
    max_hard_negatives: object = DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    top_k: object = DEFAULT_BATCH_TOP_K,
    label_source: str = "silver",
    evaluation_mode: str = DEFAULT_EVALUATION_MODE,
    benchmark_labels_path: str | Path | None = None,
    benchmark_dataset_id: str = "",
    benchmark_labels_sha256: str = "",
    refresh: bool = False,
    runtime_loader: Callable[[], object] = load_runtime_config,
) -> BatchRunRequest:
    """Validate UI form values and normalize them for Python-level batch execution."""

    errors: list[str] = []
    initial_doi_text = str(initial_doi_context).strip()

    theory_path = _resolve_repo_path(theory_config_path)
    if theory_path is None:
        errors.append("Theory config path must not be empty.")
    elif not theory_path.exists():
        errors.append(f"Theory config path does not exist: {theory_path}")
    elif not theory_path.is_file():
        errors.append(f"Theory config path is not a file: {theory_path}")

    seeds_path = _resolve_repo_path(seeds_csv_path)
    if seeds_path is None:
        errors.append("Seeds CSV path must not be empty.")
    elif not seeds_path.exists():
        errors.append(f"Seeds CSV path does not exist: {seeds_path}")
    elif not seeds_path.is_file():
        errors.append(f"Seeds CSV path is not a file: {seeds_path}")

    batch_id_text = str(batch_id).strip()
    if not batch_id_text:
        errors.append("Batch ID must not be empty.")
    elif not re.fullmatch(r"[A-Za-z0-9._-]+", batch_id_text):
        errors.append("Batch ID may contain only letters, digits, '.', '_' and '-'.")

    parsed_evaluation_mode = str(evaluation_mode).strip() or DEFAULT_EVALUATION_MODE
    if parsed_evaluation_mode not in EVALUATION_MODE_VALUES:
        supported_modes = ", ".join(EVALUATION_MODE_VALUES)
        errors.append(f"evaluation_mode must be one of: {supported_modes}.")

    parsed_label_source = str(label_source).strip() or "silver"
    if parsed_label_source not in SUPPORTED_LABEL_SOURCES:
        supported_values = ", ".join(SUPPORTED_LABEL_SOURCES)
        errors.append(f"Label source must be one of: {supported_values}.")

    parsed_max_references = _parse_int_option(max_references, "max_references", minimum=0, errors=errors)
    parsed_max_related = _parse_int_option(max_related, "max_related", minimum=0, errors=errors)
    parsed_max_hard_negatives = _parse_int_option(
        max_hard_negatives,
        "max_hard_negatives",
        minimum=0,
        errors=errors,
    )
    parsed_top_k = _parse_int_option(top_k, "top_k", minimum=1, errors=errors)

    runtime = runtime_loader()
    batch_dir = REPO_ROOT / runtime.runs_dir / BATCH_DIRNAME / batch_id_text
    if batch_id_text and batch_dir.exists():
        errors.append(f"Batch directory already exists: {batch_dir}")

    resolved_benchmark_labels_path: Path | None = None
    normalized_benchmark_dataset_id = str(benchmark_dataset_id).strip() or None
    normalized_benchmark_labels_sha256 = str(benchmark_labels_sha256).strip() or None
    if str(benchmark_labels_path).strip() if benchmark_labels_path is not None else False:
        resolved_benchmark_labels_path = _resolve_repo_path(benchmark_labels_path)
        if resolved_benchmark_labels_path is None:
            errors.append("Benchmark labels path must not be empty when provided.")
        elif not resolved_benchmark_labels_path.exists():
            errors.append(f"Benchmark labels path does not exist: {resolved_benchmark_labels_path}")
        elif not resolved_benchmark_labels_path.is_file():
            errors.append(f"Benchmark labels path is not a file: {resolved_benchmark_labels_path}")

    if parsed_evaluation_mode == EVALUATION_MODE_INDEPENDENT_BENCHMARK:
        if resolved_benchmark_labels_path is None:
            errors.append(
                "benchmark_labels_path is required when evaluation_mode is independent_benchmark."
            )
        if normalized_benchmark_dataset_id is None:
            errors.append(
                "benchmark_dataset_id is required when evaluation_mode is independent_benchmark."
            )
        if parsed_label_source == "silver":
            parsed_label_source = "benchmark"
        if resolved_benchmark_labels_path is not None and normalized_benchmark_labels_sha256 is None:
            normalized_benchmark_labels_sha256 = _sha256_file(resolved_benchmark_labels_path)

    if errors:
        raise BatchRunValidationError(errors)

    return BatchRunRequest(
        initial_doi_context=initial_doi_text,
        theory_config_path=theory_path,
        seeds_csv_path=seeds_path,
        batch_id=batch_id_text,
        batch_dir=batch_dir,
        max_references=parsed_max_references,
        max_related=parsed_max_related,
        max_hard_negatives=parsed_max_hard_negatives,
        top_k=parsed_top_k,
        label_source=parsed_label_source,
        evaluation_mode=parsed_evaluation_mode,
        benchmark_labels_path=resolved_benchmark_labels_path,
        benchmark_dataset_id=normalized_benchmark_dataset_id,
        benchmark_labels_sha256=normalized_benchmark_labels_sha256,
        metric_scope=METRIC_SCOPE_LOCAL_CORPUS_RANKING,
        refresh=bool(refresh),
    )


def run_batch_request(
    request: BatchRunRequest,
    *,
    previous_bundle: BatchUiBundle | None = None,
    theory_loader: Callable[[str | Path], TheoryConfig] = load_theory_config,
    run_batch_service: Callable[..., SeedBatchManifest] = run_seed_batch,
    batch_loader: Callable[[str | Path], BatchUiBundle] = load_batch_bundle,
) -> BatchRunOutcome:
    """Invoke the existing Python batch service and load the resulting batch for the UI."""

    try:
        theory = theory_loader(request.theory_config_path)
        manifest = run_batch_service(
            seeds_path=request.seeds_csv_path,
            theory_config_path=request.theory_config_path,
            theory=theory,
            batch_id=request.batch_id,
            max_references=request.max_references,
            max_related=request.max_related,
            max_hard_negatives=request.max_hard_negatives,
            top_k=request.top_k,
            label_source=request.label_source,
            evaluation_mode=request.evaluation_mode,
            benchmark_labels_path=request.benchmark_labels_path,
            benchmark_dataset_id=request.benchmark_dataset_id,
            benchmark_labels_sha256=request.benchmark_labels_sha256,
            refresh=request.refresh,
        )
        bundle = batch_loader(manifest.batch_dir)
    except Exception as exc:
        partial_bundle = _load_partial_bundle_if_available(request.batch_dir, batch_loader=batch_loader)
        summary = (
            build_batch_run_summary(request.initial_doi_context, partial_bundle.manifest)
            if partial_bundle is not None
            else None
        )
        return BatchRunOutcome(
            success=False,
            request=request,
            loaded_bundle=previous_bundle,
            summary=summary,
            error_message=str(exc),
            partial_bundle=partial_bundle,
        )

    return BatchRunOutcome(
        success=True,
        request=request,
        loaded_bundle=bundle,
        summary=build_batch_run_summary(request.initial_doi_context, manifest),
    )


def build_batch_run_summary(initial_doi_context: str, manifest: SeedBatchManifest) -> BatchRunSummary:
    """Build a compact UI summary from a completed or partially readable batch manifest."""

    return BatchRunSummary(
        initial_doi_context=str(initial_doi_context).strip(),
        batch_id=manifest.batch_id,
        batch_dir=manifest.batch_dir,
        theory_config=manifest.theory_config,
        seeds_csv=manifest.seeds_csv,
        seed_count=manifest.seed_count,
        completed_seed_count=manifest.completed_seed_count,
        failed_seed_count=manifest.failed_seed_count,
        status=manifest.status,
        output_paths=manifest.output_paths.model_dump(mode="json"),
    )


def _load_partial_bundle_if_available(
    batch_dir: Path,
    *,
    batch_loader: Callable[[str | Path], BatchUiBundle],
) -> BatchUiBundle | None:
    if not batch_dir.exists():
        return None
    try:
        return batch_loader(batch_dir)
    except BatchLoadError:
        return None


def _resolve_repo_path(value: str | Path) -> Path | None:
    raw_value = str(value).strip()
    if not raw_value:
        return None
    raw_value = raw_value.replace("\\", "/")
    candidate = Path(raw_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _parse_int_option(value: object, field_name: str, *, minimum: int, errors: list[str]) -> int:
    try:
        parsed_value = int(value)
    except (TypeError, ValueError):
        errors.append(f"{field_name} must be an integer.")
        return minimum
    if parsed_value < minimum:
        errors.append(f"{field_name} must be >= {minimum}.")
    return parsed_value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()
