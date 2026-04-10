from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.run_context import build_run_context_summary, load_run_context_if_present


KNOWN_EXPERIMENT_METRICS = (
    "precision_at_k",
    "recall_at_k",
    "ndcg_at_k",
)
DIAGNOSTIC_EXPERIMENT_METRICS = (
    "brier_score",
    "expected_calibration_error",
)
ALL_EXPERIMENT_METRICS = KNOWN_EXPERIMENT_METRICS + DIAGNOSTIC_EXPERIMENT_METRICS
DEFAULT_METRIC_ORDER = (
    "ndcg_at_k",
    "precision_at_k",
    "recall_at_k",
)
LOWER_IS_BETTER_METRICS = {"brier_score", "expected_calibration_error"}
UNCLASSIFIED_COHORT_KEY = "__unclassified__"


@dataclass(frozen=True, slots=True)
class ExperimentBatchRow:
    batch_id: str
    batch_dir: Path
    manifest_path: Path
    aggregate_summary_path: Path
    run_context_path: Path | None
    created_at: str | None
    created_at_display: str
    created_at_source: str
    timestamp_sort_key: float
    status: str | None
    seed_count: int | None
    completed_seed_count: int | None
    failed_seed_count: int | None
    theory_config: str | None
    seeds_csv: str | None
    max_references: int | None
    max_related: int | None
    max_hard_negatives: int | None
    top_k: int | None
    label_source: str | None
    evaluation_mode: str | None
    evidence_tier: str | None
    metric_scope: str | None
    benchmark_dataset_id: str | None
    benchmark_labels_sha256: str | None
    benchmark_labels_snapshot_path: str | None
    refresh: bool | None
    launch_source_type: str | None
    accepted_baseline_id: str | None
    benchmark_preset_id: str | None
    eval_preset_id: str | None
    launch_profile_id: str | None
    source_curation_id: str | None
    metric_means: dict[str, float | None]
    metric_medians: dict[str, float | None]
    cohort_key: str | None
    cohort_summary: str
    comparable: bool
    cohort_missing_fields: tuple[str, ...]
    manifest_payload: dict[str, Any]
    aggregate_summary_payload: dict[str, Any]
    run_context_payload: dict[str, Any] | None
    run_context_summary: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class ExperimentCohort:
    cohort_key: str
    summary: str
    size: int
    comparable: bool


def scan_experiment_batches(
    base_dir: str | Path | None = None,
) -> tuple[list[ExperimentBatchRow], list[str]]:
    batches_dir = _batches_dir(base_dir)
    if not batches_dir.exists():
        return [], []
    if not batches_dir.is_dir():
        return [], [f"Batch runs path is not a directory: {batches_dir}"]

    rows: list[ExperimentBatchRow] = []
    warnings: list[str] = []
    for batch_dir in sorted((path for path in batches_dir.iterdir() if path.is_dir()), key=lambda path: path.name.lower()):
        manifest_path = batch_dir / "batch_manifest.json"
        if not manifest_path.exists():
            warnings.append(f"Skipped `{_display_path(batch_dir)}`: missing batch_manifest.json.")
            continue

        try:
            manifest_payload = _load_json_object(manifest_path)
        except ValueError as exc:
            warnings.append(f"Skipped `{_display_path(batch_dir)}`: {exc}")
            continue

        aggregate_summary_path = batch_dir / "aggregate_summary.json"
        aggregate_payload: dict[str, Any] = {}
        if aggregate_summary_path.exists():
            try:
                aggregate_payload = _load_json_object(aggregate_summary_path)
            except ValueError as exc:
                warnings.append(f"Loaded `{_display_path(batch_dir)}` without aggregate_summary.json details: {exc}")
        else:
            warnings.append(f"Loaded `{_display_path(batch_dir)}` without aggregate_summary.json.")

        run_context_payload, run_context_warning = load_run_context_if_present(batch_dir)
        if run_context_warning:
            warnings.append(f"{_display_path(batch_dir)}: {run_context_warning}")

        rows.append(
            build_experiment_batch_row(
                batch_dir=batch_dir,
                manifest_payload=manifest_payload,
                aggregate_payload=aggregate_payload,
                run_context_payload=run_context_payload,
                manifest_mtime=manifest_path.stat().st_mtime,
            )
        )

    rows.sort(
        key=lambda row: (
            -row.timestamp_sort_key,
            row.batch_id.lower(),
            str(row.batch_dir).lower(),
        )
    )
    return rows, warnings


def build_experiment_batch_row(
    *,
    batch_dir: str | Path,
    manifest_payload: dict[str, Any],
    aggregate_payload: dict[str, Any] | None,
    run_context_payload: dict[str, Any] | None,
    manifest_mtime: float,
) -> ExperimentBatchRow:
    resolved_batch_dir = _resolve_path(batch_dir)
    normalized_aggregate = dict(aggregate_payload) if isinstance(aggregate_payload, dict) else {}
    normalized_run_context = dict(run_context_payload) if isinstance(run_context_payload, dict) else None

    created_at, created_at_display, created_at_source, timestamp_sort_key = _timestamp_fields(
        _optional_str(manifest_payload.get("created_at")),
        manifest_mtime,
    )

    metric_means = {
        metric_name: _metric_stat(normalized_aggregate, metric_name, "mean")
        for metric_name in ALL_EXPERIMENT_METRICS
    }
    metric_medians = {
        metric_name: _metric_stat(normalized_aggregate, metric_name, "median")
        for metric_name in ALL_EXPERIMENT_METRICS
    }

    seeds_csv = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "seeds_csv")),
        _optional_str(manifest_payload.get("seeds_csv")),
    )
    options_payload = _dict_value(manifest_payload, "options")
    max_references = _first_nonempty_int(
        _dict_value(normalized_run_context, "max_references"),
        _dict_value(options_payload, "max_references"),
    )
    max_related = _first_nonempty_int(
        _dict_value(normalized_run_context, "max_related"),
        _dict_value(options_payload, "max_related"),
    )
    max_hard_negatives = _first_nonempty_int(
        _dict_value(normalized_run_context, "max_hard_negatives"),
        _dict_value(options_payload, "max_hard_negatives"),
    )
    top_k = _first_nonempty_int(
        _dict_value(normalized_run_context, "top_k"),
        _dict_value(options_payload, "top_k"),
    )
    label_source = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "label_source")),
        _optional_str(_dict_value(options_payload, "label_source")),
    )
    evaluation_mode = _derive_evaluation_mode(
        label_source=label_source,
        explicit_mode=_first_nonempty(
            _optional_str(_dict_value(normalized_run_context, "evaluation_mode")),
            _optional_str(_dict_value(options_payload, "evaluation_mode")),
        ),
    )
    evidence_tier = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "evidence_tier")),
        _optional_str(_dict_value(options_payload, "evidence_tier")),
        evaluation_mode,
    )
    metric_scope = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "metric_scope")),
        _optional_str(_dict_value(options_payload, "metric_scope")),
    )
    benchmark_dataset_id = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "benchmark_dataset_id")),
        _optional_str(_dict_value(options_payload, "benchmark_dataset_id")),
    )
    benchmark_labels_sha256 = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "benchmark_labels_sha256")),
        _optional_str(_dict_value(options_payload, "benchmark_labels_sha256")),
    )
    benchmark_labels_snapshot_path = _first_nonempty(
        _optional_str(_dict_value(normalized_run_context, "benchmark_labels_snapshot_path")),
        _optional_str(_dict_value(options_payload, "benchmark_labels_snapshot_path")),
    )
    refresh_value = _first_nonempty(
        _dict_value(normalized_run_context, "refresh"),
        _dict_value(options_payload, "refresh"),
    )
    refresh = bool(refresh_value) if isinstance(refresh_value, bool) else None

    cohort_key, cohort_summary, cohort_missing_fields = build_experiment_cohort_key(
        seeds_csv=seeds_csv,
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
        top_k=top_k,
        label_source=label_source,
        evaluation_mode=evaluation_mode,
        benchmark_dataset_id=benchmark_dataset_id,
    )

    return ExperimentBatchRow(
        batch_id=_optional_str(manifest_payload.get("batch_id")) or resolved_batch_dir.name,
        batch_dir=resolved_batch_dir,
        manifest_path=resolved_batch_dir / "batch_manifest.json",
        aggregate_summary_path=resolved_batch_dir / "aggregate_summary.json",
        run_context_path=(resolved_batch_dir / "run_context.json") if (resolved_batch_dir / "run_context.json").exists() else None,
        created_at=created_at,
        created_at_display=created_at_display,
        created_at_source=created_at_source,
        timestamp_sort_key=timestamp_sort_key,
        status=_optional_str(manifest_payload.get("status")),
        seed_count=_int_or_none(manifest_payload.get("seed_count")),
        completed_seed_count=_int_or_none(manifest_payload.get("completed_seed_count")),
        failed_seed_count=_int_or_none(manifest_payload.get("failed_seed_count")),
        theory_config=_optional_str(manifest_payload.get("theory_config")),
        seeds_csv=seeds_csv,
        max_references=max_references,
        max_related=max_related,
        max_hard_negatives=max_hard_negatives,
        top_k=top_k,
        label_source=label_source,
        evaluation_mode=evaluation_mode,
        evidence_tier=evidence_tier,
        metric_scope=metric_scope,
        benchmark_dataset_id=benchmark_dataset_id,
        benchmark_labels_sha256=benchmark_labels_sha256,
        benchmark_labels_snapshot_path=benchmark_labels_snapshot_path,
        refresh=refresh,
        launch_source_type=_optional_str(_dict_value(normalized_run_context, "launch_source_type")),
        accepted_baseline_id=_optional_str(_dict_value(normalized_run_context, "accepted_baseline_id")),
        benchmark_preset_id=_optional_str(_dict_value(normalized_run_context, "benchmark_preset_id")),
        eval_preset_id=_optional_str(_dict_value(normalized_run_context, "eval_preset_id")),
        launch_profile_id=_optional_str(_dict_value(normalized_run_context, "launch_profile_id")),
        source_curation_id=_optional_str(_dict_value(normalized_run_context, "source_curation_id")),
        metric_means=metric_means,
        metric_medians=metric_medians,
        cohort_key=cohort_key,
        cohort_summary=cohort_summary,
        comparable=cohort_key is not None,
        cohort_missing_fields=tuple(cohort_missing_fields),
        manifest_payload=dict(manifest_payload),
        aggregate_summary_payload=normalized_aggregate,
        run_context_payload=normalized_run_context,
        run_context_summary=(
            build_run_context_summary(normalized_run_context)
            if isinstance(normalized_run_context, dict)
            else None
        ),
    )


def build_experiment_cohort_key(
    *,
    seeds_csv: str | None,
    max_references: int | None,
    max_related: int | None,
    max_hard_negatives: int | None,
    top_k: int | None,
    label_source: str | None,
    evaluation_mode: str | None = None,
    benchmark_dataset_id: str | None = None,
) -> tuple[str | None, str, list[str]]:
    normalized_evaluation_mode = _derive_evaluation_mode(
        label_source=label_source,
        explicit_mode=evaluation_mode,
    )
    values = {
        "seeds_csv": _optional_str(seeds_csv),
        "max_references": max_references,
        "max_related": max_related,
        "max_hard_negatives": max_hard_negatives,
        "top_k": top_k,
        "label_source": _optional_str(label_source),
        "evaluation_mode": normalized_evaluation_mode,
    }
    if normalized_evaluation_mode == "independent_benchmark":
        values["benchmark_dataset_id"] = _optional_str(benchmark_dataset_id)
    missing_fields = [field_name for field_name, value in values.items() if value in (None, "")]
    if missing_fields:
        return (
            None,
            "Unclassified (insufficient benchmark/eval metadata: "
            + ", ".join(missing_fields)
            + ")",
            missing_fields,
        )

    cohort_key = json.dumps(values, sort_keys=True)
    summary = (
        f"{values['seeds_csv']} | refs={values['max_references']} | related={values['max_related']} | "
        f"hardneg={values['max_hard_negatives']} | top_k={values['top_k']} | "
        f"label={values['label_source']} | mode={values['evaluation_mode']}"
    )
    if normalized_evaluation_mode == "independent_benchmark":
        summary += f" | benchmark={values['benchmark_dataset_id']}"
    return cohort_key, summary, []


def group_experiment_cohorts(rows: list[ExperimentBatchRow]) -> list[ExperimentCohort]:
    grouped: dict[str, list[ExperimentBatchRow]] = {}
    unclassified_rows = [row for row in rows if row.cohort_key is None]
    for row in rows:
        if row.cohort_key is None:
            continue
        grouped.setdefault(row.cohort_key, []).append(row)

    cohorts = [
        ExperimentCohort(
            cohort_key=cohort_key,
            summary=group_rows[0].cohort_summary,
            size=len(group_rows),
            comparable=True,
        )
        for cohort_key, group_rows in grouped.items()
    ]
    cohorts.sort(key=lambda cohort: (-cohort.size, cohort.summary.lower()))

    if unclassified_rows:
        cohorts.append(
            ExperimentCohort(
                cohort_key=UNCLASSIFIED_COHORT_KEY,
                summary="Unclassified (not confidently comparable)",
                size=len(unclassified_rows),
                comparable=False,
            )
        )
    return cohorts


def build_cohort_rows(cohorts: list[ExperimentCohort]) -> list[dict[str, Any]]:
    return [
        {
            "cohort": cohort.summary,
            "size": cohort.size,
            "comparable": "yes" if cohort.comparable else "no",
        }
        for cohort in cohorts
    ]


def available_experiment_statuses(rows: list[ExperimentBatchRow]) -> list[str]:
    return sorted({status for row in rows if (status := _optional_str(row.status)) is not None})


def choose_default_leaderboard_metric(
    rows: list[ExperimentBatchRow],
    *,
    preferred_metric: str | None = None,
) -> str | None:
    available_metrics = [
        metric_name
        for metric_name in DEFAULT_METRIC_ORDER
        if any(row.metric_means.get(metric_name) is not None for row in rows)
    ]
    if preferred_metric in available_metrics:
        return preferred_metric
    return available_metrics[0] if available_metrics else None


def filter_experiment_rows(
    rows: list[ExperimentBatchRow],
    *,
    cohort_key: str | None,
    statuses: list[str] | None = None,
    search_text: str | None = None,
) -> list[ExperimentBatchRow]:
    selected_statuses = {value for value in statuses or [] if value}
    needle = _optional_str(search_text)
    lowered_needle = needle.lower() if needle is not None else None

    filtered: list[ExperimentBatchRow] = []
    for row in rows:
        if cohort_key == UNCLASSIFIED_COHORT_KEY and row.cohort_key is not None:
            continue
        if cohort_key not in (None, UNCLASSIFIED_COHORT_KEY) and row.cohort_key != cohort_key:
            continue
        if selected_statuses and _optional_str(row.status) not in selected_statuses:
            continue
        if lowered_needle is not None and lowered_needle not in _search_blob(row):
            continue
        filtered.append(row)
    return filtered


def sort_experiment_rows(
    rows: list[ExperimentBatchRow],
    *,
    leaderboard_metric: str | None,
) -> list[ExperimentBatchRow]:
    metric_name = leaderboard_metric if leaderboard_metric in ALL_EXPERIMENT_METRICS else None
    lower_is_better = metric_name in LOWER_IS_BETTER_METRICS

    def sort_key(row: ExperimentBatchRow) -> tuple[Any, ...]:
        metric_value = row.metric_means.get(metric_name) if metric_name is not None else None
        missing_metric = metric_value is None
        status_priority = 0 if _optional_str(row.status) == "completed" else 1
        partial_priority = 1 if ((row.failed_seed_count or 0) > 0) else 0
        if metric_value is None:
            metric_sort_value = float("inf")
        elif lower_is_better:
            metric_sort_value = float(metric_value)
        else:
            metric_sort_value = -float(metric_value)
        return (
            missing_metric,
            status_priority,
            partial_priority,
            metric_sort_value,
            -row.timestamp_sort_key,
            row.batch_id.lower(),
        )

    return sorted(rows, key=sort_key)


def find_experiment_row(
    rows: list[ExperimentBatchRow],
    batch_id: str | None,
) -> ExperimentBatchRow | None:
    needle = _optional_str(batch_id)
    if needle is None:
        return None
    for row in rows:
        if row.batch_id == needle:
            return row
    return None


def build_experiment_table_rows(
    rows: list[ExperimentBatchRow],
    *,
    leaderboard_metric: str | None,
) -> list[dict[str, Any]]:
    metric_column_name = (
        f"{leaderboard_metric}_mean"
        if leaderboard_metric in ALL_EXPERIMENT_METRICS
        else "leaderboard_metric_mean"
    )
    table_rows: list[dict[str, Any]] = []
    for row in rows:
        table_rows.append(
            {
                "batch_id": row.batch_id,
                "created_at": row.created_at_display,
                "status": row.status,
                "completed_seed_count": row.completed_seed_count,
                "failed_seed_count": row.failed_seed_count,
                "accepted_baseline_id": row.accepted_baseline_id,
                "benchmark_preset_id": row.benchmark_preset_id,
                "eval_preset_id": row.eval_preset_id,
                "launch_profile_id": row.launch_profile_id,
                "evaluation_mode": row.evaluation_mode,
                "evidence_tier": row.evidence_tier,
                "benchmark_dataset_id": row.benchmark_dataset_id,
                metric_column_name: row.metric_means.get(leaderboard_metric) if leaderboard_metric else None,
            }
        )
    return table_rows


def build_experiment_detail(row: ExperimentBatchRow) -> dict[str, Any]:
    return {
        "identity": {
            "batch_id": row.batch_id,
            "batch_dir": row.batch_dir,
            "status": row.status,
            "created_at": row.created_at_display,
            "created_at_source": row.created_at_source,
            "cohort_key": row.cohort_key,
            "cohort_summary": row.cohort_summary,
            "comparable": row.comparable,
        },
        "manifest_fields": {
            "seeds_csv": row.seeds_csv,
            "theory_config": row.theory_config,
            "seed_count": row.seed_count,
            "completed_seed_count": row.completed_seed_count,
            "failed_seed_count": row.failed_seed_count,
            "max_references": row.max_references,
            "max_related": row.max_related,
            "max_hard_negatives": row.max_hard_negatives,
            "top_k": row.top_k,
            "label_source": row.label_source,
            "evaluation_mode": row.evaluation_mode,
            "evidence_tier": row.evidence_tier,
            "metric_scope": row.metric_scope,
            "benchmark_dataset_id": row.benchmark_dataset_id,
            "benchmark_labels_sha256": row.benchmark_labels_sha256,
            "benchmark_labels_snapshot_path": row.benchmark_labels_snapshot_path,
            "refresh": row.refresh,
        },
        "aggregate_metrics": {
            metric_name: {
                "mean": row.metric_means.get(metric_name),
                "median": row.metric_medians.get(metric_name),
            }
            for metric_name in KNOWN_EXPERIMENT_METRICS
            if row.metric_means.get(metric_name) is not None or row.metric_medians.get(metric_name) is not None
        },
        "confidence_diagnostics": {
            "confidence_brier_score_diag": {
                "mean": row.metric_means.get("brier_score"),
                "median": row.metric_medians.get("brier_score"),
            },
            "confidence_ece_diag": {
                "mean": row.metric_means.get("expected_calibration_error"),
                "median": row.metric_medians.get("expected_calibration_error"),
            },
        },
        "run_context_summary": row.run_context_summary,
        "raw_manifest": row.manifest_payload,
        "raw_aggregate_summary": row.aggregate_summary_payload,
        "raw_run_context": row.run_context_payload,
    }


def _derive_evaluation_mode(*, label_source: str | None, explicit_mode: str | None) -> str | None:
    normalized_explicit = _optional_str(explicit_mode)
    if normalized_explicit is not None:
        return normalized_explicit
    normalized_label_source = _optional_str(label_source)
    if normalized_label_source == "benchmark":
        return "independent_benchmark"
    if normalized_label_source is not None:
        return "silver_provenance_regression"
    return None


def _batches_dir(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return REPO_ROOT / "runs" / "batches"
    candidate = Path(str(base_dir).strip()).expanduser()
    if candidate.is_absolute():
        return candidate / "runs" / "batches"
    return (REPO_ROOT / candidate / "runs" / "batches").resolve()


def _resolve_path(value: str | Path) -> Path:
    candidate = Path(str(value)).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"could not read {path.name}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"malformed JSON in {path.name} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError(f"invalid {path.name}: expected a JSON object")
    return payload


def _metric_stat(
    aggregate_payload: dict[str, Any],
    metric_name: str,
    stat_name: str,
) -> float | None:
    metric_payload = _dict_value(_dict_value(aggregate_payload, "metric_aggregates"), metric_name)
    value = _dict_value(metric_payload, stat_name)
    return _float_or_none(value)


def _timestamp_fields(
    created_at: str | None,
    manifest_mtime: float,
) -> tuple[str | None, str, str, float]:
    if created_at is not None:
        parsed = _parse_timestamp(created_at)
        if parsed is not None:
            return created_at, created_at, "created_at", parsed.timestamp()
    fallback_dt = datetime.fromtimestamp(manifest_mtime, tz=timezone.utc)
    fallback_text = fallback_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return fallback_text, f"{fallback_text} (manifest mtime)", "manifest_mtime", fallback_dt.timestamp()


def _parse_timestamp(value: str) -> datetime | None:
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _first_nonempty_int(*values: Any) -> int | None:
    for value in values:
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _dict_value(payload: Any, key: str) -> Any:
    if isinstance(payload, dict):
        return payload.get(key)
    return None


def _search_blob(row: ExperimentBatchRow) -> str:
    parts = [
        row.batch_id,
        _display_path(row.batch_dir),
        row.theory_config or "",
        row.seeds_csv or "",
        row.accepted_baseline_id or "",
        row.benchmark_preset_id or "",
        row.eval_preset_id or "",
        row.launch_profile_id or "",
        row.source_curation_id or "",
        row.cohort_summary,
    ]
    return " ".join(parts).lower()


def _display_path(value: object) -> str:
    if value in (None, ""):
        return ""
    path = Path(str(value))
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)
