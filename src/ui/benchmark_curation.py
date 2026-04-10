from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import REPO_ROOT
from src.ui.comparison import COMPARISON_METRICS, TIE_TOLERANCE


CURATION_DECISION_OPTIONS = (
    "keep",
    "review",
    "exclude",
)
CURATION_DECISION_FILTER_OPTIONS = (
    "all",
    *CURATION_DECISION_OPTIONS,
)
SATURATION_THRESHOLD = 0.999


class BenchmarkCurationError(ValueError):
    """Raised when benchmark curation data cannot be exported."""


@dataclass(frozen=True, slots=True)
class BenchmarkCurationExportRequest:
    curation_id: str
    reviewer: str | None
    notes: str | None
    export_only_kept_to_csv: bool
    include_review_seeds_csv: bool
    include_markdown_summary: bool


@dataclass(frozen=True, slots=True)
class BenchmarkCurationExportResult:
    curation_id: str
    curation_dir: Path
    manifest_path: Path
    seed_decisions_path: Path
    curated_seeds_csv_path: Path
    review_seeds_csv_path: Path | None
    report_path: Path | None


def build_seed_curation_rows(
    primary_seed_rows: list[dict[str, Any]],
    *,
    comparison_rows_by_doi: dict[str, dict[str, Any]] | None = None,
    selected_metric: str | None = None,
) -> list[dict[str, Any]]:
    comparison_lookup = comparison_rows_by_doi or {}
    rows: list[dict[str, Any]] = []
    for seed_row in primary_seed_rows:
        doi = _optional_str(seed_row.get("doi")) or ""
        status = _optional_str(seed_row.get("status")) or "unknown"
        metric_values = {
            metric_name: _numeric_value(seed_row.get(metric_name))
            for metric_name in COMPARISON_METRICS
        }
        available_metric_count = sum(1 for value in metric_values.values() if value is not None)
        failed_seed = status != "completed"
        missing_metrics = available_metric_count <= 0
        saturated_ndcg = bool(
            metric_values.get("ndcg_at_k") is not None
            and float(metric_values["ndcg_at_k"]) >= SATURATION_THRESHOLD
        )
        saturated_precision = bool(
            metric_values.get("precision_at_k") is not None
            and float(metric_values["precision_at_k"]) >= SATURATION_THRESHOLD
        )

        comparison_row = comparison_lookup.get(doi, {})
        improvement_delta = _numeric_value(comparison_row.get("improvement_delta"))
        tie_like_seed = bool(
            improvement_delta is not None
            and abs(improvement_delta) <= TIE_TOLERANCE
        )
        low_signal_seed = bool(
            not failed_seed
            and (saturated_ndcg or saturated_precision or tie_like_seed)
        )

        suggested_decision, suggestion_reasons = suggest_seed_curation_decision(
            failed_seed=failed_seed,
            missing_metrics=missing_metrics,
            saturated_ndcg=saturated_ndcg,
            saturated_precision=saturated_precision,
            tie_like_seed=tie_like_seed,
            low_signal_seed=low_signal_seed,
        )

        error_parts = [
            _optional_str(seed_row.get("failed_stage")),
            _optional_str(seed_row.get("error_type")),
            _optional_str(seed_row.get("error_message")),
        ]
        error_summary = " | ".join(part for part in error_parts if part) or None
        rows.append(
            {
                "batch_index": seed_row.get("batch_index"),
                "doi": doi,
                "status": status,
                **metric_values,
                "failed_stage": seed_row.get("failed_stage"),
                "error_summary": error_summary,
                "failed_seed": failed_seed,
                "missing_metrics": missing_metrics,
                "saturated_ndcg": saturated_ndcg,
                "saturated_precision": saturated_precision,
                "low_signal_seed": low_signal_seed,
                "comparison_metric": _optional_str(selected_metric),
                "improvement_delta": improvement_delta,
                "tie_like_seed": tie_like_seed,
                "suggested_decision": suggested_decision,
                "suggestion_reasons": tuple(suggestion_reasons),
                "reason_summary": "; ".join(suggestion_reasons),
            }
        )
    return rows


def suggest_seed_curation_decision(
    *,
    failed_seed: bool,
    missing_metrics: bool,
    saturated_ndcg: bool,
    saturated_precision: bool,
    tie_like_seed: bool,
    low_signal_seed: bool,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    if failed_seed:
        reasons.append("Seed failed in the batch run.")
        return "exclude", tuple(reasons)
    if missing_metrics:
        reasons.append("Seed has no usable numeric metrics.")
        return "exclude", tuple(reasons)
    if saturated_ndcg:
        reasons.append("nDCG is saturated for this seed.")
    if saturated_precision:
        reasons.append("Precision is saturated for this seed.")
    if tie_like_seed:
        reasons.append("Comparison improvement delta is effectively zero.")
    if low_signal_seed and not reasons:
        reasons.append("Seed looks low-signal for discriminating theory changes.")
    if reasons:
        return "review", tuple(dict.fromkeys(reasons))
    return "keep", ("Seed completed with usable metrics and no obvious degeneracy flags.",)


def normalize_curation_decisions(
    seed_rows: list[dict[str, Any]],
    current_decisions: dict[str, str] | None,
) -> dict[str, str]:
    existing = dict(current_decisions or {})
    normalized: dict[str, str] = {}
    for row in seed_rows:
        doi = _optional_str(row.get("doi"))
        if doi is None:
            continue
        decision = existing.get(doi)
        if decision not in CURATION_DECISION_OPTIONS:
            decision = str(row.get("suggested_decision") or "review")
        normalized[doi] = decision
    return normalized


def filter_curation_rows(
    seed_rows: list[dict[str, Any]],
    decisions: dict[str, str] | None,
    *,
    decision_filter: str,
    only_failed: bool,
    only_saturated: bool,
    only_tie_like: bool,
    doi_filter: str,
) -> list[dict[str, Any]]:
    normalized_decisions = normalize_curation_decisions(seed_rows, decisions)
    lowered_filter = doi_filter.strip().lower()
    filtered_rows: list[dict[str, Any]] = []
    for row in seed_rows:
        doi = _optional_str(row.get("doi"))
        if doi is None:
            continue
        decision = normalized_decisions.get(doi, str(row.get("suggested_decision") or "review"))
        if decision_filter != "all" and decision != decision_filter:
            continue
        if only_failed and not bool(row.get("failed_seed")):
            continue
        if only_saturated and not bool(row.get("saturated_ndcg") or row.get("saturated_precision")):
            continue
        if only_tie_like and not bool(row.get("tie_like_seed")):
            continue
        if lowered_filter and lowered_filter not in doi.lower():
            continue
        filtered_rows.append({**row, "decision": decision})
    return filtered_rows


def summarize_curation_decisions(
    seed_rows: list[dict[str, Any]],
    decisions: dict[str, str] | None,
    *,
    comparison_context_used: bool,
) -> dict[str, Any]:
    normalized_decisions = normalize_curation_decisions(seed_rows, decisions)
    keep_count = 0
    review_count = 0
    exclude_count = 0
    failed_seed_count = 0
    usable_completed_seed_count = 0
    for row in seed_rows:
        doi = _optional_str(row.get("doi"))
        if doi is None:
            continue
        decision = normalized_decisions.get(doi, str(row.get("suggested_decision") or "review"))
        if decision == "keep":
            keep_count += 1
        elif decision == "review":
            review_count += 1
        else:
            exclude_count += 1
        if bool(row.get("failed_seed")):
            failed_seed_count += 1
        if not bool(row.get("failed_seed")) and not bool(row.get("missing_metrics")):
            usable_completed_seed_count += 1
    return {
        "total_seeds": len(seed_rows),
        "keep_count": keep_count,
        "review_count": review_count,
        "exclude_count": exclude_count,
        "failed_seed_count": failed_seed_count,
        "usable_completed_seed_count": usable_completed_seed_count,
        "comparison_context_used": bool(comparison_context_used),
    }


def build_seed_decision_rows(
    seed_rows: list[dict[str, Any]],
    decisions: dict[str, str] | None,
) -> list[dict[str, Any]]:
    normalized_decisions = normalize_curation_decisions(seed_rows, decisions)
    export_rows: list[dict[str, Any]] = []
    for row in seed_rows:
        doi = _optional_str(row.get("doi"))
        if doi is None:
            continue
        export_rows.append(
            {
                "batch_index": row.get("batch_index"),
                "doi": doi,
                "decision": normalized_decisions.get(doi, str(row.get("suggested_decision") or "review")),
                "suggested_decision": row.get("suggested_decision"),
                "status": row.get("status"),
                **{metric_name: row.get(metric_name) for metric_name in COMPARISON_METRICS},
                "failed_stage": row.get("failed_stage"),
                "error_summary": row.get("error_summary"),
                "failed_seed": bool(row.get("failed_seed")),
                "missing_metrics": bool(row.get("missing_metrics")),
                "saturated_ndcg": bool(row.get("saturated_ndcg")),
                "saturated_precision": bool(row.get("saturated_precision")),
                "low_signal_seed": bool(row.get("low_signal_seed")),
                "comparison_metric": row.get("comparison_metric"),
                "improvement_delta": row.get("improvement_delta"),
                "tie_like_seed": bool(row.get("tie_like_seed")),
                "quality_flags": {
                    "failed_seed": bool(row.get("failed_seed")),
                    "missing_metrics": bool(row.get("missing_metrics")),
                    "saturated_ndcg": bool(row.get("saturated_ndcg")),
                    "saturated_precision": bool(row.get("saturated_precision")),
                    "low_signal_seed": bool(row.get("low_signal_seed")),
                    "tie_like_seed": bool(row.get("tie_like_seed")),
                },
                "reasons": list(row.get("suggestion_reasons") or ()),
                "reason_summary": row.get("reason_summary"),
            }
        )
    return export_rows


def build_benchmark_curation_export_request(
    *,
    curation_id: str,
    reviewer: str,
    notes: str,
    export_only_kept_to_csv: bool,
    include_review_seeds_csv: bool,
    include_markdown_summary: bool,
) -> BenchmarkCurationExportRequest:
    normalized_id = _normalize_directory_name(curation_id, label="Curation ID")
    return BenchmarkCurationExportRequest(
        curation_id=normalized_id,
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        export_only_kept_to_csv=bool(export_only_kept_to_csv),
        include_review_seeds_csv=bool(include_review_seeds_csv),
        include_markdown_summary=bool(include_markdown_summary),
    )


def build_curation_manifest_payload(
    *,
    request: BenchmarkCurationExportRequest,
    curation_dir: str | Path,
    created_at: str,
    context_metadata: dict[str, Any],
    summary: dict[str, Any],
    output_paths: dict[str, str | None],
) -> dict[str, Any]:
    return {
        "curation_id": request.curation_id,
        "curation_dir": _serialize_path(curation_dir),
        "created_at": created_at,
        "reviewer": request.reviewer,
        "notes": request.notes,
        "primary_batch": context_metadata.get("primary_batch"),
        "secondary_batch": context_metadata.get("secondary_batch"),
        "selected_comparison_metric": context_metadata.get("selected_comparison_metric"),
        "source_benchmark_preset": context_metadata.get("source_benchmark_preset"),
        "counts": dict(summary),
        "output_paths": dict(output_paths),
    }


def build_curation_report_markdown(
    *,
    curation_id: str,
    reviewer: str | None,
    notes: str | None,
    created_at: str,
    context_metadata: dict[str, Any],
    summary: dict[str, Any],
    seed_decision_rows: list[dict[str, Any]],
) -> str:
    primary_batch = context_metadata.get("primary_batch") or {}
    secondary_batch = context_metadata.get("secondary_batch") or {}
    benchmark_preset = context_metadata.get("source_benchmark_preset") or {}
    excluded_rows = [row for row in seed_decision_rows if row.get("decision") == "exclude"]
    review_rows = [row for row in seed_decision_rows if row.get("decision") == "review"]
    reason_counts: dict[str, int] = {}
    for row in excluded_rows + review_rows:
        for reason in row.get("reasons") or []:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1

    lines = [
        f"# Benchmark Curation: {curation_id}",
        "",
        f"- Created at: `{created_at}`",
        f"- Reviewer: `{reviewer or 'n/a'}`",
        f"- Notes: `{notes or 'n/a'}`",
        "",
        "## Context",
        f"- Primary Batch: `{primary_batch.get('batch_id') or 'n/a'}`",
        f"- Secondary Batch: `{secondary_batch.get('batch_id') or 'n/a'}`",
        f"- Selected Comparison Metric: `{context_metadata.get('selected_comparison_metric') or 'n/a'}`",
        f"- Source Benchmark Preset: `{benchmark_preset.get('benchmark_preset_id') or 'n/a'}`",
        "",
        "## Decision Summary",
        f"- Total Seeds: `{summary.get('total_seeds')}`",
        f"- Keep: `{summary.get('keep_count')}`",
        f"- Review: `{summary.get('review_count')}`",
        f"- Exclude: `{summary.get('exclude_count')}`",
        f"- Failed Seeds: `{summary.get('failed_seed_count')}`",
        f"- Usable Completed Seeds: `{summary.get('usable_completed_seed_count')}`",
        f"- Comparison Context Used: `{summary.get('comparison_context_used')}`",
        "",
        "## Why Seeds Were Flagged",
    ]
    if reason_counts:
        for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"- {reason}: `{count}`")
    else:
        lines.append("- No review/exclude reasons were recorded.")
    lines.extend(
        [
            "",
            "## Interpretation",
            f"- Kept seeds ready for future runs: `{summary.get('keep_count')}`",
            f"- Seeds still needing human review: `{summary.get('review_count')}`",
            f"- Seeds removed from the curated export: `{summary.get('exclude_count')}`",
        ]
    )
    return "\n".join(lines) + "\n"


def export_benchmark_curation(
    *,
    base_dir: str | Path,
    request: BenchmarkCurationExportRequest,
    context_metadata: dict[str, Any],
    seed_rows: list[dict[str, Any]],
    decisions: dict[str, str] | None,
) -> BenchmarkCurationExportResult:
    primary_batch = context_metadata.get("primary_batch")
    if not isinstance(primary_batch, dict) or _optional_str(primary_batch.get("batch_id")) is None:
        raise BenchmarkCurationError("A primary batch must be loaded before exporting a benchmark curation.")

    curation_root = Path(base_dir)
    curation_root.mkdir(parents=True, exist_ok=True)
    curation_dir = curation_root / request.curation_id
    if curation_dir.exists():
        raise BenchmarkCurationError(f"Benchmark curation directory already exists: {curation_dir}")

    created_at = _utc_timestamp()
    curation_dir.mkdir(parents=False, exist_ok=False)
    seed_decision_rows = build_seed_decision_rows(seed_rows, decisions)
    summary = summarize_curation_decisions(
        seed_rows,
        decisions,
        comparison_context_used=bool(context_metadata.get("secondary_batch")),
    )

    manifest_path = curation_dir / "curation_manifest.json"
    seed_decisions_path = curation_dir / "seed_decisions.jsonl"
    curated_csv_path = curation_dir / "curated_seeds.csv"
    review_csv_path = (
        curation_dir / "review_seeds.csv"
        if request.include_review_seeds_csv and any(row.get("decision") == "review" for row in seed_decision_rows)
        else None
    )
    report_path = curation_dir / "curation_report.md" if request.include_markdown_summary else None

    curated_rows = [
        row
        for row in seed_decision_rows
        if row.get("decision") == "keep"
        or (not request.export_only_kept_to_csv and row.get("decision") == "review")
    ]
    review_rows = [row for row in seed_decision_rows if row.get("decision") == "review"]
    output_paths = {
        "curation_manifest_json": _serialize_path(manifest_path),
        "seed_decisions_jsonl": _serialize_path(seed_decisions_path),
        "curated_seeds_csv": _serialize_path(curated_csv_path),
        "review_seeds_csv": _serialize_path(review_csv_path) if review_csv_path is not None else None,
        "curation_report_md": _serialize_path(report_path) if report_path is not None else None,
    }

    _write_json(
        manifest_path,
        build_curation_manifest_payload(
            request=request,
            curation_dir=curation_dir,
            created_at=created_at,
            context_metadata=context_metadata,
            summary=summary,
            output_paths=output_paths,
        ),
    )
    _write_jsonl(seed_decisions_path, seed_decision_rows)
    _write_seed_csv(curated_csv_path, curated_rows)
    if review_csv_path is not None:
        _write_seed_csv(review_csv_path, review_rows, include_reason_summary=True)
    if report_path is not None:
        report_path.write_text(
            build_curation_report_markdown(
                curation_id=request.curation_id,
                reviewer=request.reviewer,
                notes=request.notes,
                created_at=created_at,
                context_metadata=context_metadata,
                summary=summary,
                seed_decision_rows=seed_decision_rows,
            ),
            encoding="utf-8",
        )

    return BenchmarkCurationExportResult(
        curation_id=request.curation_id,
        curation_dir=curation_dir,
        manifest_path=manifest_path,
        seed_decisions_path=seed_decisions_path,
        curated_seeds_csv_path=curated_csv_path,
        review_seeds_csv_path=review_csv_path,
        report_path=report_path,
    )


def _write_seed_csv(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    include_reason_summary: bool = False,
) -> None:
    fieldnames = ["doi"]
    if include_reason_summary:
        fieldnames.append("reason_summary")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = {"doi": row.get("doi")}
            if include_reason_summary:
                payload["reason_summary"] = row.get("reason_summary")
            writer.writerow(payload)


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise BenchmarkCurationError(f"{label} is required.")
    if normalized in {".", ".."}:
        raise BenchmarkCurationError(f"{label} must not be '.' or '..'.")
    invalid_characters = set('/\\:*?"<>|')
    if any(character in invalid_characters for character in normalized):
        raise BenchmarkCurationError(f"{label} contains invalid path characters.")
    return normalized


def _numeric_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: str | Path | None) -> str | None:
    if value in (None, ""):
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        return str(path)
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
