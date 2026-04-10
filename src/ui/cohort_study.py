from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.ui.cohort_analysis import CohortPairwiseAnalysisRow
from src.ui.experiment_matrix import ExperimentBatchRow


COHORT_STUDY_DECISION_OPTIONS = (
    "shortlist",
    "review",
    "drop",
)
COHORT_STUDY_DECISION_FILTER_OPTIONS = (
    "all",
    *COHORT_STUDY_DECISION_OPTIONS,
)
MIN_COMMON_COMPLETED_FOR_SHORTLIST = 3
HEAVY_TIE_RATE_THRESHOLD = 0.8


class CohortStudyError(ValueError):
    """Raised when a cohort study cannot be exported."""


@dataclass(frozen=True, slots=True)
class CohortStudyExportRequest:
    study_id: str
    reviewer: str | None
    notes: str | None
    include_markdown_summary: bool
    include_shortlist_csv: bool


@dataclass(frozen=True, slots=True)
class CohortStudyExportResult:
    study_id: str
    study_dir: Path
    manifest_path: Path
    leaderboard_path: Path
    decisions_path: Path
    shortlist_csv_path: Path | None
    report_path: Path | None


def build_cohort_study_context_key(
    *,
    cohort_key: str | None,
    reference_batch_id: str | None,
    selected_metric: str | None,
    candidate_batch_ids: list[str],
) -> str:
    return json.dumps(
        {
            "cohort_key": _optional_str(cohort_key),
            "reference_batch_id": _optional_str(reference_batch_id),
            "selected_metric": _optional_str(selected_metric),
            "candidate_batch_ids": sorted(_optional_str(batch_id) or "" for batch_id in candidate_batch_ids),
        },
        sort_keys=True,
    )


def suggest_cohort_study_decision(
    row: CohortPairwiseAnalysisRow,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    usable = row.pairwise_status == "usable"
    improvement_delta_mean = row.improvement_delta_mean

    if not usable or row.common_completed_seed_count <= 0:
        reasons.append("Pairwise comparison is unusable for this candidate.")
        return "drop", tuple(reasons)
    if row.guardrail_verdict == "fail":
        reasons.append("Evidence guardrails classify this candidate as fail.")
        return "drop", tuple(reasons)
    if (
        improvement_delta_mean is not None
        and improvement_delta_mean < 0
        and row.losses > row.wins
    ):
        reasons.append("Losses exceed wins with negative mean improvement delta.")
        return "drop", tuple(reasons)

    if (
        row.common_completed_seed_count >= MIN_COMMON_COMPLETED_FOR_SHORTLIST
        and improvement_delta_mean is not None
        and improvement_delta_mean > 0
        and row.wins > row.losses
        and row.guardrail_verdict != "fail"
    ):
        reasons.append(
            f"At least {MIN_COMMON_COMPLETED_FOR_SHORTLIST} common completed seeds are available."
        )
        reasons.append("Mean improvement delta is positive.")
        reasons.append("Wins exceed losses.")
        return "shortlist", tuple(reasons)

    if row.guardrail_verdict == "weak":
        reasons.append("Evidence guardrails classify this candidate as weak.")
    if row.common_completed_seed_count < MIN_COMMON_COMPLETED_FOR_SHORTLIST:
        reasons.append(
            f"Fewer than {MIN_COMMON_COMPLETED_FOR_SHORTLIST} common completed seeds are available."
        )
    if improvement_delta_mean is None or abs(improvement_delta_mean) <= 1e-9:
        reasons.append("Mean improvement delta is neutral.")
    if row.wins == row.losses:
        reasons.append("Wins and losses are tied.")
    if row.tie_rate is not None and row.tie_rate >= HEAVY_TIE_RATE_THRESHOLD:
        reasons.append("Ties dominate the pairwise comparison.")
    if not reasons:
        reasons.append("Evidence is mixed and warrants manual review.")
    return "review", tuple(dict.fromkeys(reasons))


def normalize_cohort_study_decisions(
    rows: list[CohortPairwiseAnalysisRow],
    current_decisions: dict[str, str] | None,
) -> dict[str, str]:
    existing = dict(current_decisions or {})
    normalized: dict[str, str] = {}
    for row in rows:
        decision = existing.get(row.candidate_batch_id)
        if decision not in COHORT_STUDY_DECISION_OPTIONS:
            decision, _ = suggest_cohort_study_decision(row)
        normalized[row.candidate_batch_id] = decision
    return normalized


def filter_cohort_study_rows(
    rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str] | None,
    *,
    decision_filter: str,
    only_usable: bool,
    only_unusable: bool,
    search_text: str,
) -> list[CohortPairwiseAnalysisRow]:
    normalized_decisions = normalize_cohort_study_decisions(rows, decisions)
    lowered_search = search_text.strip().lower()
    filtered: list[CohortPairwiseAnalysisRow] = []
    for row in rows:
        decision = normalized_decisions.get(row.candidate_batch_id)
        if decision_filter != "all" and decision != decision_filter:
            continue
        if only_usable and row.pairwise_status != "usable":
            continue
        if only_unusable and row.pairwise_status == "usable":
            continue
        if lowered_search and lowered_search not in _search_blob(row):
            continue
        filtered.append(row)
    return filtered


def summarize_cohort_study_decisions(
    rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str] | None,
    *,
    reference_batch_id: str,
    selected_metric: str,
) -> dict[str, Any]:
    normalized_decisions = normalize_cohort_study_decisions(rows, decisions)
    shortlist_count = 0
    review_count = 0
    drop_count = 0
    usable_count = 0
    unusable_count = 0
    for row in rows:
        if row.pairwise_status == "usable":
            usable_count += 1
        else:
            unusable_count += 1
        decision = normalized_decisions.get(row.candidate_batch_id)
        if decision == "shortlist":
            shortlist_count += 1
        elif decision == "review":
            review_count += 1
        else:
            drop_count += 1
    return {
        "reference_batch_id": reference_batch_id,
        "selected_metric": selected_metric,
        "total_candidate_rows": len(rows),
        "usable_candidate_rows": usable_count,
        "unusable_candidate_rows": unusable_count,
        "shortlist_count": shortlist_count,
        "review_count": review_count,
        "drop_count": drop_count,
    }


def build_cohort_study_table_rows(
    rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str] | None,
) -> list[dict[str, Any]]:
    normalized_decisions = normalize_cohort_study_decisions(rows, decisions)
    table_rows: list[dict[str, Any]] = []
    for row in rows:
        suggested_decision, _ = suggest_cohort_study_decision(row)
        table_rows.append(
            {
                "candidate_batch_id": row.candidate_batch_id,
                "pairwise_status": row.pairwise_status,
                "decision": normalized_decisions.get(row.candidate_batch_id),
                "suggested_decision": suggested_decision,
                "common_completed_seed_count": row.common_completed_seed_count,
                "improvement_delta_mean": row.improvement_delta_mean,
                "wins": row.wins,
                "losses": row.losses,
                "ties": row.ties,
                "guardrail_verdict": row.guardrail_verdict,
                "accepted_baseline_id": row.accepted_baseline_id,
                "launch_profile_id": row.launch_profile_id,
            }
        )
    return table_rows


def build_cohort_leaderboard_export_rows(
    rows: list[CohortPairwiseAnalysisRow],
) -> list[dict[str, Any]]:
    return [
        {
            "reference_batch_id": row.reference_batch_id,
            "candidate_batch_id": row.candidate_batch_id,
            "candidate_batch_dir": _serialize_path(row.candidate_batch_dir),
            "candidate_status": row.candidate_status,
            "common_doi_count": row.common_doi_count,
            "common_completed_seed_count": row.common_completed_seed_count,
            "selected_metric": row.selected_metric,
            "reference_mean": row.reference_mean,
            "reference_median": row.reference_median,
            "candidate_mean": row.candidate_mean,
            "candidate_median": row.candidate_median,
            "improvement_delta_mean": row.improvement_delta_mean,
            "improvement_delta_median": row.improvement_delta_median,
            "wins": row.wins,
            "losses": row.losses,
            "ties": row.ties,
            "tie_rate": row.tie_rate,
            "usable": row.pairwise_status == "usable",
            "pairwise_status": row.pairwise_status,
            "guardrail_verdict": row.guardrail_verdict,
            "guardrail_reasons": list(row.guardrail_reasons),
            "accepted_baseline_id": row.accepted_baseline_id,
            "benchmark_preset_id": row.benchmark_preset_id,
            "eval_preset_id": row.eval_preset_id,
            "launch_profile_id": row.launch_profile_id,
            "launch_source_type": row.candidate_launch_source_type,
            "source_curation_id": row.source_curation_id,
            "candidate_theory_config": row.candidate_theory_config,
        }
        for row in rows
    ]


def build_candidate_decision_rows(
    rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str] | None,
) -> list[dict[str, Any]]:
    normalized_decisions = normalize_cohort_study_decisions(rows, decisions)
    export_rows: list[dict[str, Any]] = []
    for row in rows:
        suggested_decision, suggestion_reasons = suggest_cohort_study_decision(row)
        export_rows.append(
            {
                "candidate_batch_id": row.candidate_batch_id,
                "decision": normalized_decisions.get(row.candidate_batch_id, suggested_decision),
                "suggested_decision": suggested_decision,
                "usable": row.pairwise_status == "usable",
                "pairwise_status": row.pairwise_status,
                "reasons": list(suggestion_reasons),
                "selected_metric": row.selected_metric,
                "improvement_delta_mean": row.improvement_delta_mean,
                "wins": row.wins,
                "losses": row.losses,
                "ties": row.ties,
                "guardrail_verdict": row.guardrail_verdict,
            }
        )
    return export_rows


def build_cohort_study_export_request(
    *,
    study_id: str,
    reviewer: str,
    notes: str,
    include_markdown_summary: bool,
    include_shortlist_csv: bool,
) -> CohortStudyExportRequest:
    normalized_id = _normalize_directory_name(study_id, label="Study ID")
    return CohortStudyExportRequest(
        study_id=normalized_id,
        reviewer=_optional_str(reviewer),
        notes=_optional_str(notes),
        include_markdown_summary=bool(include_markdown_summary),
        include_shortlist_csv=bool(include_shortlist_csv),
    )


def export_cohort_study(
    *,
    base_dir: str | Path,
    request: CohortStudyExportRequest,
    cohort_key: str | None,
    cohort_summary: str,
    reference_row: ExperimentBatchRow,
    selected_metric: str,
    pairwise_rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str] | None,
) -> CohortStudyExportResult:
    if not pairwise_rows:
        raise CohortStudyError("Active cohort analysis context does not contain any candidate rows.")

    study_root = Path(base_dir)
    study_dir = study_root / request.study_id
    if study_dir.exists():
        raise CohortStudyError(f"Cohort study directory already exists: {study_dir}")
    study_dir.mkdir(parents=True, exist_ok=False)

    normalized_decisions = normalize_cohort_study_decisions(pairwise_rows, decisions)
    summary = summarize_cohort_study_decisions(
        pairwise_rows,
        normalized_decisions,
        reference_batch_id=reference_row.batch_id,
        selected_metric=selected_metric,
    )
    leaderboard_rows = build_cohort_leaderboard_export_rows(pairwise_rows)
    decision_rows = build_candidate_decision_rows(pairwise_rows, normalized_decisions)

    manifest_path = study_dir / "cohort_study_manifest.json"
    leaderboard_path = study_dir / "cohort_leaderboard.jsonl"
    decisions_path = study_dir / "candidate_decisions.jsonl"
    shortlist_csv_path = study_dir / "shortlisted_batches.csv" if request.include_shortlist_csv else None
    report_path = study_dir / "cohort_study_report.md" if request.include_markdown_summary else None

    _write_jsonl(leaderboard_path, leaderboard_rows)
    _write_jsonl(decisions_path, decision_rows)
    if shortlist_csv_path is not None:
        _write_shortlist_csv(shortlist_csv_path, pairwise_rows, normalized_decisions)
    if report_path is not None:
        report_path.write_text(
            build_cohort_study_report_markdown(
                request=request,
                created_at=_utc_now_iso(),
                cohort_summary=cohort_summary,
                reference_row=reference_row,
                selected_metric=selected_metric,
                summary=summary,
                pairwise_rows=pairwise_rows,
                decisions=normalized_decisions,
            ),
            encoding="utf-8",
        )

    output_paths = {
        "cohort_leaderboard_jsonl": _serialize_path(leaderboard_path),
        "candidate_decisions_jsonl": _serialize_path(decisions_path),
        "shortlisted_batches_csv": _serialize_path(shortlist_csv_path) if shortlist_csv_path is not None else None,
        "cohort_study_report_md": _serialize_path(report_path) if report_path is not None else None,
    }
    manifest_payload = {
        "study_id": request.study_id,
        "study_dir": _serialize_path(study_dir),
        "created_at": _utc_now_iso(),
        "reviewer": request.reviewer,
        "notes": request.notes,
        "cohort_key": _optional_str(cohort_key),
        "cohort_summary": cohort_summary,
        "reference_batch": {
            "batch_id": reference_row.batch_id,
            "batch_dir": _serialize_path(reference_row.batch_dir),
            "status": reference_row.status,
            "theory_config": reference_row.theory_config,
        },
        "selected_metric": selected_metric,
        "total_candidate_rows": summary["total_candidate_rows"],
        "usable_candidate_rows": summary["usable_candidate_rows"],
        "unusable_candidate_rows": summary["unusable_candidate_rows"],
        "shortlist_count": summary["shortlist_count"],
        "review_count": summary["review_count"],
        "drop_count": summary["drop_count"],
        "output_paths": output_paths,
    }
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return CohortStudyExportResult(
        study_id=request.study_id,
        study_dir=study_dir,
        manifest_path=manifest_path,
        leaderboard_path=leaderboard_path,
        decisions_path=decisions_path,
        shortlist_csv_path=shortlist_csv_path,
        report_path=report_path,
    )


def build_cohort_study_report_markdown(
    *,
    request: CohortStudyExportRequest,
    created_at: str,
    cohort_summary: str,
    reference_row: ExperimentBatchRow,
    selected_metric: str,
    summary: dict[str, Any],
    pairwise_rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str],
) -> str:
    shortlist_rows = [
        row for row in pairwise_rows if decisions.get(row.candidate_batch_id) == "shortlist"
    ]
    review_rows = [
        row for row in pairwise_rows if decisions.get(row.candidate_batch_id) == "review"
    ]
    drop_rows = [
        row for row in pairwise_rows if decisions.get(row.candidate_batch_id) == "drop"
    ]

    def _row_line(row: CohortPairwiseAnalysisRow) -> str:
        return (
            f"- `{row.candidate_batch_id}`: improvement_mean={row.improvement_delta_mean}, "
            f"wins/losses/ties={row.wins}/{row.losses}/{row.ties}, verdict={row.guardrail_verdict or 'n/a'}"
        )

    lines = [
        f"# Cohort Study {request.study_id}",
        "",
        f"- Created at: {created_at}",
        f"- Reviewer: {request.reviewer or 'n/a'}",
        f"- Notes: {request.notes or 'n/a'}",
        f"- Cohort: `{cohort_summary}`",
        f"- Reference batch: `{reference_row.batch_id}`",
        f"- Selected metric: `{selected_metric}`",
        f"- Candidate rows: {summary['total_candidate_rows']}",
        f"- Usable / Unusable: {summary['usable_candidate_rows']} / {summary['unusable_candidate_rows']}",
        f"- Decisions shortlist / review / drop: {summary['shortlist_count']} / {summary['review_count']} / {summary['drop_count']}",
        "",
        "## Shortlisted Candidates",
    ]
    lines.extend([_row_line(row) for row in shortlist_rows[:10]] or ["- None"])
    lines.extend(
        [
            "",
            "## Review Candidates",
        ]
    )
    lines.extend([_row_line(row) for row in review_rows[:10]] or ["- None"])
    lines.extend(
        [
            "",
            "## Dropped / Unusable Candidates",
        ]
    )
    lines.extend([_row_line(row) for row in drop_rows[:10]] or ["- None"])
    lines.extend(
        [
            "",
            "## Interpretation",
            (
                "- This cohort study looks informative enough to prioritize a shortlist."
                if summary["shortlist_count"] > 0 and summary["usable_candidate_rows"] > 0
                else "- This cohort study looks weak or mostly negative; inspect review/drop rows carefully."
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def _write_shortlist_csv(
    path: Path,
    rows: list[CohortPairwiseAnalysisRow],
    decisions: dict[str, str],
) -> None:
    fieldnames = [
        "batch_id",
        "improvement_delta_mean",
        "wins",
        "losses",
        "ties",
        "accepted_baseline_id",
        "launch_profile_id",
        "theory_config",
    ]
    shortlisted_rows = [
        row for row in rows if decisions.get(row.candidate_batch_id) == "shortlist"
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in shortlisted_rows:
            writer.writerow(
                {
                    "batch_id": row.candidate_batch_id,
                    "improvement_delta_mean": row.improvement_delta_mean,
                    "wins": row.wins,
                    "losses": row.losses,
                    "ties": row.ties,
                    "accepted_baseline_id": row.accepted_baseline_id,
                    "launch_profile_id": row.launch_profile_id,
                    "theory_config": row.candidate_theory_config,
                }
            )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _search_blob(row: CohortPairwiseAnalysisRow) -> str:
    return " ".join(
        value.lower()
        for value in (
            row.candidate_batch_id,
            row.accepted_baseline_id,
            row.launch_profile_id,
            row.benchmark_preset_id,
            row.eval_preset_id,
            row.candidate_theory_config,
        )
        if isinstance(value, str) and value.strip()
    )


def _normalize_directory_name(value: str, *, label: str) -> str:
    normalized = _optional_str(value)
    if normalized is None:
        raise CohortStudyError(f"{label} is required.")
    for forbidden in ("/", "\\"):
        if forbidden in normalized:
            raise CohortStudyError(f"{label} must be a simple directory name, not a path.")
    return normalized


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _serialize_path(value: str | Path | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
