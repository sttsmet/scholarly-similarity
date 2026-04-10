from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ui.report_bundle_export import (
    EXPECTED_REPORT_ARTIFACT_TYPES,
    ReportBundleExportError,
    build_accepted_baseline_report_context,
    build_batch_report_context,
    build_comparison_report_context,
    build_preset_report_context,
    build_report_export_request,
    build_report_manifest_payload,
    export_report_bundle,
)


def _empty_context() -> dict[str, object | None]:
    return {artifact_type: None for artifact_type in EXPECTED_REPORT_ARTIFACT_TYPES}


def _write_batch_dir(base_dir: Path, batch_id: str) -> SimpleNamespace:
    batch_dir = base_dir / batch_id
    batch_dir.mkdir(parents=True)
    (batch_dir / "batch_manifest.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "created_at": "2026-03-30T12:00:00Z",
                "completed_at": "2026-03-30T12:10:00Z",
                "status": "completed",
                "seeds_csv": "data/benchmarks/seeds.csv",
                "theory_config": "configs/theory_v001.yaml",
                "seed_count": 2,
                "completed_seed_count": 2,
                "failed_seed_count": 0,
                "options": {
                    "max_references": 10,
                    "max_related": 10,
                    "max_hard_negatives": 10,
                    "top_k": 10,
                    "label_source": "silver",
                    "refresh": False,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (batch_dir / "aggregate_summary.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "batch_dir": str(batch_dir),
                "aggregated_at": "2026-03-30T12:10:00Z",
                "seed_count": 2,
                "completed_seed_count": 2,
                "failed_seed_count": 0,
                "ranking_metric": "ndcg_at_k",
                "metric_aggregates": {
                    "ndcg_at_k": {
                        "count": 2,
                        "mean": 0.75,
                        "median": 0.75,
                        "std": 0.05,
                        "spread": 0.1,
                        "min": 0.7,
                        "max": 0.8,
                    }
                },
                "best_seeds": [],
                "worst_seeds": [],
                "failed_seeds": [],
                "output_paths": {},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (batch_dir / "seed_table.jsonl").write_text(
        json.dumps({"batch_index": 1, "doi": "10.1000/seed1", "status": "completed"}) + "\n",
        encoding="utf-8",
    )
    (batch_dir / "run_context.json").write_text(
        json.dumps(
            {
                "batch_id": batch_id,
                "created_at": "2026-03-30T12:00:00Z",
                "launch_source_type": "launch_profile",
                "benchmark_preset_id": "benchmark_smoke_001",
                "eval_preset_id": "eval_micro_001",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest = SimpleNamespace(
        batch_id=batch_id,
        theory_config="configs/theory_v001.yaml",
        seeds_csv="data/benchmarks/seeds.csv",
        status="completed",
        seed_count=2,
        completed_seed_count=2,
        failed_seed_count=0,
    )
    aggregate_summary = SimpleNamespace(
        ranking_metric="ndcg_at_k",
        metric_aggregates={
            "ndcg_at_k": {
                "count": 2,
                "mean": 0.75,
                "median": 0.75,
                "std": 0.05,
                "spread": 0.1,
                "min": 0.7,
                "max": 0.8,
            }
        },
    )
    return SimpleNamespace(
        batch_dir=batch_dir,
        manifest=manifest,
        aggregate_summary=aggregate_summary,
    )


def _write_comparison_dir(base_dir: Path, *, malformed_decision_record: bool = False) -> Path:
    comparison_dir = base_dir / "comparison_001"
    comparison_dir.mkdir(parents=True)
    (comparison_dir / "comparison_manifest.json").write_text(
        json.dumps(
            {
                "comparison_id": "comparison_001",
                "selected_comparison_metric": "ndcg_at_k",
                "common_doi_count": 2,
                "common_completed_seed_count": 2,
                "primary_batch": {"batch_id": "batch_009"},
                "secondary_batch": {"batch_id": "batch_010"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if malformed_decision_record:
        (comparison_dir / "decision_record.json").write_text("{not json", encoding="utf-8")
    else:
        (comparison_dir / "decision_record.json").write_text(
            json.dumps(
                {
                    "comparison_id": "comparison_001",
                    "decision_status": "needs_review",
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
    (comparison_dir / "paired_seed_table.jsonl").write_text(
        json.dumps({"doi": "10.1000/seed1", "raw_delta": 0.1}) + "\n",
        encoding="utf-8",
    )
    return comparison_dir


def _write_accepted_baseline_dir(base_dir: Path) -> Path:
    baseline_dir = base_dir / "accepted_baseline_001"
    baseline_dir.mkdir(parents=True)
    (baseline_dir / "accepted_theory_snapshot.yaml").write_text(
        "sim_weights:\n  direct: 0.5\n",
        encoding="utf-8",
    )
    (baseline_dir / "candidate_reply.yaml").write_text(
        "summary: accepted\nchanges:\n  - path: sim_weights.direct\n    value: 0.5\n",
        encoding="utf-8",
    )
    (baseline_dir / "applied_changes.jsonl").write_text(
        json.dumps({"path": "sim_weights.direct", "status": "applied"}) + "\n",
        encoding="utf-8",
    )
    (baseline_dir / "accepted_baseline_manifest.json").write_text(
        json.dumps(
            {
                "baseline_id": "accepted_baseline_001",
                "decision_status": "accept_candidate",
                "selected_metric": "ndcg_at_k",
                "reviewer": "Alice",
                "notes": "Promoted after outcome review.",
                "source_type": "cohort_study",
                "source_study_id": "study_001",
                "source_study_dir": "runs/cohort_studies/study_001",
                "source_reference_batch_id": "batch_005",
                "source_candidate_batch_id": "batch_010",
                "source_candidate_decision": "shortlist",
                "source_selected_metric": "ndcg_at_k",
                "source_lineage": {
                    "comparison_id": "comparison_001",
                    "packet_id": "packet_001",
                    "candidate_id": "candidate_001",
                    "outcome_id": "outcome_accept_001",
                },
                "source_primary_batch": {"batch_id": "batch_009"},
                "source_secondary_batch": {"batch_id": "batch_010"},
                "outcome_summary": {
                    "common_doi_count": 2,
                    "common_completed_seed_count": 2,
                    "wins": 2,
                    "losses": 0,
                    "ties": 0,
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (baseline_dir / "promotion_record.json").write_text(
        json.dumps({"reviewer": "Alice"}, indent=2) + "\n",
        encoding="utf-8",
    )
    return baseline_dir


def test_build_comparison_report_context_warns_on_malformed_optional_json(tmp_path: Path) -> None:
    comparison_dir = _write_comparison_dir(tmp_path, malformed_decision_record=True)

    context = build_comparison_report_context(comparison_dir)

    assert context.metadata["comparison_id"] == "comparison_001"
    assert context.metadata["common_doi_count"] == 2
    assert context.warnings
    assert "Malformed JSON in decision_record.json" in context.warnings[0]


def test_export_report_bundle_copies_available_artifacts_and_writes_summary(tmp_path: Path) -> None:
    bundle = _write_batch_dir(tmp_path / "runs" / "batches", "batch_009")
    preset_path = tmp_path / "configs" / "presets" / "launch_profiles" / "launch_smoke.json"
    preset_path.parent.mkdir(parents=True)
    preset_path.write_text(
        json.dumps(
            {
                "launch_profile_id": "launch_smoke",
                "created_at": "2026-03-30T12:00:00Z",
                "accepted_baseline_id": "accepted_baseline_001",
                "accepted_theory_snapshot": "runs/accepted_baselines/accepted_baseline_001/accepted_theory_snapshot.yaml",
                "benchmark_preset_id": "benchmark_smoke_001",
                "seeds_csv": "data/benchmarks/seeds.csv",
                "eval_preset_id": "eval_micro_001",
                "max_references": 10,
                "max_related": 10,
                "max_hard_negatives": 10,
                "top_k": 10,
                "label_source": "silver",
                "refresh": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    context = _empty_context()
    context["primary_batch"] = build_batch_report_context(role="primary", bundle=bundle)
    context["launch_profile"] = build_preset_report_context(
        artifact_type="launch_profile",
        preset_id="launch_smoke",
        preset_path=preset_path,
    )
    request = build_report_export_request(
        report_id="report_001",
        reviewer="Alice",
        notes="smoke export bundle",
        include_raw_copied_artifacts=True,
        include_markdown_summary=True,
    )

    result = export_report_bundle(
        base_dir=tmp_path / "runs" / "reports",
        request=request,
        context=context,  # type: ignore[arg-type]
    )

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    included = json.loads(result.included_artifacts_path.read_text(encoding="utf-8"))

    assert result.report_dir == tmp_path / "runs" / "reports" / "report_001"
    assert manifest["included_artifact_types"] == ["primary_batch", "launch_profile"]
    assert manifest["primary_batch"]["run_context"]["launch_source_type"] == "launch_profile"
    assert (result.report_dir / "report_summary.md").exists()
    assert "primary_batch" in included["copied"]
    assert "launch_profile" in included["copied"]
    assert any(item["destination"].endswith("primary_run_context.json") for item in included["copied"]["primary_batch"])
    assert "Research Report Bundle: report_001" in result.summary_path.read_text(encoding="utf-8")  # type: ignore[union-attr]


def test_export_report_bundle_records_missing_optional_copy_sources(tmp_path: Path) -> None:
    bundle = _write_batch_dir(tmp_path / "runs" / "batches", "batch_009")
    comparison_dir = _write_comparison_dir(tmp_path / "runs" / "comparisons")
    (comparison_dir / "paired_seed_table.jsonl").unlink()

    context = _empty_context()
    context["primary_batch"] = build_batch_report_context(role="primary", bundle=bundle)
    context["comparison"] = build_comparison_report_context(comparison_dir)
    request = build_report_export_request(
        report_id="report_002",
        reviewer="",
        notes="",
        include_raw_copied_artifacts=True,
        include_markdown_summary=False,
    )

    result = export_report_bundle(
        base_dir=tmp_path / "runs" / "reports",
        request=request,
        context=context,  # type: ignore[arg-type]
    )
    included = json.loads(result.included_artifacts_path.read_text(encoding="utf-8"))

    assert "comparison" in included["missing"]
    assert included["missing"]["comparison"][0]["source"].endswith("paired_seed_table.jsonl")


def test_export_report_bundle_refuses_overwrite(tmp_path: Path) -> None:
    bundle = _write_batch_dir(tmp_path / "runs" / "batches", "batch_009")
    context = _empty_context()
    context["primary_batch"] = build_batch_report_context(role="primary", bundle=bundle)
    request = build_report_export_request(
        report_id="report_003",
        reviewer="",
        notes="",
        include_raw_copied_artifacts=False,
        include_markdown_summary=False,
    )

    export_report_bundle(
        base_dir=tmp_path / "runs" / "reports",
        request=request,
        context=context,  # type: ignore[arg-type]
    )

    with pytest.raises(ReportBundleExportError):
        export_report_bundle(
            base_dir=tmp_path / "runs" / "reports",
            request=request,
            context=context,  # type: ignore[arg-type]
        )


def test_build_report_manifest_payload_and_baseline_context_support_partial_state(tmp_path: Path) -> None:
    baseline_dir = _write_accepted_baseline_dir(tmp_path / "runs" / "accepted_baselines")
    baseline_context = build_accepted_baseline_report_context(baseline_dir)
    request = build_report_export_request(
        report_id="report_partial",
        reviewer="Alice",
        notes="partial",
        include_raw_copied_artifacts=False,
        include_markdown_summary=False,
    )
    context = _empty_context()
    context["accepted_baseline"] = baseline_context

    manifest = build_report_manifest_payload(
        request=request,
        report_dir=tmp_path / "runs" / "reports" / "report_partial",
        created_at="2026-03-30T12:00:00Z",
        context=context,  # type: ignore[arg-type]
        output_paths={"report_manifest_json": "runs/reports/report_partial/report_manifest.json"},
        warnings=["accepted_baseline: warning"],
    )

    assert baseline_context.metadata["baseline_id"] == "accepted_baseline_001"
    assert baseline_context.metadata["comparison_id"] == "comparison_001"
    assert baseline_context.metadata["source_study_id"] == "study_001"
    assert manifest["accepted_baseline"]["selected_metric"] == "ndcg_at_k"
    assert manifest["accepted_baseline"]["source_candidate_decision"] == "shortlist"
    assert "primary_batch" in manifest["missing_artifact_types"]
