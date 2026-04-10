from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import src.ui.run_context as run_context
from src.ui.run_context import (
    RunContextError,
    build_run_context_payload_from_request,
    build_run_context_summary,
    load_run_context_if_present,
    write_run_context,
)


def _request(tmp_path: Path, *, batch_id: str = "batch_010") -> SimpleNamespace:
    return SimpleNamespace(
        batch_id=batch_id,
        initial_doi_context="10.1038/nphys1170",
        theory_config_path=tmp_path / "configs" / "theory.yaml",
        seeds_csv_path=tmp_path / "data" / "benchmarks" / "seeds.csv",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
        top_k=10,
        label_source="silver",
        evaluation_mode="silver_provenance_regression",
        benchmark_labels_path=None,
        benchmark_dataset_id=None,
        benchmark_labels_sha256=None,
        metric_scope="local_corpus_ranking",
        refresh=False,
    )


def test_build_run_context_payload_from_request_supports_form_provenance(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    request = _request(tmp_path)

    payload = build_run_context_payload_from_request(
        request,
        launch_source_type="run_batch_form",
        accepted_baseline={
            "accepted_baseline_id": "baseline_001",
            "accepted_baseline_dir": tmp_path / "runs" / "accepted_baselines" / "baseline_001",
            "accepted_theory_snapshot": tmp_path / "runs" / "accepted_baselines" / "baseline_001" / "accepted_theory_snapshot.yaml",
        },
        benchmark_preset={
            "benchmark_preset_id": "benchmark_curated_001",
            "benchmark_preset_path": tmp_path / "configs" / "presets" / "benchmarks" / "benchmark_curated_001.json",
            "source_type": "benchmark_curation",
            "source_curation_id": "curation_001",
            "source_curation_dir": tmp_path / "runs" / "benchmark_curations" / "curation_001",
        },
        evaluation_preset={
            "eval_preset_id": "eval_micro_001",
            "eval_preset_path": tmp_path / "configs" / "presets" / "evals" / "eval_micro_001.json",
        },
    )

    assert payload["batch_id"] == "batch_010"
    assert payload["launch_source_type"] == "run_batch_form"
    assert payload["accepted_baseline_id"] == "baseline_001"
    assert payload["benchmark_preset_id"] == "benchmark_curated_001"
    assert payload["source_type"] == "benchmark_curation"
    assert payload["source_curation_id"] == "curation_001"
    assert payload["eval_preset_id"] == "eval_micro_001"
    assert Path(payload["theory_config_path"]) == Path("configs") / "theory.yaml"
    assert Path(payload["seeds_csv"]) == Path("data") / "benchmarks" / "seeds.csv"


def test_build_run_context_payload_from_request_supports_launch_profile_and_candidate_lineage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    request = _request(tmp_path, batch_id="batch_011")

    payload = build_run_context_payload_from_request(
        request,
        launch_source_type="candidate_apply_run",
        launch_profile={
            "launch_profile_id": "launch_smoke_001",
            "launch_profile_path": tmp_path / "configs" / "presets" / "launch_profiles" / "launch_smoke_001.json",
        },
        candidate_lineage={
            "candidate_id": "candidate_001",
            "packet_id": "packet_001",
            "comparison_id": "comparison_001",
            "candidate_run_dir": tmp_path / "runs" / "comparisons" / "comparison_001" / "review_packets" / "packet_001" / "candidate_runs" / "candidate_001",
        },
        reviewer="Alice",
        notes="Candidate parity rerun.",
        batch_status="failed",
        error_message="seed batch failed",
    )

    assert payload["launch_profile_id"] == "launch_smoke_001"
    assert payload["candidate_id"] == "candidate_001"
    assert payload["packet_id"] == "packet_001"
    assert payload["comparison_id"] == "comparison_001"
    assert payload["reviewer"] == "Alice"
    assert payload["notes"] == "Candidate parity rerun."
    assert payload["batch_status"] == "failed"
    assert payload["error_message"] == "seed batch failed"


def test_build_run_context_payload_from_request_supports_saved_study_source(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    request = _request(tmp_path, batch_id="batch_011")

    payload = build_run_context_payload_from_request(
        request,
        launch_source_type="candidate_apply_run",
        candidate_lineage={
            "candidate_id": "candidate_001",
            "packet_id": "packet_001",
            "comparison_id": "comparison_001",
        },
        study_source={
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_study_dir": tmp_path / "runs" / "cohort_studies" / "study_001",
            "source_reference_batch_id": "batch_005",
            "source_candidate_batch_id": "batch_010",
            "source_candidate_decision": "shortlist",
            "source_selected_metric": "ndcg_at_k",
        },
    )

    assert payload["source_type"] == "cohort_study"
    assert payload["source_study_id"] == "study_001"
    assert payload["source_candidate_decision"] == "shortlist"
    assert payload["source_selected_metric"] == "ndcg_at_k"


def test_write_and_load_run_context_round_trip(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    batch_dir = tmp_path / "runs" / "batches" / "batch_010"
    batch_dir.mkdir(parents=True)
    payload = build_run_context_payload_from_request(
        _request(tmp_path),
        launch_source_type="launch_profile",
    )

    written_path = write_run_context(batch_dir, payload)
    loaded_payload, warning = load_run_context_if_present(batch_dir)

    assert written_path == batch_dir / "run_context.json"
    assert warning is None
    assert loaded_payload == payload


def test_build_run_context_payload_persists_evaluation_mode_and_benchmark_metadata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    labels_path = tmp_path / "data" / "benchmarks" / "benchmark_labels.csv"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    labels_path.write_text(
        "seed_openalex_id,candidate_openalex_id,label\n"
        "https://openalex.org/WSEED,https://openalex.org/WCAND1,2\n",
        encoding="utf-8",
    )
    request = _request(tmp_path, batch_id="batch_012")
    request_payload = dict(request.__dict__)
    request_payload.update(
        {
            "evaluation_mode": "independent_benchmark",
            "label_source": "benchmark",
            "benchmark_labels_path": labels_path,
            "benchmark_dataset_id": "benchmark_dataset_001",
            "benchmark_labels_sha256": "abc123",
        }
    )
    request = SimpleNamespace(**request_payload)

    payload = build_run_context_payload_from_request(
        request,
        launch_source_type="launch_profile",
    )

    assert payload["evaluation_mode"] == "independent_benchmark"
    assert payload["metric_scope"] == "local_corpus_ranking"
    assert Path(payload["benchmark_labels_path"]) == Path("data") / "benchmarks" / "benchmark_labels.csv"
    assert payload["benchmark_dataset_id"] == "benchmark_dataset_001"
    assert payload["benchmark_labels_sha256"] == "abc123"


def test_load_run_context_if_present_handles_malformed_json(tmp_path: Path) -> None:
    batch_dir = tmp_path / "runs" / "batches" / "batch_010"
    batch_dir.mkdir(parents=True)
    (batch_dir / "run_context.json").write_text("{not json", encoding="utf-8")

    loaded_payload, warning = load_run_context_if_present(batch_dir)

    assert loaded_payload is None
    assert warning is not None
    assert "Malformed run_context.json" in warning


def test_build_run_context_summary_is_compact(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    payload = build_run_context_payload_from_request(
        _request(tmp_path),
        launch_source_type="run_batch_form",
        benchmark_preset={
            "benchmark_preset_id": "benchmark_curated_001",
            "source_curation_id": "curation_001",
            "source_curation_dir": tmp_path / "runs" / "benchmark_curations" / "curation_001",
        },
    )

    summary = build_run_context_summary(payload)

    assert summary == {
        "launch_source_type": "run_batch_form",
        "benchmark_preset_id": "benchmark_curated_001",
        "source_curation_id": "curation_001",
        "source_curation_dir": str(Path("runs") / "benchmark_curations" / "curation_001"),
    }


def test_build_run_context_summary_includes_study_source_fields(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)
    payload = build_run_context_payload_from_request(
        _request(tmp_path),
        launch_source_type="candidate_apply_run",
        study_source={
            "source_type": "cohort_study",
            "source_study_id": "study_001",
            "source_study_dir": tmp_path / "runs" / "cohort_studies" / "study_001",
            "source_reference_batch_id": "batch_005",
            "source_candidate_batch_id": "batch_010",
            "source_candidate_decision": "shortlist",
            "source_selected_metric": "ndcg_at_k",
        },
    )

    summary = build_run_context_summary(payload)

    assert summary["source_type"] == "cohort_study"
    assert summary["source_study_id"] == "study_001"
    assert summary["source_study_dir"] == str(Path("runs") / "cohort_studies" / "study_001")
    assert summary["source_candidate_decision"] == "shortlist"


def test_write_run_context_requires_existing_batch_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(run_context, "REPO_ROOT", tmp_path)

    with pytest.raises(RunContextError):
        write_run_context(
            tmp_path / "runs" / "batches" / "missing_batch",
            {"batch_id": "missing_batch"},
        )
