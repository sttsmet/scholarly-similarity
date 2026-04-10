from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

import src.eval.benchmark as benchmark
import src.ui.preset_registry as preset_registry
from src.config import DEFAULT_THEORY_PATH, RuntimeConfig, load_theory_config
from src.eval.benchmark import (
    BENCHMARK_SCHEMA_VERSION_V1,
    export_benchmark_preset_label_template,
    export_lineage_conflict_adjudication_template,
    export_stratified_lineage_annotation_batch_from_benchmark_preset,
    export_stratified_lineage_annotation_batch_from_runs,
    materialize_independent_benchmark_dataset,
    run_seed_batch,
)
from src.graph.build_local_corpus import build_local_corpus_run_id
from src.rank.ranker import rank_local_corpus
from src.ui.preset_registry import (
    build_benchmark_preset_detail,
    build_benchmark_run_batch_values,
    scan_benchmark_presets,
)


def _runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        app_name="scholarly-similarity",
        openalex_base_url="https://api.openalex.org",
        use_network=False,
        cache_dir="data/cache",
        runs_dir="runs",
        request_timeout_seconds=10.0,
    )


def _make_record(
    *,
    openalex_id: str,
    doi: str | None,
    title: str,
    publication_year: int | None = 2020,
    referenced_works: list[str] | None = None,
    related_works: list[str] | None = None,
    primary_topic: str | None = "Physics",
    topics: list[str] | None = None,
    abstract_text: str | None = "Quantum measurement experiment",
    candidate_origins: list[str] | None = None,
) -> dict[str, object]:
    return {
        "openalex_id": openalex_id,
        "doi": doi,
        "title": title,
        "publication_year": publication_year,
        "cited_by_count": 10,
        "referenced_works": referenced_works or [],
        "related_works": related_works or [],
        "primary_topic": primary_topic,
        "topics": topics or [],
        "abstract_text": abstract_text,
        "candidate_origins": candidate_origins or [],
        "source": "openalex",
    }


def _write_rank_ready_run(
    run_dir: Path,
    *,
    doi: str,
    seed_openalex_id: str = "https://openalex.org/WSEED",
    candidate_prefix: str = "",
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    candidate_one_id = f"https://openalex.org/W{candidate_prefix}CAND1" if candidate_prefix else "https://openalex.org/WCAND1"
    candidate_two_id = f"https://openalex.org/W{candidate_prefix}CAND2" if candidate_prefix else "https://openalex.org/WCAND2"
    candidate_three_id = f"https://openalex.org/W{candidate_prefix}CAND3" if candidate_prefix else "https://openalex.org/WCAND3"
    title_prefix = f"{candidate_prefix} " if candidate_prefix else ""
    seed = _make_record(
        openalex_id=seed_openalex_id,
        doi=doi,
        title="Measured measurement",
        publication_year=2018,
        referenced_works=[candidate_one_id],
        related_works=[candidate_two_id],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol for coupled systems",
    )
    candidate_one = _make_record(
        openalex_id=candidate_one_id,
        doi=None,
        title=f"{title_prefix}Reference match".strip(),
        publication_year=2019,
        referenced_works=["https://openalex.org/W1", "https://openalex.org/W2"],
        related_works=[],
        primary_topic="Physics",
        topics=["Physics", "Quantum"],
        abstract_text="Quantum measurement protocol with related systems",
        candidate_origins=["seed_reference", "direct_neighbor"],
    )
    candidate_two = _make_record(
        openalex_id=candidate_two_id,
        doi=None,
        title=f"{title_prefix}Related match".strip(),
        publication_year=2017,
        referenced_works=["https://openalex.org/W2"],
        related_works=[],
        primary_topic="Physics",
        topics=["Physics"],
        abstract_text="Measurement protocol in a related system",
        candidate_origins=["seed_related"],
    )
    candidate_three = _make_record(
        openalex_id=candidate_three_id,
        doi=None,
        title=f"{title_prefix}Background contrast".strip(),
        publication_year=2010,
        referenced_works=["https://openalex.org/W9"],
        related_works=[],
        primary_topic="Biology",
        topics=["Biology"],
        abstract_text="Cell growth observations in a lab",
        candidate_origins=["hard_negative"],
    )

    (run_dir / "seed_record.json").write_text(json.dumps(seed, indent=2), encoding="utf-8")
    with (run_dir / "papers.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        for record in (seed, candidate_one, candidate_two, candidate_three):
            handle.write(json.dumps(record))
            handle.write("\n")
    with (run_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(
            json.dumps(
                {
                    "src": seed_openalex_id,
                    "dst": candidate_one["openalex_id"],
                    "edge_type": "seed_references",
                }
            )
        )
        handle.write("\n")
        handle.write(
            json.dumps(
                {
                    "src": seed_openalex_id,
                    "dst": candidate_two["openalex_id"],
                    "edge_type": "seed_related",
                }
            )
        )
        handle.write("\n")

    rank_local_corpus(run_dir=run_dir, theory=load_theory_config(), top_k=3)


def _write_annotation_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_export_benchmark_preset_label_template_writes_blinded_rows(tmp_path: Path) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    run_id = build_local_corpus_run_id(
        doi="10.1038/nphys1170",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
    )
    _write_rank_ready_run(tmp_path / "runs" / run_id, doi="10.1038/nphys1170")

    preset_path = tmp_path / "configs" / "presets" / "benchmarks" / "benchmark_lineage.json"
    preset_path.parent.mkdir(parents=True, exist_ok=True)
    preset_path.write_text(
        json.dumps(
            {
                "benchmark_preset_id": "benchmark_lineage",
                "created_at": "2026-04-07T00:00:00Z",
                "seeds_csv": str(seeds_path),
                "description": "lineage preset",
                "tags": ["independent", "lineage"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    output_path = tmp_path / "exports" / "annotation_template.csv"
    result = export_benchmark_preset_label_template(
        benchmark_preset_path=preset_path,
        top_k=2,
        output_path=output_path,
        runs_root=tmp_path / "runs",
    )

    with output_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    assert result.seed_count == 1
    assert result.exported_count == 2
    assert output_path.exists()
    assert Path(result.manifest_path).exists()
    assert reader.fieldnames == [
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
    assert rows[0]["title"] == "Reference match"
    assert rows[0]["aspect"] == "lineage"
    assert "sim" not in reader.fieldnames
    assert "conf" not in reader.fieldnames
    assert "suggested_summary" not in reader.fieldnames
    assert "origin_flags" not in reader.fieldnames


def test_materialize_independent_benchmark_dataset_detects_conflicting_annotations(
    tmp_path: Path,
) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    annotation_a = tmp_path / "annotations" / "reviewer_a.csv"
    annotation_b = tmp_path / "annotations" / "reviewer_b.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.90",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            }
        ],
    )
    _write_annotation_csv(
        annotation_b,
        [
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "0",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "not lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            }
        ],
    )

    with pytest.raises(ValueError, match="Conflicting label values"):
        materialize_independent_benchmark_dataset(
            annotation_paths=[annotation_a, annotation_b],
            benchmark_dataset_id="benchmark_dataset_conflict",
            seeds_csv_path=seeds_path,
            output_root=tmp_path / "data" / "benchmarks" / "datasets",
        )


def test_materialize_independent_benchmark_dataset_writes_manifest_and_registry_compatible_preset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(preset_registry, "REPO_ROOT", tmp_path)

    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    annotation_a = tmp_path / "annotations" / "reviewer_a.csv"
    annotation_b = tmp_path / "annotations" / "reviewer_b.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.95",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND2",
                "title": "Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "weaker lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )
    _write_annotation_csv(
        annotation_b,
        [
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.85",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "citation lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND2",
                "title": "Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.60",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "related lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )

    result = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a, annotation_b],
        benchmark_dataset_id="benchmark_dataset_lineage_small",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
        benchmark_preset_id="benchmark_preset_lineage_small",
        benchmark_preset_base_dir=tmp_path / "configs" / "presets" / "benchmarks",
        benchmark_preset_description="tiny lineage benchmark",
        benchmark_preset_tags=["independent", "lineage"],
    )

    manifest_payload = json.loads(
        Path(result.benchmark_dataset_manifest_path).read_text(encoding="utf-8")
    )
    entries, warnings = scan_benchmark_presets(
        tmp_path / "configs" / "presets" / "benchmarks"
    )

    assert warnings == []
    assert Path(result.benchmark_labels_path).exists()
    assert Path(result.benchmark_dataset_manifest_path).exists()
    assert Path(result.benchmark_preset_path).exists()
    assert manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_lineage_small"
    assert manifest_payload["benchmark_schema_version"] == BENCHMARK_SCHEMA_VERSION_V1
    assert manifest_payload["annotation_schema_version"] == "benchmark_annotation_rows.v1"
    assert manifest_payload["aspect_scope"] == "lineage"
    assert manifest_payload["benchmark_labels_row_count"] == 2
    assert manifest_payload["seed_count"] == 1
    assert manifest_payload["per_seed_counts"] == {"https://openalex.org/WSEED": 2}
    assert manifest_payload["label_distribution"] == {"1": 1, "2": 1}
    assert manifest_payload["annotator_ids"] == ["alice", "bob"]
    assert manifest_payload["annotator_count"] == 2
    assert manifest_payload["multi_annotated_pair_count"] == 2
    assert manifest_payload["overlap_pair_count"] == 2
    assert manifest_payload["agreement_summary"]["metric_name"] == "raw_pair_agreement"
    assert manifest_payload["agreement_summary"]["agreement_rate"] == pytest.approx(1.0)
    assert manifest_payload["conflict_pair_count"] == 0
    assert manifest_payload["adjudicated_conflict_count"] == 0
    assert manifest_payload["adjudication_complete"] is True
    assert manifest_payload["benchmark_maturity_tier"] == "insufficient"
    assert manifest_payload["promotion_ready"] is False

    detail = build_benchmark_preset_detail(entries[0])
    values, value_warnings = build_benchmark_run_batch_values(entries[0])
    assert value_warnings == []
    assert detail["benchmark_dataset_manifest_path"].endswith(
        "benchmark_dataset_manifest.json"
    )
    assert detail["benchmark_schema_version"] == BENCHMARK_SCHEMA_VERSION_V1
    assert detail["benchmark_labels_row_count"] == 2
    assert detail["aspect_scope"] == "lineage"
    assert values["benchmark_dataset_id"] == "benchmark_dataset_lineage_small"
    assert values["benchmark_labels_sha256"] == result.benchmark_labels_sha256


def test_materialize_independent_benchmark_dataset_tracks_conflict_resolution_semantics(
    tmp_path: Path,
) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    annotation_a = tmp_path / "annotations" / "reviewer_a.csv"
    annotation_b = tmp_path / "annotations" / "reviewer_b.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.95",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "1",
                "adjudication_notes": "resolved to moderate lineage",
            }
        ],
    )
    _write_annotation_csv(
        annotation_b,
        [
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "0",
                "label_confidence": "0.45",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "not lineage",
                "adjudicated_label": "1",
                "adjudication_notes": "resolved to moderate lineage",
            },
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND2",
                "title": "Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "related lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )

    result = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a, annotation_b],
        benchmark_dataset_id="benchmark_dataset_resolved_conflict",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
    )

    manifest_payload = json.loads(
        Path(result.benchmark_dataset_manifest_path).read_text(encoding="utf-8")
    )

    assert manifest_payload["conflict_pair_count"] == 1
    assert manifest_payload["adjudicated_conflict_count"] == 1
    assert manifest_payload["adjudication_complete"] is True
    assert manifest_payload["agreement_summary"]["overlap_pair_count"] == 1
    assert manifest_payload["agreement_summary"]["agreement_rate"] == pytest.approx(0.0)


def test_materialize_real_lineage_tiny_dataset_assigns_prototype_tier(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    result = materialize_independent_benchmark_dataset(
        annotation_paths=[
            repo_root
            / "data"
            / "benchmarks"
            / "annotations"
            / "benchmark_dataset_lineage_tiny_v1"
            / "reviewer_alice.csv",
            repo_root
            / "data"
            / "benchmarks"
            / "annotations"
            / "benchmark_dataset_lineage_tiny_v1"
            / "reviewer_bob.csv",
        ],
        benchmark_dataset_id="benchmark_dataset_lineage_tiny_v1",
        seeds_csv_path=repo_root / "data" / "benchmarks" / "seeds_independent_lineage_tiny.csv",
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
    )

    manifest_payload = json.loads(
        Path(result.benchmark_dataset_manifest_path).read_text(encoding="utf-8")
    )

    assert manifest_payload["benchmark_dataset_id"] == "benchmark_dataset_lineage_tiny_v1"
    assert manifest_payload["seed_count"] == 1
    assert manifest_payload["benchmark_labels_row_count"] == 3
    assert manifest_payload["label_distribution"] == {"0": 1, "1": 1, "2": 1}
    assert manifest_payload["agreement_summary"]["agreement_rate"] == pytest.approx(1.0)
    assert manifest_payload["adjudication_complete"] is True
    assert manifest_payload["benchmark_maturity_tier"] == "prototype"
    assert manifest_payload["promotion_ready"] is False


def test_materialized_dataset_runs_end_to_end_in_independent_benchmark_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(benchmark, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(preset_registry, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(benchmark, "load_runtime_config", _runtime_config)

    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    annotation_a = tmp_path / "annotations" / "reviewer_a.csv"
    annotation_b = tmp_path / "annotations" / "reviewer_b.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.95",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND2",
                "title": "Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "weaker lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND3",
                "title": "Background contrast",
                "publication_year": "2010",
                "label": "0",
                "label_confidence": "0.55",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "not lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )
    _write_annotation_csv(
        annotation_b,
        [
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.85",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "citation lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND2",
                "title": "Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.60",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "related lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND3",
                "title": "Background contrast",
                "publication_year": "2010",
                "label": "0",
                "label_confidence": "0.50",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "background only",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )

    materialized = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a, annotation_b],
        benchmark_dataset_id="benchmark_dataset_lineage_e2e",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
        benchmark_preset_id="benchmark_preset_lineage_e2e",
        benchmark_preset_base_dir=tmp_path / "configs" / "presets" / "benchmarks",
        benchmark_preset_description="tiny lineage benchmark e2e",
        benchmark_preset_tags=["independent", "lineage"],
    )

    existing_run_id = build_local_corpus_run_id(
        doi="10.1038/nphys1170",
        max_references=10,
        max_related=10,
        max_hard_negatives=10,
    )
    existing_run_dir = tmp_path / "runs" / existing_run_id
    _write_rank_ready_run(existing_run_dir, doi="10.1038/nphys1170")

    manifest = run_seed_batch(
        seeds_path=seeds_path,
        theory_config_path=DEFAULT_THEORY_PATH,
        theory=load_theory_config(),
        batch_id="batch_independent_materialized",
        label_source="benchmark",
        evaluation_mode="independent_benchmark",
        benchmark_labels_path=Path(materialized.benchmark_labels_path),
        benchmark_dataset_id=materialized.benchmark_dataset_id,
        benchmark_labels_sha256=materialized.benchmark_labels_sha256,
    )

    batch_dir = Path(manifest.batch_dir)
    manifest_payload = json.loads((batch_dir / "batch_manifest.json").read_text(encoding="utf-8"))
    options_payload = manifest_payload["options"]

    assert manifest.completed_seed_count == 1
    assert options_payload["evaluation_mode"] == "independent_benchmark"
    assert options_payload["benchmark_dataset_id"] == materialized.benchmark_dataset_id
    assert options_payload["benchmark_labels_sha256"] == materialized.benchmark_labels_sha256
    assert options_payload["benchmark_schema_version"] == BENCHMARK_SCHEMA_VERSION_V1
    assert options_payload["benchmark_labels_row_count"] == 3
    assert options_payload["benchmark_maturity_tier"] == "prototype"
    assert options_payload["promotion_ready"] is False
    assert options_payload["promotion_ineligibility_reasons"]
    assert Path(options_payload["benchmark_labels_snapshot_path"]).exists()


def test_export_stratified_lineage_annotation_batch_from_benchmark_preset_is_deterministic(
    tmp_path: Path,
) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text(
        "doi,tag\n10.1000/seed-a,lineage\n10.1000/seed-b,lineage\n",
        encoding="utf-8",
    )

    for doi, seed_openalex_id, candidate_prefix in (
        ("10.1000/seed-a", "https://openalex.org/WSEEDA", "A"),
        ("10.1000/seed-b", "https://openalex.org/WSEEDB", "B"),
    ):
        run_id = build_local_corpus_run_id(
            doi=doi,
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
        )
        _write_rank_ready_run(
            tmp_path / "runs" / run_id,
            doi=doi,
            seed_openalex_id=seed_openalex_id,
            candidate_prefix=candidate_prefix,
        )

    preset_path = tmp_path / "configs" / "presets" / "benchmarks" / "benchmark_lineage_expand.json"
    preset_path.parent.mkdir(parents=True, exist_ok=True)
    preset_path.write_text(
        json.dumps(
            {
                "benchmark_preset_id": "benchmark_lineage_expand",
                "created_at": "2026-04-07T00:00:00Z",
                "seeds_csv": str(seeds_path),
                "description": "lineage expansion preset",
                "tags": ["independent", "lineage"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    export_one = export_stratified_lineage_annotation_batch_from_benchmark_preset(
        batch_id="batch_lineage_expand_001",
        benchmark_preset_path=preset_path,
        benchmark_dataset_id="benchmark_dataset_target",
        annotator_ids=["alice", "bob"],
        target_pair_count=4,
        per_seed_quota=2,
        stratum_quota_overrides={
            "strong_lineage": 2,
            "indirect_lineage": 1,
            "ambiguous_middle": 0,
            "provenance_weak": 0,
            "hard_negative_or_distractor": 1,
        },
        planned_overlap_ratio=0.5,
        output_root=tmp_path / "data" / "benchmarks" / "annotation_batches",
        runs_root=tmp_path / "runs",
    )
    export_two = export_stratified_lineage_annotation_batch_from_benchmark_preset(
        batch_id="batch_lineage_expand_002",
        benchmark_preset_path=preset_path,
        benchmark_dataset_id="benchmark_dataset_target",
        annotator_ids=["alice", "bob"],
        target_pair_count=4,
        per_seed_quota=2,
        stratum_quota_overrides={
            "strong_lineage": 2,
            "indirect_lineage": 1,
            "ambiguous_middle": 0,
            "provenance_weak": 0,
            "hard_negative_or_distractor": 1,
        },
        planned_overlap_ratio=0.5,
        output_root=tmp_path / "data" / "benchmarks" / "annotation_batches",
        runs_root=tmp_path / "runs",
    )

    manifest_payload = json.loads(Path(export_one.manifest_path).read_text(encoding="utf-8"))
    with Path(export_one.output_paths.annotator_csv_paths["alice"]).open(
        "r", encoding="utf-8", newline=""
    ) as handle:
        rows_one = list(csv.DictReader(handle))
    with Path(export_two.output_paths.annotator_csv_paths["alice"]).open(
        "r", encoding="utf-8", newline=""
    ) as handle:
        rows_two = list(csv.DictReader(handle))

    assert rows_one == rows_two
    assert manifest_payload["source_benchmark_preset_id"] == "benchmark_lineage_expand"
    assert manifest_payload["exported_pair_count"] == 4
    assert manifest_payload["overlap_pair_count"] == 2
    assert manifest_payload["overlap_ratio"] == pytest.approx(0.5)
    assert manifest_payload["planned_overlap_ratio"] == pytest.approx(0.5)
    assert manifest_payload["strata_counts"] == {
        "strong_lineage": 2,
        "indirect_lineage": 1,
        "ambiguous_middle": 0,
        "provenance_weak": 0,
        "hard_negative_or_distractor": 1,
    }
    assert manifest_payload["seed_pair_counts"] == {
        "https://openalex.org/WSEEDA": 2,
        "https://openalex.org/WSEEDB": 2,
    }


def test_run_seed_batch_supports_multi_seed_benchmark_labels_csv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(benchmark, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(benchmark, "load_runtime_config", _runtime_config)

    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text(
        "doi,tag\n10.1000/seed-a,lineage\n10.1000/seed-b,lineage\n",
        encoding="utf-8",
    )

    for doi, seed_openalex_id, candidate_prefix in (
        ("10.1000/seed-a", "https://openalex.org/WSEEDA", "A"),
        ("10.1000/seed-b", "https://openalex.org/WSEEDB", "B"),
    ):
        run_id = build_local_corpus_run_id(
            doi=doi,
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
        )
        _write_rank_ready_run(
            tmp_path / "runs" / run_id,
            doi=doi,
            seed_openalex_id=seed_openalex_id,
            candidate_prefix=candidate_prefix,
        )

    annotation_a = tmp_path / "annotations" / "annotator_a.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "WSEEDA",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WACAND1",
                "title": "A Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.90",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEEDA",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WACAND2",
                "title": "A Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "related lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEEDA",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WACAND3",
                "title": "A Background contrast",
                "publication_year": "2010",
                "label": "0",
                "label_confidence": "0.60",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "background only",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEEDB",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WBCAND1",
                "title": "B Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.90",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEEDB",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WBCAND2",
                "title": "B Related match",
                "publication_year": "2017",
                "label": "1",
                "label_confidence": "0.70",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "related lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
            {
                "seed_openalex_id": "WSEEDB",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WBCAND3",
                "title": "B Background contrast",
                "publication_year": "2010",
                "label": "0",
                "label_confidence": "0.60",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "background only",
                "adjudicated_label": "",
                "adjudication_notes": "",
            },
        ],
    )

    materialized = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a],
        benchmark_dataset_id="benchmark_dataset_lineage_multiseed_e2e",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
    )

    manifest = run_seed_batch(
        seeds_path=seeds_path,
        theory_config_path=DEFAULT_THEORY_PATH,
        theory=load_theory_config(),
        batch_id="batch_independent_multiseed",
        label_source="benchmark",
        evaluation_mode="independent_benchmark",
        benchmark_labels_path=Path(materialized.benchmark_labels_path),
        benchmark_dataset_id=materialized.benchmark_dataset_id,
        benchmark_labels_sha256=materialized.benchmark_labels_sha256,
    )

    assert manifest.completed_seed_count == 2
    assert manifest.failed_seed_count == 0


def test_export_stratified_lineage_annotation_batch_from_runs_enforces_seed_quotas(
    tmp_path: Path,
) -> None:
    run_dirs: list[Path] = []
    for doi, seed_openalex_id, candidate_prefix in (
        ("10.1000/seed-c", "https://openalex.org/WSEEDC", "C"),
        ("10.1000/seed-d", "https://openalex.org/WSEEDD", "D"),
    ):
        run_id = build_local_corpus_run_id(
            doi=doi,
            max_references=10,
            max_related=10,
            max_hard_negatives=10,
        )
        run_dir = tmp_path / "runs" / run_id
        _write_rank_ready_run(
            run_dir,
            doi=doi,
            seed_openalex_id=seed_openalex_id,
            candidate_prefix=candidate_prefix,
        )
        run_dirs.append(run_dir)

    result = export_stratified_lineage_annotation_batch_from_runs(
        batch_id="batch_lineage_quota",
        run_dirs=run_dirs,
        benchmark_dataset_id="benchmark_dataset_target",
        annotator_ids=["alice", "bob"],
        target_pair_count=6,
        per_seed_quota=2,
        stratum_quota_overrides={
            "strong_lineage": 2,
            "indirect_lineage": 2,
            "ambiguous_middle": 0,
            "provenance_weak": 0,
            "hard_negative_or_distractor": 2,
        },
        planned_overlap_ratio=0.5,
        output_root=tmp_path / "data" / "benchmarks" / "annotation_batches",
    )

    manifest_payload = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))

    assert result.exported_pair_count == 4
    assert manifest_payload["seed_pair_counts"] == {
        "https://openalex.org/WSEEDC": 2,
        "https://openalex.org/WSEEDD": 2,
    }
    assert max(manifest_payload["seed_pair_counts"].values()) == 2


def test_export_lineage_conflict_adjudication_template_round_trips_into_materialization(
    tmp_path: Path,
) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text("doi,tag\n10.1038/nphys1170,lineage\n", encoding="utf-8")

    annotation_a = tmp_path / "annotations" / "reviewer_a.csv"
    annotation_b = tmp_path / "annotations" / "reviewer_b.csv"
    _write_annotation_csv(
        annotation_a,
        [
            {
                "seed_openalex_id": "https://openalex.org/WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "https://openalex.org/WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "2",
                "label_confidence": "0.90",
                "aspect": "lineage",
                "annotator_id": "alice",
                "notes": "strong lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            }
        ],
    )
    _write_annotation_csv(
        annotation_b,
        [
            {
                "seed_openalex_id": "WSEED",
                "seed_title": "Measured measurement",
                "candidate_openalex_id": "WCAND1",
                "title": "Reference match",
                "publication_year": "2019",
                "label": "0",
                "label_confidence": "0.40",
                "aspect": "lineage",
                "annotator_id": "bob",
                "notes": "not lineage",
                "adjudicated_label": "",
                "adjudication_notes": "",
            }
        ],
    )

    adjudication = export_lineage_conflict_adjudication_template(
        annotation_paths=[annotation_a, annotation_b],
        adjudication_id="adj_lineage_conflicts",
        adjudicator_id="carol",
        output_root=tmp_path / "data" / "benchmarks" / "adjudication_batches",
        benchmark_dataset_id="benchmark_dataset_target",
    )

    with Path(adjudication.output_path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        adjudication_rows = list(reader)

    assert reader.fieldnames == [
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
        "prior_labels",
    ]
    assert adjudication.conflict_pair_count == 1
    assert adjudication_rows[0]["annotator_id"] == "carol"
    assert "alice:2" in adjudication_rows[0]["prior_labels"]
    assert "bob:0" in adjudication_rows[0]["prior_labels"]
    assert "sim" not in reader.fieldnames
    assert "conf" not in reader.fieldnames
    assert "suggested_summary" not in reader.fieldnames
    assert "origin_flags" not in reader.fieldnames

    adjudication_rows[0]["adjudicated_label"] = "1"
    adjudication_rows[0]["adjudication_notes"] = "resolved to moderate lineage"
    with Path(adjudication.output_path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(adjudication_rows)

    result = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a, annotation_b, adjudication.output_path],
        benchmark_dataset_id="benchmark_dataset_resolved_adjudication",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
    )

    manifest_payload = json.loads(
        Path(result.benchmark_dataset_manifest_path).read_text(encoding="utf-8")
    )
    assert manifest_payload["conflict_pair_count"] == 1
    assert manifest_payload["adjudicated_conflict_count"] == 1
    assert manifest_payload["adjudication_complete"] is True
    assert manifest_payload["label_distribution"] == {"1": 1}


def test_materialize_independent_benchmark_dataset_assigns_pilot_tier_for_balanced_fixture(
    tmp_path: Path,
) -> None:
    seeds_path = tmp_path / "data" / "benchmarks" / "seeds.csv"
    seeds_path.parent.mkdir(parents=True, exist_ok=True)
    seeds_path.write_text(
        "doi,tag\n"
        "10.1000/pilot-1,lineage\n"
        "10.1000/pilot-2,lineage\n"
        "10.1000/pilot-3,lineage\n"
        "10.1000/pilot-4,lineage\n"
        "10.1000/pilot-5,lineage\n",
        encoding="utf-8",
    )

    annotation_a_rows: list[dict[str, str]] = []
    annotation_b_rows: list[dict[str, str]] = []
    for seed_index in range(1, 6):
        seed_openalex_id = f"https://openalex.org/WSEED{seed_index}"
        for candidate_index in range(1, 11):
            label = str((seed_index + candidate_index) % 3)
            row = {
                "seed_openalex_id": seed_openalex_id,
                "seed_title": f"Seed {seed_index}",
                "candidate_openalex_id": f"https://openalex.org/WPILOT{seed_index}_{candidate_index}",
                "title": f"Candidate {seed_index}-{candidate_index}",
                "publication_year": str(2010 + candidate_index),
                "label": label,
                "label_confidence": "0.80",
                "aspect": "lineage",
                "annotator_id": "",
                "notes": "pilot benchmark row",
                "adjudicated_label": "",
                "adjudication_notes": "",
            }
            annotation_a_rows.append({**row, "annotator_id": "alice"})
            annotation_b_rows.append({**row, "annotator_id": "bob"})

    annotation_a = tmp_path / "annotations" / "pilot_alice.csv"
    annotation_b = tmp_path / "annotations" / "pilot_bob.csv"
    _write_annotation_csv(annotation_a, annotation_a_rows)
    _write_annotation_csv(annotation_b, annotation_b_rows)

    result = materialize_independent_benchmark_dataset(
        annotation_paths=[annotation_a, annotation_b],
        benchmark_dataset_id="benchmark_dataset_lineage_pilot_fixture",
        seeds_csv_path=seeds_path,
        output_root=tmp_path / "data" / "benchmarks" / "datasets",
    )

    manifest_payload = json.loads(
        Path(result.benchmark_dataset_manifest_path).read_text(encoding="utf-8")
    )

    assert manifest_payload["seed_count"] == 5
    assert manifest_payload["benchmark_labels_row_count"] == 50
    assert manifest_payload["annotator_count"] == 2
    assert manifest_payload["overlap_pair_count"] == 50
    assert manifest_payload["agreement_summary"]["agreement_rate"] == pytest.approx(1.0)
    assert manifest_payload["benchmark_maturity_tier"] == "pilot"
    assert manifest_payload["promotion_ready"] is False
