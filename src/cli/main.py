from __future__ import annotations

import csv
import json
import sys
from enum import Enum
from pathlib import Path

import typer

from src.agents.packet_builder import (
    GeneratorPacketBuildResult,
    GeneratorReplyApplicationResult,
    VerifierPacketBuildResult,
    VerifierReplyRecordResult,
    apply_generator_reply,
    build_generator_packet,
    build_verifier_packet,
    record_verifier_reply,
)
from src.config import REPO_ROOT, load_all_configs, load_evaluation_config, load_runtime_config, load_theory_config
from src.env_config import load_neo4j_env_config
from src.eval.benchmark import (
    BatchAggregateEvalResult,
    DEFAULT_BATCH_MAX_HARD_NEGATIVES,
    DEFAULT_BATCH_MAX_REFERENCES,
    DEFAULT_BATCH_MAX_RELATED,
    DEFAULT_BATCH_TOP_K,
    LabelTemplateExportResult,
    LocalRankingEvaluationResult,
    SeedBatchManifest,
    SilverLabelGenerationResult,
    aggregate_batch_eval,
    evaluate_local_ranking,
    export_label_template,
    generate_silver_labels,
    run_theory_eval,
    run_seed_batch,
    TheoryEvalExperimentResult,
)
from src.ingest.cache_store import CacheStore, CacheStoreError
from src.ingest.doi_resolver import DOIResolver
from src.ingest.openalex_client import OpenAlexError, OpenAlexClient
from src.graph.build_local_corpus import LocalCorpusResult, build_local_corpus
from src.rank.ranker import RankingSummary, rank_local_corpus


app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Local scholarly similarity scaffold CLI.",
)


class ConfigName(str, Enum):
    all = "all"
    theory = "theory"
    runtime = "runtime"
    evaluation = "evaluation"


class LabelSource(str, Enum):
    manual = "manual"
    silver = "silver"


@app.command()
def health() -> None:
    """Load configs and report basic scaffold health."""

    config_bundle = load_all_configs()
    neo4j_env_error: str | None = None
    try:
        neo4j_env = load_neo4j_env_config()
    except ValueError as exc:
        neo4j_env = None
        neo4j_env_error = str(exc)

    report = {
        "status": "ok",
        "python_executable": sys.executable,
        "configs_loaded": True,
        "config_versions": {
            "theory": config_bundle.theory.version,
            "runtime_app": config_bundle.runtime.app_name,
            "evaluation_metrics": config_bundle.evaluation.metrics,
        },
        "required_paths": {
            "cache_dir": (REPO_ROOT / config_bundle.runtime.cache_dir).exists(),
            "runs_dir": (REPO_ROOT / config_bundle.runtime.runs_dir).exists(),
            "benchmark_path": (REPO_ROOT / config_bundle.evaluation.benchmark_path).exists(),
            "generator_template": (REPO_ROOT / "src" / "agents" / "templates" / "generator_packet.md").exists(),
            "verifier_template": (REPO_ROOT / "src" / "agents" / "templates" / "verifier_packet.md").exists(),
        },
        "neo4j_env": {
            "configured": neo4j_env is not None,
            "uri": neo4j_env.uri if neo4j_env else None,
            "username": neo4j_env.username if neo4j_env else None,
            "password_set": bool(neo4j_env.password) if neo4j_env else False,
            "error": neo4j_env_error,
        },
    }
    typer.echo(json.dumps(report, indent=2))


@app.command("show-config")
def show_config(
    config_name: ConfigName = typer.Option(ConfigName.all, "--config-name", help="Config bundle to print."),
) -> None:
    """Print one config or all configs as JSON."""

    if config_name == ConfigName.theory:
        payload = load_theory_config().model_dump(mode="json")
    elif config_name == ConfigName.runtime:
        payload = load_runtime_config().model_dump(mode="json")
    elif config_name == ConfigName.evaluation:
        payload = load_evaluation_config().model_dump(mode="json")
    else:
        payload = load_all_configs().model_dump(mode="json")
    typer.echo(json.dumps(payload, indent=2))


@app.command("print-tree-info")
def print_tree_info() -> None:
    """Print a small summary of key directories and file counts."""

    summary = {
        "repo_root": str(REPO_ROOT),
        "directories": {
            "configs": _directory_snapshot(REPO_ROOT / "configs"),
            "src": _directory_snapshot(REPO_ROOT / "src"),
            "tests": _directory_snapshot(REPO_ROOT / "tests"),
            "data": _directory_snapshot(REPO_ROOT / "data"),
            "runs": _directory_snapshot(REPO_ROOT / "runs"),
        },
    }
    typer.echo(json.dumps(summary, indent=2))


@app.command("fetch-doi")
def fetch_doi(
    doi: str = typer.Argument(..., help="DOI to normalize and resolve."),
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cache and refetch from OpenAlex."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Ingest-only DOI fetch command backed by OpenAlex and disk cache.

    This command is allowed to use network access on cache misses. That is
    intentionally separate from the offline deterministic runtime path.
    """

    runtime = load_runtime_config()
    cache_store = CacheStore(REPO_ROOT / runtime.cache_dir)
    client = OpenAlexClient(
        base_url=runtime.openalex_base_url,
        timeout_seconds=runtime.request_timeout_seconds,
    )
    resolver = DOIResolver(client=client, cache_store=cache_store)
    try:
        result = resolver.resolve(doi, refresh=refresh)
    except (ValueError, CacheStoreError, OpenAlexError) as exc:
        _emit_command_error(
            command_name="fetch-doi",
            input_value=doi,
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_fetch_summary(result))


@app.command("build-local-corpus")
def build_local_corpus_command(
    doi: str = typer.Argument(..., help="Seed DOI used to build the one-hop local corpus."),
    max_references: int = typer.Option(10, "--max-references", min=0, help="Maximum referenced works to include."),
    max_related: int = typer.Option(10, "--max-related", min=0, help="Maximum related works to include."),
    max_hard_negatives: int = typer.Option(0, "--max-hard-negatives", min=0, help="Maximum hard negatives to sample from the local second-hop pool."),
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cache and refetch seed and neighbor works."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Build a small one-hop local corpus from a seed DOI."""

    runtime = load_runtime_config()
    cache_store = CacheStore(REPO_ROOT / runtime.cache_dir)
    openalex_client = OpenAlexClient(
        base_url=runtime.openalex_base_url,
        timeout_seconds=runtime.request_timeout_seconds,
    )
    doi_resolver = DOIResolver(client=openalex_client, cache_store=cache_store)

    try:
        result = build_local_corpus(
            doi=doi,
            max_references=max_references,
            max_related=max_related,
            max_hard_negatives=max_hard_negatives,
            refresh=refresh,
            runs_root=REPO_ROOT / runtime.runs_dir,
            doi_resolver=doi_resolver,
            openalex_client=openalex_client,
            cache_store=cache_store,
        )
    except (ValueError, CacheStoreError, OpenAlexError) as exc:
        _emit_command_error(
            command_name="build-local-corpus",
            input_value=doi,
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_corpus_summary(result))


@app.command("rank-local-corpus")
def rank_local_corpus_command(
    run_dir: Path = typer.Argument(..., help="Run directory containing seed_record.json, papers.jsonl, and edges.jsonl."),
    top_k: int = typer.Option(5, "--top-k", min=1, help="Number of top-ranked results to show in CLI output."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Rank a local one-hop corpus using only existing run artifacts."""

    theory = load_theory_config()

    try:
        result = rank_local_corpus(run_dir=run_dir, theory=theory, top_k=top_k)
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        _emit_command_error(
            command_name="rank-local-corpus",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_ranking_summary(result))


@app.command("export-label-template")
def export_label_template_command(
    run_dir: Path = typer.Argument(..., help="Run directory containing ranking artifacts."),
    top_k: int | None = typer.Option(None, "--top-k", min=1, help="Export only the top-k candidates."),
    output: Path | None = typer.Option(None, "--output", help="Optional output CSV path."),
) -> None:
    """Export a CSV template for manual labels from ranked local candidates."""

    try:
        result = export_label_template(run_dir=run_dir, top_k=top_k, output_path=output)
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="export-label-template",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_label_export_summary(result))


@app.command("generate-silver-labels")
def generate_silver_labels_command(
    run_dir: Path = typer.Argument(..., help="Run directory containing seed_record.json, papers.jsonl, and edges.jsonl."),
) -> None:
    """Generate deterministic silver labels from corpus provenance only."""

    try:
        result = generate_silver_labels(run_dir=run_dir)
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="generate-silver-labels",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_silver_label_summary(result))


@app.command("evaluate-local-ranking")
def evaluate_local_ranking_command(
    run_dir: Path = typer.Argument(..., help="Run directory containing ranking artifacts."),
    labels: Path | None = typer.Option(None, "--labels", help="Optional CSV label file for manual evaluation."),
    label_source: LabelSource = typer.Option(LabelSource.manual, "--label-source", help="Choose manual labels or generated silver labels."),
    top_k: int | None = typer.Option(None, "--top-k", min=1, help="Evaluate only the top-k ranked candidates."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Evaluate a locally ranked corpus against manual or silver labels."""

    try:
        result = evaluate_local_ranking(
            run_dir=run_dir,
            labels_path=labels,
            top_k=top_k,
            label_source=label_source.value,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="evaluate-local-ranking",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_evaluation_summary(result))


@app.command("run-theory-eval")
def run_theory_eval_command(
    run_dir: Path = typer.Argument(..., help="Base run directory containing corpus artifacts."),
    theory_config: Path = typer.Option(..., "--theory-config", help="Theory YAML used for this experiment."),
    experiment_id: str = typer.Option(..., "--experiment-id", help="Experiment folder id written under run_dir/experiments/."),
    label_source: LabelSource = typer.Option(LabelSource.silver, "--label-source", help="Currently supported experiment label source."),
    top_k: int = typer.Option(5, "--top-k", min=1, help="Number of top-ranked results to show in summaries."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Run isolated ranking and evaluation outputs for one theory revision."""

    try:
        theory = load_theory_config(theory_config)
        result = run_theory_eval(
            run_dir=run_dir,
            theory_config_path=theory_config,
            theory=theory,
            experiment_id=experiment_id,
            label_source=label_source.value,
            top_k=top_k,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="run-theory-eval",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_theory_eval_summary(result))


@app.command("run-seed-batch")
def run_seed_batch_command(
    seeds: Path = typer.Option(..., "--seeds", help="CSV file containing DOI seeds."),
    theory_config: Path = typer.Option(..., "--theory-config", help="Theory YAML used for each seed experiment."),
    batch_id: str = typer.Option(..., "--batch-id", help="Batch directory id written under runs/batches/."),
    max_references: int = typer.Option(DEFAULT_BATCH_MAX_REFERENCES, "--max-references", min=0, help="Maximum referenced works to include per seed."),
    max_related: int = typer.Option(DEFAULT_BATCH_MAX_RELATED, "--max-related", min=0, help="Maximum related works to include per seed."),
    max_hard_negatives: int = typer.Option(DEFAULT_BATCH_MAX_HARD_NEGATIVES, "--max-hard-negatives", min=0, help="Maximum hard negatives to sample per seed."),
    top_k: int = typer.Option(DEFAULT_BATCH_TOP_K, "--top-k", min=1, help="Top-k used for each seed-level theory evaluation."),
    label_source: LabelSource = typer.Option(LabelSource.silver, "--label-source", help="Currently supported batch label source."),
    refresh: bool = typer.Option(False, "--refresh", help="Force a corpus rebuild even when a matching seed run already exists."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Run deterministic corpus build, silver labeling, and theory eval across many seeds."""

    try:
        theory = load_theory_config(theory_config)
        result = run_seed_batch(
            seeds_path=seeds,
            theory_config_path=theory_config,
            theory=theory,
            batch_id=batch_id,
            max_references=max_references,
            max_related=max_related,
            max_hard_negatives=max_hard_negatives,
            top_k=top_k,
            label_source=label_source.value,
            refresh=refresh,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="run-seed-batch",
            input_value=str(seeds),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_seed_batch_summary(result))


@app.command("aggregate-batch-eval")
def aggregate_batch_eval_command(
    batch_dir: Path = typer.Argument(..., help="Batch directory containing batch manifest and seed run metadata."),
    json_output: bool = typer.Option(False, "--json", help="Emit structured JSON output."),
) -> None:
    """Aggregate seed-level evaluation summaries from an existing batch directory."""

    try:
        result = aggregate_batch_eval(batch_dir=batch_dir)
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError, csv.Error) as exc:
        _emit_command_error(
            command_name="aggregate-batch-eval",
            input_value=str(batch_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=json_output,
        )
        raise typer.Exit(code=1)

    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), indent=2))
        return

    typer.echo(_format_batch_aggregate_summary(result))


@app.command("build-generator-packet")
def build_generator_packet_command(
    run_dir: Path = typer.Argument(..., help="Base run directory containing experiment outputs."),
    baseline_experiment: str = typer.Option(..., "--baseline-experiment", help="Baseline experiment id under run_dir/experiments/."),
    packet_id: str = typer.Option(..., "--packet-id", help="Packet folder id written under run_dir/agent_loops/."),
) -> None:
    """Build a generator packet from an existing baseline experiment."""

    try:
        result = build_generator_packet(
            run_dir=run_dir,
            baseline_experiment_id=baseline_experiment,
            packet_id=packet_id,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        _emit_command_error(
            command_name="build-generator-packet",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_generator_packet_summary(result))


@app.command("apply-generator-reply")
def apply_generator_reply_command(
    run_dir: Path = typer.Argument(..., help="Base run directory containing experiment outputs."),
    baseline_experiment: str = typer.Option(..., "--baseline-experiment", help="Baseline experiment id used for the candidate theory copy."),
    packet_id: str = typer.Option(..., "--packet-id", help="Packet folder id under run_dir/agent_loops/."),
    reply: Path = typer.Option(..., "--reply", help="Structured YAML reply file from the generator."),
    candidate_id: str = typer.Option(..., "--candidate-id", help="Candidate folder id written under the packet directory."),
) -> None:
    """Validate a generator reply and materialize a candidate theory file."""

    try:
        result = apply_generator_reply(
            run_dir=run_dir,
            baseline_experiment_id=baseline_experiment,
            packet_id=packet_id,
            reply_path=reply,
            candidate_id=candidate_id,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        _emit_command_error(
            command_name="apply-generator-reply",
            input_value=str(reply),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_generator_reply_summary(result))


@app.command("build-verifier-packet")
def build_verifier_packet_command(
    run_dir: Path = typer.Argument(..., help="Base run directory containing experiment outputs."),
    baseline_experiment: str = typer.Option(..., "--baseline-experiment", help="Baseline experiment id under run_dir/experiments/."),
    candidate_experiment: str = typer.Option(..., "--candidate-experiment", help="Candidate experiment id under run_dir/experiments/."),
    packet_id: str = typer.Option(..., "--packet-id", help="Packet folder id written under run_dir/agent_loops/."),
) -> None:
    """Build a verifier packet comparing baseline and candidate experiments."""

    try:
        result = build_verifier_packet(
            run_dir=run_dir,
            baseline_experiment_id=baseline_experiment,
            candidate_experiment_id=candidate_experiment,
            packet_id=packet_id,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        _emit_command_error(
            command_name="build-verifier-packet",
            input_value=str(run_dir),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_verifier_packet_summary(result))


@app.command("record-verifier-reply")
def record_verifier_reply_command(
    run_dir: Path = typer.Argument(..., help="Base run directory containing packet outputs."),
    packet_id: str = typer.Option(..., "--packet-id", help="Packet folder id under run_dir/agent_loops/."),
    reply: Path = typer.Option(..., "--reply", help="Structured YAML reply file from the verifier."),
) -> None:
    """Validate and record a verifier reply inside the packet directory."""

    try:
        result = record_verifier_reply(
            run_dir=run_dir,
            packet_id=packet_id,
            reply_path=reply,
        )
    except (ValueError, FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        _emit_command_error(
            command_name="record-verifier-reply",
            input_value=str(reply),
            message=str(exc),
            error_type=type(exc).__name__,
            json_output=False,
        )
        raise typer.Exit(code=1)

    typer.echo(_format_verifier_reply_summary(result))


def _directory_snapshot(path: Path) -> dict[str, object]:
    file_count = sum(1 for item in path.rglob("*") if item.is_file()) if path.exists() else 0
    return {
        "exists": path.exists(),
        "file_count": file_count,
    }


def _format_fetch_summary(result) -> str:
    record = result.record
    lines = [
        f"Input DOI: {result.input_doi}",
        f"Normalized DOI: {result.normalized_doi}",
        f"Cache hit: {'yes' if result.cache_hit else 'no'}",
        f"OpenAlex ID: {record.openalex_id}",
        f"Title: {record.title}",
        f"Publication year: {record.publication_year if record.publication_year is not None else 'unknown'}",
        f"Cited by count: {record.cited_by_count}",
        f"Primary topic: {record.primary_topic or 'none'}",
        f"Topics: {', '.join(record.topics) if record.topics else 'none'}",
        f"Referenced works: {len(record.referenced_works)}",
        f"Related works: {len(record.related_works)}",
        f"Abstract: {record.abstract_text if record.abstract_text else 'not available'}",
        f"Raw cache: {result.raw_cache_path}",
        f"Record cache: {result.record_cache_path}",
    ]
    return "\n".join(lines)


def _emit_command_error(
    *,
    command_name: str,
    input_value: str,
    message: str,
    error_type: str,
    json_output: bool,
) -> None:
    if json_output:
        payload = {
            "status": "error",
            "command": command_name,
            "input": input_value,
            "error": {
                "type": error_type,
                "message": message,
            },
        }
        typer.echo(json.dumps(payload, indent=2))
        return

    typer.echo(f"{command_name} failed for '{input_value}': {message}", err=True)


def _format_corpus_summary(result: LocalCorpusResult) -> str:
    lines = [
        f"Input DOI: {result.input_doi}",
        f"Normalized DOI: {result.normalized_doi}",
        f"Seed OpenAlex ID: {result.seed_openalex_id}",
        f"Run ID: {result.run_id}",
        f"Max references: {result.max_references}",
        f"Max related: {result.max_related}",
        f"Max hard negatives: {result.max_hard_negatives}",
        f"Papers written: {result.counts.papers}",
        f"Edges written: {result.counts.edges}",
        f"Failures: {result.counts.failures}",
        f"Run directory: {result.output_paths.run_dir}",
        f"Manifest: {result.output_paths.manifest}",
        f"Papers JSONL: {result.output_paths.papers_jsonl}",
        f"Edges JSONL: {result.output_paths.edges_jsonl}",
    ]
    if result.max_hard_negatives > 0:
        lines.append(f"Hard negatives written: {result.counts.selected_hard_negatives}")
        lines.append(f"Eligible hard-negative pool: {result.counts.eligible_hard_negative_pool_size}")
        lines.append(f"Hard-negative candidates considered: {result.counts.considered_hard_negative_pool_size}")
        if result.counts.hard_negative_shortfall > 0:
            lines.append(f"Hard-negative shortfall: {result.counts.hard_negative_shortfall}")
    if result.output_paths.failures_json:
        lines.append(f"Failures JSON: {result.output_paths.failures_json}")
    lines.append(f"Completed stages: {', '.join(result.completed_stages)}")
    return "\n".join(lines)


def _format_ranking_summary(result: RankingSummary) -> str:
    lines = [
        f"Run directory: {result.run_dir}",
        f"Seed OpenAlex ID: {result.seed_openalex_id}",
        f"Candidate count: {result.candidate_count}",
        f"Scored count: {result.scored_count}",
        f"Ignored orphan edges: {result.ignored_orphan_edges}",
        f"Scored candidates JSONL: {result.output_paths.scored_candidates_jsonl}",
        f"Ranking summary JSON: {result.output_paths.ranking_summary_json}",
    ]
    if result.top_results:
        lines.append("Top results:")
        for candidate in result.top_results:
            lines.append(
                f"{candidate.rank}. sim={candidate.sim:.3f} conf={candidate.conf:.3f} "
                f"{candidate.title} [{candidate.openalex_id}]"
            )
    return "\n".join(lines)


def _format_label_export_summary(result: LabelTemplateExportResult) -> str:
    lines = [
        f"Run directory: {result.run_dir}",
        f"Seed OpenAlex ID: {result.seed_openalex_id}",
        f"Candidate count: {result.candidate_count}",
        f"Exported count: {result.exported_count}",
        f"Label template CSV: {result.output_path}",
    ]
    if result.top_k_used is not None:
        lines.append(f"Top-k used: {result.top_k_used}")
    return "\n".join(lines)


def _format_silver_label_summary(result: SilverLabelGenerationResult) -> str:
    return "\n".join(
        [
            f"Run directory: {result.run_dir}",
            f"Seed OpenAlex ID: {result.seed_openalex_id}",
            f"Candidate count: {result.candidate_count}",
            f"Silver-labeled count: {result.judged_count}",
            f"Silver labels CSV: {result.output_paths.silver_labels_csv}",
            f"Silver labels JSONL: {result.output_paths.silver_labels_jsonl}",
        ]
    )


def _format_evaluation_summary(result: LocalRankingEvaluationResult) -> str:
    metrics = result.metrics
    lines = [
        f"Run directory: {result.run_dir}",
        f"Seed OpenAlex ID: {result.seed_openalex_id}",
        f"Label source: {result.label_source}",
        f"Labels path: {result.labels_path}",
        f"Candidate count: {result.candidate_count}",
        f"Judged count: {result.judged_count}",
        f"Judged fraction: {result.judged_fraction:.3f}",
        f"Top-k used: {result.top_k_used}",
        f"Precision@k: {float(metrics['precision_at_k']):.3f}",
        f"Recall@k: {float(metrics['recall_at_k']):.3f}",
        f"DCG@k: {float(metrics['dcg_at_k']):.3f}",
        f"nDCG@k: {float(metrics['ndcg_at_k']):.3f}",
        f"Brier score: {float(metrics['brier_score']):.3f}",
        f"ECE: {float(metrics['expected_calibration_error']):.3f}",
        f"Evaluation summary JSON: {result.output_paths.evaluation_summary_json}",
        f"Judged candidates JSONL: {result.output_paths.judged_candidates_jsonl}",
        f"Evaluation cases JSON: {result.output_paths.evaluation_cases_json}",
    ]
    return "\n".join(lines)


def _format_theory_eval_summary(result: TheoryEvalExperimentResult) -> str:
    lines = [
        f"Base run directory: {result.run_dir}",
        f"Experiment ID: {result.experiment_id}",
        f"Experiment directory: {result.experiment_dir}",
        f"Theory config: {result.theory_config_path}",
        f"Theory snapshot: {result.output_paths.theory_snapshot_yaml}",
        f"Seed OpenAlex ID: {result.seed_openalex_id}",
        f"Candidate count: {result.candidate_count}",
        f"Judged count: {result.judged_count}",
        f"Label source: {result.label_source}",
        f"Scored candidates JSONL: {result.output_paths.scored_candidates_jsonl}",
        f"Ranking summary JSON: {result.output_paths.ranking_summary_json}",
        f"Evaluation summary JSON: {result.output_paths.evaluation_summary_json}",
        f"Experiment manifest JSON: {result.output_paths.experiment_manifest_json}",
    ]
    if result.output_paths.metrics_delta_json:
        lines.append(f"Metrics delta JSON: {result.output_paths.metrics_delta_json}")
    return "\n".join(lines)


def _format_seed_batch_summary(result: SeedBatchManifest) -> str:
    lines = [
        f"Batch ID: {result.batch_id}",
        f"Batch directory: {result.batch_dir}",
        f"Seeds CSV: {result.seeds_csv}",
        f"Theory config: {result.theory_config}",
        f"Seed count: {result.seed_count}",
        f"Completed seeds: {result.completed_seed_count}",
        f"Failed seeds: {result.failed_seed_count}",
        f"Batch manifest JSON: {result.output_paths.batch_manifest_json}",
        f"Seed runs JSONL: {result.output_paths.seed_runs_jsonl}",
        f"Aggregate summary JSON: {result.output_paths.aggregate_summary_json}",
        f"Seed table JSONL: {result.output_paths.seed_table_jsonl}",
        f"Worst cases JSON: {result.output_paths.worst_cases_json}",
    ]
    return "\n".join(lines)


def _format_batch_aggregate_summary(result: BatchAggregateEvalResult) -> str:
    lines = [
        f"Batch ID: {result.batch_id}",
        f"Batch directory: {result.batch_dir}",
        f"Seed count: {result.seed_count}",
        f"Completed seeds: {result.completed_seed_count}",
        f"Failed seeds: {result.failed_seed_count}",
        f"Ranking metric: {result.ranking_metric or 'none'}",
        f"Aggregate summary JSON: {result.output_paths.aggregate_summary_json}",
        f"Seed table JSONL: {result.output_paths.seed_table_jsonl}",
        f"Worst cases JSON: {result.output_paths.worst_cases_json}",
    ]
    if result.best_seeds:
        best_seed = result.best_seeds[0]
        lines.append(
            f"Best seed: {best_seed.doi} ({best_seed.ranking_metric}={best_seed.ranking_value:.3f})"
        )
    if result.worst_seeds:
        worst_seed = result.worst_seeds[0]
        lines.append(
            f"Worst seed: {worst_seed.doi} ({worst_seed.ranking_metric}={worst_seed.ranking_value:.3f})"
        )
    return "\n".join(lines)


def _format_generator_packet_summary(result: GeneratorPacketBuildResult) -> str:
    return "\n".join(
        [
            f"Run directory: {result.run_dir}",
            f"Baseline experiment ID: {result.baseline_experiment_id}",
            f"Packet ID: {result.packet_id}",
            f"Packet directory: {result.output_paths.packet_dir}",
            f"Generator packet Markdown: {result.output_paths.generator_packet_md}",
            f"Generator reply template YAML: {result.output_paths.generator_reply_template_yaml}",
            f"Generator context JSON: {result.output_paths.generator_context_json}",
        ]
    )


def _format_generator_reply_summary(result: GeneratorReplyApplicationResult) -> str:
    return "\n".join(
        [
            f"Run directory: {result.run_dir}",
            f"Baseline experiment ID: {result.baseline_experiment_id}",
            f"Packet ID: {result.packet_id}",
            f"Candidate ID: {result.candidate_id}",
            f"Changed paths: {', '.join(result.changed_paths)}",
            f"Candidate directory: {result.output_paths.candidate_dir}",
            f"Candidate theory YAML: {result.output_paths.candidate_theory_yaml}",
            f"Candidate manifest JSON: {result.output_paths.candidate_manifest_json}",
            f"Validated generator reply JSON: {result.output_paths.generator_reply_validated_json}",
        ]
    )


def _format_verifier_packet_summary(result: VerifierPacketBuildResult) -> str:
    return "\n".join(
        [
            f"Run directory: {result.run_dir}",
            f"Baseline experiment ID: {result.baseline_experiment_id}",
            f"Candidate experiment ID: {result.candidate_experiment_id}",
            f"Packet ID: {result.packet_id}",
            f"Packet directory: {result.output_paths.packet_dir}",
            f"Verifier packet Markdown: {result.output_paths.verifier_packet_md}",
            f"Verifier reply template YAML: {result.output_paths.verifier_reply_template_yaml}",
            f"Verifier context JSON: {result.output_paths.verifier_context_json}",
        ]
    )


def _format_verifier_reply_summary(result: VerifierReplyRecordResult) -> str:
    return "\n".join(
        [
            f"Run directory: {result.run_dir}",
            f"Packet ID: {result.packet_id}",
            f"Verifier pass: {'yes' if result.verifier_pass else 'no'}",
            f"Verifier score: {result.verifier_score:.3f}",
            f"Validated verifier reply JSON: {result.output_paths.verifier_reply_validated_json}",
            f"Decision JSON: {result.output_paths.decision_json}",
        ]
    )


def run() -> None:
    app()


if __name__ == "__main__":
    run()
