# Scholarly Similarity Implementation Summary

Generated from repository inspection on 2026-04-09.

## Executive Summary

`scholarly-similarity` is no longer just a minimal scaffold. The repository now contains a working local research workbench built around a deterministic OpenAlex-based pipeline:

`DOI -> local corpus -> feature scoring -> ranking -> labels -> evaluation -> batch comparison/review`

The original scaffold goals are met, but the implementation has expanded materially beyond the initial v0 brief. In addition to the core CLI pipeline, the repo now includes:

- a substantial Streamlit operator UI
- file-based generator/verifier review loops
- batch execution and aggregation
- independent benchmark curation/materialization workflows
- report/export tooling
- optional Neo4j import/export scripts
- remote deployment support files

## What Is Implemented

### 1. Deterministic local runtime path

The core local path is implemented and config-driven.

- `src/ingest/cache_store.py`
  Raw and normalized OpenAlex payloads are cached to disk under `data/cache/`.
- `src/ingest/openalex_client.py`
  Supports DOI lookup and OpenAlex work-id lookup with bounded field selection and explicit error handling.
- `src/ingest/doi_resolver.py`
  Normalizes DOI input, reconstructs abstract text from OpenAlex inverted indexes, and writes normalized cached records.
- `src/graph/build_local_corpus.py`
  Builds a one-hop local corpus from seed references and related works, with deterministic hard-negative selection and artifact writes under `runs/<run_id>/`.
- `src/features/*.py`
  Implements concrete feature functions for bibliographic coupling, direct citation, topical similarity, temporal decay, lexical semantic overlap, confidence, and structured explanations.
- `src/rank/scorer.py` and `src/rank/ranker.py`
  Apply theory-configured feature weights, compute `sim` and `conf`, rank locally, and write scored artifacts.

This path remains deterministic and auditable. Theory weights and parameters are externalized in `configs/theory_v001.yaml`.

### 2. CLI application

The CLI is materially implemented in `src/cli/main.py` and currently exposes 15 commands:

- health/config inspection
- DOI fetch
- local corpus build
- local ranking
- label template export
- silver-label generation
- local ranking evaluation
- theory experiment execution
- batch execution
- batch aggregation
- generator/verifier packet and reply handling

This is substantially beyond the original “minimal `--help` works” scaffold target.

### 3. Evaluation and benchmarking

`src/eval/benchmark.py` is the largest backend module and contains the evaluation backbone for the repo:

- silver-label generation from provenance
- manual-label and silver-label evaluation
- experiment-isolated theory evaluation outputs
- multi-seed batch execution
- batch-level metric aggregation
- benchmark preset label export
- stratified annotation batch export
- conflict adjudication template export
- independent benchmark dataset materialization

This means the repo has moved from a single-run prototype into a small evaluation platform for repeatable benchmark cycles.

### 4. File-based generator/verifier loop

The generator/verifier loop is implemented in `src/agents/packet_builder.py`, `src/agents/reply_parser.py`, and `src/agents/revision_validator.py`.

Implemented behavior includes:

- generator packet export from baseline experiment results
- YAML reply parsing and validation
- constrained theory-change application to candidate theory files
- verifier packet export comparing baseline and candidate experiments
- verifier reply validation and decision recording

This is consistent with the stated constraint that revision loops stay file-based and do not call live models in the runtime ranking path.

### 5. Streamlit operator workspace

The repo now has a real frontend, despite older docs still describing the project as having “no frontend”.

- `src/ui/streamlit_app.py` is a large application entrypoint.
- `src/ui/` contains 29 Python modules supporting:
  batch loading/running, experiment matrices, batch comparison, diagnostics, review packet export, candidate application, re-eval outcome export, baseline promotion, benchmark curation, cohort studies, provenance timelines, workspace inbox scanning, report bundle export, and preset/launch-profile registries.

Operationally, this turns the repository into a local review workspace, not just a CLI harness.

### 6. Presets, seeds, scripts, and deployment support

The surrounding operator layer is also implemented:

- `configs/presets/`
  Saved benchmark presets, evaluation presets, launch profiles, and seed policies.
- `data/benchmarks/`
  Seed CSVs and benchmark-related input files.
- `scripts/`
  Utilities for smoke validation, launch-profile batch runs, seed-review workflows, and Neo4j import/export support.
- `deploy/`
  Nginx and systemd files for remote/server deployment.
- `docs/`
  Operator and workflow documentation.

## Scope Alignment Against The Original Brief

### Fully delivered

The original scaffold definition of done is satisfied:

- project tree exists
- `pyproject.toml` exists
- `src/` package and module layout exist
- configs exist
- CLI exists and is test-covered
- tests run successfully
- README documents setup and commands

### Expanded beyond original v0 scope

The implementation now goes beyond the initial constraints in several ways:

- there is a frontend: `src/ui/streamlit_app.py`
- there is optional database tooling: Neo4j import/export scripts and env handling
- there is remote deployment support: `deploy/nginx/` and `deploy/systemd/`
- there is a broader operator workflow layer: comparisons, review packets, cohort studies, curated benchmark exports, accepted baselines, reports

This is not necessarily bad, but it means the repository should now be described as a local research workspace rather than only a micro-scaffold.

### Still deferred or simplified

Some limits from the original brief remain true:

- OAG is still not implemented
- no live LLM orchestration exists in the runtime ranking path
- semantic similarity is still a lightweight lexical overlap placeholder, not embedding-based retrieval
- the candidate pool is still local/OpenAlex-oriented rather than a large corpus retrieval system
- all core outputs remain file-based under `runs/`

## Implementation Observations And Gaps

### 1. Documentation drift exists

`README.md` still says:

- `There is no frontend.`

That statement is now outdated because the Streamlit app is implemented and documented elsewhere.

### 2. `runtime.use_network` is descriptive, not enforced

`configs/runtime.yaml` declares `use_network: false`, but repository inspection shows that this flag is not used to gate network operations in the ingest path. `fetch-doi` and corpus-building code still instantiate `OpenAlexClient` directly and may hit OpenAlex on cache miss.

### 3. Large modules concentrate a lot of workflow logic

A few files now carry substantial implementation weight:

- `src/ui/streamlit_app.py`: 7,729 lines
- `src/eval/benchmark.py`: 3,902 lines
- `src/cli/main.py`: 836 lines
- `src/agents/packet_builder.py`: 777 lines

The system works as a single-repo prototype, but those modules are now large enough that future refactoring would improve maintainability.

## Verification Snapshot

Repository scale at inspection time:

- 63 Python source modules under `src/`
- 30,478 lines across `src/`
- 29 Python modules under `src/ui/`
- 42 test modules under `tests/`
- 13,359 lines across test files
- 18 files under `docs/`

Local verification performed:

- `.venv/bin/pytest -q`
- result: full test suite passed on 2026-04-09

## Bottom-Line Assessment

The current implementation should be described as a deterministic local scholarly-similarity research workspace with three major layers:

1. a concrete OpenAlex-based ingest/rank/evaluate backend
2. a file-based theory-revision and review workflow
3. a substantial Streamlit/operator surface for batch analysis and artifact management

If the intent is to keep the project aligned with the original v0 charter, the next cleanup step is mostly documentation and boundary-setting. If the intent is to keep growing the workspace, the project already has enough implemented surface area to justify a clearer “v1 local operator platform” positioning.
