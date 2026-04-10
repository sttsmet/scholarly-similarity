# Scholarly Similarity Local Prototype

Practical local scaffold for a deterministic research harness:

`DOI -> candidate papers -> sim/conf/exp -> evaluation`

## Purpose

This repository is a local-first starting point for scholarly similarity experiments around a DOI seed. The scaffold is intentionally small, typed, documented, and file-based so the runtime path stays auditable.

## Setup

### Create a virtual environment

On Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

On Linux or a remote server:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

If `py` or `python` is unavailable, use your Python 3.11+ interpreter directly. On many servers, `python3` exists but `python` does not.

### Install dependencies

```bash
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .[dev]
```

### Optional local environment variables

If you are using Neo4j on the same server, create a `.env` file with:

```dotenv
NEO4J_URI=neo4j://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<your-password>
```

## Run the CLI

```bash
.venv/bin/python -m src.cli.main --help
.venv/bin/python -m src.cli.main health
.venv/bin/python -m src.cli.main show-config
.venv/bin/python -m src.cli.main print-tree-info
.venv/bin/python -m src.cli.main fetch-doi 10.1038/nphys1170
.venv/bin/python -m src.cli.main fetch-doi 10.1038/nphys1170 --json
.venv/bin/python -m src.cli.main fetch-doi 10.1038/nphys1170 --refresh
.venv/bin/python -m src.cli.main build-local-corpus 10.1038/nphys1170
.venv/bin/python -m src.cli.main build-local-corpus 10.1038/nphys1170 --max-hard-negatives 5
.venv/bin/python -m src.cli.main build-local-corpus 10.1038/nphys1170 --json
.venv/bin/python -m src.cli.main rank-local-corpus runs/<run_id>
.venv/bin/python -m src.cli.main rank-local-corpus runs/<run_id> --top-k 5 --json
.venv/bin/python -m src.cli.main generate-silver-labels runs/<run_id>
.venv/bin/python -m src.cli.main evaluate-local-ranking runs/<run_id> --label-source silver
.venv/bin/python -m src.cli.main evaluate-local-ranking runs/<run_id> --label-source silver --json
.venv/bin/python -m src.cli.main run-theory-eval runs/<run_id> --theory-config configs/theory_v001.yaml --experiment-id baseline --label-source silver
.venv/bin/python -m src.cli.main run-theory-eval runs/<run_id> --theory-config configs/theory_v001.yaml --experiment-id revision_a --label-source silver --json
.venv/bin/python -m src.cli.main build-generator-packet runs/<run_id> --baseline-experiment baseline --packet-id packet_a
.venv/bin/python -m src.cli.main apply-generator-reply runs/<run_id> --baseline-experiment baseline --packet-id packet_a --reply candidate_reply.yaml --candidate-id cand_a
.venv/bin/python -m src.cli.main build-verifier-packet runs/<run_id> --baseline-experiment baseline --candidate-experiment revision_a --packet-id packet_a
.venv/bin/python -m src.cli.main record-verifier-reply runs/<run_id> --packet-id packet_a --reply verifier_reply.yaml
```

`fetch-doi` is an ingest-only command. It may call OpenAlex on a cache miss, cache the raw payload under `data/cache/openalex/doi/...`, and print a normalized local record.
`build-local-corpus` reuses that seed ingest path, fetches one-hop neighbors by OpenAlex ID with cache support, and can optionally add a small deterministic set of hard negatives before writing artifacts under `runs/<run_id>/`.
`rank-local-corpus` is fully local. It reads `seed_record.json`, `papers.jsonl`, and `edges.jsonl` from an existing run directory, computes deterministic baseline `sim/conf/exp`, and writes ranking artifacts back into that run directory.
`generate-silver-labels` derives lineage-oriented labels from corpus provenance only: strong lineage links (`seed_reference` or `direct_neighbor`) map to `2`, `seed_related` maps to `1`, and deterministic hard negatives map to `0`. `evaluate-local-ranking --label-source silver` evaluates the ranked list without using manual labels or score-derived targets.
`run-theory-eval` reuses the same local run artifacts but writes ranking and evaluation outputs under `runs/<run_id>/experiments/<experiment_id>/` so multiple theory revisions do not overwrite each other.
The file-based generator/verifier loop writes packet materials under `runs/<run_id>/agent_loops/<packet_id>/`, validates structured YAML replies, and materializes candidate theory YAMLs without making live model calls.

## Project Layout

- `src/ingest`: cache, DOI resolver, OpenAlex client scaffold
- `src/graph`: local neighborhood builder scaffold
- `src/features`: placeholder feature calculators
- `src/rank`: candidate pool, scorer, ranker
- `src/eval`: benchmark loading, metrics, perturbation helpers
- `src/agents`: packet builders, parsers, validators, templates
- `src/cli`: Typer entrypoint
- `configs`: theory, runtime, evaluation YAML
- `data`: cache and benchmark seed files
- `runs`: local output directory

## Useful Commands

```bash
.venv/bin/python -m pytest
.venv/bin/python -m src.cli.main show-config --config-name theory
.venv/bin/python -m src.cli.main print-tree-info
.venv/bin/python -m src.cli.main health
.venv/bin/python -m streamlit run src/ui/streamlit_app.py
.venv/bin/python scripts/run_v1_smoke.py
```

For the local operator workflow, artifact families, and common troubleshooting notes, see `docs/V1_OPERATOR_GUIDE.md`.
For remote browser access to a logged-in Codex CLI without API keys, see `docs/CODEX_CLI_NGINX_TTYD.md`.

## Current Limitations

- OpenAlex network fetching is intentionally a safe placeholder with TODO markers.
- OAG is not implemented.
- There is no frontend.
- There are no notebooks.
- Semantic similarity is a placeholder only.
- Generator and verifier flow is file-based only.
- This scaffold does not attempt large dataset ingestion.
