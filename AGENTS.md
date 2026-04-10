# Scholarly Similarity Local Prototype

## Goal
Build a local Python research harness for:
DOI -> candidate papers -> sim/conf/exp -> evaluation

## Scope for v0
- Local micro-scale only
- Single source only: OpenAlex
- No OAG ingestion yet
- No web frontend
- No database server
- No Docker
- No cloud tasks
- No LLM calls inside the runtime ranking path
- Generator/verifier loop is file-based only

## Architecture principles
- Keep the runtime scorer deterministic
- Keep theory parameters in YAML config files
- Cache all external responses to disk
- Prefer small, auditable modules
- Prefer simple CLI commands over notebooks
- Use placeholders/stubs where implementation is deferred

## Target repo areas
- src/ingest: OpenAlex client, DOI resolver, cache store
- src/graph: local neighborhood builders
- src/features: bc, cc placeholder, direct, topical, temporal, semantic placeholder, confidence, explanation
- src/rank: candidate pool, scorer, ranker
- src/eval: metrics, perturbation, benchmark loader
- src/agents: generator/verifier packet builders and parsers
- src/cli: command-line entrypoint
- configs: theory and runtime YAML
- tests: smoke tests and config-loading tests

## Constraints
- Do not implement full OAG support yet
- Do not implement live LLM/API orchestration
- Do not fetch large datasets
- Do not hardcode theory weights inside Python modules
- Do not add unnecessary dependencies
- Do not create a UI

## Definition of done for scaffold
- Project tree exists
- pyproject.toml exists
- src package exists with minimal modules and __init__.py files
- configs/theory_v001.yaml exists
- CLI works: python -m src.cli.main --help
- tests run with pytest
- README explains setup and first commands
