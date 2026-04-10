# V1 Operator Guide

## What This System Is

This repository is a local-first scholarly-similarity workspace for deterministic theory revision and review.

- The runtime scorer is deterministic.
- The theory revision loop is file-based.
- The online ranking path does not call an LLM.
- Saved artifacts under `runs/` and `configs/presets/` are the operator-facing source of truth.

## Main Artifact Families

- `runs/batches/<batch_id>/`: saved local benchmark runs, aggregate metrics, seed tables, and optional `run_context.json`.
- `runs/comparisons/<comparison_id>/`: saved pairwise batch comparisons and decision records.
- `runs/comparisons/<comparison_id>/review_packets/<packet_id>/`: review packets, evidence summaries, baseline snapshots, and candidate reply templates.
- `runs/comparisons/<comparison_id>/review_packets/<packet_id>/candidate_runs/<candidate_id>/`: candidate replies materialized into candidate theory snapshots plus parity batch rerun records.
- `runs/comparisons/.../outcomes/<outcome_id>/`: re-eval outcomes and final decision records for one candidate run.
- `runs/accepted_baselines/<baseline_id>/`: accepted theory snapshots plus copied promotion artifacts.
- `configs/presets/benchmarks/`, `configs/presets/evals/`, `configs/presets/launch_profiles/`: reusable benchmark presets, evaluation presets, and launch profiles.
- `runs/reports/`, `runs/benchmark_audits/`, `runs/benchmark_curations/`, `runs/cohort_studies/`: copy-only reports, benchmark audits, seed-set curations, and cohort-study review bundles.

## Common Operator Flows

- Run a batch: use `Run Batch` in Streamlit or the existing CLI batch commands to create a new `runs/batches/<batch_id>/`.
- Compare batches: load a primary batch, load a secondary batch, inspect paired metrics in `Comparison`, then optionally save a comparison bundle.
- Export a review packet: from `Comparison`, save a review packet for explicit offline candidate-reply work.
- Preview and apply a candidate: in `Candidate Reply`, load a saved packet, preview a reply YAML, then explicitly run `Apply Candidate & Run Batch`.
- Save an outcome: compare the primary batch against the candidate batch, then explicitly save a re-eval outcome.
- Promote an accepted baseline: only after an accepted outcome, explicitly promote it into `runs/accepted_baselines/`.
- Create a curated benchmark preset: use `Benchmark Curation`, export a curation bundle, then optionally save it as a benchmark preset.
- Use a launch profile: select a saved launch profile to prefill `Run Batch`; review the prefill before launching.
- Export a report bundle: use `Export Bundle` to create a copy-only bundle from the current UI context.

## Safe ID Hygiene

Use new IDs every time you create a new saved artifact bundle. Reusing an existing ID usually causes a `FileExistsError` by design.

- Use new `batch_id` values for reruns.
- Use new `comparison_id`, `packet_id`, `candidate_id`, `outcome_id`, and `baseline_id` values for each saved review step.
- Use new `report_id`, `audit_id`, `curation_id`, `study_id`, and preset/profile IDs when exporting new bundles.
- Treat saved directories as immutable records. Create a new artifact instead of editing an old one in place.

## Troubleshooting Notes

- No numeric metrics available in `Comparison`: the two batches may have no common completed seeds, or the selected metric may be missing on one side.
- Weak or tie-heavy evidence: a saved comparison or outcome can be technically valid while still being scientifically weak. High tie rates, near-zero deltas, and low sample counts are warning signs, not hidden app bugs.
- Stale study-source context: if comparison batches change after cohort-study handoff, provenance can become stale. The UI warns about this; reload the intended pair explicitly.
- Old batches without `run_context.json`: this is still compatible. Newer UI features surface less provenance, but the batch remains usable.
- Missing artifact sidecars: if a saved directory is missing files like a decision record or copied snapshot, treat it as incomplete and create a new artifact rather than patching it by hand.
- Candidate reply preview says template-only or invalid: only explicit validated replies should be applied. The template file is a scaffold, not an executable reply.

## Helpful Commands

- Launch the app: `.venv/bin/python -m streamlit run src/ui/streamlit_app.py`
- Run the read-only workspace smoke validator: `python scripts/run_v1_smoke.py`
- Run the CLI help: `python -m src.cli.main --help`
