# Seed Reply Inbox Autorun V1

## Goal

Process an inbox bundle under:

- `workspace_inbox/seed_review_replies/<request_id>/`

The autorun wrapper expects exactly two files in the inbox bundle:

- `seed_cycle_materialization_request.yaml`
- `verifier_reply.yaml`

It then performs three deterministic local steps:

1. ingest the verifier reply
2. materialize the seed cycle
3. run a batch from the materialized launch profile

## Request contract

The local request file must contain the same fields as the inbox materialization request plus:

- `autorun_batch = true`
- `batch_id`

The file still keeps:

- `request_type = seed_cycle_materialization_request`
- `schema_version = 1`
- `materialize_only = true`

This keeps the request compatible with the existing materialization-only processor while allowing the autorun wrapper to add the batch-start step.

## Example setup

```bash
mkdir -p workspace_inbox/seed_review_replies/seed_cycle_autorun_request_example

cp docs/examples/seed_selection_verifier_reply.ready_example.yaml \
  workspace_inbox/seed_review_replies/seed_cycle_autorun_request_example/verifier_reply.yaml

cp docs/examples/seed_cycle_autorun_request.example.yaml \
  workspace_inbox/seed_review_replies/seed_cycle_autorun_request_example/seed_cycle_materialization_request.yaml
```

## Example command

```bash
python scripts/process_seed_reply_inbox_autorun.py \
  --inbox-dir workspace_inbox/seed_review_replies/seed_cycle_autorun_request_example
```

## Outputs

The autorun wrapper writes:

- inbox-side:
  - `workspace_inbox/seed_review_replies/<request_id>/process_report.json`
  - `workspace_inbox/seed_review_replies/<request_id>/autorun_process_report.json`

- reply-side:
  - `runs/seed_review_replies/<reply_id>/...`

- cycle-side:
  - `runs/seed_cycles/<cycle_id>/...`
  - `runs/seed_cycles/<cycle_id>/autorun_process_report.json`

- batch-side:
  - `runs/batches/<batch_id>/aggregate_summary.json`
  - `runs/batches/<batch_id>/run_context.json`

## Why this step matters

This is the first fully inbox-driven bridge from:

- external verifier output
- local deterministic validation
- local deterministic cycle materialization
- automatic batch execution from the created launch profile

It preserves provenance and keeps runtime inputs frozen and file-based.
