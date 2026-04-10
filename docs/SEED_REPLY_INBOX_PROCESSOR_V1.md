# Seed Reply Inbox Processor V1

## Goal

Process an inbox bundle under:

- `workspace_inbox/seed_review_replies/<request_id>/`

The processor expects exactly two files in the inbox bundle:

- `seed_cycle_materialization_request.yaml`
- `verifier_reply.yaml`

It then performs two local deterministic steps:

1. ingest the verifier reply
2. materialize the seed cycle

This step still does not start a batch run.

## Request contract

The local request file must contain:

- `request_type = seed_cycle_materialization_request`
- `schema_version = 1`
- `request_id`
- `created_at`
- `packet_yaml`
- `reply_id`
- `seed_set_id`
- `cycle_id`
- `benchmark_preset_id`
- `launch_profile_id`
- `accepted_baseline_id`
- `eval_preset_id`
- `description`
- `materialize_only = true`

## Example setup

```bash
mkdir -p workspace_inbox/seed_review_replies/seed_cycle_request_example

cp docs/examples/seed_selection_verifier_reply.ready_example.yaml \
  workspace_inbox/seed_review_replies/seed_cycle_request_example/verifier_reply.yaml

cp docs/examples/seed_cycle_materialization_request.example.yaml \
  workspace_inbox/seed_review_replies/seed_cycle_request_example/seed_cycle_materialization_request.yaml
```

## Example command

```bash
python scripts/process_seed_reply_inbox.py \
  --inbox-dir workspace_inbox/seed_review_replies/seed_cycle_request_example
```

## Outputs

The processor writes:

- inbox-side:
  - `workspace_inbox/seed_review_replies/<request_id>/process_report.json`

- reply-side:
  - `runs/seed_review_replies/<reply_id>/...`

- cycle-side:
  - `runs/seed_cycles/<cycle_id>/...`
  - `runs/seed_cycles/<cycle_id>/source_inbox_request.yaml`
  - `runs/seed_cycles/<cycle_id>/source_inbox_verifier_reply.yaml`
  - `runs/seed_cycles/<cycle_id>/inbox_process_report.json`

## Why this step matters

This is the first fully inbox-driven bridge between:

- external verifier output
- local deterministic validation
- local deterministic cycle materialization

It preserves provenance and keeps runtime inputs frozen and file-based.
