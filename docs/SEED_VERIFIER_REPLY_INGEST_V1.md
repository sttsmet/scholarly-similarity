# Seed Verifier Reply Ingest V1

## Goal

Validate and ingest an external AI verifier reply against a previously built
`seed_selection_review_packet.yaml`.

This step does not yet materialize a new benchmark cycle.
It only validates the reply and writes normalized reply-side artifacts.

## Inputs

- packet YAML:
  `runs/seed_review_packets/<packet_id>/seed_selection_review_packet.yaml`
- verifier reply YAML:
  external machine-readable YAML
- policy:
  `configs/presets/seed_policies/seed_selection_policy_v1.yaml`

## Outputs

Given an output directory, the ingestor writes:

- `raw_verifier_reply.yaml`
- `selected_candidates.json`
- `rejected_candidates.json`
- `expansion_requests.json`
- `ingest_report.json`
- `ingest_summary.md`

## Validation rules

The ingestor enforces all of the following locally:

1. reply type and schema version
2. packet_id match between packet and reply
3. candidate_id membership in the packet
4. no duplicate candidate_id across selected and rejected lists
5. final tags must exist in the packet tag targets
6. final roles must be one of `anchor`, `boundary`, `sentinel`
7. reason codes must belong to the policy catalog
8. `ready_for_cycle` must satisfy packet min/max totals
9. `ready_for_cycle` must satisfy per-tag quotas
10. `ready_for_cycle` must satisfy role minima
11. duplicate-cluster overflow is forbidden when the packet requires it
12. `needs_expansion` requires at least one expansion request
13. `reject_packet` requires zero selected seeds

## Example command

```bash
python scripts/ingest_seed_verifier_reply.py \
  --packet-yaml runs/seed_review_packets/seed_packet_example/seed_selection_review_packet.yaml \
  --verifier-reply-yaml docs/examples/seed_selection_verifier_reply.ready_example.yaml \
  --policy configs/presets/seed_policies/seed_selection_policy_v1.yaml \
  --reply-id seed_reply_example \
  --out-dir runs/seed_review_replies/seed_reply_example
```

## Current example semantics

The example packet from Step 2 contains six packet candidates:

- two `state_tomography`
- two `process_tomography`
- two `detector_tomography`

The ready example reply therefore selects all six candidates so that:

- packet min total is satisfied
- per-tag minima are satisfied
- role minima are satisfied

## Not in scope yet

This step still does not:

- create a frozen `seeds.csv`
- create a benchmark preset
- create a launch profile
- start a new cycle

Those are implemented in the next step.
