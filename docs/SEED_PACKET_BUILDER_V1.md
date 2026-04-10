# Seed Packet Builder V1

## Goal

Build a deterministic `seed_selection_review_packet.yaml` from a local
`candidate_pool.csv` artifact without calling Neo4j directly and without
calling any external AI.

## Inputs

- policy:
  `configs/presets/seed_policies/seed_selection_policy_v1.yaml`
- candidate pool CSV:
  local file artifact with required columns
- packet metadata:
  `packet_id`, `source_snapshot_id`, `candidate_pool_id`, `created_at`
- tag targets:
  repeated CLI values in the form `tag:min:max`

## Outputs

Given an output directory, the builder writes:

- `seed_selection_review_packet.yaml`
- `packet_manifest.json`
- `packet_summary.md`
- `candidate_snapshot.csv`

## Candidate pool CSV contract

Required columns:

- `candidate_id`
- `doi`
- `title`
- `proposed_tag`
- `secondary_tag_hints`
- `publication_year`
- `type`
- `openalex_resolved`
- `citation_count`
- `referenced_works_count`
- `graph_boundary_score`
- `graph_centrality_score`
- `duplicate_cluster_id`
- `source_snapshot_id`

Notes:

- `secondary_tag_hints` uses `|` as a delimiter in CSV.
- `openalex_resolved` accepts common boolean spellings such as `true` and `false`.
- `candidate_id` must be the only identifier selected later by the external verifier.

## Filtering behavior

The builder enforces the current policy before creating a packet.

Rows are rejected if they violate policy constraints such as:

- missing DOI
- unresolved OpenAlex mapping
- excluded type
- below minimum publication year
- below minimum citation count
- below minimum referenced works count

## Deterministic packet ordering

Within each `proposed_tag`, candidates are ordered by:

1. `graph_boundary_score` descending
2. `graph_centrality_score` descending
3. `citation_count` descending
4. `referenced_works_count` descending
5. `publication_year` descending
6. `doi` ascending
7. `candidate_id` ascending

## Example command

```bash
python scripts/build_seed_review_packet.py \
  --candidate-pool-csv docs/examples/candidate_pool.example.csv \
  --policy configs/presets/seed_policies/seed_selection_policy_v1.yaml \
  --packet-id seed_packet_example \
  --created-at 2026-04-05T12:00:00Z \
  --source-snapshot-id openalex_snapshot_001 \
  --candidate-pool-id seed_pool_example \
  --out-dir runs/seed_review_packets/seed_packet_example \
  --final-target-total 8 \
  --min-total 6 \
  --max-total 10 \
  --anchor-min 2 \
  --boundary-min 3 \
  --sentinel-min 1 \
  --max-candidates-per-tag 3 \
  --tag-target state_tomography:2:3 \
  --tag-target process_tomography:2:3 \
  --tag-target detector_tomography:2:3 ```

## Validation expectation

This step only builds the packet-side artifacts.
It does not yet validate verifier replies and does not yet materialize a new cycle.

Those are implemented in later steps.
