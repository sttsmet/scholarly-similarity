# Seed Selection Pipeline V1

## Goal

This pipeline defines how large candidate reservoirs from local Neo4j/OpenAlex data
are converted into frozen benchmark seed sets for scholarly-similarity.

## Core invariants

1. Online ranking remains deterministic and local.
2. Runtime still consumes a frozen `seeds.csv` file.
3. The external AI verifier may only select `candidate_id` values present in a review packet.
4. Freeform DOI additions are forbidden.
5. Every verifier reply must be schema-validated before any new cycle artifacts are created.
6. All outputs are additive and provenance-aware.

## Artifact families

- `configs/presets/seed_policies/`
- `runs/seed_pools/`
- `runs/seed_review_packets/`
- `runs/seed_review_replies/`
- `runs/seed_sets/`
- `runs/seed_cycles/`
- `workspace_inbox/seed_review_replies/`

## Review flow

1. Build `candidate_pool.csv` from local Neo4j/OpenAlex data.
2. Convert the pool into a `seed_selection_review_packet` YAML file.
3. Send the packet manually to an external AI verifier.
4. Receive a strict `seed_selection_verifier_reply` YAML file.
5. Validate the reply locally against schema and packet membership.
6. Materialize a frozen `seeds.csv` plus benchmark preset, launch profile, and provenance manifest.
7. Run a pilot batch for the new cycle.
8. If needed, build a benchmark repair packet and repeat.

## Candidate pool contract

The raw candidate pool is a local file artifact. It is not used directly by the runtime.

Expected columns:

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

- `secondary_tag_hints` may be encoded as a pipe-delimited string in CSV and expanded to a list in packet YAML.
- `candidate_id` is the only identifier that the external verifier may select.

## Reply contract

The external verifier must return only machine-readable YAML that matches
`schemas/seed_selection_verifier_reply_v1.schema.yaml`.

The verifier must not invent new DOI values.

If the packet is insufficient, the verifier must use `status=needs_expansion`
and provide `expansion_requests`.

## Runtime contract

The runtime seed file remains:

- `doi`
- `tag`

All richer metadata stays in manifests and reply artifacts outside the runtime CSV.

## Not in scope for v1

- direct runtime reads from Neo4j
- online LLM ranking
- mutation of historical artifacts
- UI redesign

## Step-1 implementation note

This initial step creates only policy, schema, examples, and artifact directories.

No existing application logic is modified in this step.
