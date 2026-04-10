# Neo4j Candidate Pool Exporter V1

This exporter reads the loaded Neo4j graph and writes `candidate_pool` artifacts for the existing seed-selection pipeline.

It uses `cypher-shell` only.

It is read-only and does not mutate Neo4j. The exporter runs only `MATCH`/`RETURN` style queries, parses `cypher-shell --format plain` output, and builds the final candidate rows in Python.

## Command

```bash
python scripts/export_candidate_pool_from_neo4j.py --export-spec <path> --candidate-pool-id <id>
```

Optional argument:

- `--database` defaults to `neo4j`

Neo4j credentials are resolved in this order:

1. `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` from the environment
2. Missing values fall back to repo-root `.env`

## Export-Spec Schema V1

The export spec is a YAML object with:

- `export_type`: must be `neo4j_candidate_pool_export`
- `schema_version`: must be `1`
- `export_id`: string
- `source_snapshot_id`: string
- `filters`: object with
  - `require_doi`: bool
  - `min_publication_year`: int
  - `min_cited_by_count`: int
  - `min_referenced_works_count`: int
- `tag_rules`: list of objects, each with
  - `tag`: canonical tag name such as `state_tomography`
  - `primary_topic_names`: list of exact `Work.primary_topic_name` matches
  - `primary_subfield_names`: list of exact `Work.primary_subfield_name` matches
  - `primary_field_names`: list of exact `Work.primary_field_name` matches
  - `primary_domain_names`: list of exact `Work.primary_domain_name` matches
  - `topic_names`: list of exact `(:Work)-[:HAS_TOPIC]->(:Topic)` topic-name matches

Tag matching behavior:

- `proposed_tag` is the first matching tag in spec order
- `secondary_tag_hints` contains remaining matching tags in spec order
- rows with no matching tag rules are rejected

Graph-derived fields:

- `openalex_resolved` is `true` when `openalex_id` exists on the `Work` node
- `duplicate_cluster_id` is the lowercased DOI when present, otherwise `openalex_id`
- `graph_boundary_score` is computed from distinct 1-hop `REFERENCES` and `RELATED_TO` neighbors that are also exported candidates
- `graph_centrality_score` is normalized `log1p(cited_by_count)` over the exported candidates

## Expected Outputs

The exporter writes:

- `runs/seed_pools/<candidate_pool_id>/candidate_pool.csv`
- `runs/seed_pools/<candidate_pool_id>/pool_manifest.json`
- `runs/seed_pools/<candidate_pool_id>/pool_summary.md`

`pool_manifest.json` includes:

- `candidate_pool_id`
- `export_spec_path`
- `export_id`
- `source_snapshot_id`
- `database`
- `total_work_nodes_seen`
- `total_topic_attachments_seen`
- `total_reference_edges_seen`
- `total_related_edges_seen`
- `exported_candidate_count`
- `per_tag_candidate_counts`
- `dropped_reason_counts`
- `output_paths`

## Troubleshooting

Missing `cypher-shell`:

- Install Neo4j command-line tools and ensure `cypher-shell` is on `PATH`.
- Re-run `cypher-shell --help` directly before retrying the exporter.

Missing Neo4j env vars:

- Export `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD`, or place them in repo-root `.env`.
- The exporter fails fast and lists the missing variable names.

Invalid export spec:

- Confirm `export_type: neo4j_candidate_pool_export`
- Confirm `schema_version: 1`
- Confirm `filters` values are booleans/integers
- Confirm each `tag_rules` entry has a canonical `tag` plus list-valued matcher fields

Empty export result:

- Inspect `pool_manifest.json` for `dropped_reason_counts`
- Relax filters or broaden `tag_rules`
- Query real topic and subfield values from Neo4j before editing the spec

Parser mismatch with `cypher-shell` plain output:

- The exporter expects the observed local plain format: a header row, comma-separated columns, quoted strings, `NULL` for nulls, and uppercase `TRUE`/`FALSE`
- If your installed `cypher-shell` renders plain output differently, update the exporter parser before running a real export
