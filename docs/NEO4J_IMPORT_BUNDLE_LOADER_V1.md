# Neo4j Import Bundle Loader V1

This loader imports a previously built OpenAlex import bundle into Neo4j.

It uses `cypher-shell` only.

It does not use `LOAD CSV`, and it does not require access to the Neo4j import directory. The loader reads the bundle CSV files locally in Python, converts rows to typed values, and submits deterministic batched `UNWIND` Cypher over `cypher-shell` stdin.

## Command

```bash
.venv/bin/python scripts/load_neo4j_import_bundle.py --bundle-dir runs/neo4j_import_bundles/openalex_snapshot_cache_20260405_v1 --load-id neo4j_load_openalex_snapshot_cache_20260405_v1_001
```

By default the loader targets database `neo4j`, uses batch size `500`, and refuses to load into a non-empty database unless `--allow-nonempty-db` is supplied.

Neo4j credentials are resolved in this order:

1. `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD` from the environment
2. Missing values fall back to repo-root `.env`

## Expected Outputs

After the load attempt, the loader writes:

- `runs/neo4j_loads/<load_id>/neo4j_load_report.json`
- `runs/neo4j_loads/<load_id>/neo4j_load_summary.md`

The report includes the load id, bundle path, manifest path, database, batch size, preflight emptiness result, attempted row counts per CSV category, status, and output paths.

## Post-Load Validation

Load your Neo4j credentials first if needed:

```bash
set -a && source .env && set +a
```

Count `Work` nodes:

```bash
cypher-shell --format plain -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" -d neo4j "MATCH (w:Work) RETURN count(w) AS work_count"
```

Count `Topic` nodes:

```bash
cypher-shell --format plain -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" -d neo4j "MATCH (t:Topic) RETURN count(t) AS topic_count"
```

Count `REFERENCES` edges:

```bash
cypher-shell --format plain -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" -d neo4j "MATCH ()-[r:REFERENCES]->() RETURN count(r) AS reference_edge_count"
```

Count `RELATED_TO` edges:

```bash
cypher-shell --format plain -a "$NEO4J_URI" -u "$NEO4J_USERNAME" -p "$NEO4J_PASSWORD" -d neo4j "MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS related_edge_count"
```

## Troubleshooting

Missing `cypher-shell`:

- Ensure Neo4j command-line tools are installed and `cypher-shell` is on `PATH`.
- Re-run `cypher-shell --help` directly before retrying the loader.

Missing Neo4j env vars:

- Export `NEO4J_URI`, `NEO4J_USERNAME`, and `NEO4J_PASSWORD`, or place them in repo-root `.env`.
- The loader fails fast and reports which variables are still missing.

Non-empty DB refusal:

- The loader checks `MATCH (n) RETURN count(n)` before making schema or data changes.
- If the database already contains nodes, the default behavior is to stop.
- Use `--allow-nonempty-db` only when appending into an existing graph is intentional.

Failed Cypher batch:

- The loader submits rows in deterministic `UNWIND` batches, so a failure points to a specific CSV category and batch.
- Review `runs/neo4j_loads/<load_id>/neo4j_load_report.json` and `runs/neo4j_loads/<load_id>/neo4j_load_summary.md` for the failure message and attempted counts.
- If a batch fails after earlier batches succeeded, the database may be partially loaded. Clear the database if needed, then retry with a fresh `--load-id`.
