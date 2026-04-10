# Launch Profile Batch Runner V1

## Goal

This is the standalone canonical backend runner for an already-existing launch profile.

It runs a batch from a saved launch profile without using Streamlit UI callbacks.

## Backend path used

The script follows the existing repo-native backend path:

`scan_launch_profiles(...)`
-> `find_launch_profile_entry(...)`
-> `build_launch_profile_run_batch_values(...)`
-> `build_batch_run_request(...)`
-> `run_batch_request(...)`

For UI-parity provenance, it also uses the repo-native run-context helpers to write:

- `runs/batches/<batch_id>/run_context.json`

## What it does not use

This runner does not call:

- `_submit_run_launch_profile(...)`
- `_execute_batch_run_request(...)`
- any `st.*` Streamlit UI code

## Usage

```bash
python scripts/run_launch_profile_batch.py --launch-profile-id launch_seed_inbox_example --batch-id batch_launch_seed_inbox_example_001
```

## Expected outputs

On success, expect:

- `runs/batches/<batch_id>/`
- `runs/batches/<batch_id>/aggregate_summary.json`
- `runs/batches/<batch_id>/run_context.json`

The batch directory will also contain the standard batch artifacts written by the existing backend runner, including:

- `batch_manifest.json`
- `seed_runs.jsonl`
- `seed_table.jsonl`
- `worst_cases.json`

## Troubleshooting

### Missing launch profile

If the launch profile id is not present under `configs/presets/launch_profiles/`, the script exits non-zero with a clear error.

### Existing batch id

If `runs/batches/<batch_id>/` already exists, the script exits non-zero and refuses to overwrite it.

### Failed batch

If the backend batch run fails, the script exits non-zero and surfaces the backend error from `run_batch_request(...)`.

### Missing run_context

If the batch completes but `run_context.json` cannot be written, the script exits non-zero so the missing provenance is explicit.
