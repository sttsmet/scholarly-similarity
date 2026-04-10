# Annotator Instructions

## File to Annotate
- `annotator_a` should annotate: `annotator_a.csv`
- `annotator_b` should annotate: `annotator_b.csv`

Both files are in this batch directory:
`data/benchmarks/annotation_batches/annotation_batch_lineage_pilot_round1b_20260407/`

## Columns You May Edit
- `label`
- `label_confidence` (optional decimal from `0.0` to `1.0`)
- `notes`

## Columns You Must Not Edit
- `seed_openalex_id`
- `seed_title`
- `candidate_openalex_id`
- `title`
- `publication_year`
- `aspect`
- `annotator_id`
- `adjudicated_label`
- `adjudication_notes`

Do not reorder rows, delete rows, or add columns.

## How to Record Notes
Use `notes` for short reasoning only when helpful:
- why a case is borderline
- what made the lineage connection weak or strong
- why the case should be flagged for later review

## How to Handle Uncertain Cases
- If you can make a defensible judgment, enter the best label and use a lower `label_confidence`, then explain uncertainty in `notes`.
- If you truly cannot judge the pair, leave `label` blank and explain the blocker in `notes`.

## File Naming for Return
Return the completed file with this pattern:
- `annotator_a.completed_<reviewerid>_YYYYMMDD.csv`
- `annotator_b.completed_<reviewerid>_YYYYMMDD.csv`
