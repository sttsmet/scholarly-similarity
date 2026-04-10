# Lineage Benchmark Health Audit

- Pilot dataset: `benchmark_dataset_lineage_pilot_v1`
- Round-2 dataset: `benchmark_dataset_lineage_expansion_round2_v1`
- Combined v2 created: `false`

## Round-2 Summary
- Final labeled pair count: `60`
- Final seed count: `9`
- Label distribution: `{"0": 22, "1": 24, "2": 14}`
- Agreement summary: `{"agreement_rate": 0.833333, "disagreement_pair_count": 6, "exact_match_pair_count": 30, "metric_name": "raw_pair_agreement", "overlap_pair_count": 36}`
- Conflict summary: `conflict_pair_count=6, adjudicated_conflict_count=6`
- Adjudication complete: `true`
- Maturity tier: `pilot`
- Promotion ready: `false`

## Comparison vs Pilot
- Pair-count delta vs pilot: `-10`
- Seed-count delta vs pilot: `2`
- Label-distribution delta vs pilot: `{"0": -16, "1": 2, "2": 4}`
- Less shortcut-shaped than pilot: `true`
- Indirect lineage still absent: `true`
- Overlap with pilot pairs: `39`
- Inter-round label disagreements on overlapping pairs: `16`

## Recommendation
- `benchmark expansion round 3 still recommended first`
- Rationale: Round 2 is worth freezing now, but it still has no indirect_lineage coverage and it cannot be safely unioned with the existing pilot benchmark without a separate cross-round adjudication pass over overlapping pairs whose final labels changed.
