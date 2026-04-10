# Campaign Handoff Summary

## Batch
- Batch ID: `annotation_batch_lineage_pilot_round1b_20260407`
- Aspect: `lineage`
- Annotation batch schema: `benchmark_annotation_batch.v1`
- Annotation row schema: `benchmark_annotation_rows.v1`

## Recommendation
**Proceed with caution.**

The batch is internally consistent and ready for human review, but operators should be aware of a small stratum shortfall and a few same-title candidate collisions.

## Selected Seeds
- `https://openalex.org/W1971384536` — *Efficient quantum state tomography* — `8` pairs
- `https://openalex.org/W1971487905` — *Complete Characterization of Quantum-Optical Processes* — `11` pairs
- `https://openalex.org/W2047498859` — *Tomography of quantum detectors* — `9` pairs
- `https://openalex.org/W2764035156` — *Measurement of qubits* — `9` pairs
- `https://openalex.org/W2941673255` — *Detector tomography on IBM quantum computers and mitigation of an imperfect measurement* — `12` pairs
- `https://openalex.org/W3206040918` — *Characterizing Quantum Instruments: From Nondemolition Measurements to Quantum Error Correction* — `10` pairs
- `https://openalex.org/W4407956601` — *Boosting projective methods for quantum process and detector tomography* — `11` pairs

## Source Runs
- `doi_10_1103_physreva_64_052312__refs_10__related_20__hardneg_20`
- `doi_10_1038_ncomms1147__refs_10__related_10__hardneg_10`
- `doi_10_1126_science_1162086__refs_10__related_20__hardneg_20`
- `doi_10_1103_physrevresearch_7_013208__refs_10__related_10__hardneg_10`
- `doi_10_1038_nphys1133__refs_10__related_20__hardneg_20`
- `doi_10_1103_physreva_100_052315__refs_10__related_10__hardneg_10`
- `doi_10_1103_prxquantum_3_030318__refs_10__related_10__hardneg_10`

## Counts
- Exported unique pair count: `70`
- Target pair count: `70`
- Overlap count: `49`
- Planned overlap ratio: `0.70`
- Realized overlap ratio: `0.70`
- Annotator row counts: `annotator_a = 60`, `annotator_b = 59`

## Strata
- `strong_lineage = 29`
- `ambiguous_middle = 28`
- `hard_negative_or_distractor = 13`

## QA Findings
- Annotator CSV row counts match the assignment plan exactly.
- Pair overlap planning matches `pair_assignments.jsonl`.
- No duplicate seed-candidate pairs appear within either annotator CSV.
- No obvious schema drift was found; CSV headers match `benchmark_annotation_rows.v1`.
- Annotator CSVs are blinded as expected. They do **not** expose `run_id`, `run_dir`, `stratum`, score, confidence output, explanation text, or provenance flags.

## Known Shortfalls and Warnings
- Actual stratum mix differs slightly from the nominal quota plan: `hard_negative_or_distractor` exported `13` instead of planned `14`, and `strong_lineage` exported `29` instead of planned `28`.
- Actual per-seed counts range from `8` to `12`, below the nominal per-seed quota of `12` for most seeds.
- Three same-title collisions exist within individual seeds, so reviewers should pay attention to `candidate_openalex_id` and publication year:
  - *Boosting and Additive Trees* appears twice for seed `W4407956601`
  - *The nonequilibrium cost of accurate information processing* appears twice for seed `W3206040918`
  - *Big Blue in the Bottomless Pit: The Early Years of IBM Chile* appears twice for seed `W2941673255`
