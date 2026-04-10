# Campaign Handoff Summary

## Batch
- Batch ID: `annotation_batch_lineage_expansion_round2_20260409`
- Aspect: `lineage`
- Benchmark dataset target: `benchmark_dataset_lineage_expansion_round2_v1`
- Exported unique pair count: `60`
- Overlap count: `36`
- Planned overlap ratio: `0.60`
- Realized overlap ratio: `0.60`

## Quota Policy
- Reserved pressure requested before backfill:
  - `indirect_lineage = 8`
  - `provenance_weak = 8`
  - `hard_negative_or_distractor = 20`
- Remaining capacity was allowed to backfill into `strong_lineage` and `ambiguous_middle` under the existing exporter.

## Selected Seeds
- `https://openalex.org/W1971384536` — *Efficient quantum state tomography* — DOI `10.1038/ncomms1147` — `5` pairs
- `https://openalex.org/W2047498859` — *Tomography of quantum detectors* — DOI `10.1038/nphys1133` — `6` pairs
- `https://openalex.org/W4289253527` — *Measured measurement* — DOI `10.1038/nphys1170` — `16` pairs
- `https://openalex.org/W4323075702` — *Parallel tomography of quantum non-demolition measurements in multi-qubit devices* — DOI `10.1038/s41534-023-00688-7` — `4` pairs
- `https://openalex.org/W2941673255` — *Detector tomography on IBM quantum computers and mitigation of an imperfect measurement* — DOI `10.1103/physreva.100.052315` — `8` pairs
- `https://openalex.org/W2764035156` — *Measurement of qubits* — DOI `10.1103/physreva.64.052312` — `4` pairs
- `https://openalex.org/W4407956601` — *Boosting projective methods for quantum process and detector tomography* — DOI `10.1103/physrevresearch.7.013208` — `6` pairs
- `https://openalex.org/W3206040918` — *Characterizing Quantum Instruments: From Nondemolition Measurements to Quantum Error Correction* — DOI `10.1103/prxquantum.3.030318` — `5` pairs
- `https://openalex.org/W1971487905` — *Complete Characterization of Quantum-Optical Processes* — DOI `10.1126/science.1162086` — `6` pairs

## Source Runs
- `doi_10_1038_ncomms1147__refs_10__related_10__hardneg_10`
- `doi_10_1038_nphys1133__refs_10__related_20__hardneg_20`
- `doi_10_1038_nphys1170__refs_10__related_10`
- `doi_10_1038_nphys1170__refs_10__related_10__hardneg_10`
- `doi_10_1038_s41534_023_00688_7__refs_10__related_20__hardneg_20`
- `doi_10_1103_physreva_100_052315__refs_10__related_10__hardneg_10`
- `doi_10_1103_physreva_64_052312__refs_10__related_20__hardneg_20`
- `doi_10_1103_physrevresearch_7_013208__refs_10__related_10__hardneg_10`
- `doi_10_1103_prxquantum_3_030318__refs_10__related_10__hardneg_10`
- `doi_10_1126_science_1162086__refs_10__related_20__hardneg_20`

## Strata
- `ambiguous_middle = 18`
- `hard_negative_or_distractor = 20`
- `indirect_lineage = 0`
- `provenance_weak = 8`
- `strong_lineage = 14`

## Shortfalls and Warnings
- indirect_lineage: realized 0 below requested quota 8
- No currently exportable top-level run supplies `indirect_lineage`, so that reserved bucket remained empty.
- `provenance_weak` comes entirely from seed `https://openalex.org/W4289253527`.

## Recommendation
**Proceed to human annotation.**

This batch is materially less shortcut-shaped than pilot round 1 because it raises `hard_negative_or_distractor` from `13` to `20`, introduces `provenance_weak = 8` instead of `0`, and reduces the `strong_lineage` share from `29/70` to `14/60`.
