# Campaign Handoff Summary

## Batch
- Batch ID: `annotation_batch_lineage_expansion_round3_20260410`
- Aspect: `lineage`
- Benchmark dataset target: `benchmark_dataset_lineage_expansion_round3_v1`
- Exported unique pair count: `80`
- Overlap count: `40`
- Planned overlap ratio: `0.50`
- Realized overlap ratio: `0.50`

## Quota Policy
- Reserved first:
  - `indirect_lineage = 8`
  - `provenance_weak = 8`
  - `hard_negative_or_distractor = 24`
- Filled after reserved strata:
  - `ambiguous_middle = 28`
  - `strong_lineage = 12`

## Selected Seeds
- `https://openalex.org/W4407956601` — *Boosting projective methods for quantum process and detector tomography* — DOI `10.1103/PhysRevResearch.7.013208` — `8` pairs
- `https://openalex.org/W3206040918` — *Characterizing Quantum Instruments: From Nondemolition Measurements to Quantum Error Correction* — DOI `10.1103/PRXQuantum.3.030318` — `8` pairs
- `https://openalex.org/W1971487905` — *Complete Characterization of Quantum-Optical Processes* — DOI `10.1126/science.1162086` — `8` pairs
- `https://openalex.org/W2941673255` — *Detector tomography on IBM quantum computers and mitigation of an imperfect measurement* — DOI `10.1103/PhysRevA.100.052315` — `8` pairs
- `https://openalex.org/W4289253527` — *Measured measurement* — DOI `10.1038/nphys1170` — `10` pairs
- `https://openalex.org/W2764035156` — *Measurement of qubits* — DOI `10.1103/physreva.64.052312` — `8` pairs
- `https://openalex.org/W2027919910` — *Quantum state tomography by continuous measurement and compressed sensing* — DOI `10.1103/PhysRevA.87.030102` — `7` pairs
- `https://openalex.org/W1988974364` — *Quantum-process tomography: Resource analysis of different strategies* — DOI `10.1103/physreva.77.032322` — `8` pairs
- `https://openalex.org/W1974169933` — *Self-consistent quantum process tomography* — DOI `10.1103/PhysRevA.87.062119` — `7` pairs
- `https://openalex.org/W2047498859` — *Tomography of quantum detectors* — DOI `10.1038/nphys1133` — `8` pairs

## Source Runs
- `doi_10_1103_physreva_77_032322__refs_0__related_20__hardneg_20`
- `doi_10_1103_physreva_77_032322__refs_10__related_20__hardneg_20`
- `doi_10_1126_science_1162086__refs_0__related_20__hardneg_20`
- `doi_10_1126_science_1162086__refs_10__related_20__hardneg_20`
- `doi_10_1038_nphys1133__refs_0__related_20__hardneg_20`
- `doi_10_1038_nphys1133__refs_10__related_20__hardneg_20`
- `doi_10_1103_physreva_64_052312__refs_0__related_20__hardneg_20`
- `doi_10_1103_physreva_64_052312__refs_10__related_20__hardneg_20`
- `doi_10_1038_nphys1170__refs_10__related_10`
- `doi_10_1038_nphys1170__refs_10__related_10__hardneg_10`
- `doi_10_1038_nphys1170__refs_0__related_10__hardneg_10`
- `doi_10_1103_physreva_87_030102__refs_10__related_20__hardneg_20`
- `doi_10_1103_physreva_87_062119__refs_10__related_20__hardneg_20`
- `doi_10_1103_physreva_100_052315__refs_10__related_20__hardneg_20`
- `doi_10_1103_prxquantum_3_030318__refs_10__related_20__hardneg_20`
- `doi_10_1103_physrevresearch_7_013208__refs_10__related_20__hardneg_20`

## New Runs Created
- `doi_10_1038_nphys1133__refs_0__related_20__hardneg_20`
- `doi_10_1103_physreva_64_052312__refs_0__related_20__hardneg_20`
- `doi_10_1038_nphys1170__refs_0__related_10__hardneg_10`
- `doi_10_1103_physreva_77_032322__refs_0__related_20__hardneg_20`
- `doi_10_1126_science_1162086__refs_0__related_20__hardneg_20`

## Strata
- `indirect_lineage = 8`
- `provenance_weak = 8`
- `hard_negative_or_distractor = 24`
- `ambiguous_middle = 28`
- `strong_lineage = 12`

## Shortfalls and Warnings
- None. All requested stratum quotas were realized.
- `indirect_lineage` is now present because cached `refs=0` runs surfaced pure `seed_related` candidates inside ranks `1-5`.
- `provenance_weak` still comes entirely from seed `https://openalex.org/W4289253527`.
- This batch is meaningfully less shortcut-shaped than round 2: `strong_lineage` fell from `14/60` to `12/80`, while `indirect_lineage` rose from `0/60` to `8/80`.

## Recommendation
**Proceed to human annotation.**

This round-3 batch is operationally ready: it hits the target size, stays seed-balanced, preserves the only weak-provenance source, and finally surfaces exporter-visible `indirect_lineage` without changing scorer or candidate-generation code.
