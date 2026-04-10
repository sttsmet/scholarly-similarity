# Lineage Theory Summary

Date: 2026-04-10

Purpose: concise handoff for another AI agent to inspect the implemented mathematical theory, the current accepted baseline, the evidence policy around theory changes, and the current state of the lineage benchmark program.

## 1. Executive Summary

The implemented runtime is a deterministic local ranking system for scholarly lineage-style similarity:

`DOI -> local candidate corpus -> per-feature similarity scores -> aggregated similarity sim -> confidence conf -> ranked list -> benchmark evaluation`

The current ranking theory is still a small hand-authored weighted model. It does not use embeddings, neural scoring, learned calibration, or LLMs in the ranking path.

The active accepted baseline is:

- baseline snapshot: `runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml`
- current standalone supportive benchmark: `data/benchmarks/datasets/benchmark_dataset_lineage_expansion_round3_v1/`
- round-3 benchmark maturity: `pilot`
- round-3 benchmark promotion status: `promotion_ready = false`

Important current conclusion: the benchmark is now good enough to support constrained theory rounds, but not to promote a new baseline automatically. The last constrained round on this benchmark found no positive directional gain and kept the baseline unchanged.

## 2. Data The Theory Actually Sees

The runtime scorer operates on normalized OpenAlex work records with these fields:

- `openalex_id`
- `doi`
- `title`
- `publication_year`
- `cited_by_count`
- `referenced_works`
- `related_works`
- `primary_topic`
- `topics`
- `abstract_text`
- `candidate_origins`

That schema lives in `src/ingest/doi_resolver.py` as `NormalizedOpenAlexRecord`.

The current runtime has `use_network: false` in `configs/runtime.yaml`, so intended operation is cache-backed and deterministic.

## 3. Candidate Supply And Corpus Construction

The scoring model does not search the whole corpus. It ranks a small deterministic local corpus built from OpenAlex neighborhood fields.

From `src/graph/build_local_corpus.py`:

- seed DOI is normalized and resolved to one OpenAlex work
- the local corpus includes:
  - the seed itself
  - up to `max_references` seed references
  - up to `max_related` seed related works
  - optionally up to `max_hard_negatives` deterministic hard negatives

The default theory config uses:

- `max_candidates: 25`
- `include_related_works: true`
- `include_references: true`
- `include_citations: false`
- dedupe key: `openalex_id`

Candidate pool building in `src/rank/candidate_pool.py` is simple:

- dedupe deterministically
- drop the seed itself
- keep first `max_candidates`

So the mathematical theory is a reranker over a deterministic micro-corpus, not a full retrieval system.

## 4. Hard-Negative Selection Theory

Hard negatives are generated upstream, not learned.

Selection process from `src/graph/build_local_corpus.py`:

1. Build a second-hop pool from the positive neighbors' `related_works` and `referenced_works`.
2. Exclude:
   - the seed
   - seed references
   - seed related works
   - already seen paper ids
3. Reject any candidate with a direct citation link to the seed.
4. Keep only candidates with:
   - same `primary_topic`, or
   - non-zero topic overlap
5. If both publication years are known, require:
   - `abs(seed_year - candidate_year) <= 5`
6. Rank candidate hard negatives by:
   - `primary_match` descending
   - `topic_overlap` descending
   - `year_gap` ascending
   - `openalex_id` ascending

The ranking tuple is effectively:

```text
hard_negative_rank = (primary_match, topic_overlap, year_gap)
```

One practical limitation: the implementation only scores the first `max_hard_negatives` eligible second-hop ids after deterministic enumeration, instead of globally reranking the full eligible second-hop pool.

## 5. Implemented Similarity Features

Feature registry: `src/features/__init__.py`

The runtime has exactly five similarity feature families.

### 5.1 Bibliographic Coupling

File: `src/features/bibliographic_coupling.py`

Formula:

```text
seed_refs = set(seed.referenced_works)
cand_refs = set(candidate.referenced_works)
overlap = |seed_refs ∩ cand_refs|

s_bc = overlap / sqrt(max(1, |seed_refs|) * max(1, |cand_refs|))
```

Range: `[0, 1]`

Interpretation: cosine-like overlap over outgoing reference sets.

### 5.2 Direct Citation

File: `src/features/direct_citation.py`

Formula:

```text
s_direct = 1.0 if
    candidate.openalex_id in seed.referenced_works
    or seed.openalex_id in candidate.referenced_works
else 0.0
```

Range: `{0, 1}`

Interpretation: explicit one-edge citation adjacency in either direction.

### 5.3 Topical Similarity

File: `src/features/topical.py`

Topical similarity is the mean of whichever topical components are available:

1. primary-topic exact match:

```text
primary_match = 1.0 if seed.primary_topic == candidate.primary_topic else 0.0
```

2. topic-list Jaccard:

```text
topic_jaccard = |seed_topics ∩ cand_topics| / |seed_topics ∪ cand_topics|
```

Final formula:

```text
s_topical = mean(available topical components)
```

If neither component is available, the feature returns `None`.

### 5.4 Temporal Similarity

File: `src/features/temporal.py`

Formula:

```text
year_gap = abs(seed.publication_year - candidate.publication_year)
s_temporal = exp(-year_gap / temporal_tau)
```

If either year is missing, the feature returns `None`.

Interpretation: exponential decay by publication-year distance.

### 5.5 Semantic Similarity

File: `src/features/semantic.py`

This is not an embedding model. It is a deterministic lexical Jaccard over tokenized title + abstract text.

Tokenization:

- regex: `[A-Za-z0-9]+`
- lowercased
- tokens of length `> 1`

Formula:

```text
seed_tokens = tokenize(seed.title + seed.abstract_text)
cand_tokens = tokenize(candidate.title + candidate.abstract_text)

s_semantic = |seed_tokens ∩ cand_tokens| / |seed_tokens ∪ cand_tokens|
```

If either token set is empty, the feature returns `None`.

## 6. Similarity Aggregation

File: `src/rank/scorer.py`

Raw feature weights come from theory config:

- `bibliographic_coupling`
- `direct_citation`
- `topical`
- `temporal`
- `semantic`

The scorer computes:

```text
sim = sum_i(w_i * s_i) / sum_i(w_i over features where s_i is available)
```

Properties:

- it is a weighted average, not a plain weighted sum
- missing features do not directly penalize the candidate
- instead, available active weights are renormalized on the fly
- final `sim` is rounded to 6 decimals

This renormalization is important for interpreting the accepted baseline.

## 7. Current Theory Parameters

### 7.1 Original Runtime Spec

From `configs/theory_v001.yaml`:

```text
bibliographic_coupling = 0.35
direct_citation        = 0.25
topical                = 0.20
temporal               = 0.10
semantic               = 0.10
temporal_tau           = 5.0
```

These sum to `1.00`.

### 7.2 Current Accepted Baseline

From `runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml`:

```text
bibliographic_coupling = 0.35
direct_citation        = 0.30
topical                = 0.18
temporal               = 0.10
semantic               = 0.10
temporal_tau           = 5.0
```

These sum to `1.03`, not `1.00`.

Because the scorer renormalizes by the sum of available active weights, the effective normalized mix when all five features are present is approximately:

```text
effective_bc       = 0.35 / 1.03 = 0.3398
effective_direct   = 0.30 / 1.03 = 0.2913
effective_topical  = 0.18 / 1.03 = 0.1748
effective_temporal = 0.10 / 1.03 = 0.0971
effective_semantic = 0.10 / 1.03 = 0.0971
```

Interpretation: the accepted baseline modestly increased direct citation relative influence and modestly reduced topical influence.

## 8. Confidence Theory

File: `src/features/confidence.py`

Confidence is independent from `sim`, but used as the second ranking key.

Three components are computed.

### 8.1 Coverage

```text
coverage = (# active features with non-None score) / (# active features)
```

### 8.2 Support

```text
shared_refs = |seed.referenced_works ∩ candidate.referenced_works|
direct_signal = 1.0 if direct_citation == 1.0 else 0.0
shared_signal = shared_refs / (shared_refs + support_eta)

support = max(direct_signal, shared_signal)
```

Current parameter:

```text
support_eta = 3.0
```

### 8.3 Maturity

For each available publication year:

```text
age = max(0, observation_year - publication_year)
maturity_signal = 1 - exp(-age / maturity_tau)
```

Then:

```text
maturity = mean(available maturity signals for seed and candidate)
```

Current parameters:

```text
observation_year = 2026
maturity_tau = 8.0
```

### 8.4 Final Confidence

Weighted average:

```text
conf = (
    coverage * 0.40
  + support  * 0.35
  + maturity * 0.25
) / (0.40 + 0.35 + 0.25)
```

Current factor weights already sum to `1.00`, so the denominator is effectively `1.0`.

Interpretation:

- coverage rewards metadata completeness
- support rewards direct edges or shared references
- maturity rewards older papers

This can create a bias toward older, structurally supported candidates.

## 9. Final Ranking Rule

File: `src/rank/ranker.py`

Candidates are sorted by:

1. `sim` descending
2. `conf` descending
3. `publication_year` descending
4. `title.lower()` ascending
5. `openalex_id` ascending

So confidence is a tie-breaker, not part of the primary similarity score.

## 10. Explanation Theory

File: `src/features/explanation.py`

The runtime explanation is deterministic and post hoc.

For each non-missing feature:

```text
weighted_contribution_i = w_i * s_i
```

The explanation then:

- sorts features by `weighted_contribution` descending
- takes the top `k` features, where `k = top_k_features`
- computes each feature's contribution share over total positive contribution
- lists masked features whose score was `None`

Current explanation config:

```text
top_k_features = 3
include_raw_scores = true
include_notes = true
```

The explanation does not change ranking. It only exposes the dominant weighted contributors.

## 11. Silver Label Theory

File: `src/eval/benchmark.py`

Silver labels are derived only from provenance, not from the model score.

Origin flags include:

- `seed_reference`
- `direct_neighbor`
- `seed_related`
- `hard_negative`

Silver label mapping:

```text
if seed_reference or direct_neighbor:
    label = 2
    confidence = 0.95 if more than one positive origin flag else 0.85
elif seed_related:
    label = 1
    confidence = 0.65
elif hard_negative:
    label = 0
    confidence = 0.40
else:
    label = None
```

This is the implemented lineage weak supervision theory for silver regression runs.

## 12. Independent Benchmark Stratification Theory

For lineage annotation batch export, each candidate is assigned a stratum in `src/eval/benchmark.py`:

```text
if seed_reference or direct_neighbor:
    strong_lineage
elif hard_negative:
    hard_negative_or_distractor
elif seed_related and rank <= 5:
    indirect_lineage
elif seed_related:
    ambiguous_middle
elif rank <= 10:
    ambiguous_middle
else:
    provenance_weak
```

This matters because the benchmark program is explicitly trying to test:

- strong lineage without overfitting to shortcuts
- indirect lineage
- weak provenance
- hard negatives / distractors

## 13. Evaluation Metrics

Files:

- `src/eval/metrics.py`
- `src/eval/benchmark.py`
- `src/ui/comparison.py`

### 13.1 Local Ranking Metrics

At evaluation time, judged labels inside the top-k window are used.

Implemented metrics:

```text
precision_at_k = (# labels >= 1 in top-k judged window) / (# judged rows in window)

recall_at_k = (# labels >= 1 in top-k judged window) / (# labels >= 1 in all judged rows)

dcg_at_k = sum_i(label_i / log2(i + 1))

ndcg_at_k = dcg_at_k / ideal_dcg_at_k
```

### 13.2 Confidence Diagnostics

Brier and ECE use binary targets:

```text
binary_target = 1 if label >= 1 else 0
probability = conf
```

So confidence is currently interpreted as the probability of being at least weakly relevant, not the probability of label `2`.

### 13.3 Directional Movement Diagnostics

The comparison layer also computes label-order metrics between baseline and candidate rankings:

- `pairwise_label_order_accuracy`
- `weighted_pairwise_label_order_accuracy`
- `cross_label_order_reversal_count`
- `same_label_order_reversal_count`
- `headline_flat_but_directional_gain`
- `headline_flat_but_directional_loss`
- `directional_signal_strength`

Pairwise label-order accuracy is computed over pairs of judged items with different labels:

```text
accuracy = concordant_pairs / (concordant_pairs + discordant_pairs)
```

Weighted accuracy uses pair weights equal to absolute label difference:

```text
pair_weight = |label_a - label_b|
weighted_accuracy = weighted_concordant / (weighted_concordant + weighted_discordant)
```

This is the main movement-sensitive diagnostic now used in constrained lineage rounds.

## 14. Benchmark Maturity Policy

File: `src/eval/benchmark.py`

### Prototype

Requires:

- at least 3 labeled pairs
- at least 2 distinct labels
- all conflicts resolved

### Pilot

Requires:

- at least 5 labeled seeds
- at least 50 labeled pairs
- no label share above 70%
- all conflicts adjudicated
- overlap agreement exists
- raw agreement >= 0.70

### Promotion Ready

Requires:

- at least 10 labeled seeds
- at least 150 labeled pairs
- labels 0, 1, and 2 all present
- no label share above 60%
- all conflicts adjudicated
- overlap agreement exists
- raw agreement >= 0.80

Only promotion-ready independent benchmark evidence may support automatic baseline promotion.

## 15. Candidate-Revision Policy In Recent Constrained Rounds

File: `src/agents/revision_validator.py`

The recent constrained lineage rounds allow changes only to:

- `sim_weights.bibliographic_coupling`
- `sim_weights.direct_citation`
- `sim_weights.topical`
- `sim_weights.temporal`
- `sim_weights.semantic`
- `sim_parameters.temporal_tau`

Extra constraints:

- candidate `sim_weights` must sum exactly to `1.0`
- only local search over those keys is legal
- allowed `temporal_tau` values are `{4.0, 5.0, 6.0, 7.0}`

This is stricter than the currently accepted baseline snapshot, which sums to `1.03`.

## 16. Current Evidence State

### Round 2 Standalone Benchmark

From `data/benchmarks/datasets/benchmark_dataset_lineage_expansion_round2_v1/benchmark_dataset_manifest.json`:

- 60 labeled pairs
- 9 seeds
- labels: `0=22, 1=24, 2=14`
- raw agreement: `0.833333`
- maturity: `pilot`
- promotion ready: `false`

### Round 3 Standalone Benchmark

From `data/benchmarks/datasets/benchmark_dataset_lineage_expansion_round3_v1/benchmark_dataset_manifest.json` and `benchmark_health_audit_vs_lineage_expansion_round2_v1.json`:

- 60 labeled pairs
- 10 seeds
- labels: `0=29, 1=20, 2=11`
- raw agreement: `1.0`
- maturity: `pilot`
- promotion ready: `false`
- only ineligibility reason: fewer than 150 labeled pairs

Returned round-3 pair strata:

- `strong_lineage = 10`
- `indirect_lineage = 7`
- `ambiguous_middle = 17`
- `provenance_weak = 7`
- `hard_negative_or_distractor = 19`

Important change versus round 2:

- `indirect_lineage` is now present
- strong-lineage share is lower
- evidence diversity is better
- benchmark is less shortcut-shaped than before

## 17. What Recent Constrained Theory Rounds Found

### Round 2

From `runs/theory_rounds/lineage_round2_20260409T150410Z/round2_directional_closeout.json`:

- three conservative reweight-only candidates were tested
- all were rejected
- headline metrics were flat
- silver directional movement was weakly negative
- supportive benchmark signal was effectively neutral

### Round 3

From `runs/theory_rounds/lineage_round3_20260410T140818Z/round3_directional_closeout.json`:

- the same constrained surface was used
- three new conservative candidates were tested
- all were rejected
- all had:
  - `silver_global_delta = 0`
  - `strong_lineage_delta = 0`
  - `ambiguous_middle_delta = 0`
  - `hard_negative_or_distractor_delta = 0`
  - `pairwise_label_order_accuracy_delta = 0`
  - `weighted_pairwise_label_order_accuracy_delta = 0`
  - `cross_label_order_reversal_count = 0`

Interpretation:

- the new round-3 benchmark was good enough to reopen constrained testing
- but the tested revisions were too small or too equivalent under current candidate supply
- the round ended with `keep baseline unchanged`

## 18. Most Important Decision-Critical Caveats

1. The implemented semantic feature is lexical Jaccard, not embedding similarity.
2. Direct citation is binary and likely very influential for lineage ranking.
3. Missing features renormalize the similarity weights, so metadata sparsity changes effective feature balance.
4. Confidence is not part of the primary score, but it does affect ranking order as the second key.
5. Confidence is calibrated only against binary relevance `(label >= 1)`, not full 3-class lineage depth.
6. Hard-negative generation is deterministic and upstream; it may limit what the downstream scorer can prove.
7. The current accepted baseline snapshot has raw similarity weights summing to `1.03`, while constrained search now forces legal candidates to sum to exactly `1.0`.
8. The standalone round-3 benchmark is supportive but not promotion-capable, because it is still too small.

## 19. Questions A Next-Step AI Agent Should Probably Answer

1. Is the next best move still reweight-only, or is the constrained search surface now too saturated to matter?
2. Is the bigger bottleneck the semantic feature family being too weak, or candidate supply / benchmark coverage?
3. Should future theory work stay on the same five-feature surface, or widen only after more benchmark evidence arrives?
4. Should confidence continue to target binary relevance, or should future work separate label-1 from label-2 confidence?
5. Is the accepted baseline's `1.03` raw weight sum worth normalizing in a controlled round, given the scorer's renormalization behavior?
6. Is indirect-lineage improvement more likely to come from better retrieval/candidate generation than from further small weight shifts?

## 20. Minimal Artifact Set For A Follow-On Agent

Core math and config:

- `src/rank/scorer.py`
- `src/rank/ranker.py`
- `src/features/bibliographic_coupling.py`
- `src/features/direct_citation.py`
- `src/features/topical.py`
- `src/features/temporal.py`
- `src/features/semantic.py`
- `src/features/confidence.py`
- `src/features/explanation.py`
- `configs/theory_v001.yaml`
- `runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml`

Benchmark and evidence:

- `data/benchmarks/datasets/benchmark_dataset_lineage_expansion_round3_v1/benchmark_dataset_manifest.json`
- `data/benchmarks/datasets/benchmark_dataset_lineage_expansion_round3_v1/benchmark_health_audit_vs_lineage_expansion_round2_v1.json`
- `configs/presets/benchmarks/benchmark_preset_independent_lineage_expansion_round3_v1.json`

Recent theory rounds:

- `runs/theory_rounds/lineage_round2_20260409T150410Z/round2_directional_closeout.json`
- `runs/theory_rounds/lineage_round3_20260410T140818Z/round3_directional_closeout.json`

