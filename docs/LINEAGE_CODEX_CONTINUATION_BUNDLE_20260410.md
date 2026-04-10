# Lineage Codex Continuation Bundle

Date: 2026-04-10
Audience: Codex operating inside the existing VS Code workspace
Purpose: Continue the current implementation without resetting architecture, and open the first widened revision surface after the exhausted reweight-only rounds.

---

## 0. Mission

Continue the existing deterministic lineage-ranking system exactly as implemented today.
Do **not** redesign the runtime into a neural, embedding-first, or online-LLM system.
Do **not** change the candidate-supply contract unless explicitly instructed in a later phase.
Do **not** reopen reweight-only theory search as the main workstream.

The next implementation target is:

**A1: deterministic local bridge/path graph signal for feature computation only**

This bundle assumes the current project already has:

- deterministic local candidate-corpus construction
- five similarity features (`bibliographic_coupling`, `direct_citation`, `topical`, `temporal`, `semantic`)
- weighted-renormalized similarity aggregation
- separate confidence/tie-break logic
- deterministic explanation output
- independent benchmark governance with round-3 lineage benchmark at `pilot`
- constrained theory rounds that found no positive directional gain on the reweight-only surface

---

## 1. Non-negotiable guardrails

Codex must preserve all of the following:

1. **Deterministic online scorer remains deterministic**.
2. **No LLM in runtime ranking path**.
3. **No network dependency in the online path**.
4. **No retrieval redesign in this phase**.
5. **No candidate-pool contract change in this phase**.
6. **Bridge nodes may be used only for feature computation, not for returned rankable candidates**.
7. **Explanation output must remain deterministic**.
8. **Any new theory surface must pass explicit legality validation**.
9. **Because the current independent benchmark is still `pilot`, no automatic baseline promotion may be introduced in this phase**.
10. **The goal is `shadow_accept` / `reject` / `park_as_neutral` / `surface_exhausted`, not promotion**.

---

## 2. What already exists and must be treated as the base state

Codex must begin from the implemented system described below and modify it incrementally rather than reimagining it.

### Runtime and scoring base

- The system is a deterministic local ranking pipeline: DOI -> local candidate corpus -> per-feature scoring -> aggregated similarity -> confidence -> ranked list.
- The runtime is cache-backed with `use_network: false`.
- The scorer currently aggregates the five implemented features with a renormalized weighted mean over available features.
- The accepted baseline remains the active baseline.

### Existing feature families

- bibliographic coupling
- direct citation
- topical similarity
- temporal similarity
- semantic lexical Jaccard

### Governance and evidence base

- The active standalone supportive benchmark is lineage expansion round 3.
- Its maturity is `pilot`, not `promotion_ready`.
- Recent constrained rounds on the reweight-only surface produced no positive directional gain and kept the baseline unchanged.

### Implication for implementation

The correct next move is **not** another local weight search. The correct next move is to widen the theory surface with a new deterministic graph-structural observable.

---

## 3. Phase ordering

Codex must implement in this order:

### Phase A1 — implement now

**Add a deterministic local bridge/path graph feature**:

- new feature name: `graph_path`
- purpose: detect short indirect lineage structure unavailable to the current five-feature surface
- scope: feature computation only
- no change to rankable candidate set

### Phase B — not yet, only scaffold if needed

**Graph-vs-cold branch scoring**

- do not implement active branch scoring in this bundle
- at most leave TODO hooks or design comments if they help A1 integration

### Phase C — not yet

**Semantic replacement**

- do not replace the existing lexical semantic feature in this bundle

---

## 4. Target mathematical addition

Implement a new deterministic feature `graph_path` that uses a bounded bridge graph around the seed and current candidate set.

### 4.1 Local latent graph

For seed paper `p` and existing local rankable candidate set `C_p`, build a bounded auxiliary bridge node set `B_p`.

Use:

- seed `p`
- existing rankable candidates `C_p`
- deterministic bridge nodes `B_p` collected from a bounded second-hop neighborhood

This graph is **only** for feature computation.

### 4.2 Edge sources

Use only already available cached graph fields, especially:

- `referenced_works`
- `related_works`

No incoming-citation dependency is required in A1.
No new external retrieval or online enrichment.

### 4.3 Edge typing and weights

Start with deterministic typed edge weights:

- citation/reference edge weight = `1.0`
- related-work edge weight = `rho`

Legal A1 grid:

- `rho in {0.25, 0.50}`

### 4.4 Path score

For candidate `q`, compute short simple-path evidence from seed `p` to `q`.

Recommended implementation envelope:

- allowed path lengths: `2` and `3`
- exclude length `1` so direct citation remains a separate feature
- decay by path length using `lambda`

Suggested raw score:

`g_path(p,q) = sum over simple typed paths pi from p to q of length l in {2,3} [ lambda^(l-2) * product_of_edge_weights(pi) ]`

Legal A1 grid:

- `lambda in {0.40, 0.60, 0.80}`

### 4.5 Bounded normalization

Map the raw path mass to `[0,1]` deterministically:

`graph_path = 1 - exp( - g_path / kappa )`

Legal A1 grid:

- `kappa in {0.50, 1.00, 2.00}`

### 4.6 Feature weight integration

Add `graph_path` as a sixth similarity feature.

Candidate theory variants may tune:

- `sim_weights.graph_path`
- legal compensating reductions in existing weights

Do **not** allow arbitrary theory redesign in this phase.

---

## 5. File-level implementation plan

Codex should work against the current codebase and adapt file paths if names differ slightly, but it should assume the following existing anchors from the current implementation summary:

- `src/graph/build_local_corpus.py`
- `src/rank/candidate_pool.py`
- `src/rank/scorer.py`
- `src/rank/ranker.py`
- `src/features/__init__.py`
- `src/features/bibliographic_coupling.py`
- `src/features/direct_citation.py`
- `src/features/topical.py`
- `src/features/temporal.py`
- `src/features/semantic.py`
- `src/features/confidence.py`
- `src/features/explanation.py`
- `src/agents/revision_validator.py`
- `configs/theory_v001.yaml`
- `runs/accepted_baselines/baseline_001/accepted_theory_snapshot.yaml`
- `configs/presets/benchmarks/benchmark_preset_independent_lineage_expansion_round3_v1.json`

### 5.1 New graph utility layer

Create or extend a graph utility module, for example one of:

- `src/graph/bridge_graph.py`
- `src/features/graph_path.py`
- or a small helper module close to the feature implementation

Responsibilities:

1. deterministically collect bridge nodes from second-hop neighborhoods
2. cap bridge-node count
3. build typed local adjacency
4. enumerate legal simple paths of length 2 and 3 only
5. compute raw path mass and normalized `graph_path`

### 5.2 New feature file

Add:

- `src/features/graph_path.py`

The feature should:

- accept the same normalized record objects used by existing features
- return `None` only when no legal graph evidence can be computed
- otherwise return a stable float in `[0,1]`
- be free of randomness and external calls

### 5.3 Feature registry

Update:

- `src/features/__init__.py`

Register `graph_path` alongside the existing five features.

### 5.4 Scorer integration

Update:

- `src/rank/scorer.py`

Requirements:

1. add `graph_path` into the similarity feature map
2. preserve the existing renormalized weighted-mean logic
3. keep missing-feature renormalization behavior unchanged
4. round final `sim` as currently done
5. avoid any coupling that changes confidence behavior in this phase

### 5.5 Theory config

Update the theory config so the system can express the new feature.

Recommended actions:

- add commented default surface for `graph_path`
- keep the active baseline unchanged unless explicitly creating a candidate theory variant
- preserve ability to run baseline parity without the new feature active

Suggested initial dormant config pattern:

```yaml
sim_weights:
  bibliographic_coupling: ...
  direct_citation: ...
  topical: ...
  temporal: ...
  semantic: ...
  graph_path: 0.00

sim_parameters:
  temporal_tau: 5.0
  graph_path:
    max_bridge_nodes: 40
    allowed_path_lengths: [2, 3]
    related_edge_weight: 0.50
    path_length_decay: 0.60
    saturation_kappa: 1.00
```

### 5.6 Explanation integration

Update:

- `src/features/explanation.py`

Requirements:

1. `graph_path` contributions must appear naturally in top-factor decomposition
2. explanation should optionally expose compact graph notes such as:
   - number of supporting paths
   - top path motifs
   - whether support was mostly citation-based, related-based, or mixed
3. keep output deterministic and concise
4. do not emit verbose path dumps by default

### 5.7 Revision legality widening

Update:

- `src/agents/revision_validator.py`

Legal widened A1 surface should allow only:

- `sim_weights.graph_path`
- compensating changes in the current five similarity weights
- `sim_parameters.graph_path.related_edge_weight`
- `sim_parameters.graph_path.path_length_decay`
- `sim_parameters.graph_path.saturation_kappa`
- `sim_parameters.graph_path.max_bridge_nodes`

Strongly recommended legal sets:

- `related_edge_weight in {0.25, 0.50}`
- `path_length_decay in {0.40, 0.60, 0.80}`
- `saturation_kappa in {0.50, 1.00, 2.00}`
- `max_bridge_nodes in {20, 40, 60}`
- path lengths fixed to `{2,3}` for A1

Preserve these constraints:

- similarity weights must sum exactly to `1.0` for candidate revisions
- no confidence-surface changes in A1
- no branching in A1
- no semantic-family replacement in A1

### 5.8 Evaluation and audit additions

Add a dedicated audit script, for example:

- `scripts/run_graph_path_separability_audit.py`

It should evaluate on the current independent lineage round-3 benchmark and report:

1. overall metric deltas
2. directional movement deltas
3. slice metrics for:
   - `strong_lineage`
   - `indirect_lineage`
   - `ambiguous_middle`
   - `provenance_weak`
   - `hard_negative_or_distractor`
4. conditioned slices where `direct_citation == 0`
5. per-feature separability statistics

Recommended diagnostic output names:

- `feature_separability_by_label_pair`
- `weighted_pairwise_label_order_accuracy`
- `cross_label_order_reversal_count`
- `headline_flat_but_directional_gain`
- `headline_flat_but_directional_loss`

### 5.9 Theory-round preset for A1

Create a new preset / manifest for graph-surface rounds, for example:

- `configs/presets/theory_rounds/theory_round_lineage_graph_path_a1.json`

The preset should:

- point to the round-3 independent benchmark
- explicitly mark evidence tier as supportive only
- forbid automatic promotion
- allow closeout states:
  - `reject`
  - `park_as_neutral`
  - `shadow_accept`
  - `surface_exhausted`

---

## 6. Acceptance policy for this bundle

Because the current benchmark is still `pilot`, Codex must not implement any automatic promotion action.

### 6.1 Minimum verifier logic to support

A candidate A1 theory may be considered for `shadow_accept` only if all of the following are true:

1. non-regressive on strong-lineage ordering
2. positive or clearly improved directional signal on `indirect_lineage` plus `provenance_weak`
3. no increase in cross-label reversals on the supportive benchmark
4. no evidence of damaging hard-negative inflation

### 6.2 Operational interpretation

It is acceptable in this phase if headline metrics are flat **but** directional slice signal improves.

It is **not** acceptable to claim a new accepted baseline solely from this benchmark tier.

---

## 7. Explicit non-goals for this bundle

Codex must avoid the following in this implementation pass:

1. replacing lexical semantic with embeddings
2. changing confidence from tie-breaker behavior
3. changing benchmark maturity policy
4. changing the independent benchmark labels
5. adding online retrieval
6. adding citations / incoming-citation infrastructure
7. making co-citation the first dependency for A1
8. redesigning hard-negative generation
9. changing candidate-pool size policy as part of A1
10. opening branch scoring as an active runtime path

---

## 8. Concrete Codex execution sequence

Run the following work in order.

### Step 1 — repo inspection and diff-safe planning

Codex task:

- inspect the current repository tree
- confirm which of the referenced files already exist
- map any path/name drift from the summary to real paths in the repo
- produce a compact implementation note before changing code

### Step 2 — add bridge graph utilities and `graph_path` feature

Codex task:

- implement bounded bridge-node collection
- implement deterministic typed adjacency
- implement path enumeration for lengths 2 and 3
- implement saturation-normalized `graph_path`
- add tests for determinism and value bounds

### Step 3 — integrate `graph_path` into scorer and explanation

Codex task:

- register the feature
- integrate the feature into similarity scoring
- extend explanation output
- preserve backward-compatible behavior when `graph_path` weight is zero

### Step 4 — widen revision legality only for A1

Codex task:

- extend revision validator
- encode discrete legal parameter grids
- keep weights-sum-to-1 rule for candidate revisions
- disallow any non-A1 theory changes

### Step 5 — add audit tooling and A1 theory-round preset

Codex task:

- add a graph-path separability audit script
- add A1 round preset / manifest
- ensure outputs can feed existing generator/verifier workflow

### Step 6 — run tests and baseline parity

Codex task:

- run unit tests
- run smoke tests
- verify that with `graph_path: 0.0` the baseline ranking behavior is unchanged
- verify explanations remain deterministic

### Step 7 — prepare first shadow round

Codex task:

- create 2-4 conservative legal A1 theory candidates
- run deterministic evaluation on the frozen round-3 benchmark
- emit a closeout packet suitable for verifier review
- do not promote baseline automatically

---

## 9. Ready-to-paste Codex prompts

Use these prompts inside Codex from the workspace root.

### Prompt A — inspect current repo and produce implementation map

```text
You are continuing an existing deterministic lineage-ranking implementation.
Do not redesign the system. Inspect the repository and map the currently implemented files to this target plan:
- add a deterministic graph_path feature using bounded bridge nodes and short local paths
- preserve candidate-pool contract
- preserve deterministic scorer and current confidence behavior
- preserve existing explanation style while allowing graph_path contributions
- widen revision legality only for the A1 graph surface

First produce a compact implementation map:
1. which referenced files already exist,
2. which exact paths differ from the plan,
3. where graph utilities should live,
4. what tests already exist that must be extended,
5. which configs/presets must change.
Then stop and show the plan before editing code.
```

### Prompt B — implement A1 graph feature

```text
Continue from the current repository state.
Implement Phase A1 only: a deterministic local graph_path feature for feature computation only.
Requirements:
- use existing cached graph fields only
- no network calls
- no change to rankable candidate set
- path lengths only 2 and 3
- related edge weight legal values {0.25, 0.50}
- path decay legal values {0.40, 0.60, 0.80}
- saturation kappa legal values {0.50, 1.00, 2.00}
- deterministic behavior only
- return graph_path in [0,1]
- backward compatible when graph_path weight is 0

Update code, configs, and tests accordingly. Then summarize exact files changed and any follow-up TODOs for Phase B later.
```

### Prompt C — widen revision validator for A1 only

```text
Widen the theory revision legality surface only for Phase A1 graph_path.
Allow legal revisions only in:
- sim_weights.graph_path
- compensating existing sim weights
- sim_parameters.graph_path.related_edge_weight
- sim_parameters.graph_path.path_length_decay
- sim_parameters.graph_path.saturation_kappa
- sim_parameters.graph_path.max_bridge_nodes

Constraints:
- candidate sim weights must sum exactly to 1.0
- no confidence changes
- no branch scoring
- no semantic replacement
- no other model-class changes

Update validator code and add tests for legal vs illegal revisions.
```

### Prompt D — add separability audit and first A1 preset

```text
Add a graph-path separability audit for the current independent lineage round-3 benchmark.
The audit must report:
- overall benchmark metrics
- directional metrics
- slice metrics for strong_lineage, indirect_lineage, ambiguous_middle, provenance_weak, hard_negative_or_distractor
- conditioned analysis where direct_citation == 0
- per-feature separability by label pair

Also add a theory-round preset for Phase A1 graph_path with supportive-only evidence and no automatic promotion.
Return the exact command lines needed to run the audit and the first shadow round.
```

### Prompt E — run parity and first conservative shadow round

```text
Using the current repository state, run:
1. tests,
2. baseline parity with graph_path weight = 0,
3. a first conservative A1 shadow round with 2-4 legal candidates.

Acceptance policy for this phase:
- pilot evidence only
- no automatic promotion
- output closeout status among reject / park_as_neutral / shadow_accept / surface_exhausted
- strong-lineage non-regression required
- directional improvement on indirect_lineage and provenance_weak preferred

Return a concise execution summary and the generated artifact paths.
```

---

## 10. Suggested shell command skeletons inside VS Code terminal

Replace commands with the repo’s real task runner if it differs.

```bash
# 1) start with repo inspection in Codex
codex
```

Then paste Prompt A.

For non-interactive runs after inspection:

```bash
codex exec -m gpt-5.4 -a never -s workspace-write "$(cat /path/to/prompt_b.txt)"
codex exec -m gpt-5.4 -a never -s workspace-write "$(cat /path/to/prompt_c.txt)"
codex exec -m gpt-5.4 -a never -s workspace-write "$(cat /path/to/prompt_d.txt)"
```

If the repo already uses a test runner, use it after each change set, for example:

```bash
pytest -q
```

If there is a project CLI for benchmark runs, prefer that instead of inventing a new runner.

---

## 11. Done definition for this bundle

This continuation bundle is complete only when all of the following are true:

1. `graph_path` exists as a deterministic feature in the runtime codebase.
2. `graph_path` can be turned off with zero weight without changing baseline behavior.
3. the scorer and explanation layers understand the new feature.
4. the revision validator allows only the A1 widened surface.
5. an audit exists for feature separability and directional benchmark behavior.
6. a graph-surface theory-round preset exists.
7. no automatic promotion path has been added.
8. the repo is ready for a first `shadow_accept`-only graph-surface round.

---

## 12. One-sentence instruction for Codex

Continue the current deterministic lineage reranker as implemented, add only the first widened revision surface via a bounded deterministic `graph_path` feature, preserve the current candidate-supply and online-scoring contracts, and prepare the repo for supportive-evidence shadow rounds rather than promotion.
