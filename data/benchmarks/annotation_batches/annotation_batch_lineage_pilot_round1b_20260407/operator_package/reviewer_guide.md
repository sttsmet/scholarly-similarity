# Lineage Reviewer Guide

## Purpose
Label each seed-candidate pair for **lineage utility**: how useful the candidate would be to a reviewer who is trying to trace the seed paper's scholarly lineage or immediate lineage neighborhood.

## Labels
- **2 = clear lineage utility**  
  The candidate is a strong lineage-positive item for the seed. It is a direct predecessor, direct follow-on, close methodological antecedent, close measurement or instrumentation antecedent, or another paper that would clearly help a reviewer trace the seed's development path.
- **1 = partial or indirect lineage utility**  
  The candidate is related enough to be potentially useful context, but the lineage connection is weaker, broader, or more indirect. It helps with surrounding context more than with the seed's core lineage.
- **0 = no meaningful lineage utility**  
  The candidate is mostly a distractor, only broadly related, or connected mainly by wording, venue, author overlap, or general topic rather than lineage.

## What Lineage Utility Means
Ask: **If someone were auditing the seed paper's research lineage, would this candidate materially help them trace where the seed came from or what it directly led to?**

## What Should Not Be the Main Criterion
Do **not** use the following as the main basis for the label:
- title or keyword overlap alone
- same broad field alone
- same authors, institution, venue, or publication year alone
- system rank, similarity score, confidence, explanations, provenance, or any hidden retrieval signal

## Edge-Case Rules
1. Lexical overlap without real lineage connection should usually be `0`.
2. Broadly related background papers that are useful context but not close lineage are usually `1`, not `2`.
3. Review, survey, or tutorial papers should be `2` only if they are genuinely strong lineage anchors for the seed; otherwise use `1`.
4. If two rows have the same or very similar title, judge each row by its own seed-candidate record and OpenAlex ID, not by title alone.
5. When in doubt between `1` and `2`, reserve `2` for cases with clearly stronger lineage usefulness.

## Conflict and Adjudication
- Annotators should work independently and record brief notes on difficult cases.
- Conflicts are expected on some overlap pairs and will be resolved later by adjudication.
- Do not overwrite another reviewer's work. The adjudicator will set the final `adjudicated_label` if needed.
