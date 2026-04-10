# Adjudicator Instructions

## When Adjudication Is Needed
Adjudication is needed when:
- two reviewers assign different raw labels to the same seed-candidate pair
- reviewer notes show unresolved disagreement or persistent uncertainty

## How to Use Prior Labels
- Treat prior reviewer labels and notes as inputs, not as votes to average mechanically.
- Re-evaluate the pair using the lineage rubric and choose the single best final label.
- Do not let hidden model outputs, retrieval rank, explanations, provenance flags, or stratum labels influence the decision.

## How to Fill the Adjudication Fields
- Enter the final decision in `adjudicated_label` using `0`, `1`, or `2`.
- Enter a short rationale in `adjudication_notes`.
- Leave the original reviewer `label` values unchanged.
- In a conflict template that includes `prior_labels`, do not edit `prior_labels`.

## Leakage Avoidance
Do not introduce score or provenance leakage during adjudication. The decision should come from the scholarly relationship itself, not from runtime metadata or benchmark construction details.
