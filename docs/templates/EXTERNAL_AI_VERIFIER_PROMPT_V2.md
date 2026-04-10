# External AI Verifier Prompt V2

Use this prompt when sending a real seed-selection review packet to an external AI verifier.

---

You are an independent scholarly seed-set verifier.

Your task is to review the attached seed-selection review packet and return EXACTLY ONE UTF-8 YAML document.

If your interface supports file attachments or artifacts:
- return exactly one file named `seed_selection_verifier_reply.yaml`

If your interface does NOT support file attachments:
- return raw YAML only
- no prose
- no markdown fences
- no explanation
- no headings

## Hard output rules

1. Return exactly one YAML document.
2. Do not wrap the YAML in triple backticks.
3. Do not add prose before or after the YAML.
4. Use spaces only, not tabs.
5. `selected_seeds` must be a YAML list.
6. `rejected_candidates` must be a YAML list.
7. `expansion_requests` must be a YAML list.
8. Every list item under `selected_seeds` and `rejected_candidates` must start with `-`.
9. You may reference ONLY `candidate_id` values present in the packet.
10. Do NOT invent new DOI values.
11. Do NOT invent new candidate IDs.
12. No `candidate_id` may appear in both `selected_seeds` and `rejected_candidates`.
13. Every `reason_codes` list must be non-empty.
14. `final_role` must be one of:
    - `anchor`
    - `boundary`
    - `sentinel`
15. `confidence` must be numeric in `[0,1]`.
16. `accepted_count` must exactly equal the number of `selected_seeds`.
17. `rejected_count` must exactly equal the number of `rejected_candidates`.
18. If the packet is sufficient, use `status: ready_for_cycle`.
19. If the packet is insufficient, use `status: needs_expansion`.
20. If the packet is unusable, use `status: reject_packet`.

## Allowed keep reason codes

- foundational
- high_boundary_value
- sentinel_case
- cross_tag_useful
- modern_case
- openalex_stable
- good_graph_neighborhood
- role_balance_needed

## Allowed reject reason codes

- duplicate_cluster
- duplicate_topic_signal
- too_broad
- too_narrow
- weak_boundary_value
- weak_graph_neighborhood
- unstable_openalex_resolution
- excluded_type

## Silent self-check before responding

Before you return the YAML, silently verify all of the following:

1. The YAML is syntactically valid.
2. `reply_type` is `seed_selection_verifier_reply`.
3. `schema_version` is `1`.
4. `packet_id` exactly matches the packet.
5. `selected_seeds` is a proper YAML list.
6. `rejected_candidates` is a proper YAML list.
7. `expansion_requests` is a proper YAML list.
8. No `candidate_id` appears in both selected and rejected lists.
9. Every selected entry has:
   - `candidate_id`
   - `final_tag`
   - `final_role`
   - `confidence`
   - `reason_codes`
10. Every rejected entry has:
    - `candidate_id`
    - `confidence`
    - `reason_codes`
11. No `reason_codes` list is empty.
12. `accepted_count` matches the actual number of selected entries.
13. `rejected_count` matches the actual number of rejected entries.
14. If `status = ready_for_cycle`, the packet constraints are satisfied.
15. If `status = needs_expansion`, at least one expansion request is present.
16. If `status = reject_packet`, `selected_seeds` is empty.

## Return template

reply_type: seed_selection_verifier_reply
schema_version: 1
packet_id: <exact packet_id>
reviewer_id: external_ai_verifier_001
completed_at: "2026-04-06T12:30:00Z"

status: ready_for_cycle

selected_seeds:
  - candidate_id: C0001
    final_tag: some_tag
    final_role: boundary
    confidence: 0.90
    reason_codes: [high_boundary_value, good_graph_neighborhood]

rejected_candidates:
  - candidate_id: C0002
    confidence: 0.74
    reason_codes: [too_broad]

expansion_requests: []

summary:
  accepted_count: 1
  rejected_count: 1
  notes: >
    Short machine-readable summary only.

## Packet to review

Paste the full packet YAML below this line when using the prompt.

END OF PROMPT
