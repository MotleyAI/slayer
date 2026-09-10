# DEV-1841 — approved value / error / SQL / input divergences

Every behavior flip this change ships, enumerated for approval. Fully
attributable queries and broadcast defaults stay byte-identical (existing
golden corpus green; `bcast/` goldens pin the bytes).

## Value flips: a local aggregate over an unattributable dimension broadcasts

Uniform classification routes a LOCAL aggregate whose grain dimension is
unattributable through the same safe-grain producer as a cross-model one — it
broadcasts the safe-grain total (with a `broadcast` warning) instead of a naive
join-multiplied per-cell value. `associate` mode recovers the per-cell value.

- `tests/test_dev1836_producer_execution.py::TestBroadcast::test_unproven_hop_dim_broadcasts`
  — local `amount:sum` sliced by `customers.segments.label` (fanning hop):
  per-label values → `AMOUNT_TOTAL` broadcast.
- `tests/test_dev1853_traversal_execution.py::TestOrientedCardinalityValues::test_local_measure_with_reverse_fan_out_dim_both_modes`
  — `spend:sum` on customers sliced by `orders.status` (reverse fan-out): the
  pinned exact dedup (250/100/60) is now the `associate` result; the default
  broadcasts the distinct-customer total (310).

## Input rejection: `strict` retired for `to_many_handling`

`SlayerQuery.strict` is removed; a fresh `strict` is rejected by a before-
validator naming `to_many_handling` (stored v3 payloads migrate `strict: true`
→ `"error"`). Behavior under `error` mode is identical to the old strict bool,
so every strict test ports verbatim to `to_many_handling="error"`.

- Ported: `test_dev1836_broadcast_strict.py`, `test_dev1840_strict_metadata.py`,
  `test_dev1842_broadcast_strict.py`, `test_dev1836_unsafe_inputs.py`,
  `test_dev1853_pushdown.py`, `test_dev1858_mcp_query_tidy.py` (in-query field),
  and REST/MCP surface + run-by-name rejection
  (`test_dev1841_surfaces.py`, `test_api_server.py`).

## Warning-surface additions

- New `kind: "associated"` warning on every `associate`-mode aggregate over an
  unattributable dimension (cells overlap, not additive); suppressed for
  explicit `partition_by=`.
- New response-only `kind: "semi_join_pushed"` informational entry for
  DEV-1840 EXISTS-pushed conjuncts (every mode; amends DEV-1840's silence — no
  Python warning, never an error).
- Broadcast payload gains a dice–slice hint field; the broadcast reason now
  distinguishes "crosses a fanning or unproven join hop" from "unreachable".

## SQL-shape flips (goldens re-blessed)

- `tests/golden/dev1841_sql_baseline.json` — every `assoc/*` case flips from a
  feature-missing raise to the two-level dedup-then-aggregate GROUP BY
  (ALLOWED_DELTAS emptied again). `median` stays a recorded
  `NotImplementedError` on T-SQL and MySQL (neither has a grouped-median form —
  a general dialect limitation, pinned not hidden).
- `tests/golden/dev1824_sql_baseline.json`,
  `tests/golden/dev1739_sql_baseline.json` — the `partition_by` guard raise's
  reason wording improves ("crosses a fanning or unproven join hop" replaces the
  ungrammatical "unreachable"); pure message text, no shape change.
