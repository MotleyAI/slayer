# Design — DEV-1868 closure sweep

## Context

See proposal.md — Why. Post-DEV-1871 the four DEV-1868 deferral raises live in
`slayer/engine/elaborate_env.py::check_partitioned_measures` (two arms),
`slayer/sql/generator.py::_composite_agg_builder`, and
`elaborate_env.py::check_computed_dimension` (transform arm); the `time_shift`
composite-rejection family (`ValueError`, outside the ratchet) lives in
`generator.py::_validate_time_shift_input`. The lifting substrate all exists:
`_synthesize_cross_model_producer` (`compile/stages.py:1208`) already takes explicit
`partition_keys` as the requested grain and installs ranked kernels for cross-model
first/last (`:1396`); composites whose dependencies are all regroup placeholders route to
the combined SELECT (`compile/projection.py:366`); transform chains render as step CTEs
under alias-based Kahn readiness. Plan reviewed by Codex (9 findings, all folded — F1–F9
below). Constraints: DEV-1836 D10 divergence protocol; guard ratchet + raise-ledger byte
parity; `architecture/index.yaml` `guards.baseline` only ever lowered; engine P9 (every
user-facing algebra type error raises in THE checker).

## Goals / Non-Goals

**Goals:** lift guards W1–W3 and the `time_shift` arms (a)/(c)/(d-aggregate-typed);
pronounce + document the computed-dimension transform residue (split per F7); relocate the
surviving row-leaf arm to the checker (P9); ratchet 5 → 1; axiom 9 → enforced.

**Non-Goals:** row-level leaves in aggregation sources / `time_shift` inputs (DEV-1859);
query-backed models (DEV-1878); cross-model expression aggregation (DEV-1832); any change
to already-legal shapes' SQL (byte-identical goldens, class-a).

## Decisions

- **D1 — W1 is characterization-first (F4).** The cross-model producer already carries an
  explicit partition grain and ranked kernels, so W1 = remove the raise, add a
  guard-bypassed characterization test, and fix only the stage that actually fails (if
  any), documented by stage. No pre-emptive grain plumbing.
- **D2 — W2 reuses the one composition primitive.** The inner cross-model partitioned
  aggregate desugars via `_synthesize_cross_model_producer` into a placeholder the
  transform consumes, exactly as local partitioned inners do; no transform-specific
  producer variant. Plan-structure tests pin producer count, substitution ownership,
  attach phase, and complete join grain, including two consumers sharing one inner (F6).
- **D3 — W3 desugars before phase classification (F5).** Cross-model operands of
  composites become placeholders during regroup discovery, so
  `_regroup_substituted_composite_phase` (all-placeholder rule) routes the composite to
  the combined SELECT. Phase behavior is defined and planner-asserted for remote+local,
  remote+literal, and multi-remote composites; only after all shapes are proven unreachable
  does `_composite_agg_builder`'s raise become an internal invariant (non-
  `NotImplementedError`, ledger class `internal`). Alternative rejected: teaching the
  AGGREGATE-phase seam to render cross-model operands inline — violates sql P10 (one
  composition primitive).
- **D4 — One series-shift emission mode for W5+W6+predicates (F2, F3, F1).** A `time_shift`
  input containing a nested transform, a cross-model leaf, or an aggregate-typed predicate
  root shifts its *materialised series*: the shifted CTE selects from the input's step /
  combined output by alias (no join discovery, no re-applied WHERE), joined back on the
  series grain + shifted bucket; LEFT JOIN yields NULL outside the series. This is a
  deliberate semantic regime distinct from the re-aggregation path (which recomputes from
  source rows and can resolve buckets outside the date range) — new shapes get series
  semantics; existing all-local composites keep re-aggregation byte-identically.
  Alternative rejected (F3): per-leaf `_cm_` reads inside the re-aggregation CTE —
  undefined grouping semantics for attached values at mixed grains.
- **D5 — The predicate arm lifts; no residue pronouncement (F1).** "No value slot" is
  representation-inspection, not typing: an aggregate-typed predicate is a boolean series
  and shifts under D4. Row-level predicates fall under the row-leaf arm (DEV-1859).
  `change`/`change_pct` over booleans stay rejected by the existing
  boolean-in-arithmetic typing contract.
- **D6 — Residue split (F7).** `check_computed_dimension`'s transform arm becomes two
  `ValueError`s: transform-needs-aggregate-input (axiom 11) and
  every-contained-aggregate-declares-`partition_by` (self-referential-grain rule). Both
  messages are permanent type rules with remedies, no issue refs. `NotImplementedError` →
  `ValueError` also removes them from the ratchet's scan by construction.
- **D7 — P9 relocation.** The surviving row-leaf validation moves from
  `generator.py::_validate_time_shift_input` to a checker (`check_time_shift_input` in
  `elaborate_env.py`), message unchanged; the generator keeps only internal invariants.
  DEV-1859 is notified of the new location.
- **D8 — Law pools grow with the algebra.** Cross-model partitioned operands
  (`customers.spend:sum(partition_by=region)`, `customers.spend:last(partition_by=region)`)
  join `OPERANDS`/`MEASURE_POOL` + `OPERAND_GRAIN`; shape ids reshuffle (accepted); any
  latent failure surfaced is fixed in this PR. `EXPECTED_RAISES` stays empty.

## Risks / Trade-offs

- [W6 attach grain mismatch: the producer's grain may not match the shifted CTE's group
  keys] → verified at implementation time by plan-structure assertions before any
  emission work; D4 sidesteps per-leaf reads entirely.
- [D4's NULL-outside-series edge differs from re-aggregation's date_range-exempt lookback]
  → deliberate, specified in `queries/transforms`; regime boundary is typed (input shape),
  never silent; executed tests pin both regimes at the range edge.
- [Series-shift CTE topology may perturb existing transform-chain SQL] → golden tripwire:
  existing legal `time_shift` forms byte-identical (class a); every changed golden
  enumerated under D10 (F9).
- [Law-pool reshuffle surfaces unrelated latent failures] → accepted by user; fixed here.
- [Dialect divergence in new emission (grouped booleans, alias reuse)] → compile-level
  generated-SQL coverage across Tier-1 dialects for the new shapes (F8); executed oracles
  on SQLite + DuckDB.

## Migration Plan

Single PR; no storage or API migration. Golden re-blessing per D10 with per-class
enumeration in the PR. Rollback = revert.

## Open Questions

None — W1's exact failing stage (if any) is characterization output, not a design unknown.
