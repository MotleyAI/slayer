# Design: dev-1751-pg-facade-one-sided-time-filter-emits-between-x-and-null

## Context

The facade's comparator lift (`_lift_time_comparator` + the post-construction
`date_range` merge in `_apply_where`, `slayer/facade/translator.py`) predates
DEV-1732. Back then `date_range` was the only filter carrier excluded from
trailing-window / time-shift CTEs, so comparators had to lift to get frame-bound
semantics. `slayer/core/time_bounds.py` has since generalized frame-bound detection
to any verbatim relational bound with a temporal literal on a time dimension's raw
column — the two spellings are now semantically identical downstream.
`SlayerQuery.filters` are parsed as Mode-B Python-AST DSL, which cannot express
`BETWEEN`; `date_range` is BETWEEN's exact inclusive carrier.

## Goals / Non-Goals

**Goals:** kill the silent-zero-rows class (half-open `date_range`) at both the
producer (facade) and the consumer (planner); preserve exact comparator semantics
(strictness included).

**Non-Goals:** `TimeDimension` `validate_assignment` hardening (DEV-1695); verbatim
non-time `BETWEEN` failing Mode-B parse (pre-existing, loud); wrong-length
`date_range` semantics (ratified DEV-1745 no-op + warning); whether well-formed
`date_range` should apply on non-`ModelScope` stages (pre-existing skip, untouched
for well-formed ranges).

## Decisions

- **Delete the comparator lift entirely** rather than pairing `>=`/`<=` into
  `date_range` (the issue's original option 1). Ratified in interview: lifting buys
  nothing post-DEV-1732 and loses strictness — `>= a AND < b` lifted to an
  *inclusive* `BETWEEN a AND b`, wrongly including the upper boundary instant.
  Verbatim translation is exact and removes the pairing logic ("needs care" in the
  issue) altogether. Deliberate delta from the issue text: paired `>= AND <=` no
  longer lifts either.
- **Keep the `BETWEEN` lift** (`_lift_time_between`, both bounds literal): Mode-B
  filters cannot parse `BETWEEN`, and the lift is a faithful 1:1 translation.
  `_apply_where` assigns `td.date_range = [lo, hi]` directly (both bounds guaranteed),
  dropping the `[None, None]` merge and its `# type: ignore[assignment]`.
- **Fail-closed check placement** (Codex finding): validate None bounds in the
  planner's `date_range` loop *before* the `isinstance(scope, ModelScope)` skip, so
  non-`ModelScope` stages raise instead of silently ignoring the range. A `None`
  bound is inexpressible regardless of scope, so this cannot break a legitimate
  query. `ValueError` matches the surrounding planner convention.
- **Fail-closed tests go through the public planning path** (Codex finding), not the
  helper directly — a helper-level test would have missed the scope-skip bypass.

## Risks / Trade-offs

- [Metabase's common `>= AND <` shape no longer populates `date_range` metadata] →
  audited all consumers (planner, prebound bookkeeping, frame-bound stripping,
  generator WHERE-inheritance); all treat the verbatim spelling identically.
- [Existing tests pin the old lift] → two tests in `tests/facade/test_translator.py`
  rewritten with explicit consent; `test_between_lifts_to_date_range` stays as the
  lift guard.
- [Docs contradict new behavior] → `docs/interfaces/flight-sql.md:105` ("Same lift
  for time bounds") rewritten explicitly (Codex finding); `docs/concepts/queries.md`
  gains the two-bound contract sentence.

## Migration Plan

Pure behavior fix, no storage or API surface change; single PR. Rollback = revert.
