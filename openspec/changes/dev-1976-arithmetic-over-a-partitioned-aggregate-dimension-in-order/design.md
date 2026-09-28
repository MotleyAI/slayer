## Context

See proposal.md › Why. The three sites that decide "is this a dimension value" today:

- **Checker** (`slayer/engine/elaborate_env.py`): `type_position_conjunct` types filters and order targets field-first — `_field_blockers` empty ⇒ FIELD, else `_measure_blockers` over `dim_keys`. Declared measures are never typed; `build_environment` stamps them `_MEASURE`.
- **Discovery** (`slayer/engine/compile/discovery.py` + `PositionClasses.combined_admits`): `order` and `measure_filter` skip `dim_key` nodes (DEV-1964); `measure` skips only partition-key subtrees, so a measure over a dimension value gets a redundant combined attach, and a measure equal to a finer-grained dimension aggregate fails `check_combined_partition_keys`.
- **Generator** (`slayer/sql/generator.py::_build_base_select_for_planned`): an AGGREGATE-phase composite renders with `CompositeFacilities` only (no scope, no attached map) → `RenderContextMissingFacilityError` on any column operand; a ROW-phase non-dimension composite raises "needs an aggregation"; measure-typed HAVING masks render via `_filter_render_context` (row scope + `regroup_env`) behind an untyped "not in GROUP BY" guard.

Combined / derived stages already resolve dimensions by column alias and are unaffected.

Architecture: Axiom 9 (closure), Axiom 13 (positions), Law 4 (position parity) in `semantics.arc42.md`; engine P4 (interning), P6 (placement from stage/typing), P9 (every user-facing type error in the checker); sql P5 (one ValueKey renderer), P9 (fail closed), P10 (substitution by structural identity).

## Goals / Non-Goals

**Goals:** one "dimension value" concept consulted by checker, discovery and generator; the whole bug class (proposal) unrepresentable rather than patched per shape.

**Non-Goals:** aggregate-free order-target grammar (DEV-2003); combined / derived stage render paths; raw-rows mode (no measures; field-typed order sorts rows).

## Decisions

### D1 — Dimension value
A sub-expression of a measure / order / filter root structurally equal to a query dimension's bound key (`dim_keys`, as built by `position_classes`) is that dimension's grouped value. Equality is key identity — the same predicate `walk_consumer_positions(dim_keys=…)` already marks as `dim_key`. The boundary: aggregate and transform internals (source, args, kwargs, partition keys, transform input) are never dimension values — they are row-level inputs of their node (Axiom 2.3/2.4). Exception until DEV-1963: a computed dimension containing a transform is not a dimension value in measure position (its measure form keeps query-grain evaluation).

### D2 — Checker types measures at query grain
Declared measures call the measure-availability primitive directly — `_measure_blockers(root, dim_keys)` — with a forced measure verdict; the field-first `type_position_conjunct` is NOT reused for them (it would type `amount + 1` as FIELD and never consult the blockers).

- Error: `PositionTypingError`, `location=f"measure {name!r}"` / `"order item …"`, summary naming the blockers via `_key_display` ("…row-level amount, not available at the query grain (not among the query dimensions)"). For a measure with no aggregate/transform at all, the summary also carries "'amount' needs an aggregation inside an expression" and the suggestion names `sum(amount)` / `avg(amount)` / `count(*)` — the three existing tests pinning that phrase (`tests/test_named_measures.py`, `tests/integration/test_integration.py` round/abs) keep passing unchanged.
- Order of checks: the new check runs at the checker's position-typing checkpoint AFTER the existing measure checks (partition keys, transform inputs, re-aggregation, time axis), so no existing typed error changes family. An existing error test that would change type is a STOP-and-ask, not a re-bless.
- Alternative rejected: a second "a measure must contain an aggregate" rule — a position-specific refusal of a well-typed term (Axiom 9) that drifts from the filter rule.

### D3 — Aggregate-free measures over dimension values are legal
Falls out of D2 with no extra rule; evaluates once per result cell in the base grouped SELECT.

### D4 — Discovery: measures resolve dimension values like order / measure-filter
`PositionClasses.combined_admits(position="measure")` returns False for `dim_key` nodes too (keeping its partition-key skip). Consequences: no combined attach for a dimension value inside a measure (one producer, row attach only); `check_combined_partition_keys` skips it (row 9 legal). A partitioned aggregate nested inside a band/transform dimension is not `dim_key` and keeps the combined-consumer rule (DEV-1964 / DEV-1850 band tests unchanged). …except `dim_key` nodes of a transform-bearing dimension.

### D5 — One grouped-SELECT render context
`_build_base_select_for_planned` builds, once per grouped base SELECT, a render context used for (a) projected non-dimension composites (AGGREGATE phase, incl. aggregate-free measures), (b) hidden order composites — AGGREGATE phase, or ROW phase and non-dimension (reachable only as a function of dimension values after D2), and (c) measure-typed HAVING masks in `_build_where_having_from_planned`.

- Facility: a map `dimension slot key → its GROUP BY expression` (the `group_by_keys` entries already built for ROW dimension slots, keyed by slot key). `render_value_key` consults it by structural identity for composite operands only — never inside `AggregateKey` / `TransformKey` rendering, which goes through the existing aggregate builder with its own scope (D1 boundary).
- No row scope is supplied, so a stray row-level column fails closed through the existing `RenderContextMissingFacilityError` (unreachable after D2) — sql P9.
- Deleted: the HAVING "not in GROUP BY" `ValueError` guard; the ROW-branch "needs an aggregation" `ValueError` in the grouped path. Field-typed WHERE masks keep `_filter_render_context` (row scope).
- Alternative rejected: pass a row scope + `regroup_env` into the projection render (band-aid) — keeps three contexts, re-derives joined/computed/stage dimensions from their row definitions (a joined dimension would re-enter join discovery in a throwaway frame) instead of reusing their GROUP BY expressions.
- Alternative rejected: a plan-time key rewrite to a new "dimension reference" key kind — every kind dispatch must learn it (system P13) and it perturbs interning between a filter and its twin measure; the render facility mirrors the existing `regroup_env` / alias facilities (sql P10).

### D6 — Architecture
No principle text changes. Approved: Axiom 13 gains `[enforced: test:tests/test_dev1976_dimension_values.py]`.

## Risks / Trade-offs

- [Golden SQL drift] Plans for measures containing a dimension value lose the redundant combined attach; measure-typed HAVING masks switch context. → HAVING expected byte-identical (GROUP BY expression = scope rendering for plain columns). Re-bless only diffs that are exactly the dropped attach; record each below under "Approved golden divergences". Any other diff is a STOP.
- [Preempting existing typed errors] → D2 ordering; existing error tests must pass unchanged.
- [Dimension lookup leaking into aggregate internals] → D5 boundary + tests with `q2 = quantity * 2` beside `sum(quantity * 2)` and `rd = P` beside `sum(P)`.
- Grouped-order rule dropped: a bare row-column order target is the spec'd MIN/MAX wrap.

## Approved golden divergences

(none yet — pr-implement records each re-blessed baseline here with the user's OK)
