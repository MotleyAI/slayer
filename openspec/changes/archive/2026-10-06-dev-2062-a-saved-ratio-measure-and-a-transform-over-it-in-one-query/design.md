## Context

See proposal.md — Why. Three code paths decide how a later stage references an interned
(slotted) value:

- **Staging** (`slayer/engine/compile/staging.py`, `_direct_deps` → `needs_column`):
  descends through un-interned composite/transform structure and stops at any interned
  slot. A selected composite is therefore a dependency as a whole; its operand aggregates
  get no `_base` column (`slayer/sql/staged_plan.py` `base_render_order`).
- **Alias-mode renderer** (`slayer/sql/render/value_expr.py` `render_value_key`): reads a
  composite by alias only if its slot id is in `AliasFacilities.composite_alias_slot_ids`,
  filled from `SQLGenerator._dimension_composite_slot_ids` (computed dimensions only);
  a measure composite re-renders from operands and misses their aliases.
- **Window-transform input** (`SQLGenerator._render_window_transform_sql`): composite
  input → inline render; otherwise the input must be an available slot, else
  `RuntimeError("transform input not materialised")` — the DEV-1962 crash for a literal.

The decision "alias vs inline" is taken from key shape at render time — engine P6 forbids
that; staging already owns placement.

## Goals / Non-Goals

**Goals:** one reference rule shared by staging and rendering, so a consumer can never
look for a column staging did not produce; the window-transform input rendered by that
same rule.

**Non-Goals:** changing staging (`_direct_deps` / `needs_column` is already the correct
rule); `first`/`last` aggregate-free dispatch (DEV-1969 owns the typed error); time_shift's
shifted-producer path (already correct — it is pinned, not changed).

## Decisions

1. **Alias mode reads any available slot by its slot.** At the top of `render_value_key`,
   when `ctx.aliases` is set and `slot_id_by_key[key]` is available
   (`available_alias_by_slot_id` or `value_by_slot_id`), resolve through `_render_via_alias`
   whatever the key kind. Otherwise: an alias-slotted kind (`ColumnKey`, `ColumnSqlKey`,
   `TimeTruncKey`, `AggregateKey`, `TransformKey`) still raises
   `RenderContextMissingFacilityError`; a composite (or literal) renders structurally.
   *Why over the alternatives:* passing staging's dependency set into the render context
   would encode the same fact twice; widening `composite_alias_slot_ids` to all composites
   would make `_render_via_alias` raise for a same-stage composite whose slot exists but is
   not yet a column (e.g. two COMBINED composites rendered in one SELECT). "Available ⇒
   alias, else structure" is exactly `_direct_deps` read from the consumer side.
2. **Retire `composite_alias_slot_ids` and `_dimension_composite_slot_ids`.** A computed
   dimension's grouped alias is available wherever it was previously listed, so rule 1
   subsumes it; keeping the field would leave two rules.
3. **One input render in `_render_window_transform_sql`.** `measure =
   render_value_key(key=key.input, ctx=self._alias_render_ctx(...))` for every input
   shape. A slotted input reads its alias (as before); a composite reads available
   sub-slots and renders the rest; a literal renders as a literal.
4. **Law-harness coverage.** Add a local ratio composite (`amount:sum / *:count`) and a
   transform over it to `MEASURE_POOL`, plus one fixed shape selecting both, placed among
   the mandatory shapes of `sample_shapes()` so the seeded fill cannot drop it.

## Risks / Trade-offs

- [The outer-wrapper / attach contexts resolve slots through qualified `_base` columns or
  producer expressions in `value_by_slot_id`, which now win over structural rendering for
  composites too] → executed regressions with a cross-model-operand composite selected
  plus transform/filter/order consumers, split-equality asserted, no leaked placeholder;
  golden SQL on PostgreSQL and T-SQL.
- [An interned composite that is unavailable must render structurally while an
  unavailable leaf/aggregate/transform must raise — an implementation keyed only on
  `slot_id_by_key` would alias-resolve same-stage composites and raise] → renderer unit
  tests for both cases, with `value_by_slot_id` counting as available.
- [`ntile` over an all-tied input assigns rows to buckets arbitrarily] → the spec and tests
  assert only the bucket multiset.
- [Golden SQL of existing tests may shift where a composite's alias is now read instead of
  re-rendered] → re-bless only after confirming executed values are unchanged.
