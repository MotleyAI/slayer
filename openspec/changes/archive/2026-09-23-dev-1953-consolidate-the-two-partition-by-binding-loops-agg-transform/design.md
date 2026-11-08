## Context

See proposal.md — Why. Two loops bind `partition_by=` elements in
`slayer/engine/binding.py`: `_bind_agg_partition_keys` (alias branch via
`dim_alias_map`, then `_bind`, `ColumnKey`/`ColumnSqlKey` required) and the rank-family
arm of `_bind_transform` (no alias branch). The alias map is already built on the
measure path (`_declared_measures_from_query`) and already reaches `_bind_transform`
from `_bind` and `_bind_agg_arg`, so the issue's literal repro (an alias inside a
transform's INPUT) works since DEV-1946; only the transform's OWN keys bind blind.

Throwaway probes (2026-09-23, all reverted). Routing the transform arm through the
aggregate helper makes `rank(sum(amount), partition_by=ureg)` (`ureg = upper(region)`)
execute on SQLite and DuckDB in the measure, parameter, filter, order and
dimension-position-member shapes. Two shapes stay broken downstream: (G) an
attach-carrying dimension (`spend_band`) as the transform's own key — measure position
dies in `_assert_total_routing` (its walk reaches the band's inner partitioned aggregate
through `TransformKey.partition_keys`), with that bypassed the generator raises
"transform partition_key not materialised" (the raw key never receives the DEV-1847
shape B placeholder substitution applied to `AggregateKey.partition_keys`), parameter
position "Combined regroup attach is missing a host / producer grain slot"; filter and
order positions execute. (I) an alias-resolved key that is a query dimension but no
operand-grain member in dimension position raises an internal "not materialised" error,
while its plain-column twin and the measure/filter twins silently execute by widening
the producer's GROUP BY (Axiom 11.2 unenforced).

Constraints: engine P3 (binding stays a pure function of parsed, scope, bundle, alias
map), P9 (a user-facing algebra type error raises in the checker), semantics Axiom 9
(no new "not supported" refusal; the guard ratchet forbids a new deferral site) and
Axiom 11 (operand grain per 11.1, membership per 11.2, collapsing per 11.3b, nesting
per 11.5). `rewrite_rank_partition_keys` hands its callback the PRE-rebuild node (a
contract pinned by `tests/test_dev1827_value_key_traversal.py`), so a check that needs
the rewritten inner keys cannot live inside `_validate_partition_keys`.

## Goals / Non-Goals

**Goals:**
- One partition-key binding behaviour for both constructs, so the alias asymmetry cannot
  recur; one contextual error.
- Axiom 11.2 enforced as a type rule in the checker, every position, on a recursive
  operand grain that honours 11.1, 11.3b and 11.5.
- No internal error reachable through an alias-resolved transform key; the two DEV-1960
  shapes fail with their current planner errors and are pinned narrowly.
- One parse-time rule for repeated keyword arguments.

**Non-Goals:**
- Planner support for an attach-carrying dimension as a transform's own key in the
  measure / parameter positions (DEV-1960: extend the shape B substitution to
  `TransformKey.partition_keys`; make the routing walk opaque below them).
- Replacing `regroup_root_grain` / `constituent_grain` consumers with the new recursive
  grain (they rely on the collapsing lowering that runs later; untouched).
- Ranking at the operand grain before broadcasting (Axiom 11.4 fidelity for a measure
  whose inner is explicitly grained) — unchanged.

## Decisions

- **D1 One helper.** `_bind_partition_keys(value, *, scope, bundle, dim_alias_map,
  label) -> Grain` in binding.py: tuple-or-single elements; a bare `Ref` named in
  `dim_alias_map` resolves to the dimension's bound key; else `_bind(..., in_filter=False)`
  with no `alias_map` / `measure_ctx` (a measure is illegal inside a partition) and the
  result must be `ColumnKey` / `ColumnSqlKey`, else `ValueError(f"{label} partition_by
  must resolve to a column reference; got {kind}.")`. Labels `"aggregation"` and
  `f"transform {op!r}"` keep both current messages byte-identical. `_bind_transform`
  declares `partition_keys: Grain = Grain.EMPTY` and assigns the helper's result once.
  Alternative rejected: keep two loops and copy the alias branch — the asymmetry that
  caused the bug.
- **D2 Repeated keywords die in the parser (P2).** The Call branch of `_convert` in
  syntax.py raises `ValueError` naming the call and the keyword when `node.keywords`
  repeats a name, before any construct-specific dispatch — one rule for aggregations
  (functional and colon) and transforms. Today an aggregation keeps the last occurrence
  and a transform concatenates; D1 would otherwise silently pick one behaviour.
- **D3 Operand grain is a core primitive.** `transform_operand_grain(input, *,
  query_grain, active_bucket) -> Grain` in keys.py, recursive: `AggregateKey` →
  `partition_keys` if explicit else `query_grain`, `| {active_bucket}` when
  `window_kwarg_of` is set and the bucket exists; `TransformKey` → its own operand grain,
  minus `{time_key}` when `op in AXIS_COLLAPSING_TRANSFORMS` and the axis is set (11.3b,
  11.5); `ColumnKey` / `ColumnSqlKey` / `TimeTruncKey` leaf → `{leaf}` (a projected free
  axis, per DEV-1835); `LiteralKey` / `StarKey` → nothing; composites → union of children;
  an input with no `AggregateKey` below it → `query_grain` (11.1 degenerate identity: a
  leaf- or literal-only operand is evaluated once per query-grain cell, so
  `rank(city, partition_by=region)` keeps executing). Alternative rejected:
  `constituent_grain` — flat union over every grained inner, blind to a nested collapsing
  transform's axis drop (Codex finding 2).
- **D4 Membership is a post-rewrite checker pass.** `check_transform_partition_keys_in_
  operand_grain(*, roots, query_grain, active_bucket)` in elaborate_env.py (mirrors
  `check_time_shift_input`): for every rank-family `TransformKey` with partition keys
  reachable from a root, every key must be `in transform_operand_grain(key.input, ...)`;
  else `ValueError("Transform 'rank': partition_by column 'product' is not a member of
  the transform's operand grain (city, region); a rank-family transform partitions its
  operand's cells. Add it to the inner aggregate's partition_by=, or partition by one of:
  city, region.")` (keys displayed with `dotted_key_display`, sorted). bind_inputs.py
  calls it right after the `_rw` partition-key rewrite over declared measures (dimensions
  included), filters and order specs, with `_query_grain` and `active_td_key`. Ordering
  gives the precedence the spec states: `check_computed_dimension` (residue) fires at
  declaration time, `check_partition_key_resolves` (not a query dimension) inside `_rw`.
  Alternative rejected: inside `_validate_partition_keys` — sees pre-rebuild inner keys
  (Codex finding 1); changing the walker's contract breaks pinned tests.
- **D5 DEV-1960 pins are narrow.** Characterization tests assert the exact current
  failure per position (`pytest.raises(ValueError, match="no routing disposition")` for
  the measure, `pytest.raises(RuntimeError, match="missing a host / producer grain
  slot")` for the parameter) after asserting at bind level that the key resolved to the
  dimension's value; a broad strict xfail would hide an alias or membership regression.
- **D6 Codex finding 4 rejected.** `test_explicit_partition_by_on_transform_wins`
  (test_dev1837) declares `region` as a query dimension, D4b grains the ungrained inner at
  `[region, band]`, `region` is a member; the region-less variant already fails today with
  the query-dimension error. No existing test is touched; if the suite proves otherwise
  at implement time, stop and ask.
- **D7 Ledger.** One `_EE` row for the new checker raise (category checker, family
  `local-partitioned`, user-facing). Binding raises are outside the ledger's modules.

## Risks / Trade-offs

- [The membership rule rejects a shape some existing test executes] → the shapes found
  by probing are untested; any red is reported and the test is never relaxed without an
  explicit OK.
- [`transform_operand_grain` disagrees with `constituent_grain` for a lowered collapsing
  constituent] → the new function is used by the membership rule only; the collapsing
  lowering keeps feeding the existing consumers.
- [The D2 parser rule rejects a query that previously parsed] → repeated keywords were
  never meaningful; the error names the keyword.
- [DEV-1960 changes the pinned failure text] → the pins are its first red tests, flipped
  to executed values in that change.
