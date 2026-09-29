## Context

"Does X determine this key" is answered today by four hand-aligned rules:
`join_safety.grain_determines` (leaves, aggregates, members — `False` for any composite),
`stages._grain_expression_determined` + `_non_aggregate_leaf_check` (composites, but only
aggregate-carrying ones, and only with exact grain membership of the embedded aggregates'
partition keys), `stages._home_determines_grain_member` ("an expression key never"), and
`stages._param_is_determined` (its own leaf decomposition via `parameter_row_leaves`).
Governing text: semantics.arc42 Axioms 1, 2.2 (combination), 2.3 (attached constituents
opaque, typed by grain), 2.7 (spelling-invariance), 7 (attributability), 8 (mode axis);
engine P10 (closures); system P9 (`__` internal only), P13 (kind dispatch fails closed).

A throwaway probe (grain_determines recursing composites + the expression-carrier arm
disabled) produced every expected value and kept the full unit suite green, including the
`tests/test_dev1847_nesting.py` band-dimension oracles that used to take the carrier arm.

## Goals / Non-Goals

**Goals:**
- One determination rule, closed under row-level combination, shared by every
  "determines" site (outer re-aggregation grain, parameter typing, model-home check of
  attached-parameter grains).
- One route for attributable outer dimensions, aggregate-carrying or not.
- Diagnostics that name dimensions by query name and give the right reason/remedy.

**Non-Goals:**
- Volatile functions (`random()`): treated as deterministic — needs volatility metadata.
- A finer time bucket determining a coarser one (day → month): a `TimeTruncKey` is judged
  through its column, so this fails closed.
- The identity-based degenerate check: unchanged.
- Inline expression parameters (refused at bind): DEV-2016.
- `RegroupAttachPlan.partition_display` (plan-inspection only, no production reader) keeps
  the internal spelling.

## Decisions

1. **One combinator, witness form** (`join_safety`). It returns the first undetermined
   sub-key (a witness) or `None`:
   literal → determined; `ColumnKey`/`ColumnSqlKey` → the caller's leaf rule; `TimeTruncKey`
   → its column; `SLOT_COMPOSITE_KINDS` and `SqlFragmentKey` → every child; `AggregateKey`
   → `partition_keys is not None` and every partition key (recursively); any other kind
   (`StarKey`, `TransformKey`, unknown) → itself as witness (fail closed, P13).
   `grain_determines` is the boolean wrapper with its closure/pinning leaf rule and a
   grain-membership short-circuit at EVERY node (an expression grain member stays
   determined as itself). `_home_determines_grain_member` supplies its own leaf rule
   (`shared_join_key_reroot` or `grain_member_attributable`) and drops "expression never".
   Alternatives rejected: patching `_grain_expression_determined`'s guard (band-aid; two
   rules remain) and a local recursion in `_home_determines_grain_member` (a second copy).
2. **Outer grain: attributable ⇔ `grain_determines(g, union_grain)`.** Delete
   `_grain_expression_determined`, `_non_aggregate_leaf_check`, `_reaggregation_determined`,
   the `expression_determined` list and its constituent-append loop, and the redundant
   `g in union_grain` pre-check. Aggregate-carrying dimensions take the outer plan's
   internal-attach route bare aggregate dimensions already take — which also fixes an
   embedded aggregate grained by a determined non-member (the carrier needed ⊆).
3. **Parameter typing** (`_param_is_determined`): a non-transform value is judged by
   `grain_determines(value)` directly; the ungrained-aggregate shortcut and the transform arm
   stay; `parameter_row_leaves` is no longer consulted there.
4. **Diagnostics — identity vs display.** `_regroup_grain_name` keeps only internal roles
   (producer column names, partition ordering, `partition_display`). A new public, total
   renderer `key_display` in `core.refs` covers every `KIND_POLICY` kind (time buckets
   keep their granularity; BETWEEN, IN, SQL fragments, embedded aggregates render as
   formula text); `dotted_key_display` falls back to it instead of `str()`.
   `_value_key_display` is untouched (it derives result names — identity). Warning/error
   construction takes an explicit key→name map from the query's declared dimensions: the
   first declared name when several aliases intern to one key, else `key_display`. Sites:
   `_UnattributableDim.name` (both), degenerate-warning grains, both `active_td_name`s,
   `_grain_display`.
5. **Broadcast reason from the witness.** A to-one-reachable column witness → "not
   determined by the operand grain — add `<witness>` (or its model's key) to the inner
   partition_by="; a witness whose closure crosses a fanning hop → the existing
   `key_broadcast_reason` (names the hop); an unanalysable closure or unsupported kind →
   "cannot be analysed". Replaces the `key_host_path(g) and grain_member_attributable(...)`
   condition that sent host-local keys to "unreachable … no join path".

## Risks / Trade-offs

- Emitted SQL for re-aggregations over aggregate-carrying dimensions changes (the embedded
  aggregate is computed by the outer plan's attach, possibly one extra producer CTE). No
  golden covers it; new tests pin the shapes by executed values.
- Response texts change: `BroadcastDimension.dimension` for a joined dimension becomes
  dotted; reasons/remedies change. Existing tests asserting `__` spellings or the old
  "unreachable" reason need expected-string updates — list them for user OK before editing.
- Widening determination could admit a shape the carrier/attach machinery cannot render;
  mitigated by the fail-closed kind dispatch and by executed-value tests of every newly
  admitted kind family.
