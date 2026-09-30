## Why

A second-order aggregation broadcasts across an outer dimension that is a row-level
expression over the operand grain's columns (`city == 'Alpha'`, `upper(city)`,
`customers.regions.name == 'North'`) instead of partitioning its cells exactly — a
silently wrong, repeated coarser value, usually with a spurious warning. The cause is
structural: "does X determine this key" is answered by several predicates with different
composite handling, so a determined expression is refused where its leaf spelling is
accepted (Axioms 2.2 and 2.7). The same class also refuses a cross-model attached
parameter grained by a computed dimension over home-determined columns, and lets an
aggregate-carrying expression grained by a determined non-member broadcast while its bare
spelling partitions.

## What Changes

- Determination becomes closed under row-level combination through ONE combinator:
  literals are determined, time buckets through their column, a derived column when every
  column its definition reads is, composites (arithmetic,
  comparison, scalar calls, conditionals, BETWEEN, IN) and bound parameter fragments iff
  every child is, embedded aggregates iff their `partition_by=` members are; any other
  kind fails closed.
- A re-aggregation's outer dimension is attributable iff the operand grain determines it;
  the separate expression-carrier arm is removed, so aggregate-carrying dimensions take
  the same route as bare aggregate dimensions.
- Aggregation-parameter typing and the model-home check of attached-parameter grains use
  the same rule (a computed-dimension grain member over home-determined columns is legal).
- Warnings and errors name dimensions by their query name (declared name, else canonical
  dotted path / formula text) — never `grain_<hash>` or `__`-joined aliases; the
  re-aggregation broadcast reason names the undetermined witness and the right remedy.
- Visible value change: `BroadcastDimension.dimension` for a joined dimension changes from
  `customers__regions__name` to `customers.regions.name` (bug fix).
- Out of scope: volatile functions (treated as deterministic), a finer time bucket
  determining a coarser one (fails closed), the identity-based degenerate check
  (unchanged), inline expression parameters (refused at bind; DEV-2016).

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: "Second-order aggregation over attached values" (determination
  closed under combination + scenarios), "Aggregation parameters are typed by the home
  dataset's grain" (aggregate parameters judged by determination of their `partition_by=`
  members + computed-dimension grain scenario), "Loud degradation" (dimensions named by
  query name, never an internal alias).

## Impact

- `slayer/engine/join_safety.py` — the determination combinator (witness form) and
  `grain_determines` over it.
- `slayer/engine/compile/stages.py` — re-aggregation attributability, `_param_is_determined`,
  `_home_determines_grain_member`, diagnostic naming and broadcast reasons; removal of
  `_grain_expression_determined`, `_non_aggregate_leaf_check`, `_reaggregation_determined`.
- `slayer/core/refs.py` — a public, total key display renderer; `dotted_key_display`
  falls back to it.
- `docs/concepts/formulas.md`; `architecture/semantics.arc42.md` (enforcement tags only on
  Axioms 2 and 7).
- Emitted SQL for re-aggregations over aggregate-carrying dimensions changes (no golden
  covers it); response warning/error texts change for expression and joined dimensions.
