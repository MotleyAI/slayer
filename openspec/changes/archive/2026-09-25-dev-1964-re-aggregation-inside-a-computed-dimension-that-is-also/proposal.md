## Why

An aggregate expression consumed inside a computed dimension AND in another position
(measure, measure-typed filter, order target) leaks internals — a row-phase placeholder
in HAVING, a `MaterialisationStageError`, an unresolved hidden ORDER BY slot, an internal
`grain` name collision, or a join-back `RuntimeError` instead of the typed
`PartitionKeyError`. Each is an Axiom 9 closure violation reachable from a well-formed query.
The cause is structural: the discovery walk reports every (root, phase) occurrence, but the
planner, the dimension classifier and the partition-key rule each diverge from it.

## What Changes

- A re-aggregation consumed at several attach phases attaches once per phase over one
  interned producer and one placeholder (the "row wins" precedence is removed), like every
  other producer root.
- A re-aggregation is classified by its type: an explicitly grained re-aggregation inside a
  dimension transform (`rank(R)`) makes that transform a dimension transform root, which
  owns the re-aggregation's whole subtree.
- A re-aggregation in a computed dimension may declare an outer grain finer than the query
  dimensions (like a plain partitioned aggregate already may); it is synthesized at its own
  declared grain.
- The "partition key must be a query dimension" rule is decided once, in the checker, after
  filters and order targets are typed, for plain, cross-model and re-aggregation outer keys
  alike; the position-blind re-aggregation copy of the rule is removed.
- Synthesized producer grain names are unique per key; two dimension-borne producers never
  collide.
- An ORDER BY on an expression that is also a computed dimension's transform or
  re-aggregation resolves at plan time.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: the combined-consumer partition-key requirement is
  clarified to apply after position typing and to re-aggregation outer keys in every
  position; new scenarios for the dimension + measure-typed filter shapes.
- `queries/computed-dimensions`: a new requirement — the same aggregate expression
  consumed in a computed dimension and in another position evaluates per position.

## Impact

- `slayer/engine/compile/stages.py` — regroup grouping (`_group_reaggregation_roots` removed,
  folded into `_group_routed_roots`), re-aggregation synthesis, grain naming.
- `slayer/engine/compile/discovery.py` — dimension walk ownership.
- `slayer/engine/elaborate_env.py`, `slayer/engine/bind_inputs.py`, `slayer/engine/elaborate.py`
  — type-based classification; post-typing partition-key check.
- `architecture/engine.arc42.md` P4 — one clause (approved).
- SQL goldens may be re-blessed (values unchanged).
