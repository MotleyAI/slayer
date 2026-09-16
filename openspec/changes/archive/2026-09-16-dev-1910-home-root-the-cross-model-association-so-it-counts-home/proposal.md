## Why

Under `to_many_handling: "associate"`, a cross-model aggregate associated over an
unattributable dimension must aggregate over its own home dataset, each home row once
(Axioms 3, 4 and 8 in `architecture/semantics.arc42.md`). The association producer is
host-rooted, so a home entity with no population row — a customer with no orders — never
enters a cell: `customers.spend:sum` by a regions-level dimension gives South = 140 where
the oracle is 195. The host rooting is a second copy of the target-rooted producer's
rooting, filter-disposition, compile and attach logic; the bug is what the copy costs.

## What Changes

- **Association is defined by the home's join path.** An entity belongs to a cell iff its
  own join path from the aggregate's home dataset reaches the cell's dimension values, so a
  home entity absent from the population still counts in the cells its path reaches. A
  dimension reached only back through the population root associates an entity only when a
  population row carries it — never a manufactured NULL cell — while a population rooted at
  the home keeps its own LEFT-JOIN NULL cell.
- **The association producer roots at the home.** One target-rooted synthesis with an
  association arm (entity dedup kernel) and a plain arm (broadcast / error), sharing root,
  grain split, filter disposition, compile and attach; the host-rooted synthesis is deleted.
- **Reachable filters apply inline on the association's joins.** Every reachable conjunct
  is inlined on the home-rooted level 1 (the per-entity dedup makes the fan-out harmless),
  so membership equals the semi-join semantics and a conjunct sharing a hop with an
  association dimension is satisfied by the same related row (as the existing oracle
  `channel = 'app'` by `status` already requires). The informational entry a semi-join-pushed
  conjunct carries is kept for these conjuncts. Out-of-scope conjuncts stay dropped and
  warned. A population rooted at the home follows the same rule (a customers-rooted
  `spend:sum` by `orders.status` filtered on `orders.channel` couples the filter and the
  dimension instead of the current decoupled EXISTS).
- **The reverse hop generalizes to a reverse path**, so a home two hops from the population
  root reroots population-side dimensions and filters; a chain of provably one-to-one reverse
  hops becomes attributable.
- Behaviour-preserving wherever every home entity has a population row; goldens re-blessed
  for the changed association SQL only.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities
- `queries/attribution-modes`: "Distinct-entity association semantics" — association by the
  home's join path; home entities absent from the population count; presence rule for
  dimensions reached through the population root.
- `queries/cross-model-aggregates`: "Producer filter routing" — the association producer is
  home-rooted and applies reachable conjuncts inline on its joins, same-row coupling with
  association dimensions, informational entry kept.

## Impact

- `slayer/engine/compile/stages.py` (one producer synthesis, two arms), `slayer/engine/join_safety.py`
  (reverse path, host-locus wraps never re-routed), `slayer/ir/planned.py` (kernel presence keys,
  attach-plan restricted-filter texts), `slayer/sql/generator.py` (level-1 presence guard),
  `slayer/engine/query_engine.py` (informational entry source).
- Tests: the DEV-1910 strict xfail is removed; DEV-1841 / 1847 / 1859 / 1892 / 1900 goldens with
  association SQL are re-blessed; one DEV-1841 plan-shape test flips from semi-join to inline.
- Docs: one sentence in `docs/concepts/queries.md`; `architecture/semantics.arc42.md` Axiom 8
  gains an enforcement tag (approved).
