## Why

A re-aggregation used both on its own and as a constituent of a mixed row-level
aggregation in one query fails closed today with a checker error, although the
values are well-defined and the plain-aggregate analog already works. Two structural
gaps cause it: the planner pre-substitutes re-aggregation roots deeper than discovery
looks, and the renderer orders a hoisted nested-producer CTE by insertion position
instead of a declared dependency. Fixing the class (not the instance) also closes an
adjacent silent-wrong-number gap: a mixed re-aggregation constituent's producers do
not inherit the population-filter disposition, so a fanning-hop population filter
multiplies its carrier's rows.

## What Changes

- Planner: the re-aggregation pre-substitution mirrors root discovery (a consumer-scoped
  substitution beside the consumer walk), so a root nested in a row-attach aggregation's
  source keeps its row-phase attach; the shared placeholder is dual-role and staged BASE.
- Renderer: hoisted CTEs carry declared dependencies through every hoisting depth
  (per-statement dependency scope, typed split entries, the combined preparer emitting
  nodes like the row preparer, the multi-stage pipeline assembled by declared edges);
  the test-harness scope validator additionally rejects any forward CTE reference.
- Re-aggregation producers (standalone root, mixed constituent, and the carrier) inherit
  the population-filter disposition and report its semi-join and dropped-conjunct
  diagnostics like plain producers.
- The checker guard `check_reaggregation_not_standalone_and_mixed` is deleted with its
  raise-ledger row; the pinned fail-closed test flips to executed values.
- Docs: one clause in `docs/concepts/formulas.md`; `architecture/sql.arc42.md` P6 is tagged
  enforced by the new CTE-dependency test.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/partitioned-aggregates`: "Mixed sources carry the full expression-source
  surface" — the deferral scenario becomes an executes scenario; "Nested producers compose
  to arbitrary depth" — a producer attached at two phases emits its nested producer before
  every consumer on every dialect.
- `queries/semantics`: "Filters restrict by association or fail loudly" — a fanning-hop
  population filter restricts a mixed re-aggregation constituent's producers, and
  re-aggregation producers report a dropped out-of-scope conjunct loudly.

## Impact

- `slayer/core/keys.py` (consumer-scoped substitution), `slayer/engine/compile/stages.py`
  and `slayer/engine/compile/regroup.py` (substitution parameter, population-filter and
  diagnostic threading), `slayer/engine/elaborate_env.py` + `slayer/engine/bind_inputs.py`
  (guard removal), `slayer/sql/generator.py` (statement-scoped dependency registry, typed
  split, node-emitting combined preparer, multi-stage assembly), `slayer/sql/scope_check.py`
  (forward-reference validator).
- Tests: four new test modules + one golden baseline; the DEV-1832 pin flips; the
  DEV-1871 ledger row goes; goldens whose CTE order shifts under declared dependencies are
  re-blessed.
- No API or persistence change; `guards.baseline` unchanged (the guard was a ValueError).
