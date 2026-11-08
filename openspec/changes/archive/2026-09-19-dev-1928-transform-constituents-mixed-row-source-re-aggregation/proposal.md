## Why

DEV-1832 pinned three well-typed shapes as fail-closed typed errors and deferred them
here: a mixed row source with a re-aggregation constituent (the `first`/`last` collapse
`sum(amount * last(X))` and the general hand-written `sum(amount * min(X, partition_by=region))`),
a windowed inner under a transform constituent, and a cross-model grained inner under a
transform constituent. Under Axiom 9 a well-typed term refused for an implementation
reason is a closure violation: the first two are exactly that (the hand-written form even
surfaces a user-reachable internal assertion today), while the third is a correct Axiom 8
mode-axis rejection that must stop masquerading as a deferral.

## What Changes

- **Re-aggregation constituents in a mixed source.** An aggregation source mixing
  row-level leaves with a re-aggregation constituent — an aggregate over attached values,
  hand-written or produced by the `first`/`last` collapse — compiles and executes: the
  constituent is evaluated at its own grain and broadcast per partition onto the source's
  rows (an empty grain broadcasts one value onto every row). The DEV-1832 fail-closed
  guard for the collapse-in-mixed shape is removed, and the general form no longer reaches
  the internal grain-cover assertion. A re-aggregation attached as a parameter is covered
  by the same path.
- **Windowed inner under a transform constituent.**
  `sum(rank(amount:sum(window='90d', partition_by=region)))` executes: the query's active
  time bucket reaches the nested producer, so the windowed inner resolves it and the
  constituent is grained by it.
- **Cross-model grained inner under a transform constituent is a permanent boundary in
  every mode.** A target-homed inner whose `partition_by=` names the host's time axis — an
  `orders` column reachable from the inner's `customers` home only across the fanning
  `customers → orders` hop — stays refused by the partition-key attributability error in
  every mode, associate included; reclassified from a deferral to the Axiom 8
  mode-invariant input-safety rule it is (a fanning-crossing partition key, per DEV-1911).
  The associate-mode probe (task 1.4) confirmed it raises there too, so the value —
  well-defined under distinct-entity association — is tracked as a follow-up (DEV-1941),
  not delivered here. No axiom change.
- The `time_dimension=` axis kwarg idea is dropped (not pursued, no issue).
- **One bounded new boundary.** A re-aggregation used both on its own (a standalone
  re-aggregation) and as a mixed row-level constituent in the same query fails closed
  with a typed checker error: the one shared producer would need attaching at two phases
  and its nested-producer CTE then emits out of dependency order on strict dialects.
  Deferred to DEV-1942, never a dialect-inconsistent result.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/partitioned-aggregates`: MODIFIED *Re-aggregation consumes attached operands
  as datasets* (the windowed-inner scenario becomes an executed one; the cross-model
  grained inner naming a host time axis is recorded as a permanent attributability
  boundary in every mode — the fanning-crossing time key is a mode-invariant input-safety
  error, associate included, tracked forward as DEV-1941 — beside a positive
  pin for a to-one cross-model partition key) and *Mixed sources
  carry the full expression-source surface* (executed scenarios for a re-aggregation
  constituent — hand-written, collapse-produced, empty-grain, as a parameter, combined with
  a coarser measure, in filter and order position — and the mode axis).

## Impact

- `slayer/engine/compile/stages.py` (row-attach path: attached-constituent synthesis
  context, join pairs from the producer's projected grain; the nested producer receives
  the query's time dimension), `slayer/engine/elaborate_env.py` and
  `slayer/engine/bind_inputs.py` (collapse-in-mixed guard removed; a new checker guard
  `check_reaggregation_not_standalone_and_mixed` added for the standalone+mixed boundary,
  with its `tests/_dev1871_raise_ledger.py` row — a `ValueError`, so `guards.baseline`
  stays unchanged).
- Tests: `tests/test_dev1832_transform_source.py` (three pins flipped or reclassified, new
  executed cases), `tests/test_dev1832_fixtures_smoke.py` (oracle derivations),
  `tests/_dev1871_raise_ledger.py` (row removed), `tests/test_dev1832_golden_sql.py` +
  `tests/golden/dev1832_sql_baseline.json` (named new cases only).
- Docs: `docs/concepts/formulas.md` (one sentence).
- No arc42 or `index.yaml` change; `guards.baseline` unchanged (the removed guard is a
  `ValueError`, not a ratcheted `NotImplementedError`).
- DEV-1832 has landed and archived on main (merged here 2026-09-18); the PR targets main.
