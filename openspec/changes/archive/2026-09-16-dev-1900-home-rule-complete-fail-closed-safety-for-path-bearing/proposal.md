## Why

Every planner predicate that asks "what does this column reference read" inspects the
reference's structural join path only; whether to look inside a derived column's
`Column.sql` is decided ad hoc per collector, so a path-bearing derived reference whose
definition crosses a fanning hop slips past input safety as an argument, a keyword, a
definition default, a re-aggregation parameter, a dimension, or a filter — and the
multiplying join is emitted (every gap in the issue reproduces on a four-line fixture
extension). The five gaps are instances of one omission; fixing them one by one leaves the
class open.

## What Changes

- **One dependency closure.** Every predicate that classifies the row-level data
  dependencies of a column reference — input safety, attributability, grain
  determination, filter disposition — consumes the reference's *dependency closure*: its
  join path plus every path the referenced `Column.sql` crosses, recursively. A derived
  reference therefore behaves exactly like a structural reference in every position.
- **Aggregate inputs fail closed on derived dependencies**: a positional or keyword
  argument, a definition default (bare, dotted, or expression), or a source that names a
  derived column whose definition crosses a hop not provably to-one from the aggregate's
  root fails with the existing unproven-join-hop error; on a re-aggregation such a
  parameter fails the existing parameter-typing error (the grain does not determine it).
  A dependency that cannot be analyzed is unsafe, never empty (new typed error).
- **Definition defaults join the home rule**: the home dataset is chosen over every
  resolved input, defaults included (paths verbatim; reverse-hop cancellation is DEV-1908).
- **Derived fanning dimensions route through the mode axis** (broadcast with a warning,
  associate, or error) like structural fanning dimensions, for local and cross-model
  aggregates alike.
- **Interim population-filter guard**: a row-level filter conjunct that reaches the
  population root only across a fanning hop, in a query keeping an aggregate inline over
  the population rows, fails with a typed error instead of double counting (structural and
  derived alike; DEV-1909 replaces the error with association semantics).
- **One model map**: the source bundle exposes the host-inclusive model map every walker
  and scanner uses, so hand-built bundles resolve reverse hops to the host deterministically.
- Behaviour-preserving otherwise; goldens byte-identical.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/cross-model-aggregates`: MODIFIED *Unsafe aggregate inputs fail closed* — an
  input's dependencies include the definition of any derived column it names,
  recursively, and definition defaults; an unanalyzable definition fails closed.
- `queries/semantics`: MODIFIED *Aggregation parameters are typed by the home dataset's
  grain* — a derived parameter is determined only when every dependency of its definition
  is; MODIFIED *Filters restrict by association or fail loudly* — the interim
  population-filter guard.
- `queries/attribution-modes`: MODIFIED *Query-level mode selection* — a dimension's
  attributability is judged on its dependency closure.

## Impact

- `slayer/ir/source_bundle.py` (`models_by_name`); new `slayer/engine/reference_closure.py`
  absorbing `slayer/engine/aggregate_input_paths.py` and
  `slayer/engine/column_filter_paths.py`; `slayer/engine/filter_reachability.py`,
  `slayer/engine/join_safety.py`, `slayer/engine/compile/stages.py` (parameter resolution
  moves out; home path; safety and attributability call sites; population guard),
  `slayer/engine/elaborate_env.py` (two checker rules), `slayer/engine/binding.py`
  (import), the model-map idiom sweep across engine / ir / sql.
- `tests/_dev1871_raise_ledger.py` (two rows); new `tests/_dev1900_fixtures.py`,
  `tests/test_dev1900_*` suites, `tests/golden/dev1900_sql_baseline.json`.
- `docs/concepts/queries.md`, `docs/concepts/models.md` (one sentence each);
  `architecture/engine.arc42.md` principle 10 (approved normative edit).
