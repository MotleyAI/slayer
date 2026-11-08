## Why

The binder accepts a literal, a nested aggregate or a column reference as an aggregation
argument and refuses a transform, so a grained transform can never be an aggregation
parameter (`weight=rank(...)`, `weight=cumsum(...)`) although the spec already says the
parameter may be "an aggregate or a grained transform" and Axioms 2.3 / 11.4 make the
parameter and source-constituent positions indistinguishable — a "not supported inside"
refusal of a well-typed term (Axiom 9). The refusal is one symptom of a structural gap:
"attached value" is classified by hand at every site, and only the source position was
generalised to transforms, so lifting bind alone leaves five parameter shapes wrong
(no determination check, no axis check, no query-grain normalisation of ungrained inners,
no first/last collapse, and an association / re-aggregation path that silently misreads
the parameter).

## What Changes

- The binder binds a transform argument to a transform key; positional transforms fold
  onto the declared parameter name like any positional value.
- One attached-input law: every rewrite and check that classifies an aggregation's
  attached values — the query-grain normalisation of ungrained transform inners, the
  first/last collapse lowering, the time-axis check, the parameter resolver, the
  partition-key leniency of attached operands — covers the source, positional and keyword
  positions through the one existing classifier, instead of the source alone.
- One attached-parameter grain: a transform parameter is typed at its result grain (the
  union of its inner grains, the active bucket joining when an inner is windowed, the
  axis dropped for `first`/`last`), and the existing home-determination (every mode) and
  operand-grain (re-aggregation outer) checks consume that grain — so an undetermined
  transform parameter is refused with the existing typed error, never a value.
- A transform parameter is picked like an aggregate parameter by the association and
  trailing-window kernels, and rides the carrier of a re-aggregation outer.
- The three "(Target behaviour, deferred to …)" placeholders on the attached-parameter
  requirement are retired, and the two strict xfails citing DEV-1903 are removed.
- Axiom 11.4 names the parameter position beside the source-constituent position (arc42
  edit approved 2026-09-21).

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: "Attached parameters on row-level sources" — a
  grained transform parameter executes at its result grain in every position and mode,
  and fails closed exactly where an aggregate parameter would; "Re-aggregation consumes
  attached operands as datasets" — the outer parameter may be a grained transform at the
  operand grain.
- `aggregations/functional-form`: "Positional parameters fold onto declared parameter
  order" — a positional transform parameter folds onto the declared name.

## Impact

- `slayer/engine/binding.py` (`_bind_agg_arg`), `slayer/core/keys.py` (argument union,
  the attached-input rewriter, the attached-parameter grain, the two normalisers, the
  lenient attached set), `slayer/core/refs.py` (alias fragment),
  `slayer/engine/reference_closure.py` (parameter resolver),
  `slayer/engine/elaborate_env.py` (time-axis check), `slayer/engine/compile/stages.py`
  (home-determination check, association arm, re-aggregation outer, context threading).
- Tests: two markers removed in `tests/test_dev1919_home_rooted_attached_inputs.py`; new
  `tests/test_dev1946_transform_parameter.py` + `tests/_dev1946_fixtures.py`; two golden
  cases in `tests/test_dev1859_golden_sql.py`.
- Docs: one sentence each in `docs/concepts/formulas.md` (two places) and
  `docs/concepts/queries.md`; arc42: Axiom 11.4 wording + an `enforced:` tag.
- Guards baseline and legacy-arrow baseline unchanged; no new kernel; expression-valued
  parameters stay refused (out of scope).
