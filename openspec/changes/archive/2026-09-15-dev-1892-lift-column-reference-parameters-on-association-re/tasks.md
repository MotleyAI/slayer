# Tasks — DEV-1892 column-reference parameters on the two-level kernel

## 1. Failing test suite (spec-tests stage)

- [x] 1.1 `tests/test_dev1892_parameter_typing.py` — executed values on SQLite + DuckDB:
  association `weighted_avg(weight=customers.spend)` (ok 43400/420, new 34100/290),
  `wsum` default (ok 43400, new 34100), `weight=customers.regions.pop` (NULL weight
  for c4), explicit vs positional spelling parity; re-aggregation
  `weighted_avg(INNER_CR, weight=count(id, partition_by=[city, region]))` by region
  with a hand-computed oracle from the `sales` seed rows, equal to a manual two-stage
  `source_queries` encoding; `corders` operand grain `customer_id` with
  `weight=customers.region_id`; explicit outer `partition_by=` with a parameter;
  degenerate identity re-aggregation with a parameter; verified: all fail before
  implementation, pass after.
- [x] 1.2 `tests/test_dev1892_residue.py` — typed errors at plan time, ref-free (no
  `DEV-\d+`): `wavg(INNER_CR, weight=id)`, the column-default `wavg(INNER_CR)`,
  `weight=sum(amount, partition_by=product)` over a `[city, region]` operand, the
  attached-parameter-on-row-level-source shape in all three modes; REST surface returns
  400 for the residue; verified by pytest.
- [x] 1.3 `tests/test_dev1892_plan_structure.py` — the kernel carries `picked_params`;
  a parameter aggregate's kwarg is a registered placeholder resolving through the carrier;
  the same parameter aggregate consumed twice interns to one producer; depth-2
  re-aggregation with a parameter; bind-stage regression: source and weight share an
  inner grain absent from the query dimensions (no partition-key error);
  `assert_scope_closed`, flat `WITH`, cardinality invariant; verified by pytest.
- [x] 1.4 `tests/test_dev1892_fold_equivalence.py` — the folded pick path keeps executed
  values for expression source, derived `Column.sql` source (local and cross-model),
  explicit `column_type`, `*:count`, measure-local filter on a bare target column and on a
  target-relative to-one path, NULL parameter values; definition default whose bare
  column name exists on both host and target, and a target-relative expression default;
  verified by pytest.
- [x] 1.5 Determination pair (spec: semantics › nested-path seeds): outer dimension
  seeded by `partition_by=customers.id` → attributable, no warning; seeded only by
  `customers.region_id` → unattributable; verified by pytest.
- [x] 1.6 `tests/test_dev1892_golden_sql.py` + `tests/golden/dev1892_sql_baseline.json`
  across the seven Tier-1 dialects for the lifted shapes; verified by the golden harness.
- [x] 1.7 Re-point approved existing tests: `test_dev1841_association_errors.py::
  TestColumnReferenceParameter` → executed-value; `test_dev1841_surfaces.py::
  test_association_slayer_error_maps_to_400` → residue shape; `test_dev1847_gate.py`
  two column-parameter tests → the one-rule message; verified by pytest.

## 2. Core and bind

- [x] 2.1 `slayer/core/keys.py`: `_AggregateArgValue` admits `AggregateKey`;
  `reaggregation_operand_keys` walks a re-aggregation root's args and kwargs; verified by
  1.3's bind-stage regression.
- [x] 2.2 `slayer/engine/binding.py`: `_bind_agg_arg` binds an `AggCall` through `_bind`;
  positional values fold onto declared names as today; verified by 1.1 parity cases.
- [x] 2.3 `slayer/engine/bind_inputs.py`: call `check_attached_param_requires_attached_source`
  on pre-lowering roots next to `check_time_shift_input`; verified by 1.2.

## 3. Checker

- [x] 3.1 `slayer/engine/elaborate_env.py`: add `check_parameter_determined` and
  `check_attached_param_requires_attached_source` (ref-free messages, remedies; DEV-1859
  pointer in the latter's docstring only); delete `check_association_column_param` and
  `check_reaggregation_no_column_param`; verified by `tests/test_dev1871_raise_parity.py`
  after 6.1.

## 4. Compiler

- [x] 4.1 `slayer/engine/join_safety.py`: `grain_determines` (grain member; aggregate
  grained ⊆ grain; column seeded from a grain member's model at any path over provably
  to-one hops); `_reaggregation_determined` / `_grain_expression_determined` delegate to
  it; verified by 1.5 and the DEV-1847 suites.
- [x] 4.2 One parameter-resolution helper (explicit non-scalar args/kwargs +
  non-overridden definition defaults; bare identifier → `ColumnKey` in owner coordinates,
  expression → owner-anchored Mode-A fragment); verified by 1.4's default cases.
- [x] 4.3 `_synthesize_association_producer`: after input safety, resolve → check against
  requested ∪ entity keys → `picked_params`; gate call removed; re-point the stale
  DEV-1892 comment to DEV-1884; verified by 1.1 association cases.
- [x] 4.4 `_synthesize_reaggregation_producer`: resolve → check against the union grain →
  augment constituents → placeholder map → carrier → substitute the whole outer key →
  `picked_params`; gate call removed; verified by 1.1/1.3 re-aggregation cases.

## 5. IR and renderer

- [x] 5.1 `slayer/ir/planned.py`: `PickedParam` (Pydantic) and
  `AssociationProducerKernel.picked_params`; verified by 1.3.
- [x] 5.2 `slayer/sql/generator.py`: one pick helper (type-branched: derived expansion /
  `render_value_key` / owner-anchored Mode-A entry; `column_type` cast and owner-anchored
  measure-local filter preserved); both `_v` branches replaced by it; parameters picked
  as `_p<i>`; level-2 `agg_kwargs` reference `_base._p<i>` as `kind="expr"`; delete
  `_assert_association_no_column_default_params`; verified by 1.1/1.4/1.6.

## 6. Ledger, goldens, suite

- [x] 6.1 `tests/_dev1871_raise_ledger.py`: two rows out, two in; verified by the parity
  test.
- [x] 6.2 Full golden suite: every moved key (DEV-1841 `assoc/*`, any other) enters its
  module's `ALLOWED_DELTAS` with the reason "DEV-1892: one pick path for value and
  parameters", is re-blessed with `SLAYER_UPDATE_GOLDEN=1`, manifest emptied; verified by
  the golden harness green with empty manifests.
- [x] 6.3 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`,
  `poetry run basedpyright` (no new errors), `poetry run python tools/arch_check.py` all
  green.

## 7. Docs, architecture, Linear

- [x] 7.1 Docs, one sentence each: `docs/concepts/queries.md` associate-eligibility list
  (drop the column-reference-parameter exclusion), `docs/concepts/formulas.md`
  re-aggregation section (parameters are cells of the operand dataset or columns its
  grain determines), `docs/examples/07_aggregations/aggregations.md` (one weighted
  re-aggregation example); verified by grep for the stale exclusion wording.
- [x] 7.2 `architecture/semantics.arc42.md` axiom 2 `[review]` →
  `[enforced: test:tests/test_dev1892_parameter_typing.py]` — present the exact diff
  and wait for the OK before editing; verified by `tools/arch_check.py` green.
- [x] 7.3 Linear: append the post-merge function/line names to the DEV-1859 deferral
  comment (spec-review stage); verified by the posted comment.
