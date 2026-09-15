# Tasks — DEV-1800 planner-owned materialisation stage

## 1. Failing test suite (spec-tests stage)

- [ ] 1.1 `tests/_dev1800_fixtures.py`: dataset where the cross-model inner varies along the
  time axis (customers with signup months Jan/Feb/Mar; orders per customer), SQLite +
  DuckDB seeding, hand-computed oracle table in the docstring; verified by importing it
  from the execution test.
- [ ] 1.2 `tests/test_dev1800_execution.py` (SQLite + DuckDB): the failing class on the
  DEV-1750 dataset (`change(cm) + amount:sum`, `+ *:count`, `+ amount:max`,
  `time_shift(cm,-1) + amount:sum`, `cumsum(cm) + amount:sum`, `change(wscaled) +
  amount:sum`, `iif(change(cm) > 0, cm, amount:sum)`, with a `status` dimension), the
  operand-also-selected invariance, filter-only and order-only positions, direct
  assertions for `change` / `change_pct` of `amount:wscaled_sum` and `customers.spend:sum`,
  and the attributable dataset (`change(orders.amount:sum)` by signup month = [None, 65,
  −93], `change_pct` = [None, 1.857, −0.93]; `change(customers.spend:sum)` by
  `customers.signup_at` = [None, 50, −120]; mixed-grain `change_pct(customers.spend:sum /
  amount:sum)`); cardinality invariant per shape; verified: fails before, passes after.
- [ ] 1.3 `tests/test_dev1800_materialisation_stage.py` (plan structure): stage per slot
  for local agg (BASE), combined placeholder (PRODUCER), row-attach placeholder (BASE),
  transform (CHAIN 1), nested transform (CHAIN 2), transform whose partition key / time
  key is a transform, composite over transform (POST), composite over placeholder
  (COMBINED), computed dimension (BASE, dependency-terminal), masks by typing;
  `needs_column` for the failing shape's hidden leaf, join-back keys, HAVING deps incl.
  hidden local first/last, order-only per stage, mask-only vs mask+projection vs
  mask+order; `series` regime for local composite (False), cross-model composite (True),
  local-partitioned composite (False), nested transform (True), predicate (True);
  validator rejects a later-stage reference, a partially staged plan, and an unstaged
  nested producer plan; generator belt refuses an unstaged plan; traversal contract
  (opaque key raises, TimeTrunc is the slot); cycle raises; verified: fails before, passes
  after.
- [ ] 1.4 Filter-placement executed tests: row mask vs local-aggregate mask, windowed-value
  filter with and without transforms, transform-composite filter; verified on SQLite +
  DuckDB.
- [ ] 1.5 Golden SQL `tests/test_dev1800_golden_sql.py` + `tests/golden/dev1800_sql_baseline.json`
  for the new shapes on postgres, sqlite, duckdb, tsql, bigquery (same harness as
  `test_dev1747_golden_sql.py`); verified by the harness's record/compare loop.
- [ ] 1.6 Transition parity test: stage-derived base column set equals
  `_collect_base_aux_slot_ids`-based `base_render_order` over the law-harness shapes
  (retires with the legacy collector in 3.2); verified by pytest.
  **Carried into §2 commit 1** — needs both the new staging pass and the still-live
  legacy collector to exist together.
- [ ] 1.7 Retarget approved pins: `test_dev1827_value_key_traversal.py::TestCollectBaseAuxSlotIds`
  → staging traversal; `test_dev1777_emit_step_cte.py` post-slot fakes gain
  `stage`/`needs_column`; `test_dev1838_transform_predicate_scope.py`, `test_planned.py`
  (3 sites), `test_dev1733_order_only_transform_composite.py` (1), and
  `test_dev1746_projection_order.py` (2, if hand-built) hand-built slots gain `stage=`;
  assertions unchanged; verified by pytest.
  **Carried into §2 commit 1** — needs the concrete `Stage` API and the deletion order;
  doing it in the spec-tests stage would red 5+ green files on guessed `stage=` values.

**Spec-tests stage status (2026-09-15):** §1.1–§1.5 landed and verified (20 measure-position
cases red today with `RenderContextMissingFacilityError`; the structure suite red at
collection until §2.1 defines `Stage`/`StageKind`/`MaterialisationStageError`). §1.6 and
§1.7 are carried into §2 commit 1 as noted above. Three §1.3 items are deferred to §2 as
white-box work rather than pinned here: the full `series` regime matrix (pinned instead by
the §1.5 golden baseline — shifted-CTE vs window, byte-for-byte), the row-attach-placeholder
BASE stage (no query in the dev1800/dev1750 datasets produces a row-phase attach), and the
staging cycle-raises check (a slot-dep cycle is not representable via public ValueKeys).
Full detail in the DEV-1800 spec-tests resume comment.

## 2. Stage model + staging pass (commit 1 — no consumer change)

- [x] 2.1 `slayer/ir/planned.py`: `StageKind`, frozen `Stage(kind, level)` with total order,
  `ValueSlot.stage: Optional[Stage]`, `needs_column: bool`, `series: Optional[bool]`;
  `PlannedQuery` validator (every slot staged, dep-ordered, strict for transforms,
  recursive into producer plans); `slayer/core/errors.py::MaterialisationStageError`;
  verified by 1.3 validator tests.
- [x] 2.2 `slayer/engine/compile/staging.py`: the one staging function (D3 rules, D4
  needs_column, D6 regime, cycle detection) over `_iter_slot_deps` shared from
  `compile/projection.py`; wired at the end of `compile_prebound` and into every producer
  body; delete `_toposort_slot_ids`'s straggler fallback; verified by 1.3 stage tests and
  the full suite green with goldens byte-identical.
- [x] 2.3 Generator belt in `generate_from_planned` (refuse unstaged plan) next to
  `_validate_transform_input_shapes`; ledger row in `tests/_dev1871_raise_ledger.py`;
  verified by `tests/test_dev1871_raise_parity.py` + `tests/test_law_guard_ratchet.py`.

## 3. Base column set from stage (commit 2 — fixes the class)

- [x] 3.1 Both render paths derive `base_render_order` as BASE ∧ needs_column in plan order;
  `host_combined_ids`, hidden placeholder projection and the outer-trim / hidden-order
  refs read `needs_column`; verified by 1.2 green and 1.6 parity.
- [ ] 3.2 Delete `_collect_base_aux_slot_ids`, `_composite_has_remote_operand`,
  `_add_local_aux_slots`, the outer-composite walk + DEV-1838 `continue`,
  `order_only_local_ids`, the leaf loop; `_build_base_select_for_planned`'s foreign-phase
  `continue` → invariant; retire 1.6; verified by goldens byte-identical (divergences →
  individually approved with executed-value parity).

## 4. Chain, post step, regime from stage (commit 3)

- [ ] 4.1 `_run_transform_chain` groups layers by slot level (window batch, then
  time_shift, then cp per level); delete `_transform_layer_deps_ready`,
  `_classify_ready_transform_layers`, the deadlock RuntimeError; verified by transform
  goldens byte-identical.
- [ ] 4.2 `_unmaterialised_post_slots` = POST ∧ needs_column; regime read from
  `slot.series`; delete `_classify_walk` / `_classify_time_shift_composite` /
  `_time_shift_series_mode`; verified by 1.3 regime tests + series-shift goldens.

## 5. Filters, order, isolated sets from stage (commit 4)

- [ ] 5.1 `_lower_positions` placement = f(stage, mask typing); `_classify_order_scope`
  derived from stage + attach kernel + hidden; delete `_combined_placeholder_slot_ids`,
  `_windowed_agg_slot_ids`, `_composite_reads_an_isolated_cte`; verified by 1.4 and the
  DEV-1747 / DEV-1745 suites.
- [ ] 5.2 Divergence ledger: list every golden diff the rule produced with executed-value
  parity evidence in the PR description; each approved before re-bless; verified by the
  ledger matching the re-blessed baselines.

## 6. Docs, architecture, hygiene (commit 5)

- [ ] 6.1 `docs/concepts/formulas.md` nesting paragraph: one sentence on composites mixing
  a transform with other aggregates; verified by grep.
- [ ] 6.2 arc42 (approved text, re-presented as the exact diff before applying):
  `engine.arc42.md` P6 with `[enforced: test:tests/test_dev1800_materialisation_stage.py]`,
  `sql.arc42.md` P11 clause; verified by `poetry run python tools/arch_check.py`.
- [ ] 6.3 `poetry run pytest -m "not integration" -n auto`, `poetry run ruff check slayer/
  tests/`, `poetry run basedpyright` (baseline not grown), `npx -y likec4@1.47.0 validate
  architecture`; verified green.
- [ ] 6.4 Merge `origin/main` forward once DEV-1859 lands; re-run 6.3 and `openspec
  validate --strict`; verified green.
