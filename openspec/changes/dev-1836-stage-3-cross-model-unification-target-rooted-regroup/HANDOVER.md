# DEV-1836 — implementation handover (resume state)

Branch: `egor/dev-1836-stage-3-cross-model-unification-target-rooted-regroup`.
Resume via `/spec` Step 1 (Linear DEV-1836 + this change folder). Absolute paths omitted; all under the worktree root.

## Status at a glance

- **All 168 DEV-1836 tests pass** (`tests/test_dev1836_*.py`, sqlite + duckdb).
- **dev-1835 merged** in (PR #346 review rounds 1–3) — clean auto-merge, no conflicts. HEAD merge commit present; a WIP commit precedes it (`7dc409b8`), plus post-merge bug-fix commits.
- **Lint clean** (`ruff check slayer/ tests/`).
- **~238 pre-existing cross-model tests fail** — these are the **D10-anticipated** golden re-blessings + guard-flips + `_cm_` shape-suite rewrites (NOT bugs). See "Regression reconciliation" below. **This is the next work item and needs the author's go-ahead to modify existing tests** (global rule: never modify tests without consent).
- 2 genuine bugs found + fixed post-merge (query-backed stamping over-stamp; cross-model measure metadata misclassification) — see "Bug fixes".

## What was implemented (Commits 0–4 of the migration plan)

The cross-model aggregate compiler was migrated off the bespoke `_cm_`
(`classify_isolation` → `CrossModelAggregatePlan`) path onto **target-rooted
regroup producers**. A cross-model aggregate now roots a producer at the model
whose rows it aggregates, computes at the fan-out-safe subset of its requested
grain, and broadcasts across the rest.

**Commit 0 — safety primitive + metadata (no query-behavior change):**
- `slayer/engine/join_safety.py` (NEW): `provably_to_one`, `safe_reachable`,
  `audit_join_safety`, `JoinSafetyFinding`. Structural PK/unique proof +
  declared m:1/1:1; composite full-coverage (F6); no synthesized reverse
  traversal (F1).
- DEV-1689 stamping in `query_engine._expand_query_backed_model` (the `cols=[...]`
  build, ~line 2965): stamps `primary_key` on grain columns when the backing
  query aggregates or is dim-only-distinct; excludes reserved-leaf placeholders.
- Cube mapping in `cube/converter.py`: `_map_relationship` + `_RELATIONSHIP_CARDINALITY`
  table, wired into `_convert_joins`; unknown → None + LOSSY_MAPPING warning.
- `cli.py`: `_collect_all_models` + `_print_join_safety_section`, wired into the
  text path of `_run_validate_models`.

**Commits 1–4 — the producer migration (all in `slayer/engine/stage_planner.py`
+ `slayer/sql/generator.py`):**
- New synthesis in `stage_planner.py`: `_synthesize_cross_model_producer`
  (the heart), `_discover_cross_model_combined`, `_is_cross_model_agg`,
  `_key_host_path`, `_attributable_from_root`, `_broadcast_reason`,
  `_grain_member_attributable`, `_shared_join_key_reroot` (query-backed shared
  join keys), `_cross_model_input_paths` + `_assert_cross_model_inputs_safe`,
  `_cross_model_inherited_filters`, `_assert_partition_key_attributable`.
- Integrated into `_plan_regroups`: discovers `cm_combined` (measures/order/
  filters → combined attach) and `cm_row` (computed-dimension roots → row
  attach); both share the placeholder registry / substitution; synthesis loop
  appends `RegroupAttachPlan`s. Windowed cross-model folds the active TD into the
  producer grain (`window_td_key`). D4 nested attaches enabled via
  `enable_producer_regroups`.
- `RegroupAttachPlan` (`planned.py`) gained: `producer_root_model`,
  `dropped_filter_warnings`, `broadcast_measure`, `broadcast_dimensions`.
- `generator._producer_render_bundle` swaps the render bundle's `source_model`
  to the producer's root for target-rooted producers (used at both
  `_prepare_combined_regroup_attaches` and `_prepare_regroup_attaches`).
- Filter routing: `_split_partitioned_filter_conjuncts` routes cross-model
  aggregate predicates to the combined (outer-WHERE) scope.
- Warning collector rebuilt in `query_engine.py`: `_walk_regroup_attaches`,
  `_collect_broadcast_warnings`, `_raise_on_strict_events`; `_collect_dropped_filter_warnings`
  now also walks the regroup IR; `_emit_dropped_filter_warnings` emits the
  broadcast Python warning. Wired at `_prepare_pipeline` (strict raises there).
- New types: `core/errors.BroadcastGrainWarning` (UserWarning carrier);
  `core/warnings.BroadcastGrainWarningPayload` + `BroadcastDimension` (added to
  `AnySlayerWarning`); `SlayerQuery.strict` (+ REST `QueryRequest.strict`, MCP
  `query(strict=...)`).
- Guards lifted (messages removed from source, verified by the matrix residue
  test): row-attach × cm (`generator.py`), cross-model source in computed dim +
  producer-needs-cm-CTE (`stage_planner._plan_regroups`), windowed cross-model
  G3 (`_reject_unsupported_windowed_key`), window+partition cross-model
  (`_guard_partitioned_measures`). CTE-body deferral (`generator.py`) scoped to
  LOCAL combined attaches so a plain cm measure still renders in a CTE body.

## Bug fixes (post-merge, genuine regressions from the migration)

Both from the same root cause — a cross-model MEASURE is now substituted to a
reserved-leaf `ColumnKey` placeholder, which is ROW-phase, so downstream code
that classifies by phase/row-slot misread it:
- `query_engine._expand_query_backed_model`: grain stamp now excludes
  `REGROUP_LEAF_PREFIX` placeholder slots (was stamping a cm-measure column as PK).
  Fixes `test_query_backed_typed_expansion::...canonical_alias_no_longer_resolves`.
- `response_meta.build_response_metadata`: a combined placeholder is a measure,
  not a dimension; `placeholder_original` map routes its label/format to the
  ORIGINAL aggregate key. Fixes `test_response_meta::...format_integer_for_count`.

## Regression reconciliation (the next work item — NEEDS CONSENT)

~238 pre-existing cross-model tests fail because they pin the **replaced**
`_cm_` behavior. Verified by sampling: the SQL my migration emits is **correct**
(e.g. `FROM customers ... GROUP BY ...` target-rooted), just shaped/aliased
differently than the old forward/re-rooted `_cm_` CTEs, and the guard-flip tests
assert guards this change intentionally lifts. Per design **D10** (author-approved):
"_cm_ SQL-shape suites rewritten preserving intent; goldens re-blessed per
approved batch; DEV-1837 matrix cells flip." Categories:
- **Golden SQL re-bless (class b)** — `test_dev1739/1745/1747/1748/1750/1824_golden_sql.py`
  (~95). Re-record goldens; pin executed values unchanged where they were safe.
- **Guard-flip (class b/d)** — `test_dev1837_dimension_measure_matrix.py` (the 4
  `…×cm` strict-xfail cells XPASS → move to supported table), `test_dev1824/1825/
  1829/1835/1837_guards.py`, `test_dev1824_remaining_guards.py`,
  `test_dev1824_computed_dim_execution.py`, `test_sql_generator.py::TestWindowedMeasureGuards`.
- **`_cm_` plan-structure / shape suites** — `test_cross_model_planner_wiring.py`,
  `test_dev1728_derived_shared_grain.py`, `test_dev1747_reroot_filter_routing.py`,
  `test_cross_model_rename_dev1448.py`, `test_dev1445_cross_model_alias_filter.py`,
  `test_dev1450fix_*`, `test_filtered_local_isolation.py`, `test_dev1708_stage4_cte_scope.py`,
  `test_dev1746_*`, `test_dev1748_ranked_plan.py`, `test_dev1769_routed_filter_path_validation.py`,
  `test_dev1733_order_only_transform_composite.py`, etc. Rewrite preserving intent.

To get the current list: `grep FAILED` in the last full-suite run, or re-run
`poetry run pytest -m "not integration" -q`. Baseline before this change: 14522
passed. Current: 14462 passed / 240 failed (2 of which were the now-fixed bugs).

## Still TODO after reconciliation

- **Commit 5 (D7 + retirement):** the total-routing invariant test
  (`test_dev1836_total_routing::test_unrouted_aggregate_raises_explicit_planner_error`)
  passes via the existing "not a query dimension" backstop — a dedicated
  post-discovery invariant is not yet added (optional; low risk to add). Full
  `classify_isolation` / `cross_model_planner.py` **deletion is NOT done** — it
  still handles the LOCAL filtered-local (HOST_ROOTED) and ranked-host paths my
  migration does not cover, so deleting it needs those migrated first OR the
  acceptance criterion relaxed. No DEV-1836 test gates the deletion; the
  guard-message-residue tests all pass.
- **Docs (task 8.2):** `docs/architecture/composable-attach.md`,
  `docs/concepts/queries.md` (strict + broadcast), `formulas.md`, `models.md`,
  `.claude/skills/slayer-query.md`; zensical nav check.
- **tasks.md** checkboxes: Commits 0–4 substantially done; tick after reconciliation.

## Key invariants / gotchas for whoever resumes

- A cross-model MEASURE → reserved-leaf `ColumnKey` placeholder (ROW-phase) at
  the consumer; the value comes from a LEFT-JOINed producer CTE. Anything that
  classifies slots by phase/row-vs-aggregate must special-case
  `REGROUP_LEAF_PREFIX` (see the two bug fixes).
- Attributability rule (`_attributable_from_root`): a host path P is attributable
  from root R (host path = target_path) iff P starts with target_path AND the
  residual is `safe_reachable` from R. Plus `_shared_join_key_reroot` for a
  host-local dim that IS a join key of the single hop to R.
- The producer measure keeps the CANONICAL alias, never the consumer's public
  name (a target model column can shadow it, e.g. `pop` over `regions.pop`).
- `assert_scope_closed` + no `__regroup__` leak are asserted on every DEV-1836
  emission; keep that invariant when touching the generator.
