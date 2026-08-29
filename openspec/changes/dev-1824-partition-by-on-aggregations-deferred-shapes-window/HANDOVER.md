# DEV-1824 implementation handover

Resume with `/spec "continue on branch egor/dev-1824-partition_by-on-aggregations-deferred-shapes-window"`.
We are in **Step 6 (implement until tests pass)**. The plan is `design.md`
(decisions D1–D11), acceptance criteria are the `tests/test_dev1824_*` suites,
the live checklist is `tasks.md`. This file is the fast-start map: current state,
exact changes, the precise remaining plan, the architecture, and the gotchas.

Working directory: this worktree. Run tests with
`poetry run pytest -m "not integration" -q`. The **invariant after every change**:
`… | grep -E "^(FAILED|ERROR)" | grep -v dev1824` must be EMPTY — no non-DEV-1824
regression. Existing goldens stay byte-identical except approved divergences.

---

## 1. Status — COMPLETE: 247 DEV-1824 tests pass (incl. golden), 0 non-1824 regressions, ruff clean

**Session 2 — feature complete.** All guard lifts landed (window=, first/last,
transform-nesting, filter, computed-dimension symmetry incl. transform/window/
first-last in a dimension), the producer CTE-hoist works on all 5 dialects, and
the BigQuery/T-SQL dotted-alias round-trip is repaired by the scope-aware
`unmangle_dotted_table_refs`. The golden baseline (`dev1824_sql_baseline.json`)
is blessed. Docs (architecture/composable-attach.md + nav, queries/formulas/
aggregation examples, slayer-query skill, DECISIONS) updated. Remaining before
merge: Step 7 commit/push/PR (git add the NEW files listed in §8 below).

Only deferred (stage 3, still fail closed): CROSS-MODEL sources in any composed
shape. The one non-test hardening left is the explicit D8 producer-unique-key
FIELD (task 2.4) — cardinality neutrality is proven by executed-value tests.

## 1b. Original status — 77/117 DEV-1824 tests pass, 0 non-1824 regressions, ruff clean

The `test_dev1824_golden_sql.py` suite additionally ERRORs on every case until
the baseline is blessed (task 4.1) — that is its designed missing-baseline state,
not a failure to chase per-case.

**Landed + verified (7 lifts + 1 dialect fix):**
- **2.1** allocator threading + producer `(ctes, select)` split — byte-identical.
- **3.4** first/last + partition (local). Producer collapses to a ranked CTE via
  `_collapses_to_ranked_cte` (no hoist). Temporal partition key does not hijack.
- **3.5** transform over a LOCAL partitioned aggregate as a measure.
- **3.8** combined-attach coexistence with cross-model / windowed / ranked /
  transform measures.
- **3.2** row+combined coexistence (core) — 14/16 tests; the 2 ORDER-BY-raw cases
  are D9 (task 2.6, still failing).
- **3.7 (partial)** first/last inside a dimension expression (bare and in a CASE).
- **SQLite temporal-cast bug** (pre-existing) — a first/last VALUE of a temporal
  column is no longer CAST to its declared type (SQLite gave it numeric affinity,
  truncating a date to its year). Regression test:
  `tests/test_dev1824_ranked_temporal_cast.py` (NEW — needs `git add`).

**Test-reconciliation policy = A1** (Egor's call): a lift that supersedes a
DEV-1739/1740/1829 deferral test → rewrite that test to assert the NEW behavior,
reviewed in the PR. Log in §7.

**Follow-up work tracked:** DEV-1835 (Stage 2) + DEV-1836 (Stage 3) created, with
worktrees `slayer.worktrees/egor__dev-183{5,6}-…` off `f9013934` — merge stage 1
into them once DEV-1824 lands.

---

## 2. Exact production changes so far

`slayer/sql/generator.py`
- import: `REGROUP_LEAF_PREFIX`, `_ranked_value_cast_type`.
- `generate_from_planned(…, reuse_allocator=False)` — renders against this
  generation's allocator instead of a fresh one (D2 hoist precondition).
- `_render_producer_split(producer, bundle)` (NEW) — renders a producer with
  `reuse_allocator=True`, splits any internal `WITH` into hoistable `(name, ast)`
  pairs, returns `(hoisted, body_sql)`. Used by both `_prepare_regroup_attaches`
  (row) and `_prepare_combined_regroup_attaches` (combined).
- coexistence guard (`_generate_from_planned_impl`, ~L1595): combined attach may
  coexist with isolated features and with a row attach; row attach + isolated
  feature / CTE-body still raises.
- transform auto-partition (`_render_window_transform_sql`, ~L6690): skip
  `REGROUP_LEAF_PREFIX` ROW slots.
- `_render_cross_model_transform_chain` prelude (~L4960): include `*cm_regroup_ctes`
  and `*[(e.name,e.query) for e in row_regroup_ctes]`.
- combined-SELECT placeholder projection (~L4650): a HIDDEN placeholder stays
  projected under its `_cm_` column when `transform_layers` (transform input).
- base ROW-slot render (~L3009): a placeholder `ColumnKey` resolves via
  `regroup_env` (bare-aggregate computed dimension), GROUP BY the attached value.
- `_render_with_cross_model_plans` (~L3898, ~L4216, ~L4227, ~L4980): prepares ROW
  regroup attaches and threads `regroup_env`/`regroup_join_specs` into the `_base`
  build + where/having, and adds row producer CTEs to the WITH (`_base` depends on
  them). This is what makes **row+combined** render.
- ranked first/last VALUE cast (~L3707): `_ranked_value_cast_type(agg_slot.type)`
  suppresses the temporal cast (SQLite fix).

`slayer/engine/stage_planner.py`
- `_guard_partitioned_measures` — first/last arm and transform arm narrowed to
  CROSS-MODEL only (local lifted). window= arm and filter arm still fire (tasks
  3.3/3.6).
- `_windowed_grain_partition` — skip `REGROUP_LEAF_PREFIX` ROW slots.
- `_plan_regroups` (~L1372) — row+combined coexistence allowed (guard removed);
  producer-needs-CTE guard (~L1463) now allows a RANKED producer (windowed /
  transform / cross-model / nested-regroup still deferred).
- `_guard_computed_dimension` (~L2837) — first/last-in-dimension arm removed
  (local lifted); transform arm and window arm still fire.

`slayer/engine/ranked_planner.py`
- import `REGROUP_LEAF_PREFIX`; `_host_grain` skips placeholder ROW slots.

`slayer/sql/render/value_expr.py`
- `_ranked_value_cast_type(dt)` (NEW) — None for DATE/TIMESTAMP, else `dt`.

**One principle recurs:** a COMBINED regroup placeholder is a `ColumnKey` ROW
slot by substitution but is an aggregate VALUE, so every grain-derivation over
row slots (transform auto-partition, `_windowed_grain_partition`, `_host_grain`)
MUST exclude `REGROUP_LEAF_PREFIX` leaves.

---

## 3. Remaining work (dependency-ordered, with the concrete next step)

### 3.6 / 2.5 — filter on a partitioned aggregate (10 sub-tests: `TestFilterOnPartitioned`)
D7. `bind_filter` does NOT split top-level `AND`s (one predicate = one
`BoundFilter`), so the router must split conjuncts itself — but ONLY for filters
touching a partitioned aggregate (splitting all filters churns existing
goldens). Steps:
1. Lift the filter arm of `_guard_partitioned_measures` (currently raises
   "Filtering on a partition_by aggregate").
2. Extend discovery: `combined_partitioned_aggregates` (in `regroup_planner.py`)
   currently walks measures + orders only — add `bound_filters` so a filter's
   partitioned aggregate gets a combined producer + placeholder.
3. Per top-level conjunct, resolve each operand's AVAILABILITY SCOPE (base-row /
   base-grouped(HAVING) / combined(post-attach)); a raw non-dimension column
   (e.g. `status`) is base-row-ONLY, a partitioned-agg placeholder is
   combined-ONLY, a plain aggregate is {base-grouped, combined}, a dimension is
   {base-row, combined}. Route the conjunct to the earliest scope in the
   INTERSECTION of its operands' scope-sets; empty intersection → clean "split the
   filter" error (test `test_no_common_scope_fails_closed`).
4. Placement decided PRE-substitution (substitution lowers phase to ROW —
   `regroup_planner.substitute_in_bound_filter`), carried into
   `PlannedQuery.outer_where_filter_ids` (the existing outer-WHERE hook the
   generator already consumes at `_render_with_cross_model_plans`). The generator
   already resolves a combined placeholder operand in a WHERE via
   `regroup_placeholder_to_cm` (~L3951).
5. `partition_by=[]` filter → keyless CROSS-JOIN producer (already works for
   measures; verify for filters).

### 2.6 — D9 order-form discrimination (4 sub-tests: `TestRowCombinedCoexistence` order cases)
Ordering by the RAW partitioned aggregate `amount:sum(partition_by=city)` (top-
level `AggregateKey`) must route to a COMBINED attach; ordering by a computed-
dimension NAME (value_key is the CASE expr / dimension key) keeps ROW routing.
Discriminate by whether the order-spec's value_key IS a partitioned `AggregateKey`
at top level (→ combined) vs a computed expression (→ row). Then:
- `combined_partitioned_aggregates`' order-spec loop currently EXCLUDES keys in
  `row_agg_set` — for a raw-aggregate order it must NOT exclude (D10 duplicate
  producer is fine).
- The combined use must be validated STRICTLY (partition keys must be query
  dimensions) even when the same key is lenient as a dimension — this yields the
  clean "not a query dimension" error for `test_order_by_raw_finer_grain_…`. The
  leniency lives in `bind_query_inputs._validate_partition_keys` (`lenient = key
  in _dim_agg_keys`); the order's combined role needs a strict pass.

### 3.3 + D5 — window= + partition (6: `TestWindowPlusPartition`) — needs the REAL hoist
1. Lift the window= arm of `_guard_partitioned_measures` (local only) and the
   window arm of `_guard_computed_dimension`.
2. Producer synthesis (D5): a windowed producer must synthesize a `TimeTruncKey`
   at the consumer's active-TD granularity into the producer (declared main time
   key, included verbatim in the attach keys). Regroup producers currently set
   `main_time_key=None` (`stage_planner._regroup_producer_prebound`, ~L1289 in the
   original numbering); the windowed machinery needs a real interned time slot
   (`_build_windowed_plans`, ~L536).
3. Lift the producer-needs-CTE guard for WINDOWED producers.
4. **THE HOIST BLOCKER:** a windowed producer renders `WITH _base, _wm_…, SELECT`
   — `_render_producer_split` already extracts that WITH, BUT the internal `_base`
   name is a hardcoded literal (`CteEntry(name="_base", …)`) in many places, so
   hoisting two producers (or a producer + the consumer's own `_base`) COLLIDES.
   Fix: make the base CTE name allocator-minted (thread a per-render base name
   through `_render_with_cross_model_plans` / the transform chain) so
   `reuse_allocator=True` uniquifies it. Alternatively, rename the hoisted CTEs in
   `_render_producer_split` (rewrite the AST references) — messier. The `_rk_`,
   `_cm_`, `_wm_` names are ALREADY allocator-minted, so only `_base` (and the
   transform `step<n>`, which uses `cte_allocator`) need attention.

### 3.7 (rest) — transform / window inside dimensions (10: `TestTransformInDimension`, `TestWindowInDimension`) — needs the REAL hoist
Same hoist blocker: `rank(amount:sum(partition_by=region))` as a DIMENSION is a
ROW-attach producer that computes the partitioned sum AND ranks over it at the
PRODUCER grain (D4 context-grain) — an internal WITH. Once the hoist works, lift
the transform arm of `_guard_computed_dimension` (for grain-self-contained
expressions) and thread the row producer's hoisted CTEs (already returned by
`_render_producer_split` and added to the row `ctes` list).

### 3.7 error surface (2: `TestDimensionErrorSurface`, `TestDimensionGrainSelfContainment`)
- `test_transform_over_bare_aggregate_in_dimension` expects the error to name
  `partition_by` — the transform arm of `_guard_computed_dimension` must, for a
  transform whose inner aggregate lacks `partition_by`, raise the partition_by
  directive (grain-self-containment) rather than the generic transform message.
- `test_aggregate_over_attached_value_rejected` /
  `test_measure_partitioned_by_computed_dimension_rejected` — the
  `requires_nested_attach` shape (an aggregate over an attached value) must fail
  closed with "not yet supported" (D3). `test_grain_circular_dimension_rejected`
  must name the offending dimension.

### 3.1 (finish) — TestThreeLevelNesting (4): the real hoist end-to-end (window + dim-transform + ranked in one query).

### 3.3 cross-model guard (1: `TestCrossModelPartitionedStillGuarded::test_cross_model_window_plus_partition`)
Currently the window= arm fires with a "window=" message before the cross-model
check; narrow arm1 to cross-model (like arms 2/3) so the cross-model message wins.

### 2.2 / 2.4 (as needed)
2.2 recursive grain classification (D3) — formalize
self_contained/consumer_composable/requires_nested_attach if a vertical slice
misclassifies (the fail-closed is `test_aggregate_over_attached_value_rejected`).
2.4 structural cardinality (D8) — add a producer-unique-key field to
`RegroupAttachPlan` and assert join_pairs cover it exactly / keyless is provably
one-row.

### 4.x validation, 5.x docs
- Bless the golden baseline: `SLAYER_UPDATE_GOLDEN=1 poetry run pytest
  tests/test_dev1824_golden_sql.py` (writes `tests/golden/dev1824_sql_baseline.json`).
  Run once ALL shapes are lifted; review the JSON. Then the whole suite green with
  `SLAYER_VALIDATE_SCOPES` exercised (the golden harness forces it on).
- `docs/architecture/composable-attach.md` (+ `zensical.toml` nav entry same
  commit); update `docs/concepts/queries.md`/`formulas.md` + aggregation examples;
  `.claude/skills/slayer-{query,models,overview}.md`; append `DECISIONS.md`.

---

## 4. Architecture map

Regroup primitive (DEV-1825/1739/1829): a partitioned aggregate desugars into a
synthesized PRODUCER sub-plan + a grain-keyed ATTACH + placeholder SUBSTITUTION.
- `regroup_planner.py` — placeholder registry (injective by `AggregateKey`),
  discovery (`dimension_partitioned_aggregates` = computed-dim rows;
  `combined_partitioned_aggregates` = measure/order, EXCLUDES cross-model source),
  `classify_regroup_filter`, `substitute_in_bound_filter`. `walk_value_keys`
  ALREADY recurses into `TransformKey.input` / `ArithmeticKey.operands`.
- `stage_planner.py::_plan_regroups` — orchestration: discover row + combined
  aggs, one producer per (partition set, phase) via `_regroup_producer_prebound`
  → `plan_query(disable_host_rooted_isolation=True)` (that flag stops infinite
  regroup recursion — the partitioned agg IS the producer's own answer), build
  `RegroupAttachPlan`s, rewrite prebound to placeholders.
  `_guard_partitioned_measures` (~L460) is the pre-substitution deferral gate.
- `planned.py::RegroupAttachPlan` — `producer_plan`, `attach_phase` row|combined,
  `join_pairs: List[(host ValueKey, producer SlotId)]`, `substitutions`,
  `partition_display`.
- `generator.py` — ROW attach renders in the plain-base path
  (`_prepare_regroup_attaches` → `_cm_` CTEs joined into `_base`, placeholders in
  `regroup_env`) OR, when a combined attach coexists, inside
  `_render_with_cross_model_plans` (task 3.2 wiring). COMBINED attach always
  renders in `_render_with_cross_model_plans` (`_prepare_combined_regroup_attaches`
  → `_cm_` CTEs joined at the combined SELECT, `placeholder_to_cm` for
  resolution). `assemble_with_chain` owns the flat WITH (declared `depends_on`,
  topological). A ranked-only plan collapses to a single CTE via
  `_collapses_to_ranked_cte` → `_render_collapsed_ranked_plan` →
  `_render_ranked_cte_from_planned` (this is why first/last needed no hoist).
- `AliasAllocator` (`naming.py`) — `allocate`/`allocate_val`/`allocate_cte`,
  `folds_case`; one per `generate_from_planned` unless `reuse_allocator`. `_base`
  and `step<n>` are the names NOT yet allocator-minted (the hoist blocker).

---

## 5. Gotchas

- Byte-identity tripwire after any change: `pytest -m "not integration"` then grep
  failures for non-`dev1824` — must be empty.
- `reuse_allocator=True` is byte-identical only because a PLAIN producer mints no
  names. A producer that mints names (hoist) MUST thread the allocator, and its
  literal `_base`/`step` names must be uniquified (see 3.3 blocker).
- Dotted aliases (`orders.amount_sum`) inside WHERE/OVER re-parse wrong on
  BigQuery/TSQL — flat-rename wrapper + `combined_aliases_by_slot_id` handle it;
  keep new placeholder projections consistent with the windowed side.
- A first/last VALUE is the raw picked column — never cast it to a declared
  temporal type (`_ranked_value_cast_type`); an aggregate that CHANGES the value's
  type (avg, etc.) keeps its cast.
- The combined-attach placeholder is a `ColumnKey` ROW slot — exclude it from
  every grain derivation over row slots.

---

## 6. Test-reconciliation log (policy A1 — all reviewed in the PR)

- `test_dev1829_planner.py` — `TestDispatchDeferrals` → `TestCoexistenceWithIsolatedFamilies`
  (combined + cross-model/windowed/ranked/transform now plan+render);
  `test_row_and_combined_attach_coexistence` (now plans both phases).
- `test_dev1829_guards.py` — `test_first_last_plus_partition_lifted`,
  `test_nested_transform_lifted`, `TestSharedAggregateRowPlusCombined.test_same_key_as_dimension_and_measure`.
- `test_dev1739_guards.py` — `test_partitioned_aggregate_nested_in_transform_lifted`,
  `test_first_last_plus_partition_lifted`.
- `test_dev1740_regroup_guards.py` — `test_first_last_partition_in_dimension_expression_lifted`.
- `test_dev1740_regroup_golden_sql.py` — re-blessed the 5 `guard/transform_in_dim`
  message entries (message now names `partition_by=` for grain-self-containment,
  superseding the generic deferral text; `test_transform_in_dimension_expression`
  still asserts NotImplementedError + DEV-1824).
- `test_dev1739_golden_sql.py` — removed cases `guard/first`, `guard/last`,
  `guard/nested_transform` (+ their 15 baseline entries), superseded by DEV-1824
  goldens `lift/first_last_partition`, `lift/cumsum_partitioned`.

- `test_dev1739_guards.py::test_partitioned_aggregate_in_filter_lifted`,
  `test_dev1829_guards.py::test_partitioned_measure_in_filter_lifted`,
  `test_partitioned_filter_lifted_with_computed_dim`,
  `test_dev1829_routing.py::test_filter_over_partitioned_composite_lifted` — the
  filter lift (3.6/D7) supersedes their in-filter deferral raises.
- `test_dev1739_golden_sql.py` — removed case `guard/in_filter` (+ its 5 baseline
  entries); lifted golden lives in `test_dev1824_golden_sql.py::lift/filter_partitioned`.

- `test_dev1739_guards.py::test_window_plus_partition_lifted`,
  `test_dev1829_guards.py::test_window_plus_partition_lifted` — the window=+partition
  lift (3.3).
- `test_dev1740_regroup_guards.py::test_window_plus_partition_in_dimension_expression_lifted`
  — window=-in-dimension lift (3.7).
- `test_dev1739_golden_sql.py` — removed case `guard/window_plus_partition`
  (+ 5 baseline entries); lifted golden lives in `test_dev1824_golden_sql.py::lift/window_partition`.

Every remaining deferred shape is CROSS-MODEL only and keeps its deferral test.
