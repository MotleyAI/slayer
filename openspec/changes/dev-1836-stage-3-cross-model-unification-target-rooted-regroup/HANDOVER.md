# DEV-1836 — implementation handover (resume state)

Branch: `egor/dev-1836-stage-3-cross-model-unification-target-rooted-regroup`.
Resume via `/spec` Step 1 (Linear DEV-1836 + this change folder).

## Status at a glance

- **Implementation complete (Commits 0–4). All 168 `test_dev1836_*` pass** (sqlite+duckdb).
- **dev-1835 merged** (PR #346 rounds 1–3, clean).
- **Migration proven semantics-preserving** by a differential-execution harness
  (see below) and **all cross-model golden SQL re-blessed** (6 suites, 115 keys).
- **DEV-1837 matrix cm cells flipped** to supported.
- **Full non-integration suite: 14580 passed / ~114 failed.** The remaining
  ~114 are plan-structure / guard **assertion** tests that pin the OLD `_cm_`
  internals — their query RESULTS are already proven identical; they need
  mechanical rewriting to the new plan shape. **This + Commit 5 + docs is the
  remaining work** (below).
- Lint clean.

Commit trail (newest first): `40f99c38` matrix flip + harness · `2897aea1`
golden re-bless · `0f1accca` post-merge bug fixes · `f9e9ba85` merge dev-1835 ·
`7dc409b8` implementation WIP.

## The equivalence proof (the rigorous protocol — reusable)

`tests/_dev1836_equiv_driver.py` is a differential-execution harness: it seeds
RANDOM data (respecting PK/FK/arity so join fan-out is actually exercised) into
each golden suite's fixture schema, executes EVERY case query through the engine
on old vs new code, and compares result sets row-for-row. Run it in two
worktrees against SHARED seeded DBs:

```
# new (current) tree:
poetry run python tests/_dev1836_equiv_driver.py /tmp/new.json /tmp/seeded_dbs
# old tree (pre-change commit 7dc409b8^ = f5645de5), same seeded DBs:
git worktree add /tmp/old_tree 7dc409b8^
poetry run python tests/_dev1836_equiv_driver.py /tmp/old.json /tmp/seeded_dbs /tmp/old_tree
# then diff old.json vs new.json per case: ok/ok same rows = PRESERVED, etc.
```

Result over the 6 golden suites (122 cases): **113 PRESERVED, 8 both-raise, 1
newly-works (guard lifted), 0 REGRESSIONS, 0 unexplained value changes.**
Executing the SQLite rendering proves the PLAN logic; per-dialect goldens are
syntactic re-renders. The one type-changed raise (`dev1824::guard/dim_cross_model_source`,
NotImplementedError→ValueError) is CORRECT: its partition key `region` is an
orders column, unattributable from `customers`, so DEV-1836 rightly rejects it
(more precisely than the old blanket guard).

**Before rewriting the remaining assertion tests, extend `MODULES` in the
driver with each behavioral module's queries and re-run the differential** — that
turns "rewrite to the new shape" into a value-verified change, not a guess. The
behavioral tests reuse the same fixtures the golden differential already covered,
so this is a confirmation step, not a re-derivation.

## Remaining work

### A. Reconcile the ~114 assertion-test failures (needs the harness as guard-rail)

Get the live list: `poetry run pytest -m "not integration" -q | grep FAILED`.
Two kinds:

1. **Guard-flip tests** (assert an old guard raises; the combo now works or
   raises a refined error). Flip to assert the new behavior — the new behavior is
   already pinned by `tests/test_dev1836_*`. Files:
   `test_dev1837_guards.py` (2), `test_dev1824_computed_dim_execution.py` (2),
   `test_dev1750_guard_ownership.py` (2), `test_dev1750_guard_lift.py` (2),
   `test_dev1835_guards.py` (1), `test_dev1829_guards.py` (1),
   `test_dev1824_remaining_guards.py` (1), `test_dev1739_guards.py` (1),
   `test_dev1745_warning_contract.py` (1).
2. **Plan-structure / `_cm_`-shape tests** (assert `cross_model_aggregate_plans`
   counts, specific CTE aliases/bodies, isolation-classifier kinds). These assert
   OLD internals that no longer exist — a cross-model aggregate is now a
   `RegroupAttachPlan` (with `producer_root_model` set), NOT a
   `CrossModelAggregatePlan`, and its CTE is a producer rooted `FROM <target>`.
   Rewrite each to assert the new shape (see "New plan shape" below). Files (count):
   `test_dev1747_reroot_filter_routing.py` (19),
   `test_cross_model_rename_dev1448.py` (13),
   `test_dev1728_derived_shared_grain.py` (12),
   `test_dev1708_stage4_cte_scope.py` (8),
   `test_dev1748_ranked_plan.py` (6),
   `test_cross_model_planner_wiring.py` (6),
   `test_sql_generator.py` (5),
   `test_dev1747_prebound_planner.py` (4),
   `test_dev1746_consumer_materialization.py` (4),
   `test_filtered_local_isolation.py` (3),
   `test_dev1769_routed_filter_path_validation.py` (3),
   `test_dev1733_order_only_transform_composite.py` (3),
   `test_dev1746_isolation_classifier.py` (2),
   `test_dev1450fix_reroot_strategy.py` (2),
   `test_dev1445_cross_model_alias_filter.py` (2),
   `test_dev1829_planner.py` (1), `test_dev1825_regroup_planner.py` (1),
   `test_dev1747_order_entry.py` (1), `test_dev1746_projection_order.py` (1),
   `test_dev1746_empty_base_plan.py` (1), `test_dev1744_naming_allocator.py` (1),
   `test_dev1712_order_only_hidden_slots.py` (1),
   `test_dev1645_invalid_postgres_sql.py` (1),
   `test_dev1476_first_last_explicit_time.py` (1).

   **New plan shape** a cross-model aggregate now produces (assert this instead):
   - `planned.cross_model_aggregate_plans == []`; the aggregate is a
     `planned.regroup_attach_plans` entry with `attach_phase in {"row","combined"}`
     and `producer_root_model == "<target model>"`.
   - Its producer CTE renders `FROM <target> ... GROUP BY <safe grain>` (a
     `_cm_*` CTE name is still used, but rooted at the target, aliased to the
     producer relation — not the host-prefixed forward form).
   - broadcast/dropped-filter metadata is on the attach
     (`broadcast_measure`, `broadcast_dimensions`, `dropped_filter_warnings`),
     surfaced by the collector (`query_engine._collect_broadcast_warnings` /
     `_collect_dropped_filter_warnings`).
   For each, prefer asserting behavior (executed values / warnings) over internal
   shape where the old test over-pinned internals.

### B. Commit 5 — D7 invariant + retire `classify_isolation` (tasks 7.x)

- The total-routing test passes via the existing backstop; a dedicated
  post-discovery invariant is not yet added (optional hardening).
- `classify_isolation` / `cross_model_planner.py` are NOT deleted — they still
  serve the LOCAL filtered-local (HOST_ROOTED) and ranked-host paths this
  migration does not cover. Deleting them needs those migrated first, OR the
  acceptance criterion scoped to "no cross-model dispatch" (which is already
  true — cross-model aggregates never reach `classify_isolation`, they are
  substituted to placeholders first). Recommend: scope the deletion to the
  cross-model arms and keep the local seam, or file a follow-up.

### C. Docs (task 8.2) + wrap-up (8.3)

- `docs/architecture/composable-attach.md` (stage-3 section: target-rooted
  producers, safe-grain/broadcast, filter inheritance), `docs/concepts/queries.md`
  (strict + broadcast warnings), `docs/concepts/formulas.md` (cross-model in
  dims/window), `docs/concepts/models.md` (join cardinality + validation),
  `.claude/skills/slayer-query.md`; zensical nav check.
- Integration SQLite/DuckDB green; perf corpus re-record; archive post-merge.

## Implementation map (what changed)

- `slayer/engine/join_safety.py` (NEW) — safety predicate + `audit_join_safety`.
- `slayer/engine/stage_planner.py` — `_synthesize_cross_model_producer` (heart),
  `_discover_cross_model_combined`, attributability (`_attributable_from_root`,
  `_grain_member_attributable`, `_shared_join_key_reroot`), input/filter/partition
  safety, integrated into `_plan_regroups`; guards lifted.
- `slayer/sql/generator.py` — `_producer_render_bundle` (roots the producer
  render at its target); CTE-body deferral scoped to LOCAL combined attaches.
- `slayer/engine/planned.py` — `RegroupAttachPlan.{producer_root_model,
  dropped_filter_warnings, broadcast_measure, broadcast_dimensions}`.
- `slayer/engine/query_engine.py` — warning collector rebuilt on the regroup IR;
  strict; DEV-1689 stamping (excludes reserved-leaf placeholders).
- `slayer/engine/response_meta.py` — combined placeholder classified as a
  measure; label/format via the original aggregate key.
- `slayer/core/{errors,warnings,query}.py` — `BroadcastGrainWarning`(+payload),
  `SlayerQuery.strict`; REST/MCP pass-through.
- `slayer/cube/converter.py`, `slayer/cli.py` — Cube mapping, validate-models
  join-safety section.

## Gotchas

- A cross-model MEASURE → reserved-leaf `ColumnKey` placeholder (ROW-phase);
  anything classifying slots by phase must special-case `REGROUP_LEAF_PREFIX`
  (two bugs already fixed this way).
- Attributability: host path P attributable from root R iff P starts with
  target_path AND residual `safe_reachable` from R; plus `_shared_join_key_reroot`
  for a host-local dim that is a join key of the single hop to R.
- The producer measure keeps the CANONICAL alias (a target column can shadow the
  public name, e.g. `pop` over `regions.pop`).
