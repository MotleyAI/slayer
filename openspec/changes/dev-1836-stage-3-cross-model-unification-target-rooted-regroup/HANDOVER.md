# DEV-1836 — implementation handover (resume state)

Branch: `egor/dev-1836-stage-3-cross-model-unification-target-rooted-regroup`.
Resume via `/spec` Step 1 (Linear DEV-1836 + this change folder). This doc is
deleted only when the WHOLE task is done.

## Status at a glance (updated mid-reconciliation)

- **Implementation complete (Commits 0–4).** All 168 `test_dev1836_*` pass.
- **Merged:** `dev-1835` (PR #346 round 4) + `origin/main` (DuckDB docs + the
  DEV-1835 OpenSpec archive) — both clean.
- **Migration proven semantics-preserving** by the differential harness
  (`tests/_dev1836_equiv_driver.py`); cross-model golden SQL re-blessed.
- **Started from 114 non-integration assertion failures. 64 reconciled + green
  and COMMITTED.** Remaining: ~50 (see "Remaining work").
- **Decision B RESOLVED → B1 (scope + follow-up).** See below.
- **Two real regressions found + fixed (not just test rewrites).** See below.

Commit trail this session (newest first):
`37212376` dev1769 drop+warn · `b75d437f` bug#2 order-composite fix ·
`4fa148fd` 59 tests + bug#1 declared-type fix · (pre-session: `73374b99` merge
dev-1835, then the origin/main merge).

## Decision B — RESOLVED: B1 (scope + follow-up)

Keep `cross_model_planner` / `classify_isolation` for the still-live
host-rooted / ranked-host / filtered-local cross-model paths (DEV-1836 did NOT
migrate those). Scope the "retire classify_isolation" acceptance to "no
cross-model TARGET-ROOTED dispatch" (already true). Do NOT delete
`classify_isolation`. Add the D7 total-routing invariant (task 7.1). Leave the
now-dead arms (e.g. the `stage 7b.15e` raise branch in generator.py, unreachable
after DEV-1836) in place. **A comment describing the remaining retirement work
is already posted on DEV-1838 (Stage 4)** — that is its natural home; do NOT
file a separate follow-up issue.

## Two bugs fixed this session (both have failing-without-fix coverage)

1. **Declared `type=` dropped on cross-model measures** (`4fa148fd`). The
   target-rooted producer cast the aggregate to the SOURCE column's type instead
   of the query field's declared type → wrong values via INTEGER truncation.
   Fixed in `stage_planner.py`: threaded the declared type through
   `_discover_cross_model_combined` (now returns a `type_by_agg` map) →
   `_synthesize_cross_model_producer(declared_type=)` →
   `_regroup_producer_prebound(declared_type_by_agg=)`, where it wins over
   `measure_key_type`. No-op for untyped measures. Covered by
   `test_cross_model_rename_dev1448::...type_propagates` (REAL vs INTEGER).
2. **Order-only composite over only cross-model aggregates** (`b75d437f`).
   `customers.revenue:sum / suppliers.revenue:sum` / `abs(customers.revenue:sum)`
   in `order=` regressed: an all-placeholder composite buckets ROW → base ROW
   arm → "`__regroup__...` needs an aggregation". Fixed in `generator.py`
   (`_render_with_cross_model_plans`, the `outer_composite_slot_ids` loop): scan
   `row_slots` too, EXCLUDING computed dimensions (`slot.is_dimension` — they
   stay grouped in `_base`). Covered by the 2 restored `test_dev1733` composite
   tests.

## Reconciliation patterns (the target-rooted regroup shape) — REUSE THESE

1. Producer CTE is rooted `FROM <target>`; inner aliases are CANONICAL
   `"target.col"` (NOT host-prefixed `"host.target.col"`). Final result key in
   the OUTER SELECT is UNCHANGED (still `"host.target.col"`).
2. Ranked (first/last) producer: OLD `_rk_<...>` CTE → NEW `_cm_<...>` CTE
   wrapping `FROM (SELECT ... ROW_NUMBER() OVER(...) AS _ranked_rn ...) AS
   _ranked_src`; grain materialised as `_val_N`. `_split_at_ranked_subquery`
   still works (splits at `FROM (`). So `_extract_cte_body(sql, r"_rk_\w+")` →
   `r"_cm_\w+"`.
3. Cross-model aggregate-phase FILTER: OLD `HAVING SUM(...) > N` inside producer
   → NEW outer `WHERE _cm_....."target.col" > N` on the attached value. Compound
   arith (`x:sum + 1 > 5`) → `WHERE (cte.col + 1) > 5`. Semantic: uniform
   row-restriction (failing groups DROPPED, not kept-with-NULL) — a class-(c)
   divergence, pinned by the ledger + `test_dev1836_filter_inheritance`.
4. Guard LIFTS → broadcast (CROSS JOIN + BroadcastGrainWarning), not raise:
   intermediate-hop dim, cross-model-source-in-computed-dim, band×cross-model,
   time_shift-over-single-cross-model (test_dev1750), intermediate-hop derived
   grain (test_dev1728).
5. Time-grain producer alias gains `_month` (canonical granularity suffix);
   host `_base` alias + final result key stay un-suffixed. Join-back compares.
6. Producer aggregate is CAST-wrapped to the (declared or source) type:
   `CAST(SUM(x) AS REAL)` / `AS INTEGER`. OLD goldens sometimes had no CAST — add
   it. Routed filters on the producer are CAST-wrapped too.
7. `cross_model_aggregate_plans == []` now; the aggregate is a
   `regroup_attach_plans` entry (attach_phase row|combined, producer_root_model
   == target). Cross-model MEASURE → reserved-leaf placeholder ColumnKey
   `__regroup__N__<...>` (ROW phase — special-case the prefix). dropped_filter /
   broadcast metadata live on the attach.
8. Refined guards (still raise, NEW message + TYPE changed
   NotImplementedError→ValueError): unsafe explicit partition key → "declares
   partition_by=X, which unreachable from the aggregate's root ... attributable
   from <target>"; windowed cross-model unattributable TD → "Windowed cross-model
   aggregate 'w' needs the query's active time dimension ('...') attributable
   from <target>...".
9. Unreachable host-sibling FILTER (path doesn't start with target) → dropped
   from producer + `UnreachableFilterDroppedWarning` (D3), not applied. Reachable
   (path starts at target) → rides into producer with its join.
10. Machinery tests that read `planned.cross_model_aggregate_plans` for
    dropped-filter warnings etc. → read `regroup_attach_plans` (recurse into
    `.producer_plan.regroup_attach_plans`).

## Oracles

- Positive: `tests/test_dev1836_*.py` (100 tests, all pass). `matrix_flip` pins
  band×cm, cross-model-in-dim executes, intermediate-hop broadcasts.
- Value preservation: `tests/_dev1836_equiv_driver.py` (golden suites, proven).
- Divergence ledger: `openspec/changes/.../divergences.md` (class c/d flips).

## Remaining work

### A. Reconcile the remaining assertion failures

Get the live list: `poetry run pytest -m "not integration" -q | grep -E "^FAILED"`
(NB: the FAILED node ids wrap; extract file via `grep -oE "tests/[a-z0-9_]+\.py"`).

**B-INDEPENDENT still to do (~9, mechanical, use patterns above):**
`test_dev1733_order_only_transform_composite` (1: windowed guard-refine →
ValueError), `test_dev1825_regroup_planner` (1: partition guard-refine),
`test_dev1829_planner` (1: count 2→1, regroup shape),
`test_dev1747_order_entry` (1: Law-3 trigger[target-grain+path]),
`test_dev1746_projection_order` (1: precondition → regroup_attach_plans),
`test_dev1746_empty_base_plan` (1), `test_dev1712_order_only_hidden_slots`
(1: host-prefix→canonical), `test_dev1645_invalid_postgres_sql` (1:
host-prefix→canonical + NULLS LAST), `test_dev1476_first_last_explicit_time` (1).

**B-DEPENDENT machinery cluster (~43, per B1 rewrite to regroup shape, keep
host-rooted cases — do NOT delete classify_isolation):**
`test_dev1747_reroot_filter_routing` (19), `test_cross_model_planner_wiring`
(6), `test_dev1748_ranked_plan` (6), `test_dev1747_prebound_planner` (4),
`test_filtered_local_isolation` (3), `test_dev1746_isolation_classifier` (2),
`test_dev1450fix_reroot_strategy` (2), `test_dev1744_naming_allocator` (1).
These exercise `cross_model_planner`/`classify_isolation` internals directly:
for queries whose cross-model aggregate now routes to a `regroup_attach_plan`,
assert the new shape; for still-live HOST-ROOTED cases (`amount:wscaled_sum` →
`cte_root_model=="orders"`) keep the `CrossModelAggregatePlan` assertions.

### B. Commit 5 — task 7.1 only (7.2 deferred to DEV-1838 per B1)
Add the post-discovery total-routing D7 invariant (optional hardening; the
total-routing test passes via the existing backstop). Do NOT delete
classify_isolation.

### C. Docs (task 8.2) + wrap-up (8.3)
`docs/architecture/composable-attach.md` (stage-3: target-rooted producers,
safe-grain/broadcast, filter inheritance), `docs/concepts/queries.md` (strict +
broadcast warnings), `docs/concepts/formulas.md` (cross-model in dims/window),
`docs/concepts/models.md` (join cardinality + validation),
`.claude/skills/slayer-query.md`; zensical nav check. Integration SQLite/DuckDB
green; perf corpus re-record. **Re-validate the delta specs against the corpus
(the DEV-1835 archive updated computed-dimensions/partitioned-aggregates specs)
before `openspec validate --strict`.** Lint (`ruff check slayer/ tests/`).
Then Step 7 gate: ask to commit/push/PR. Archive post-merge (Step 8).
**Delete this HANDOVER.md when the whole task is done.**

## The equivalence proof (rigorous protocol — reusable)

`tests/_dev1836_equiv_driver.py` seeds RANDOM data (respecting PK/FK/arity) into
each golden suite's fixtures, executes EVERY case old vs new, compares row-for-
row. Two worktrees, shared seeded DBs:
```
poetry run python tests/_dev1836_equiv_driver.py /tmp/new.json /tmp/seeded_dbs
git worktree add /tmp/old_tree 7dc409b8^   # pre-change f5645de5
poetry run python tests/_dev1836_equiv_driver.py /tmp/old.json /tmp/seeded_dbs /tmp/old_tree
```
Result over 6 golden suites (122 cases): 113 PRESERVED, 8 both-raise, 1
newly-works, 0 REGRESSIONS. Extend `MODULES` with a behavioral module's queries
to value-verify before rewriting its assertion tests.

## Implementation map (what changed)

- `slayer/engine/join_safety.py` (NEW) — safety predicate + `audit_join_safety`.
- `slayer/engine/stage_planner.py` — `_synthesize_cross_model_producer` (heart),
  `_discover_cross_model_combined`, attributability, filter/partition safety,
  declared-type threading (bug#1), integrated into `_plan_regroups`; guards lifted.
- `slayer/sql/generator.py` — `_producer_render_bundle` (roots the producer at
  its target); order-only all-cross-model composite routing (bug#2).
- `slayer/engine/planned.py` — `RegroupAttachPlan.{producer_root_model,
  dropped_filter_warnings, broadcast_measure, broadcast_dimensions}`.
- `slayer/engine/query_engine.py` — warning collector on the regroup IR; strict.
- `slayer/core/{errors,warnings,query}.py` — `BroadcastGrainWarning`,
  `SlayerQuery.strict`; REST/MCP pass-through.

## Gotchas

- A cross-model MEASURE → reserved-leaf `ColumnKey` placeholder (ROW-phase);
  anything classifying slots by phase must special-case `REGROUP_LEAF_PREFIX`
  (three bugs fixed this way now, incl. bug#2's order composite).
- Attributability: host path P attributable from root R iff P starts with
  target_path AND residual `safe_reachable` from R; plus `_shared_join_key_reroot`.
- The producer measure keeps the CANONICAL alias; the user name lands on the
  consumer projection via the placeholder substitution.
