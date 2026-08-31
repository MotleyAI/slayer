# DEV-1835 Stage 2 — implementation handover (mid-implementation, session 2)

Resume state for a fresh session. Plan lives in this change folder
(`proposal.md`, `design.md`, `tasks.md`, `specs/`); read those first, then this.
Branch: `egor/dev-1835-stage-2-local-family-unification-attach-dedup-on-the-regroup`.

## What is DONE this session (A, B, D complete; C mostly)

All in `slayer/sql/generator.py` unless noted.

### Task A — D4 regroup-aware windowed/ranked KERNELS (DONE; all 32 matrix wm/rk cells pass)
- `_render_window_measure_cte_from_planned` + `_render_ranked_cte_from_planned`
  take `regroup_env` / `regroup_join_specs`: computed-dim grain keys render via
  `render_value_key` (placeholder → producer column), nested ROW producers LEFT
  JOIN into `_src` / the ranked inner. Ranked also routes a bare `__regroup__`
  ColumnKey grain through `render_value_key`.
- `_prepare_combined_regroup_attaches`: host_key of a computed-dim combined-attach
  grain is DESUGARED (row-attach substitution map + `substitute_value_keys`) so
  `slot_by_key` matches the desugared host slot.
- `stage_planner.py`: `enable_producer_regroups` also fires for a bare
  partitioned-aggregate grain (`_is_local_partitioned_agg`); `_windowed_grain_partition`
  / `ranked_planner._host_grain` exclude ONLY combined placeholders (a ROW-attach
  placeholder is a real grain dim) via a new `combined_placeholder_keys` param.
- **Collapse now folds row attaches (no `_wm_`/`_rk_` surfaces):**
  `_collapses_to_windowed_cte` / `_collapses_to_ranked_cte` allow ROW attaches;
  `_render_collapsed_windowed_plan` / `_render_collapsed_ranked_plan` prepare the
  nested producers and emit them as a WITH prelude (`assemble_with_chain`) that
  `_render_producer_split` hoists flat. `_build_windowed_grain_base` is
  regroup-aware too.

### Task B — D7 transform over an attached placeholder (DONE; 14 guard_dissolution pass)
- Composite routing (`_render_with_cross_model_plans`): a composite wrapping a
  transform (`change`/`change_pct` → `x - time_shift(x)`) over a LOCAL combined
  placeholder skips the inline outer-composite path → the transform chain owns it.
  **Narrowed to require a combined placeholder** so `change(amount:wscaled_sum)`
  (cross-model) still raises `RenderContextMissingFacilityError` (DEV-1836).
- `_prepare_combined_regroup_attaches` returns a 5th value `shift_specs`
  (`(cte, [(host_KEY, grain_alias)])`); the transform chain merges combined
  placeholders into `regroup_env` and combined `shift_specs` into
  `regroup_join_specs` so the shifted CTE joins the combined producer.

### Task C — D10 dedup (PARTIAL)
- Simple dual-role DONE: `_prepare_regroup_attaches` takes `dedup_producers`
  (built from combined attaches, keyed by `_regroup_attach_identity`); a ROW attach
  matching a COMBINED producer reuses its CTE (env + join redirected, no twin
  emitted, `_base` depends on the reused CTE). `test_dual_role_aggregate_shares_one_producer` passes.
- **STILL FAILING (2):** `test_dual_role_beside_bare_windowed_is_two_producers`
  (sqlite+duckdb). VALUES correct; the count is off by one — the windowed producer
  `w` has its OWN nested `rband` producer that is a structural twin of the
  top-level `rt` producer but is NOT deduped (cross-producer boundary). Needs
  either (a) cross-level dedup: thread the parent combined dedup index into
  `_render_producer_split` → producer render (mint combined CTE names + build the
  index BEFORE rendering producers, pass index-minus-self), or (b) functional-
  dependence grain exclusion in the planner (drop a computed-dim grain key whose
  inner aggregate partitions on a subset of the other grain dims — `rband=f(region)`,
  region in grain → redundant; `band=f(city)`, city not in grain → keep).

### Task D — order-only bare windowed binding (DONE)
- `stage_planner.py` order-spec bind: preserve the `raw_formula` when it carries
  `window` (not just `partition_by`) so `_bare_combined_roots` sees the window in
  the order role. `TestOrderOnlyHiddenProducers` passes.

## What REMAINS

### C. Cross-level dual-role dedup — see above (2 tests).

### E. Union-lift (task 3.7) — NOT STARTED
Delete the residual guard (`stage_planner.py` ~line 3319, message "broadcasting a
windowed / first / last aggregate across the union grain … DEV-1835") AND extend
`regroup_planner.regroup_root_grain` / the union-grain producer to EFFECTIVE grains
for windowed / first-last inner aggregates (windowed = partition_by ∪ {active
bucket}; first/last = partition_by), with the synthesized bucket = the query's
bucketed TD (design D9). `regroup_root_grain` currently unions only `partition_keys`
(misses the window bucket). Unblocks `test_dev1835_union_grain.py` (strict-xfail
today — remove the `LIFT_XFAIL` marks) and `test_dev1835_guards.py[residual-union-guard]`.
Fixtures: `UNION_WM_DIM`/`UNION_RK_DIM`, `UNION_WM_RANK`/`UNION_RK_RANK`.

### F. SQL-shape suites + golden rebless (D10 — NEEDS BATCH APPROVAL, do LAST)
~450 full-suite failures are the EXPECTED migration: bare/partitioned windowed &
ranked producers now render as one `_cm_` CTE (no `_wm_`/`_rk_` relation), and
band/computed-dim producers collapse the same way. VALUES are unchanged (execution
suites green). Suites to rewrite preserving intent / rebless:
`test_dev1748_golden_sql` (~175), `test_sql_generator` (~76),
`test_dev1824_golden_sql` (~55), `test_dev1748_ranked_plan` (~31, `_rk_` shape),
`test_dev1732_frame_bound_filters` (~20, asserts `_wm_`), the `*_golden_sql`
baselines, `test_dev1476_first_last_explicit_time`, `test_dev1733_order_only_transform_composite`,
`test_dev1746_*` / `test_dev1750_*` / `test_carrier_scope_matrix` shape pins, and
`tests/golden/dev1835_sql_baseline.json` (`SLAYER_UPDATE_GOLDEN=1`). Enumerate
divergences with before/after SQL → batch approval BEFORE re-blessing. **Do this
LAST, after C+E, so the shape is final.** The one REAL regression found (cross-model
`change`/`change_pct` scope) is already fixed.

### G. Wrap-up (3.8/3.9/3.10)
Perf corpus re-record; docs (`docs/architecture/composable-attach.md`,
`docs/concepts/formulas.md`, `docs/concepts/queries.md`,
`.claude/skills/slayer-query.md`); `poetry run ruff check slayer/ tests/` +
`openspec validate <change> --strict`.

## Verify the DEV-1835 feature core is green (all pass except the 3 noted)
```
poetry run pytest tests/test_dev1835_desugar.py tests/test_dev1835_dedup.py \
  tests/test_dev1835_guards.py tests/test_dev1835_guard_dissolution.py \
  tests/test_dev1835_semantic_pins.py tests/test_dev1837_dimension_measure_matrix.py \
  tests/test_dev1837_guards.py tests/test_dev1750_execution.py \
  -q -p no:cacheprovider
```
Expected residual failures: 2 × `test_dual_role_beside_bare_windowed_is_two_producers`
(Task C cross-level), 1 × `test_deleted_message_has_no_remaining_references[residual-union-guard]`
(Task E). Everything else in these files passes.

## Gotchas
- Producers render via `_render_producer_split(as_cte_body=True, as_hoistable_producer=True)`;
  internal WITH is hoisted flat. Never let `__regroup__` leak or `_wm_`/`_rk_` name
  a producer relation (`assert_scope_closed`, golden no-prefix check).
- Files touched this session: `slayer/sql/generator.py`, `slayer/engine/stage_planner.py`,
  `slayer/engine/ranked_planner.py`. No new files created.
- Do NOT delete this HANDOVER.md until E+F+G are done and the full non-integration
  suite is green (per the original `delete once done implementing` instruction).
