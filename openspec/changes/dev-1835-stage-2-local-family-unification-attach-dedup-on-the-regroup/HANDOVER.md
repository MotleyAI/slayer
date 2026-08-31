# DEV-1835 Stage 2 — implementation handover

Resume state for a fresh session. The plan lives in this change folder
(`proposal.md`, `design.md`, `tasks.md`, `specs/`); read those first, then this.
Branch: `egor/dev-1835-stage-2-local-family-unification-attach-dedup-on-the-regroup`.

`origin/main` is merged (DEV-1839 archived into the corpus). The MODIFIED
"Transforms inside dimension expressions" requirement in
`specs/queries/computed-dimensions/spec.md` is already re-baselined against the
post-1839 corpus (task 3.7 spec part); `openspec validate <change> --strict` is
green.

## State of the tree

**Zero executed-value regressions** — DEV-1824/1839 execution and
DEV-1748 first/last execution suites are all green. The failing suites are the
un-implemented DEV-1835 features below plus the SQL-shape suites the migration
rewrites (D10).

Verify the baseline quickly:
```
poetry run pytest tests/test_dev1835_desugar.py tests/test_dev1835_dedup.py \
  tests/test_dev1835_guards.py tests/test_dev1824_partitioned_execution.py \
  tests/test_dev1839_measure_execution.py tests/test_dev1748_first_last_matrix.py \
  -q -p no:cacheprovider
```

## What is implemented (files + symbols)

All in `slayer/engine/stage_planner.py` unless noted.

1. **Desugar (2.1)** — `_bare_combined_roots`, `_is_bare_local_regroup_root`,
   `_effective_root_grain`, `_windowed_or_ranked_identity`,
   `_partition_free_identity`. Bare windowed / first-last measures join the
   COMBINED roots in `_plan_regroups`; grouping key is
   `(effective_grain, windowed_or_ranked_identity)` so twins collapse and
   different window/filter/ranking stay separate. Producer measures are deduped
   by partition-free identity (twin → one shared column).
2. **Filter routing (D8)** — `regroup_planner.is_local_combined_regroup_ref`
   broadens `conjunct_scope` and `_split_partitioned_filter_conjuncts` to bare
   wm/rk.
3. **Empty-base fix** — `_plan_empty_base_grain` now counts combined regroup
   placeholder slots as isolated (`regroup_combined_slot_ids`), so a keyless
   bare-ranked query alone no longer emits an empty `_base`.
4. **D3 naming** — `generator._collapses_to_windowed_cte` +
   `_render_collapsed_windowed_plan` + `_build_windowed_grain_base` render a
   windowed producer as one self-contained `_cm_` CTE (grain rows inlined via a
   `base_relation` arg on `_render_window_measure_cte_from_planned`). Ranked
   internals renamed in `sql/render/ranked.py` (`RANK_COLUMN`/`RANKED_SOURCE_ALIAS`
   → `_ranked_rn`/`_ranked_src`) so no `_rk_` substring survives.
5. **Guard dissolution (2.6, partial)** — deleted G4/G5/G6/G7 (in
   `_guard_windowed_measures`), the post-projection twin (in `plan_query`), the
   coexistence arm and time_shift-over-ranked (both in `generator.py`);
   re-pointed G3 → DEV-1836. STILL PRESENT (delete in 3.7): the residual union
   guard (`_guard_computed_dimension`, message "broadcasting a windowed / first /
   last aggregate across the union grain … DEV-1835").
6. **D4 planning** — windowed/ranked producers build nested ROW attaches for
   computed-dim grains: grain dims marked `is_dimension=True` in
   `_regroup_producer_prebound`; own-grain exclusion uses the FULL grain
   (`_root_grain` folds the active bucket back in); `enable_producer_regroups`
   turned on when the grain has a `ScalarCallKey`/`ArithmeticKey`/`TransformKey`;
   `_validate_nested_producer_plan` skips row attaches; join-pairs fall back to
   projection POSITION when a computed grain key was desugared inside the
   producer; the DEV-1838 CTE-body row-attach guard exempts hoistable producers.
7. **Test helper bug fix** — `tests/_dev1835_fixtures.py::cte_aliases` read
   `args.get("with")`; sqlglot 30.x stores it under `with_`. Fixed to read both.
   (Assertions unchanged — the helper was silently returning `[]`.)

## What remains (priority order, with the concrete next step)

### A. D4-rendering — the windowed/ranked KERNELS must go regroup-aware
The deepest piece. Planning succeeds for band/expr/rank/mixed grains, but
rendering fails: `ScopeFrame.resolve does not yet handle ref type ScalarCallKey`
(`sql/scope.py:403`), raised from `_render_window_measure_cte_from_planned`'s
grain resolution (`src_scope.resolve(dslot.key)`) and the analogous ranked
`_ranked_scope_expr`.

The windowed `_src` subquery and the collapse's `_build_windowed_grain_base`
(and the ranked inner select) must:
- resolve computed-dim grain keys via the full value-key renderer
  (`render_value_key` with a `RenderContext` carrying the producer's
  `regroup_env` / join specs), not the bare `src_scope.resolve`; and
- JOIN the producer's nested row producers so a placeholder grain key
  (a band's city total) resolves to its producer column — the stage-1a D3
  pattern already used by the shifted-CTE emitter (see `RenderState.regroup_env`
  / `regroup_join_specs` and `_prepare_regroup_attaches`).

Unblocks the 32 `test_dev1837_dimension_measure_matrix.py` cells, the 2
`test_dev1837_guards.py` arm-renders, and the dual_role desugar/dedup cells.
Oracles: `WM_X`/`RK_X` in `tests/_dev1835_fixtures.py`.

### B. D7 — transform over an attached placeholder
`change`/`change_pct`/`time_shift` over a partitioned/bare wm/rk placeholder.
`cumsum`/`rank` already work; the `time_shift` shifted-CTE path fails with
`RenderContextMissingFacilityError` (the transform isn't materialised because
the shifted CTE re-aggregates the source and the placeholder isn't joined in).
Fix: join the combined producer into the shifted CTE so the placeholder resolves
(analogous to A). Unblocks 12 `test_dev1835_guard_dissolution.py`.

### C. D10 cross-phase dedup — dual-role
The same `amount:sum(partition_by=region)` used as a computed dimension (row
attach) AND a measure (combined attach) currently ships two producers; should be
one, serving both attaches. Needs the producer identity to be shared across
phases (design D6 IR split, or a render-time producer-CTE dedup by canonical
identity). Unblocks `test_dev1835_dedup.py::test_dual_role_aggregate_shares_one_producer`
and the two `dual_role_beside_bare_windowed` cases (which also need A).

### D. order-only bare windowed (binding)
`order=[{"column":"amount:sum(window='90d')"}]` binds to the plain `amount:sum`
(canonical-name collision drops the window). The order binding must preserve the
window so `_bare_combined_roots` discovers it in the order role.
`test_dev1835_desugar.py::TestOrderOnlyHiddenProducers` (the windowed one;
the last/filter ones pass).

### E. union-lift (3.7)
Delete the residual union guard (`_guard_computed_dimension`) and extend
`regroup_root_grain`/grouping to effective grains for windowed/first-last inner
aggregates. Then `test_dev1835_union_grain.py` (strict-xfail today) flips to
pass and `test_dev1835_guards.py[residual-union-guard]` passes.

### F. SQL-shape suite rewrites + golden rebless (D10 — needs batch approval)
The migration changes emitted SQL for every bare-windowed / ranked shape.
Rewrite preserving intent: `test_dev1748_ranked_plan.py` (~31, `_rk_` shape),
`test_dev1732_frame_bound_filters.py` (~20, asserts `_wm_` presence),
DEV-1824/1837 golden baselines, and bless `tests/golden/dev1835_sql_baseline.json`
(`SLAYER_UPDATE_GOLDEN=1`). Per design D10, enumerate divergences with
before/after SQL and get batch approval BEFORE re-blessing. Do this LAST, after
A–E, so the shape is final.

### G. Wrap-up (3.8/3.9/3.10)
Perf corpus re-record; docs (`docs/architecture/composable-attach.md`,
`docs/concepts/formulas.md`, `docs/concepts/queries.md`,
`.claude/skills/slayer-query.md`); `ruff check` + `validate --strict`.

## Gotchas
- Run the FULL non-integration suite before declaring done and fix real
  failures; SQL-shape suites (F) are rewrites, not fixes — keep them separate.
- Producers render via `_render_producer_split` with `as_cte_body=True,
  as_hoistable_producer=True`; their internal WITH is hoisted flat.
- Never let `__regroup__` leak into emitted SQL or `_wm_`/`_rk_` into a producer
  relation name (`assert_scope_closed`, the golden no-prefix check).
