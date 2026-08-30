# Tasks — dev-1837 Stage 1a

## 1. Tests first (TDD — land before implementation)

- [x] 1.1 `tests/_dev1837_fixtures.py`: hand-computed oracles on the DEV-1739/1824 dataset for D-band / D-bare / D-rank × {time_shift, change, change_pct, cumsum, lag, lead, consecutive_periods, rank-of-measure} plus the fixed M-part × time_shift; verify by importing and cross-checking two oracles by hand in review
- [x] 1.2 `tests/test_dev1837_dimension_measure_matrix.py`: cell-table-driven matrix (D-col/D-expr/D-band/D-bare/D-rank × M-plain/M-part/M-win-part/M-last-part/M-wm/M-rk/M-cm/M-transform) on SQLite + DuckDB; supported cells assert oracle or solo-equality + no `__regroup__` in SQL + `assert_scope_closed`; guarded cells `xfail(strict=True, raises=NotImplementedError, reason=issue)`; cardinality-neutrality for D-band, D-bare, AND D-rank (Codex F7); verify: file collects and currently fails/xfails for the right reasons (feature missing, not setup)
- [x] 1.3 Filter-placement tests (D7/D9) in `tests/test_dev1837_filter_placement.py`: POST filter on `change(...)` with band dim; conjunct `band == 1 AND change(...) > 0` splits; conjunct `band == 1 AND status == 'ok'` splits too (D9 general rule); shifted-CTE WHERE assertion (regroup predicate renders against the producer column, no placeholder); mixed OR still fails closed with the split directive; verify: fail-for-the-right-reason before implementation
- [x] 1.4 Guard tests both directions: exact messages for the three new arms + the two as_cte_body arms; lifted arm's old message asserted absent; verify by running the file
- [x] 1.5 `tests/test_dev1837_golden_sql.py` + `tests/golden/dev1837_sql_baseline.json`: band×time_shift (plain), band+M-part×time_shift (cross-model), band×cumsum, D-bare×cumsum, fixed M-part×time_shift; plus generation-smoke on tsql/bigquery/one case-folding dialect (parses, single flat WITH, scope-closed) per design D7; verify: harness runs (baselines blessed during implementation)

## 2. Implementation

- [ ] 2.1 Shared transform-grain helper (design D1/D2): projected ROW-phase `is_dimension` slots minus TimeTruncKey minus combined placeholders; wire into `_render_window_transform_sql`, `_emit_time_shift_ctes_for_planned`, and the consecutive_periods partition aliases; verify: matrix window/cp cells + the placeholder-leak cell pass
- [ ] 2.2 Guard rework (design D5): split arms, exact messages, narrow ref re-pointing (`stage_planner.py:1509`, `:1591`); verify: 1.4 passes
- [ ] 2.3 Plain path: producer CTEs as real `CteEntry`s ahead of `base` with `depends_on` + allocator reservation; verify: band×cumsum and band×rank-of-measure matrix cells pass
- [ ] 2.4 Shifted-CTE regroup-awareness (design D3): `regroup_env` + join specs threaded into the emitter scope AND `_build_shifted_cte_where_parts`; verify: band×time_shift/change/change_pct cells + 1.3 shifted-WHERE assertion pass
- [ ] 2.4b General AND split (design D9): route each `split_top_level_and` conjunct through `classify_regroup_filter` separately; flip `test_dev1825_regroup_planner.py::test_mixed_and_in_one_filter_raises_directive` to pin the working query (user-approved); mixed OR keeps the directive; verify: 1.3 passes
- [ ] 2.5 Cross-model path: prelude passes dependency-preserving `CteEntry`s (design D4); chain time_shift emitter gets the same regroup threading; verify: D-band × M-part × transform cells + cross-model golden pass
- [ ] 2.6 Full non-integration suite green; enumerate Option A golden divergences with before/after SQL → batch approval → re-bless; verify: `poetry run pytest -m "not integration"` clean

## 3. Docs & wrap-up

- [ ] 3.1 `docs/architecture/composable-attach.md`: stage 1a in the roadmap, step-layer grain rule, stage 4 = DEV-1838; `docs/concepts/formulas.md`: auto-partition rule + newly legal combination; grep docs + `.claude/skills/` for stale can't-combine claims; verify: grep clean
- [ ] 3.2 Any deferred cell: comment on the target issue (DEV-1835/1836/1838/new) describing the exact shape (design D8); verify: comment URLs recorded in PR description
- [ ] 3.3 `openspec validate dev-1837-stage-1a-computed-dimension-transform-measure-coexistence --strict` green; `poetry run ruff check slayer/ tests/` clean
