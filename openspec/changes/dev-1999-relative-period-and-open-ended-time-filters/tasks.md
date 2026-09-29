## 1. Tests first (pr-tests stage)

- [ ] 1.1 Add a shared fixture module with a pinned clock (`2026-09-29 12:00:00`, Tuesday, plus the rollover reading `2027-01-01 00:00:00.500`) and seeded SQLite + DuckDB TIMESTAMP/DATE tables with rows at midnight and mid-day on every boundary used by the scenarios; one SQLite table stores timestamps `T`-separated, one space-separated; verify the fixture seeds and a trivial query runs on both engines
- [ ] 1.2 Write pure unit tests for time-point parsing and resolution (every period-literal form, invalid periods, every relative-token row, case/whitespace, rollover, leap day, ISO week 53, `last N` at sub-day units); verify they fail for the missing module
- [ ] 1.3 Write executed-value tests for every `queries/time-points` scenario (comparison table incl. literal-on-left, single-string `in`/`not in` vs tuple `in`, temporal operands: joined, derived, stage, query-backed, `max(...)` measure filter; typing errors; SQLite `T` storage; `gran(col)` in filters, measures and computed dimensions, wrong shapes); verify they fail for the right reason
- [ ] 1.4 Write executed-value tests for the `queries/date-range` deltas (whole-day upper bound, single period, period and relative bounds, inclusive instant, one-sided both ways, reversed, stage and multi-stage models, construction errors, DATE sub-day error)
- [ ] 1.5 Write frame-bound tests: relative, period, `in`, `gran(col)` and `date_range` spellings give identical trailing-window and `time_shift` values; period `=` is stripped from windowed `_src`, instant `=` is kept
- [ ] 1.6 Write `whole_periods_only` tests (the snapping table, hourly current-bucket exclusion, snapped filter bounds with a residual conjunct, day+month in both orders, week+month warning, future lower bound, trailing-window combo, stage time dimension)
- [ ] 1.7 Write clock tests: one clock call per `execute` across stages and a spliced query-backed model with a clock that advances per call; different clock days give different SQL
- [ ] 1.8 Write per-dialect emission tests in `tests/dialects/` for all 8 Tier-1 dialects (lowered `date_range`, sub-day relative bound incl. the SQLite `DATETIME` wrap, `gran(col)` non-literal comparison), and facade tests for instant-ization (lifted `BETWEEN`, verbatim comparators, typed `DATE` literal, reversed order, executed midnight semantics)
- [ ] 1.9 Apply the user-approved logic changes to existing tests: `test_dev1751_date_range_null_bound.py` (one-sided cases become legal, both-null keeps an error), `_dev1871_raise_ledger.py` rows, `test_dev1745_date_range_warning.py` (warning tests removed; single → period; empty/three → errors), `test_time_dimensions_filters.py` malformed-range tests, `test_models.py::TestWholePeriodsOnly` snap tests (removed; `test_period_start_quarter` kept), `test_dev1883_errors.py::test_granularity_in_filter_keeps_unknown_aggregation_error` (inverted), `test_dev1732_frame_bound_filters.py::test_equality_on_time_dimension_column_is_kept_in_src` (split period vs instant); verify each now fails against the unchanged code for the intended reason
- [ ] 1.10 Mechanical updates only (no intent change): `BetweenKey` samples/tests in the key-traversal modules re-expressed on `TemporalLiteralKey` / `TimePointCmpKey` samples; expected literals `'2024-12-31'` → `'2025-01-01'` and `<=` → `<` where the lowering changes them; goldens `dev1745`, `dev1747`, `dev1958` left for re-blessing in implementation
- [ ] 1.11 Codex-review the test suite against the specs and fold in valid findings

## 2. Core time points

- [ ] 2.1 Add `slayer/core/time_points.py` (parse instants/periods/relative tokens, resolve against `now`, `start`/`next_start`/`floor_g`/`ceil_g`); verify 1.2 passes
- [ ] 2.2 Add `TemporalLiteralKey` and transient `TimePointCmpKey` to `slayer/core/keys.py` with `children`/`map_children`, register them in every total walker, delete `BetweenKey` and its dispatch arms; verify the totality tests pass
- [ ] 2.3 Extend `time_bounds.is_temporal_literal` to accept `TemporalLiteralKey`; verify frame-bound unit tests pass

## 3. Query input

- [ ] 3.1 Change `TimeDimension.date_range` to `str | list[str | None] | None` with construction checks (shape + time-point syntax) and schema advertisement; delete `SlayerQuery.snap_to_whole_periods`; remove `MALFORMED_DATE_RANGE`; verify construction tests pass
- [ ] 3.2 Parser: single-string RHS of `in`/`not in` → distinct node; verify parser tests and tuple-`in` regressions pass

## 4. Binding and resolution

- [ ] 4.1 Engine `clock` parameter; read once in `execute` into `ResolvedSourceBundle.now`, carried through reroot, stages, splices, dry-run and cache refresh; verify 1.7 passes
- [ ] 4.2 Binder: `gran(col)` → `TimeTruncKey` in `_bind` (wrong shapes → `GranularityCallError`); time-point comparisons and `date_range` → `TimePointCmpKey`; verify binding unit tests
- [ ] 4.3 Checker: `temporal_type` and the resolution function (typing errors as `QueryTypeError` subclasses, lowering incl. `gran(col)` exact bounds, non-temporal fallback); `bind_inputs` runs the pass over every bound expression; compile fails closed on an unresolved key; add raise-ledger rows; verify 1.3–1.5 pass
- [ ] 4.4 Typed `whole_periods_only` pass (earliest floor per column, clamp to now, non-nesting warning); verify 1.6 passes

## 5. SQL rendering

- [ ] 5.1 Render `TemporalLiteralKey` comparisons via one dialect hook (SQLite `DATETIME` wrap for timestamp-kind literals, plain literal elsewhere); `TimeTruncKey` arm in `row_expr.py`; verify 1.8 dialect tests and SQLite `T`-storage tests pass
- [ ] 5.2 Re-bless goldens `dev1745`, `dev1747`, `dev1958` and review each diff is BETWEEN → half-open only

## 6. Facade

- [ ] 6.1 AST-level instant-ization of date-only (and typed DATE/TIMESTAMP) literals in lifted `BETWEEN` and verbatim comparators; verify facade tests pass

## 7. Docs and surfaces

- [ ] 7.1 New `docs/concepts/time.md` (single temporal reference, frame-bound section moved from `formulas.md`) linked in `zensical.toml` nav; `queries.md`, `formulas.md`, `docs/reference/mcp.md`, `docs/interfaces/mcp.md`, `docs/reference/rest-api.md` reduced to pointers; verify every docs page is in the nav
- [ ] 7.2 MCP `query` tool docstring: compact time-point summary (forms, grammar, `last N` excludes current, one-sided/single-period `date_range`); verify the MCP docs test lists the forms
- [ ] 7.3 `docs/examples/04_time/time.md` shows the new forms; update and re-execute `time_nb.ipynb` if its cells are affected

## 8. Gates

- [ ] 8.1 Full non-integration suite green (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, basedpyright no new errors vs baseline, `la-arch-check`
