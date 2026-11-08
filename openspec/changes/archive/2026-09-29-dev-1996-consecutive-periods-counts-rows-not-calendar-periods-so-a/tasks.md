## 1. Tests first (pr-tests)

- [x] 1.1 Add `tests/test_consecutive_periods_calendar.py` (no issue number in the name): the spec's `orders` fixture on SQLite + DuckDB (in-process, unit — not `integration`), one test per "consecutive_periods counts calendar periods" scenario except the server-dialect and golden ones; verify each fails on the current code for the right reason (row-based values), except scenarios the current code already satisfies
- [x] 1.2 Add the `period=` rejection test and verify it fails today (the keyword is silently accepted)
- [x] 1.3 Add SQLite sub-day tests: executed `time_shift` / `change` at hour, minute, second (fail today: all NULL) and direct render assertions that SQLite day/week/month/quarter/year offsets and `week_sunday` truncation are unchanged (pass today)
- [x] 1.4 Add a gapped-series executed case to each server integration file (`tests/integration/test_integration_postgres.py`, `_mysql`, `_clickhouse`, `_sqlserver`) over DATE- and TIMESTAMP-typed columns at month, quarter and week_sunday, marked `integration`
- [x] 1.5 Add golden-SQL cases for a gap-aware `consecutive_periods` query on postgres/sqlite/duckdb/tsql/bigquery, following the existing golden-test pattern (baselines recorded in pr-implement)

## 2. Implementation (pr-implement)

- [x] 2.1 Extract the calendar-offset helper from the `time_shift` join-back lookup and route `time_shift` through it; verify every existing `time_shift` / `change` test and golden is unchanged
- [x] 2.2 SQLite `build_time_offset_expr`: `DATETIME(...)` for hour/minute/second, `DATE(...)` otherwise; verify 1.3 passes
- [x] 2.3 `consecutive_periods` emitter: predecessor CTE (`LAG(bucket)` over the auto-grain partition) and the calendar-aware reset flag; allocator-named, declared CTE deps, predicate only in condition positions; verify 1.1 passes
- [x] 2.4 Drop `period` from the `consecutive_periods` keyword allowlist; verify 1.2 passes
- [x] 2.5 Re-bless every changed `consecutive_periods` golden baseline and record the new 1.5 baselines; inspect each diff is only the new CTE / flag
- [x] 2.6 Any existing execution test that now fails because it asserted row-based values across a gap: STOP and ask the user per test — never edit its logic unilaterally
- [x] 2.7 With any transform in the plan, lower every measure-typed filter to the outer WHERE after the transform chain (Axiom 14); executed cumsum/lag cases under a measure filter; update the routing assertion of test_aggregate_and_post_filters_route_independently

## 3. Architecture and docs

- [x] 3.1 Apply the user-approved Axiom 11.3 sentence to `architecture/semantics.arc42.md` exactly as approved (calendar adjacency; `lag` / `lead` the only row steppers; `[enforced: test:tests/test_consecutive_periods_calendar.py]`); verify `uvx --no-build --from living-architecture==0.2.0 la-arch-check` passes
- [x] 3.2 `docs/concepts/formulas.md` `consecutive_periods` section: one sentence — a missing bucket breaks the streak, and the series is the query's rows (date range included)

## 4. Verification

- [x] 4.1 `poetry run pytest -m "not integration"` all green
- [x] 4.2 Integration suite with the CI invocation (CLAUDE.md) green, plus the MySQL/ClickHouse/SQL Server files run locally where their servers are available
- [x] 4.3 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `openspec validate dev-1996-consecutive-periods-counts-rows-not-calendar-periods-so-a --strict`
