## 1. Failing tests (pr-tests)

- [x] 1.1 `tests/test_dev2018_logical_types.py` (unit; SQLite, DuckDB, and Postgres via pytest-postgresql with a non-UTC session zone such as `America/Sao_Paulo`). Executed values and Python types (SQLite text shape) for every built-in granularity, a `month`-based and a `minute`-based custom granularity, over DATE and TIMESTAMP sources and NULL. Verify: it fails today on the type assertions.
- [x] 1.2 In the same file, the issue's three examples over DATE and TIMESTAMP sources (`max(date_trunc('month', x)) >= 'last month'`, `date_diff('month', date_trunc('month', a), b)`, `date_add(date_trunc('month', x), 1, 'month')`), plus the `queries/date-functions` and `queries/time-points` scenarios. Verify: green on SQLite and DuckDB where the behaviour already holds, failing where the bucket type changes.
- [x] 1.3 Mixed DATE/TIMESTAMP (`queries/mixed-type-coercion` scenarios). Verify: today SQLite returns 0 rows for `d = ts` and `date_trunc('day', ts) = ts`.
  - `d = ts`, `date_trunc('day', ts) = ts`, `date_trunc('day', ts) = d`, `d = date_trunc('hour', ts)`;
  - `in`, `IS [NOT] DISTINCT FROM`;
  - `coalesce(shipped_at, d)` / `greatest` / `iif` arms against a TIMESTAMP;
  - `nullif(d, ts)` in both argument orders, with type and NULLs.
- [x] 1.4 SQLite storage spellings (space, `T`, date-only, fractional, invalid text) through buckets, mixed and same-type comparisons, null-safe equality, `in` and the spine join. Verify: the `T` / date-only rows fail today.
- [x] 1.5 Time spine over a DATE axis on SQLite and DuckDB, month and sub-day. Two-stage propagation: an inner stage with month(DATE), hour(DATE), month(TIMESTAMP) and a custom bucket; the outer stage re-buckets, compares, orders, and applies `date_add` / `date_diff`, asserting `StageSchema` types and values.
- [x] 1.6 Over DATE and TIMESTAMP buckets: trailing windows, `whole_periods_only`, consecutive periods, ±`time_shift` (day, sub-day, month-end), and `week_sunday` Sunday/Monday boundaries.
- [x] 1.7 `temporal_type` / `value_type` unit cases: `hour(d)` TIMESTAMP, `fiscal_year(d)` DATE, `quarter_hour(d)` TIMESTAMP, `week_sunday(d)` DATE, `month(ts)` TIMESTAMP; `nullif(d, ts)` DATE.
- [x] 1.8 `tests/dialects/`: `build_bucket(operand=…)` emission per Tier-1 dialect for each unit class and source type; promotion emission for comparisons, conditionals and join-backs; BigQuery zoned-`TIMESTAMP` buckets (built-in, custom, spine) read through `read_column` once; Tier-2 inherited emission. Verify: it fails today on the operand argument.
- [x] 1.9 pg facade: a bucket projection advertises the DATE OID for month(DATE) and the TIMESTAMP OID for hour(DATE), with binary and text encoding (`tests/` pg-facade unit suite).
- [x] 1.10 Ossie export: none here — a bucket does not export as standard SQL (DEV-2266), and a column's `sql` is author SQL exported as written.
- [x] 1.11 Integration type matrices (same assertions as 1.1 and 1.3) in `tests/integration/test_integration_{mysql,clickhouse,sqlserver,trino,snowflake}.py`.
- [x] 1.12 Codex review of the tests against this change; resolve findings with the user.

## 2. Core typing

- [x] 2.1 Add the bucket type rule beside `granularity_parts` / `nests_into`. `temporal_type` and `value_type` read it for `TimeTruncKey`. Verify: 1.7 passes.
- [x] 2.2 Add the DATE-in-TIMESTAMP-position rule in `core.keys` beside `numeric_boolean_positions`: comparisons, `in`, null-safe equality, TIMESTAMP-typed conditional arms, and `nullif`'s comparison. Verify: unit cases for positions.

## 3. Dialect emission

- [x] 3.1 `SqlDialect.build_bucket(col_expr, granularity, operand)` returns exactly the bucket type. Add the `demote_to_date` hook. Base / Postgres / DuckDB truncate `CAST(x AS TIMESTAMP)` and cast DATE results back. Custom granularities demote. `week_sunday` keeps the operand type. Verify: 1.8 base/Postgres/DuckDB and 1.1 on DuckDB and Postgres.
- [x] 3.2 SQLite: canonical TIMESTAMP bucket text at every unit; spelling-independent temporal comparison form (decision 18) for non-literal pairs. Verify: 1.1, 1.3 and 1.4 on SQLite.
- [x] 3.3 MySQL, ClickHouse, Snowflake, T-SQL, Trino and BigQuery overrides per decision 9. Verify: 1.8 for each, and 1.11 in the path-gated integration workflows.
- [x] 3.4 `bucket_offset` / `bucket_step` take the bucket's type. Delete `bucket_comparand` and its generator call sites. Verify: 1.6 and the time-shift goldens.

## 4. Rendering and generation

- [x] 4.1 Generalise `numeric_parts` into the one operand-coercion function (boolean→int and DATE→TIMESTAMP). Apply it in both renderers' comparison, `in`, null-safe, conditional and `iif` paths. Render mixed `nullif` as a `CASE`. Verify: 1.3.
- [x] 4.2 `_build_date_trunc(key, column_type)` at every generator bucket site (base grouping, trailing windows, ranked expressions, regroup, time-shift re-bucketing, consecutive periods, spine series), and both renderers' `TimeTruncKey` branches. Verify: 1.1, 1.5 and 1.6.
- [x] 4.3 Typed join-back pairs: `_keyed_pairs` keeps member keys; `build_grain_joinback_condition` takes types and promotes; a temporal pair with an unknown type fails closed; time-shift and consecutive-period members are keyed, not `None`. A spine bound applied to a DATE axis becomes `ceil_day` as a DATE literal. Verify: 1.5 and 1.8.
- [x] 4.4 pg facade `_record_time_grain` advertises the bucket type. Verify: 1.9.

## 5. Architecture, docs, goldens

- [x] 5.1 Apply sql principle 14 to `architecture/python.sql.arc42.md` verbatim (decision 6, user-approved wording). Verify: `poetry run la-arch-check`.
- [x] 5.2 Docs: `docs/concepts/time.md` (bucket type, midnight rule, breaking output types), `docs/concepts/references.md` (date functions), and `docs/concepts/queries.md` (mixed types). Verify: `zensical.toml` nav unchanged (no new pages).
- [x] 5.3 Re-bless the goldens under `tests/golden/` and `tests/dialects/`, each changed line confirmed as caused by a bucket or a promotion. Re-execute notebooks with SQLite / DuckDB outputs. Verify: golden tests and `tests/integration/test_notebooks.py` pass.

## 6. Gates

- [x] 6.1 Local test scope per `/la:pr` (`la-affected-tests`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`, and `npx -y likec4@1.47.0 validate architecture`.
