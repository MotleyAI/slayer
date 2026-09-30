## 1. Tests first (pr-tests stage — all must fail before implementation)

- [x] 1.1 `tests/test_dev1737_date_functions_parse.py`: unit/part literal validation, case folding and interning, non-literal unit rejection, zero-arg `current_date()`/`now()`, arity + keyword rejection, every `interval` rewrite form (both commuted `+`, `-`, left-folded chains) interning identically to explicit `date_add`, and every rejected `interval` position (standalone, left of `-`, negated, multiplied, compared, nested, interval+interval, function argument); verify each fails on the current tree
- [x] 1.2 `tests/test_dev1737_date_functions_typing.py`: checker rejections (TEXT/INT/untyped column, arithmetic, number, non-ISO string) as the new `QueryTypeError` subclass naming the operand; accepted operands (declared DATE/TIMESTAMP local/joined/stage columns, `min`/`max`/`first`/`last`, `date_add`, clock calls, conditionals incl. `coalesce(shipped_at, now())`); ISO literal typing incl. inside `coalesce`, invalid calendar values, `{variable}` placeholders; count rules (fractional/bool/NULL/string literal rejected, non-numeric computed count rejected); `temporal_type` unit tests; response metadata types for measures and expression dimensions
- [x] 1.3 `tests/test_dev1737_scalar_specs.py`: spec-table derivation (`SCALAR_FUNCTIONS`, arity, `SCALAR_PASSTHROUGH` subset excludes date fns/`interval`/`like`/`iif`), legacy `parse_formula` recognises the date functions, OSI does not carry `DATE_ADD`/`DATEDIFF` over verbatim, custom aggregations named after the new functions rejected
- [x] 1.4 Approved test edits (user OK in pr-plan Q11): `tests/test_dev1744_value_expr.py:1870` and `tests/test_dev1753_last_four_scalars.py:78` assert `SCALAR_FUNCTIONS - SCALAR_PASSTHROUGH == {"like", "iif", "date_part", "date_diff", "date_add", "interval", "current_date", "now"}`; import `SCALAR_PASSTHROUGH` from its new home (mechanical) in those files and `tests/test_dev1576_heals.py`
- [x] 1.5 `tests/dialects/test_date_functions.py`: exact SQL per Tier-1 dialect (sqlite, postgres, duckdb, mysql, clickhouse, tsql, snowflake, bigquery) for every `date_part` part, every `date_diff` unit (DATE, TIMESTAMP and mixed operands), every `date_add` unit (DATE and TIMESTAMP operand, literal and computed count), `current_date`, `now`, temporal literals; BigQuery DATE_* vs TIMESTAMP_* branches; T-SQL `day_of_week` independent of `DATEFIRST`
- [x] 1.6 `tests/dialects/test_dev1753_scalar_emission.py`: exact per-dialect SQL for `ceiling`, `sign`, `ltrim`, `rtrim`, `substring` on all 8 Tier-1 dialects (issue acceptance: emission coverage in `tests/dialects/`)
- [x] 1.7 `tests/dialects/test_sqlite_date_udf.py`: `slayer_date_add` unit tests — clamping both directions, leap years, date-only shape preserved for ≥day units, time kept, fractional seconds, `T` separator, NULL and malformed input → NULL
- [x] 1.8 `tests/test_dev1737_date_functions_exec.py` (unit suite, SQLite + DuckDB via in-process engines): every part/unit end to end incl. Dec 31→Jan 1, 23:59→00:01, Jan 31→Feb 1, reversed operands, Monday/Sunday week edges, Dec 30 2024 = ISO week 1 / iso_year 2025, Sunday = 7, clamping, leap day, DATE-in→DATE-out, sub-day on DATE, computed counts ±2.7 → ±2, NULL operands/counts, fractional-second timestamps, operator vs `date_add` same result, ISO literals, `current_date()`/`now()` relative sanity, SQLite malformed stored date → NULL
- [x] 1.9 Regressions (unit suite): SQLite hourly `time_shift` (spec scenario values), month `time_shift` on a month-end daily bucket on SQLite + DuckDB, one-month trailing window ending at month-end on SQLite + DuckDB; each verified failing on the current tree
- [x] 1.10 All-dialect parity emission tests for the unified date-add primitive: `time_shift` offsets per granularity (incl. quarter, week, week_sunday), `week_sunday` truncation, trailing-window frames with multi-part durations, positive and negative frames
- [x] 1.11 Cache: a query with `now()` / `current_date()` (root or inside a stage) is neither served from nor stored in the result cache, including refresh re-population; a plain date-function query still caches
- [x] 1.12 Integration execution: Postgres (runs in CI) for every function/unit incl. clamps and boundaries; MySQL, ClickHouse, SQL Server in their existing integration files (skip when the server is unavailable)

## 2. Scalar spec table and parsing

- [x] 2.1 `SCALAR_SPECS` / `ScalarSpec` in `slayer/core/keys.py`; derive `SCALAR_FUNCTIONS`, `SCALAR_FUNCTION_ARITY`, `SCALAR_PASSTHROUGH`; `core/formula.py` recognises against `SCALAR_FUNCTIONS`; `osi/expression.py` imports the passthrough subset; verify 1.3/1.4 pass
- [x] 2.2 `DatePart` enum in `core/enums.py`; parser unit/part literal validation + lower-casing; `interval` rewrite + stray-`interval` rejection in `engine/syntax.py`; verify 1.1 passes

## 3. Typing

- [x] 3.1 Extend `Scalar` / `normalize_scalar` with `datetime.date` / `datetime.datetime`; binder converts ISO literals in temporal positions (incl. nested conditionals) and checks literal counts; verify ISO-literal cases of 1.2
- [x] 3.2 `temporal_type` in `core`; new `QueryTypeError` subclass raised from `engine/elaborate_env.py` for non-temporal operands and non-numeric counts; verify 1.2 checker cases
- [x] 3.3 Result types in `engine/key_metadata.py` (`measure_key_type`, `dimension_key_metadata`) incl. joined and stage-backed columns; verify 1.2 metadata cases

## 4. Rendering

- [x] 4.1 Typed hooks on `SqlDialect` (`build_date_part`, `build_date_diff`, `build_date_add`, `build_current_date`, `build_current_timestamp`, `build_temporal_literal`) with the composed base; `render_scalar_call` / `render_value_key` / `render_row_expression` dispatch date names with operand types from `temporal_type` + `ScopeFrame.column_type`; `_literal` handles date values via the hook; verify 1.5 base (postgres) cases
- [x] 4.2 Per-dialect overrides in `slayer/sql/dialects/{sqlite,duckdb,mysql,clickhouse,tsql,snowflake,bigquery}.py`; verify 1.5 passes for every dialect
- [x] 4.3 SQLite `slayer_date_add` UDF registered in `register_udfs`; verify 1.7
- [x] 4.4 Replace `build_time_offset_expr` and `duration_interval_exprs` with `build_date_add` at every call site (`sql/generator.py` time_shift join-back and window frames, base `week_sunday` truncation; T-SQL/SQLite overrides removed); verify 1.9 and 1.10, re-bless byte-equivalence goldens whose SQL changed
  - User-approved (pr-tests, 2026-09-28): existing tests pinning the deleted hooks (`build_time_offset_expr`, `duration_interval_exprs`, `add_intervals_expr`) in `tests/dialects/test_{base,sqlite,tsql,postgres,duckdb,mysql,clickhouse,generator_dispatch,generator_delegation,dev1934_dialect_hooks}.py`, `tests/test_sql_generator.py`, `tests/integration/test_integration_sqlserver.py`, and the SQLite SQL-text assertions in `tests/test_time_shift_period_boundary.py`, may be ported to `build_date_add` where their intent survives (delegation, quarter/week normalisation, T-SQL `DATEADD`) or deleted where the 1.10 parity/golden tests supersede them — no per-test stop; list every such edit in the commit message
- [x] 4.5 Clock cache bypass (`CLOCK_FUNCTIONS`, `reads_clock` across all emitted stages) in `engine/query_engine.py` / `engine/cache.py`; verify 1.11
- [x] 4.6 T-SQL 2-arg `substring` / `substr` emit `SUBSTRING(s, p, DATALENGTH(s))` (user-approved in pr-tests; `LEN` → `DATALENGTH` approved in pr-review, `LEN` drops trailing spaces); verify `tests/dialects/test_dev1753_scalar_emission.py` and the SQL Server `test_two_arg_substring_executes`

## 5. Architecture and docs

- [x] 5.1 Apply the approved `architecture/system.arc42.md` §3.10 edit: "one canonical `SCALAR_PASSTHROUGH` set" → "one canonical scalar allowlist"; run `uvx --no-build --from living-architecture==0.2.0 la-arch-check`
- [x] 5.2 `docs/concepts/references.md`: scalar table + "Date and time functions" section (every unit/part per function, boundary counting + elapsed-units recipe, month-end clamp, ISO `day_of_week`/`week`/`iso_year` and the year+week trap, operand typing and rejection, ISO literals, `interval` spelling, count truncation, SQLite malformed → NULL, clock zone per backend, cache bypass); `docs/concepts/queries.md` allowlist sentence; caching docs; `.claude/skills/slayer-query.md` scalar list; verify every new/changed page is in `zensical.toml` nav (`.claude/skills/slayer-query.md` was deleted, so there is no skill list to update)

## 6. Verification

- [x] 6.1 `poetry run pytest -m "not integration"` fully green
- [x] 6.2 Integration suite with the CI invocation from CLAUDE.md green
- [x] 6.3 `poetry run ruff check slayer/ tests/` clean; `poetry run basedpyright` no new errors vs baseline; `npx -y likec4@1.47.0 validate architecture`
