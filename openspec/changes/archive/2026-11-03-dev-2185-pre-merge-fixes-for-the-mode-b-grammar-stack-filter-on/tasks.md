## 1. Tests first (pr-tests stage)

- [x] 1.1 Cast type grammar and lossless check: one test per `queries/casts` scenario (precision before time-zone words, dropped precision refused, UNSIGNED on MySQL vs Postgres, malformed type error names the type, array cast per dialect, SQLite array refused, `T[][]` refused, accepted aliases like `INT4` / `CHARACTER VARYING(10)` stay accepted); verify they fail on the current code
- [x] 1.2 Array cast typing: `contains(CAST(codes AS INT[]), 3)` binds and plans as list of INT (Postgres/DuckDB execution), `INT UNSIGNED` plans INT on MySQL; verify they fail now
- [x] 1.3 TRY_CAST integer rule: execution on SQLite, DuckDB, Postgres (pytest-postgresql) in the unit suite — `'1.5'`, `'1e3'`, `'-12'`, `' 7 '`, `'12abc'`, `'99999'` → SMALLINT, DOUBLE 1.5 / 2.0, DOUBLE target `'1.5'`, `TRY_CAST` to `INT[]` refused; emission tests for MySQL, ClickHouse, SQL Server, Trino, Snowflake, BigQuery; integration scenarios added to the MySQL, ClickHouse, SQL Server, Trino suites and to the BigQuery / Snowflake live suites; verify the unit ones fail now
- [x] 1.4 Comments: one test per `queries/expression-grammar` › SQL comments scenario, plus error-column exactness after a comment and a `{var}` inside a comment; verify they fail now
- [x] 1.5 Canonical printer: property test `parse_expr(canonical_measure_text(t))` equals `parse_expr(t)` over a corpus covering every operator level, associativity, unary chains, `**` right-associativity, casts, FILTER, CASE-lowered `iif`; the `(a + b) * c` / `a + b * c` and `-(-b)` scenarios; verify they fail now
- [x] 1.6 Stored upgrade: the "Adjacent minus signs" scenario plus multi-line text, a string holding `--`, and Python `#` comments in legacy text; verify they fail now

## 2. Type grammar and casts

- [x] 2.1 Rewrite `_TYPE_SPEC` in `slayer/core/syntax.py` (words, parameter list between words, more words, optional `[]`, accurate malformed-type error) for both `CAST` and `::`; verify 1.1 parse tests pass
- [x] 2.2 Normalized cast-signature comparison in `slayer/sql/dialects/base.py` plus the per-dialect expressibility hook (unsigned families, arrays, vendor types) in each `dialects/*.py`; verify 1.1 refusal tests pass and no golden changes except intended refusals
- [x] 2.3 `CastKey` element type, `cast_data_type` over the parsed type, `_bind_cast` population, `list_element_type` and `key_metadata` branches; verify 1.2 passes
- [x] 2.4 Refuse `TRY_CAST` to an array type with the typed cast error; verify its 1.3 test passes

## 3. TRY_CAST integer validity

- [x] 3.1 Integer-text predicate (optionally signed digits, spaces) beside the existing number predicate, selected from the target type; numeric input valid iff integral and in range; verify predicate unit tests
- [x] 3.2 Apply it per dialect: SQLite UDF, MySQL guard, DuckDB, Postgres 16 (check native already matches), ClickHouse, SQL Server, Trino, Snowflake, BigQuery — each in its own `dialects/*.py`; verify 1.3 unit + emission tests pass

## 4. Comments and canonical text

- [x] 4.1 Region-aware comment masking in `core.syntax` (spaces one-for-one, newlines kept, nesting, unterminated-`/*` error at its start), run before placeholder validation; verify 1.4 passes
- [x] 4.2 Precedence-aware `canonical_measure_text`; verify 1.5 passes and re-bless only goldens/messages whose text gained necessary parentheses
- [x] 4.3 Audit every site that composes Mode-B text by concatenation (grep `core/`, `engine/`, `storage/` for string-built formulas/filters); fix any whose trailing `--` could swallow text, or raise to the user if outside DEV-2187's scope; record the result here
  - Result: fixed `bind_variables` (a negative value after `-` made `--`; now parenthesised) and the dbt `offset_window` → `time_shift(x, -{count})` composition (a negative count made `--`). Left to DEV-2187 (text slicing of Mode-B, display-only or erroring): `_functional_suffix_text` (recommend path), `granularity_call_argument` and `_reject_time_dimension_placeholders` in `core/query.py`.

## 5. Stored upgrade

- [x] 5.1 Confirm the DEV-2101 version step is unreleased (not in 1.5.0 on main); extend `slayer/storage/sql_string_migration.py` to space adjacent `-` tokens (`end == start`); verify 1.6 passes and the 0.10.2 store scenario still returns its recorded rows

## 6. Docs and gates

- [x] 6.1 One sentence each in `docs/concepts/references.md` (type grammar + arrays; comments; TRY_CAST integer rule with the `round` remedy); verify `zensical.toml` nav unaffected
- [x] 6.2 Full unit suite `poetry run pytest -m "not integration"`, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check`; all green
