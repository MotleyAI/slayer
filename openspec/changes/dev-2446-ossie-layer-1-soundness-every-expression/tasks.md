## 1. Tests first (pr-tests stage)

- [x] 1.1 Cast matrix for "CAST has one meaning on every dialect" (BOOLEAN from numbers incl. NULL/0/negative/decimal and `= TRUE` filters, integer rounding incl. 2.675 and ties, DECIMAL scale rounding from numbers and numeric text, bare `DECIMAL` / `NUMERIC` / `DECIMAL(p)` scale 0, `VARCHAR(n)` incl. non-ASCII `'ñandú'`, NULL and trailing spaces): executed on SQLite and DuckDB in the default suite, Postgres via pytest-postgresql, MySQL/ClickHouse/SQL Server/Trino in their `tests/integration/test_integration_<db>.py` suites, emission assertions for Snowflake and BigQuery; each test fails on main
- [x] 1.2 `TRY_CAST` scenarios (number → integer rounds, rounding decides the range at ± half a unit around signed bounds for SMALLINT/INT/BIGINT, number → BOOLEAN, DECIMAL rounding and overflow → NULL, NaN/infinity where the dialect has them, ClickHouse `Nullable`) on the same dialect spread; fail on main
- [x] 1.3 Apply the three approved alignments (design decision 5): `tests/_cast_fixtures.py` `TRY_CAST_TARGET_CASES` (`CAST(1.5 AS DOUBLE)`→INT and `1.5`→INT expect 2); `tests/test_try_cast_integer_targets.py` `test_fractional_double_column` expects `{1: 1, 2: 2, 3: 2, 4: 3, 5: 1}` and `TestJsonSource` `{"v": 1.5}` → SMALLINT expects 2; any further test pinning the old behaviour → STOP and ask before changing it
- [x] 1.4 SQLite requirement "SQLite casts to boolean, decimal and varchar types" (boolean words / other text → NULL, `'12abc'` → DECIMAL NULL, decimal overflow NULL, never a numeric prefix); fail on main
- [x] 1.5 Regression tests that internal casts keep their values (design decision 18): declared boolean result casts, aggregate boolean restoration, `read_column` of zoned columns, JSON `RETURNING`, list element coercion, temporal comparisons; pass on main and must keep passing
- [x] 1.6 Library matrix (`tests/_dev2120_fixtures.py`): negative-digit `round` cases (`round(i, -2)` over 1250 / -1250 / 1249, `round(d, -1)` over 1234.5678) executed on SQLite and DuckDB and emitted for every Tier-1 dialect; fail on SQLite on main
- [x] 1.7 `AGG` in a field expression: save refusal (`AGG(amount)`, `agg(amount) + 1`) with the typed error and wording; query-time backstop for a dataset saved with `_validate=False` with no SQL executed; Mode-A field `filter` unaffected; fail on main
- [x] 1.8 SlayerModel v29 migration: a v28 stored field `AGG(amount) + 1` loads as `amount + 1` with one logged warning naming model and field and returns the same rows; migration-chain tests from before v20 (`agg(amount)` → `agg_aggregation(amount)`, left unchanged by v29), from v20–28, and with a custom `agg` aggregation; fail on main
- [x] 1.9 Strict filter cases (`tests/test_dev2217_filter.py` or a new `tests/test_dev2446_*` file): `LOG(10, x)` admitted; `LOG(x)`, `ROUND(x)`, `TRUNC(x)`, `SUBSTRING(s, 1)`, `REGEXP_LIKE` / `REGEXP_REPLACE` with flags → `E_INVALID_QUERY` and answer natively; `TIMESTAMP_NTZ` cast admitted; `APPROX_PERCENTILE`, `percentile_disc(x, p)` → `E_INVALID_QUERY`; `MEDIAN` → `E_AGGREGATE_IN_WHERE`; aliases `STDDEV` / `VARIANCE` / `APPROX_COUNT_DISTINCT` and percentile shapes; `ABS(x, 1)` / `UPPER(s, 1)` → `E_INVALID_QUERY`; fail on main
- [x] 1.10 Mechanical alignment (Ossie-determined, design decision 22): `tests/test_dev2217_filter.py` `round` and `trunc` cases flip to refused and `test_a_scalar_is_admitted_exactly_when_flagged_ossie` judges by the case's argument count against the Ossie argument counts
- [x] 1.11 REQUIRED-construct manifest test (design decisions 15, 24): one row per construct of the vendored `tests/fixtures/ossie_corpus/core-spec/expression_language.md` REQUIRED tables plus the prose requirements (`FILTER`, `AGG` reservation, numeric semantics, `CAST` / `TRY_CAST`, window NULL order), each `admitted` (Ossie's value on DuckDB and SQLite, admitted under strict) or `gap: DEV-2282` (refused under strict); a registry test parses the REQUIRED tables and headings and fails on any construct or "(REQUIRED)" heading without a row; a second registry test cross-checks the declared Ossie argument counts against the spec's Syntax column
- [x] 1.12 Codex-review the tests against this change; fold or record each finding in this file
  - Folded: Postgres TRY_CAST pins aligned (`tests/test_cast_postgres.py`); doubles at the BIGINT bounds run on SQLite too. Rejected: Postgres <16 / 16 profile tests already exist (`tests/test_dev2101_casts.py::TestPostgresTryCast`).

## 2. Author casts (implement stage)

- [x] 2.1 Dedicated author-cast lowering hook in `slayer/sql/dialects/base.py`, called only from the author-cast render (`render_cast`, `slayer/sql/render/row_expr.py`) with the source type; internal `build_cast` / `build_try_cast` callers unchanged; verify 1.5 stays green
- [x] 2.2 Base conversion definition: BOOLEAN (`x <> 0` for numbers), integer rounding half away from zero, DECIMAL scale rounding with omitted scale 0 / precision 38, `VARCHAR(n)` truncation by code points; verify 1.1 on DuckDB
- [x] 2.3 Per-dialect overrides where native differs (SQLite, SQL Server, ClickHouse rounding; ClickHouse DECIMAL; MySQL / SQL Server BIT boolean; Snowflake / BigQuery `VARCHAR(n)` errors; BigQuery `BIGNUMERIC(38, 0)`), typed refusal where a dialect cannot express a rule; verify 1.1 emission for every Tier-1 dialect and live runs where available
- [x] 2.4 SQLite: boolean / decimal / varchar emulation (UDF or AST) per "SQLite casts to boolean, decimal and varchar types"; verify 1.4
- [x] 2.5 `TRY_CAST` built from the same definition: NULL → round → range check on the rounded value → guarded native cast inside the `CASE`; number → BOOLEAN; DECIMAL rounding and overflow NULL; a JSON number with a fraction read as a number on every Tier-1 dialect (decision 25); verify 1.2 and 1.3

## 3. ROUND

- [x] 3.1 SQLite two-argument `round` with a negative digits argument rounds left of the point half away from zero; verify 1.6 on every dialect

## 4. AGG in field expressions

- [x] 4.1 One save-time rule refusing `AGG` in a field's Mode-B expression, shared with the Ossie-document rule and its wording; verify 1.7 save cases
- [x] 4.2 Query-time backstop: binding a field expression that carries an `AGG`-wrapped reference raises the same typed error; verify 1.7 backstop case
- [x] 4.3 SlayerModel v29 migration (`slayer/storage/`, registered after v28; `CURRENT_VERSIONS["SlayerModel"] = 29`) unwrapping `AGG(` in field Mode-B expressions with a logged warning; verify 1.8

## 5. Strict judgement

- [x] 5.1 `ScalarSpec` gains the Ossie argument counts (declared in `slayer/core/keys.py`; `min_args` / `max_args` unchanged); `log` → {2}; `OSSIE_ARITIES` removed; verify 1.9 / 1.10 and the 1.11 cross-check
- [x] 5.2 Aggregate admission from the canonical parse (Ossie spelling + normalises to a built-in + supported shape); verify 1.9 aggregate cases
- [x] 5.3 `TIMESTAMP_NTZ` in the strict cast types; scalar wrong argument count → `E_INVALID_QUERY` (judged before lowering); verify 1.9
- [x] 5.4 Under strict, a bare Ossie keyword SLayer does not support yet (CURRENT_TIME) raises E_INVALID_QUERY, not E_NAME_NOT_FOUND; verify 1.11

## 6. Docs and close-out

- [x] 6.1 `docs/concepts/references.md` cast bullets (lines on `CAST` / `TRY_CAST` semantics) and `docs/concepts/expressions.md` `round` row: new meanings; verify by reading the rendered text against the spec
- [x] 6.2 `docs/upgrading/2.0.md` Part 1: one section per break (SQLite casts, integer rounding, bare `DECIMAL`, `VARCHAR(n)`, `TRY_CAST` fractional / boolean, SQLite negative-digit `round`, `AGG` in field expressions, strict argument counts), each with before / after
- [x] 6.3 `docs/interfaces/ossie-layer-3.md`: strict filter argument-count rule and refused aggregates
- [x] 6.4 Post release-note lines for every break as a comment on DEV-2220
- [x] 6.5 Enforcement bundle green: `poetry run la-arch-check`, `npx likec4 validate`, `la-typecheck`; `poetry run ruff check slayer/ tests/`; local test scope per the /la:pr flow
