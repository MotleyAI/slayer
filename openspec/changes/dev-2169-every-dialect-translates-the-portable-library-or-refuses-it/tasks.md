## 1. Tests first (pr-tests)

- [x] 1.1 `tests/test_dev2169_dialect_library_support.py`: every registered dialect plus MariaDB × every library function (each spelling: `~`, `~*`, `->>`, `json_extract*`, `POSITION`, `strpos`), explicit `TRY_CAST` and implicit text-to-number coercion (filters, arithmetic, list membership, saved measures, computed dimensions, stages, ORDER BY, dry run) against a golden expected-set table: supported → emits, parses in that dialect, has the expected shape and none of the forbidden Postgres tokens (`JSONB_`, `PG_INPUT_IS_VALID`, `REGEXP_MATCH`, `REGEXP_I_LIKE`, `~`, the `'g'` flag, …); unsupported → `UnsupportedServerFunctionError` at plan time naming function and database. Verify: fails on the current code.
- [x] 1.2 Same file: direct `render_scalar_call` / `render_cast` on an unsupported dialect (and on the undeclared-backend `SqlDialect()`) refuses; every vendor-primitive hook on `SqlDialect` refuses; no base default outside the library (passthroughs, date functions, aggregates) renders an `exp.Anonymous`. Verify: fails today.
- [x] 1.3 Same file: every list construct (list function, literal, element access, list child, child filter, nested hop, stage, `collect`) refuses with `ListTypeError` on every non-native-list dialect, and emits and parses on every native-list dialect including Presto. Verify: runs in the unit suite.
- [x] 1.4 Docs-table test: parse the support table in `docs/database-support.md` and compare it with every dialect's declared set. Verify: fails until the table exists.
- [x] 1.5 Presto emission tests (`tests/dialects/test_default_tier2.py` or a new file): JAVA flavour with `(?s)` and `\z`, `REGEXP_EXTRACT`, `JSON_EXTRACT_SCALAR`, Presto `split_part` / `repeat`, no Postgres-only token.
- [x] 1.6 MySQL version gates: regex functions / operators, numeric `TRY_CAST` and coercion need 8.0; `json_value` 8.0.21 (8.0.20 refuses, 8.0.21 emits); unknown emits; version parsing of `8.0.21-…`, `10.11.6-MariaDB`, malformed; MariaDB refusals intact; `profile_needed` true for implicit numeric `TRY_CAST`, aliases and regex operators.
- [x] 1.7 `ServerProfile.version` as `(major, minor, patch)`: Postgres, SQL Server and MySQL parsers; the version error prints every part.
- [ ] 1.8 Live Postgres (`pytest-postgresql`, unit suite): `VARCHAR[]` and `BIGINT[]` list columns × `contains` / `contains_any` / `contains_all` against text / integer literals and against another list column (`TEXT[]`, `INTEGER[]`).
- [x] 1.9 `collect` over a column whose physical type differs from its declared DataType, all-NULL group → `[]`, on DuckDB and live Postgres. The pre-seeded BigQuery `test_collect_over_exact_numerics` stays as written.
- [x] 1.10 `_PG_OID_MAP` codes 1185, 1014, 2951, 199, 3807, 1001 → `list`, one assertion each.
- [x] 1.11 ClickHouse cell reader round-trip over every escape, Windows paths, doubled / trailing backslash, malformed `\x`; unknown-escape expectation taken from a live ClickHouse round-trip (testcontainers) and pinned.
- [x] 1.12 Library-matrix domain-edge rows (`tests/_dev2120_fixtures.py`): `ln` / `log10` / `log2` / `log(x)` / `log(2, x)` / `sqrt` over 1, 0, -1; `pow` / `power` over (-8, 0.5), (-8, 3), (0, -1), (4, 0.5) — NULL, never an error, on every Tier-1 matrix backend; literal out-of-domain calls (`ln(0)`, `sqrt(-1)`, `log(1, x)`, `pow(-8, 0.5)`) → `ScalarArgumentError`; SQLite `slayer_*` UDFs return NULL.
- [x] 1.13 SQL Server integration (`tests/integration/test_integration_sqlserver.py`): `json_value` over a 5,000-character scalar raises on 2022+ (grouped), and missing / JSON null / object / array / malformed / short cases, root / nested / quoted paths, in GROUP BY / HAVING / ORDER BY and with `RETURNING`; `split_part` returns a 5,000-character field; `json_value(payload, '$')` over the scalar document `"rootscalar"` reads `'rootscalar'`.
- [x] 1.14 Codex review of the tests against this change's specs and design (pr-tests stage).

## 2. Support set and the one check

- [x] 2.1 `SqlDialect.library_functions` (empty default) and per-dialect sets per design D3; `require_library(dialect, name)` raising `UnsupportedServerFunctionError`. Verify: 1.1 sets agree.
- [x] 2.2 Call `require_library` at elaboration after every rewriting / coercion pass over the final keys, and at render dispatch in `render_library_call` and `render_cast`. Verify: 1.1, 1.2 pass.
- [x] 2.3 List refusal suggestion text names Presto. Verify: 1.3.

## 3. Base class: neutral AST or refusal

- [x] 3.1 Move every Postgres spelling (text, math, JSON, regex, `TRY_CAST`, list, `collect`) from `SqlDialect` into `PostgresDialect`; vendor-primitive hooks on the base raise the typed error. Verify: 1.2 passes; the Tier-1 library matrix still passes.
- [x] 3.2 Give every Tier-1 dialect that inherited a Postgres spelling its own (DuckDB, BigQuery, Snowflake, ClickHouse, MySQL, T-SQL, SQLite). Verify: `tests/test_dev2120_library_matrix.py` and the dialect test files pass.
- [x] 3.3 `extremum_propagates_null` flag; remove the Trino family / MySQL `build_extremum` overrides; Oracle sets it. Verify: matrix `greatest` / `least` rows pass.

## 4. Presto and Tier 2

- [x] 4.1 Move the Trino library overrides and `regex_flavour = JAVA` to `PrestoFamilyDialect`; `TrinoDialect` keeps what differs; check each against the prestodb docs (the container check dropped, user); leave out what Presto lacks. Verify: 1.5 passes; Trino live suite unchanged.
- [x] 4.2 Tier-2 translations for `like`, `ilike`, `trim`, `ltrim`, `rtrim`, `replace`, `instr` / `strpos`, `greatest`, `least` on Redshift, Databricks, Spark, Oracle, checked against vendor docs (references in the PR description). Verify: 1.1 Tier-2 rows pass.

## 5. Item fixes

- [x] 5.1 MySQL `server_profile_sql` / `parse_server_profile` / `profile_needed` and gates; `ServerProfile.version` normalised; Postgres / SQL Server parsers adapt. Verify: 1.6, 1.7.
- [x] 5.2 Postgres `contains_any` / `contains_all` canonical array casts. Verify: 1.8.
- [x] 5.3 One per-dialect untyped empty list, used by each `build_collect` and by the join-back `EmptyList` fallback; the typed `empty_list` goes. Verify: 1.9; BigQuery CI job green.
- [x] 5.4 `_PG_OID_MAP` additions. Verify: 1.10.
- [x] 5.5 ClickHouse `_quoted` full escape set. Verify: 1.11.
- [x] 5.6 Domain guards (NULLed argument, after the log fix-up), SQLite UDFs return `None`, `check_domain` literal cases. Verify: 1.12.
- [x] 5.7 T-SQL `json_value` strict-reread guard (2022+ / unknown) and nested-`CHARINDEX` `split_part`; `ISJSON(…, VALUE)` on 2022+ / unknown, so a scalar document is JSON. Verify: 1.13 (SQL Server workflow or local Docker).

## 6. Docs and architecture

- [x] 6.1 `docs/database-support.md`: per-dialect support table, SQL Server gates and the 4,000-character and scalar-document caveats below 2022, a Postgres section (TRY_CAST / coercion / `json_value` need 16+), SQLite `slayer_*` functions and plain-`sqlite3` caveat, Tier-2 + Presto refusals and Oracle `''` caveat, MySQL gates, ClickHouse re-ingest note, Snowflake untyped-ARRAY inference, BigQuery `count(list)`, `collect` aggregation row, list exclusions naming every refusing database. Verify: 1.4 passes.
- [x] 6.2 `docs/concepts/references.md`: out-of-domain list, overflow sentence, list-rejecting databases. Verify: matches 6.1 and the specs.
- [x] 6.3 `architecture/system.arc42.md` 10.2: `[target: DEV-2169]` → `[enforced: test:tests/test_dev2169_dialect_library_support.py]` (approved wording); `poetry run la-arch-check` passes.

## 7. Gates

- [x] 7.1 Full unit suite, the CI integration invocation, `ruff check slayer/ tests/`, `basedpyright` (no new errors), `la-arch-check` all pass.
