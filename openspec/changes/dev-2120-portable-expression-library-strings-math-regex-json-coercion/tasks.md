## 1. Tracker records (pr-plan)

- [x] 1.1 DEV-2120 body: point to this change and list the scope settled in planning (initcap dropped; TEXT side always DOUBLE; LIKE pattern language; regex `\n \t \r` and replacement `\\`; SQLite LIKE via GLOB, case-sensitive whatever the collation; `split_part` literal index; Postgres reference / out-of-domain NULL / version gating rules; JSON `db_type` at ingestion); verify by reading the issue

## 2. Failing tests (pr-tests)

- [x] 2.1 Registry-driven matrix `tests/test_dev2120_library_matrix.py`: one case table per library function and Mode-B operator; fails for any `SCALAR_SPECS` entry or operator without cases; executes on SQLite and DuckDB, emits on every Tier-1 dialect; verify it fails on the current code
- [x] 2.2 `queries/scalar-functions` scenarios, plus R2 boundaries: `asin` / `acos` at and beyond ±1, `div` by zero and every sign combination, `split_part` index 0 / negative / beyond range / non-literal, `chr` surrogates and > 0x10FFFF, all-NULL and every-NULL-position `greatest` / `least` (3+ args); each with a literal (plan-time error) and a column (NULL) argument; non-ASCII `é`, `€`, U+1F600; `initcap` unknown; `//` hint → `div` runs; verify failing
- [x] 2.3 `queries/regex` scenarios, plus trailing-newline probes for `^`, `$`, alternation and `'i'`; bracketed classes; variable patterns with bad constructs; replacement `$`, `&`, `\\`, `\1`–`\9`, group beyond the pattern; SQL Server 2022 / unknown-profile emission; verify failing
- [x] 2.4 `queries/json` scenarios, plus missing / JSON null / object / array / wrong type / unconvertible / genuine `0` and `false` for every `RETURNING` type; arrow-chain precedence and the bare `->` hint; aliases; ingestion → render for each native JSON form (Postgres json / jsonb, MySQL, DuckDB, BigQuery JSON, Snowflake VARIANT, ClickHouse JSON) and for a missing `db_type`; verify failing
- [x] 2.5 `queries/mixed-type-coercion` scenarios, plus `BETWEEN`, `IS DISTINCT FROM`, `greatest` with mixed args, `sum(text)` remedy names `TRY_CAST`, author `CAST` untouched, Postgres 15 / 16 / unknown emissions; verify failing
- [x] 2.6 `queries/string-matching` scenarios on every Tier-1 dialect (escaped `%` / `_`, literal `[`, lone backslash, column-valued pattern, GLOB metacharacters on SQLite, Mode-A LIKE unchanged on SQLite); verify failing
- [x] 2.7 `queries/date-functions` `date_trunc` scenarios (row value, dimension, order key, unknown unit, reserved name); verify failing
- [x] 2.8 Integration: the matrix case table runs live in each integration suite (Postgres, MySQL, ClickHouse, Trino, SQL Server parameterised 2022 / 2025, Snowflake, BigQuery); verify they run (skipping where the DB is absent)
- [x] 2.9 `queries/variables` digit-and-comma brace scenario and the regex name-shaped-brace hint scenario; verify failing
- [x] 2.10 Codex review of the tests against this change

## 3. Parser and core syntax

- [ ] 3.1 Regex-pattern, LIKE-pattern and JSON-path parsers in `slayer/core/` (frozen Pydantic ASTs, cached, typed errors naming the construct); verify the parser-level tests in 2.3 / 2.4 / 2.6
- [ ] 3.2 Grammar (`slayer/core/syntax.py`): `~` / `~*` / `!~` / `!~*` → `regexp_like`; `->` / `->>` at the `||` level → `json_value` (bare `->` hint); `RETURNING` → `TRY_CAST` over `json_value`; JSON aliases; `date_trunc` → granularity call; `strpos` → `instr`; verify the parse tests
- [ ] 3.3 `SCALAR_SPECS` entries for every new function with arity and result-type rules; `initcap` absent; verify `tests/test_dev1737_scalar_specs.py` and 2.1's registry check
- [ ] 3.4 Variable substitution: a brace pair inside a string literal holding only digits, commas and spaces stays text; the undefined-variable error from a regex pattern argument adds the `{{` / `}}` hint; verify 2.9

## 4. Binding and checker

- [ ] 4.1 Plan-time validation: patterns, replacements, paths, `regexp_extract` group, `split_part` index, literal out-of-domain arguments; variable-supplied patterns named in errors; verify 2.2–2.4 error scenarios
- [ ] 4.2 Coercion (design D4): checker wraps TEXT operands in `TRY_CAST(… AS DOUBLE)`, converts TEXT literals or raises; `sum` / `abs` over TEXT remedies name `TRY_CAST`; verify 2.5
- [ ] 4.3 `json_value` accepts opaque JSON-family columns; verify 2.4 typing scenarios

## 5. Dialect lowerings

- [ ] 5.1 Typed function-family hooks in `SqlDialect` and `render_scalar_call` dispatch (design D1); string and math lowerings with domain guards (D6) per dialect; verify 2.2 on SQLite / DuckDB and emissions
- [ ] 5.2 `greatest` / `least` coalesce form on NULL-propagating engines (D7); verify 2.2
- [ ] 5.3 Regex flavour records and the shared emitter (D2); per-dialect function lowerings incl. SQL Server `REGEXP_INSTR(…) > 0`, Snowflake `REGEXP_INSTR`, DuckDB extract `''` → NULL, Postgres `'g'`; verify 2.3
- [ ] 5.4 JSON lowerings per dialect and input form (D3, D10), typed `RETURNING` native pairs; verify 2.4
- [ ] 5.5 `build_like` hook (D9): `ESCAPE '\'`, ClickHouse / BigQuery native escapes, SQL Server `[` bracketing incl. column patterns, `ILIKE` fallback; verify 2.6
- [ ] 5.6 SQLite registered functions (D8): regex trio, `slayer_like_glob`, any missing string functions; `LIKE` → `GLOB`, `ILIKE` → native `LIKE`; verify 2.3 / 2.6 on SQLite
- [ ] 5.7 Server-profile predicate generalised (D5); SQL Server `server_profile_sql` / `parse_server_profile`; version errors for regex and `greatest` / `least`; verify 2.2 / 2.3 version scenarios

## 6. Ingestion

- [ ] 6.1 Keep `db_type` for JSON-family types across inspectors (Postgres, MySQL, DuckDB, BigQuery, Snowflake, ClickHouse); verify 2.4 ingestion scenarios and the ingestion suites

## 7. Architecture

- [ ] 7.1 Apply the approved edit: `architecture/system.arc42.md` principle 10, tag after "Every Mode B construct means the same on every dialect." `[target: DEV-2120]` → `[enforced: test:tests/test_dev2120_library_matrix.py]`; verify `la-arch-check`

## 8. Docs, agent surfaces, comparison

- [ ] 8.1 `docs/concepts/references.md` (scalar library table, regex / JSON / coercion / collation / LIKE sections, remove the `greatest` / `least` caveat, SQLite LIKE exception, 2^53 note, Postgres 16 / SQL Server 2022 / 2025 requirements), `formulas.md`, `queries.md` (filters); verify `grep` finds no remaining "backend-specific" caveat left unaddressed
- [ ] 8.2 MCP `Field` descriptions and `slayer/memories/help_content/*`; verify `tests/test_mcp_surface_budget.py` and the documented-idioms tests
- [ ] 8.3 `examples/comparisons/probes.yaml` `C9-slayer-*` probes (regex, strings, JSON, TRY_CAST, greatest / least); C9 SLayer verdict → yes with the Postgres 16+ / SQL Server 2025+ caveat in `matrix.yaml`; regenerate `matrix.json`; verify the comparison tooling's checks
- [ ] 8.4 Release-notes draft (untracked, never staged): `greatest` / `least` NULLs; SQLite LIKE case; LIKE `\` / `[`; TEXT vs number; SQL Server version errors; no `initcap`; ClickHouse JSON columns need re-ingest

## 9. Gates

- [ ] 9.1 `poetry run pytest -m "not integration"`, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `la-arch-check`; verify all green
- [ ] 9.2 pr-review gate: the PR's Snowflake and BigQuery suites executed (not skipped) before C9 = yes is accepted; verify in the PR's check logs
