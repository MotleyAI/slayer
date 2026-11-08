## 1. Architecture

- [x] 1.1 Apply the approved `architecture/model.c4` edit (`surfaces` `specs ['mcp']` → `specs ['mcp', 'cli', 'demo']`, design.md decision 10, verbatim) and verify `poetry run la-arch-check` passes with the new `cli` / `demo` capabilities attached. Gotcha: `openspec/specs/cli` and `openspec/specs/demo` only exist after archive; if `la-arch-check` rejects an attachment to a missing spec dir, land this edit in the archive commit (pr-review stage) instead, verbatim.

## 2. Failing tests (pr-tests stage)

Reflection (specs/models/fk-ingestion; design D1):
- [x] 2.1 DuckDB key reading: PK single + composite; FK single + composite in a non-default schema; an attached second catalog with same-named tables and different keys contributes nothing; UNIQUE single + composite marks `Column.unique` / feeds join cardinality (FK to a UNIQUE column → to-one); unique index over a quoted column → key; expression unique index → no key. Verify they fail on the current code.
- [x] 2.2 Inspector-bypass proof: monkeypatch SQLAlchemy's `get_pk_constraint` / `get_foreign_keys` / `get_unique_constraints` / `get_columns` to raise like SA 2.0.55; DuckDB ingestion still yields every key, join and column (fails without the fix on any SA).
- [x] 2.3 Demo on DuckDB ingests its six FK joins and composite `supplies` PK; the README query returns rows.
- [x] 2.4 A failing key read logs one WARNING naming the table, the read and the error, and the table is still ingested (caplog).
- [x] 2.5 Base hooks keep today's behaviour on SQLite and Postgres (pytest-postgresql): PK, FK, UNIQUE, whole-table unique index, partial unique index excluded; SQLite PK Inspector-authoritative (no information_schema PK query issued).
- [x] 2.6 `reflect_columns`: DuckDB types + comments from `duckdb_columns()`, catalog-scoped, in column order; base fallback when the Inspector raises; the moved comment / columns catalog queries (MySQL, Snowflake, ClickHouse, Postgres, BigQuery dataset source) emitted as expected SQL text (emission tests).
- [x] 2.7 Dialect resolver: every registered dialect resolves from each SQLAlchemy backend name it declares (`postgresql`, `mssql`, `duckdb`, `sqlite`, `mysql`, `mariadb`, …); a duplicate declaration fails at registry build; an undeclared backend gets the base.
- [x] 2.8 Law test: no `dialect.name` / dialect-name comparison remains in `slayer/engine/ingestion.py` or `slayer/engine/introspect_utils.py` (python.sql P2).

CLI (specs/cli/commands; design D2, D3, D5.3, D6):
- [x] 2.9 Error door: the README query against join-less models prints exactly one stderr line `Error: …`, stdout has no error text, exit 1; an injected driver error prints class + message + `--debug` hint; `--debug` shows the traceback as `slayer --debug query …`, `slayer query … --debug` and `slayer datasources refresh-samples --debug`; formerly-stdout `Error:` lines (e.g. memory save argument errors) now on stderr.
- [x] 2.10 `slayer --version` prints `slayer <importlib.metadata.version("motley-slayer")>`, exit 0.
- [x] 2.11 `slayer search revenue` == `slayer search --question revenue`; both given → error exit 1; `slayer search refresh-samples` → error naming `slayer datasources refresh-samples`, exit 1.
- [x] 2.12 `slayer datasources refresh-samples`: text and `--format json`; unresolved `--model` → envelope lists it, exit 1; no help-memory seeding on this path.
- [x] 2.13 Table output: a currency-formatted measure renders `$193.65K` (via the MCP markdown renderer); `--format json` carries the raw value.

Default order (specs/queries/result-order; design D4):
- [x] 2.14 Emitted SQL: outermost ORDER BY = projected dimensions then time dimensions in result-column order, ASC, NULLs as an explicit ASC order emits them; identical to the explicit spelling; explicit order used verbatim; raw-rows and measure-only queries unordered.
- [x] 2.15 Execution (SQLite + DuckDB): rows sorted, NULL dimension last; LIMIT/OFFSET page picks `b`, `c` from `a..d`; computed and joined (`customers.name`) dimensions sort by their projected values.
- [x] 2.16 Only the outermost query: query-list inner stages, query-backed model backing queries / `source_queries` used as sources carry no ORDER BY; a saved query run by name, refined and unrefined, is ordered on its final stage.
- [x] 2.17 Combinations: transform measure (`change_pct(sum(x))`), a hidden order-only slot alongside (explicit order present → no default; separately a filter-only hidden value with no order → default over the projected dimensions only), a time-spine query, and queries through the pg and Flight facades.

Demo (specs/demo/jaffle-shop; design D5):
- [x] 2.18 Money columns are `DECIMAL(12,2)`; `sum(order_total)` by store and month is an exact `Decimal`; demo models declare `db_type` `DECIMAL(12,2)` on money columns; a user-set non-empty `db_type` survives setup.
- [x] 2.19 Healing: an old-schema demo DB (DOUBLE money, built by the 1.4.1 schema) is rebuilt; stored join-less demo models regain their joins and keep a changed description; a second setup does not rebuild; a rebuild while this process holds an open factory engine on the file succeeds.

Usage reporting (specs/usage-reporting/collection; design D7):
- [x] 2.20 A spooled batch with `cli:search.refresh-samples` (2 ok) + `cli:other` (1 ok) + `cli:query` loads as `cli:other` 3 ok + `cli:query` unchanged; an unknown `pg:` key is dropped alone; errors per token are summed too.

Existing tests whose logic changes under the approved plan:
- [x] 2.21 Update refresh-samples call sites to `datasources refresh-samples` in `tests/test_help_seed.py`, `tests/test_search_surfaces.py`, `tests/test_search_writeback.py`, `tests/test_usage_reporting_payload.py`, `tests/integration/test_integration.py`; update tests pinning `_exit_with_error`'s stdout output; mechanically drop `pk_columns=` / `fk_relationships=` kwargs in `tests/test_validate_models.py` and `tests/test_ingestion_schema_qualification.py`. Verify each changed test still asserts the same behaviour at its new spelling.

## 3. Implementation (pr-implement stage)

- [x] 3.1 `slayer/sql/dialects`: `ReflectedColumn`, `ReflectedForeignKey`, `TableKeys` (Pydantic); backend-name declarations + resolver with duplicate check; the type-string → `DataType` mapping moved from `introspect_utils`. Verify 2.7.
- [x] 3.2 Base `reflect_columns` / `reflect_table_keys` (today's Inspector + information_schema behaviour, catalog queries as sqlglot AST + `exec_driver_sql`); per-dialect comment queries and the BigQuery columns source as overrides; SQLite/BigQuery PK-authoritative overrides; SQLite `probes_integer_affinity` flag. Verify 2.5, 2.6.
- [x] 3.3 DuckDB overrides via `duckdb_columns()` / `duckdb_constraints()` / `duckdb_indexes()` (index `sql` parsed with sqlglot; plain columns only). Verify 2.1, 2.2.
- [x] 3.4 Route ingestion, schema drift (`_introspect_one_table` columns only; delete `LiveTable.pk_columns` / `fk_relationships`), the forced-filter column probe and `introspect_table_to_model` through the hooks; delete the superseded engine helpers listed in design D1.3; WARNING logging per failed key read. Verify 2.3, 2.4, 2.8 and the DEV-2118 tests `test_join_is_generated_for_a_non_default_schema` / `TestParallelFksAreAddressable[duckdb]` on the latest-SA leg.
- [x] 3.5 CLI error door in `main()`, `--debug` on every subparser via one parser-tree walk (`default=argparse.SUPPRESS` below the root), delete `_exit_with_error` and per-command catches, stderr for every `Error:` line; `--version`. Verify 2.9, 2.10.
- [x] 3.6 `search` positional question + `--question` alias + moved-command refusal; `datasources refresh-samples` (+ `--format`); usage key rename. Verify 2.11, 2.12.
- [x] 3.7 CLI table cells via `SlayerResponse._format_value`. Verify 2.13.
- [x] 3.8 Default order: fill the final query's empty `order` from its projected dimensions right after slack normalization, through the explicit-order path. Verify 2.14–2.17; then enumerate the golden baselines that gained an outermost ORDER BY and re-bless each individually, checking each diff is only that ORDER BY.
- [x] 3.9 Demo: `DECIMAL(12,2)` schema + exact cents conversion; `_ColumnEnrichment.db_type` gap-fill; canonical-projection schema check; sibling-build + factory release + `os.replace` rebuild; setup always via `ingest_datasource_idempotent` + enrichment; delete the fast path and model-overwrite path. Verify 2.18, 2.19.
- [x] 3.10 `Batch` before-validator folding retired usage keys through the recorder's shared unknown → other rule. Verify 2.20.

## 4. CI and docs

- [x] 4.1 `.github/workflows/ci.yml`: blocking unit-test job (Python 3.11) that upgrades `sqlalchemy` (`>=2.0,<2.1`), `duckdb`, `duckdb-engine` to their latest allowed versions, prints them, and runs `pytest tests/ -m "not integration"` with the main leg's flags. Verify locally by running the unit suite in a scratch venv with those upgrades.
- [x] 4.2 Docs (one sentence per change): `docs/interfaces/cli.md`, `docs/reference/cli.md` (refresh-samples under datasources, `search <question>`, `--version`, `--debug`, one-line errors, formatted table output); `docs/concepts/ingestion.md`, `docs/concepts/search.md`, `docs/concepts/models.md` (new refresh-samples spelling); `docs/concepts/queries.md` (default order); DuckDB keys from DuckDB's own catalog (ingestion / database-support page); demo money `DECIMAL(12,2)` wherever the demo schema is described. Verify with a grep for `search refresh-samples` returning only intentional mentions, and every touched page still linked from `zensical.toml`.

## 5. Gates

- [x] 5.1 Full unit suite (`poetry run pytest -m "not integration"`), integration suite with the CI invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`, `openspec validate dev-2157-duckdb-ingestion-keeps-its-keys-on-a-fresh-install --strict` — all green.
