## Context

See proposal.md - Why.

### Verified facts (repro, 2026-10-27)

- Fresh install resolves SQLAlchemy 2.0.55 + duckdb 2.0.0 + duckdb-engine 0.17.0 (lock: 2.0.49 / 1.5.2 / 0.17.0).
- SA ≥ 2.0.55 on DuckDB: `get_pk_constraint`, `get_foreign_keys`, `get_unique_constraints` raise
  `Catalog Error: Type with name REGCLASS does not exist!`; `get_check_constraints` raises NotImplementedError.
- On EVERY SA version, DuckDB's inspector returns `[]` for the PK (rescued only by our information_schema
  fallback) and `[]` for UNIQUE constraints (never rescued — DuckDB UNIQUE keys have never been ingested).
  `get_indexes` is always `[]` (duckdb-engine does not reflect indexes).
- `get_columns` fails (`pg_collation`) on duckdb 1.5.2 with SA 2.0.x and on duckdb 2.0.0 with SA 2.1.6;
  the information_schema column fallback rescues it today.
- No connection poisoning: each Inspector call checks out its own connection (the issue's "aborted
  transaction" claim does not reproduce).
- On SA 2.0.55 the demo ingests with `joins: []` on every model; the README query fails with
  `UnrelatedDatasetError` and a full traceback. `slayer --version` is unrecognized. The emitted SQL has
  no ORDER BY. Money sums carry float noise (`107404.74000000313`) in table and JSON output.
  `slayer search revenue` fails with `invalid choice: 'revenue' (choose from 'refresh-samples')`.
- DEV-2118 proposal item 2 (DuckDB FK reflection) is subsumed by this change; its two failing tests
  (`tests/test_ingestion_schema_qualification.py::TestForeignKeysAreSchemaAware::test_join_is_generated_for_a_non_default_schema`,
  `tests/test_dev2073_edge_naming.py::TestParallelFksAreAddressable[duckdb]`) are this change's
  fails-without-the-fix proof on the latest-SA CI leg.

### Structural diagnosis

Symptom: DuckDB loses FKs (and PK/UNIQUE) on SA 2.0.55.
Structural cause: SLayer reads DuckDB's structure through SQLAlchemy's Postgres catalog queries
(duckdb-engine inherits the PG dialect), and patches the gaps with dialect-name branches inside
`slayer/engine` (`_PK_AUTHORITATIVE_DIALECTS`, the DuckDB information_schema PK fallback,
`_COMMENT_FALLBACK_SQL` keyed by dialect name, the BigQuery branch in `_info_schema_columns_query`,
the SQLite gate in `_sqlite_probe_integer_columns`). Schema drift runs its own PK/FK reads into
`LiveTable` fields nothing reads. Grounding: `python.sql.arc42.md` P2 ("Dialect quirks live only in
`dialects/` … No `if dialect == …` outside `dialects/`"); system P6 (SQL built as sqlglot AST, never
string concatenation — `_COMMENT_FALLBACK_SQL` is `.format`-assembled fragments).
Fix: reading a table's structure is a dialect-owned hook with typed results; one door used by every
reader (ingestion, schema drift, the forced-filter column probe, dbt/OSI importers via ingestion).

Symptom: CLI tracebacks. Cause: no single error door — five per-command catches (`_exit_with_error`,
printing to stdout) plus ad-hoc `print("Error …")` sites; everything else escapes as a traceback.
Fix: one door in `main()`.

Symptom: unsorted rows. Not structural: no default order exists. Fix by reduction to the existing
explicit-order path.

Symptom: `slayer search revenue`. Cause: `search` is both a command and a subcommand group.
Fix: `search` stops being a group.

### arc42 principles that apply

- system P1 (layering engine → sql → ir → core): the new hooks live in `slayer.sql.dialects` and take
  primitives (`schema: str | None`, `catalog: str | None`), never `slayer.engine.schema_scope.SchemaRef`.
  The type-string → `DataType` mapping the hooks need moves with them into `slayer/sql/dialects`.
- system P6 (SQL as sqlglot AST): catalog/information_schema queries are built as sqlglot AST with
  escaped string literals and executed with `exec_driver_sql` (no `sa.text` fragment assembly).
- system P7 (async-first): reflection stays synchronous as today (called from sync ingestion /
  `asyncio.to_thread`); event-loop work is DEV-2117's.
- system P12 (Pydantic, no dataclasses): typed reflection results are Pydantic models.
- system P14 (ingestion idempotent and additive-only): the demo now ingests only through
  `ingest_datasource_idempotent`; the demo's overwrite path is deleted.
- `python.sql` P2 (dialect quirks only in `dialects/`): satisfied by the reflection hooks.
- `python.sql` P14 (one engine owner): reflection uses the engine it is given; it never creates or
  disposes engines.
- `python.engine` P3 (storage consulted once) / P9: unaffected — default order is decided at bind
  time of the outermost stage from already-bound dimension slots.
- `semantics.arc42.md` / system P8 (adding a measure never changes cardinality or other values):
  the default order only sorts; it never changes the row set.

No arc42 edit. One normative-harness edit (approved, see decisions log): `architecture/model.c4`
`surfaces` gains specs `cli` and `demo`.

## Goals / Non-Goals

**Goals:** the requirements in this change's spec deltas, met through the structural fixes in Decisions D1-D8.

**Non-Goals:**

- Lifting the SQLAlchemy `<2.1` cap, Snowflake on SA 2.1, the clickhouse extra (DEV-2118 items 1, 3).
- Moving `schema_scope`'s catalog-qualifying dialect set (`_CATALOG_QUALIFYING_DIALECTS`) — identity /
  naming, not reflection; file not touched.
- Event-loop placement of introspection (DEV-2117).
- Decimal-as-JSON-number serialization.
- A schema-drift check for stale uniqueness claims (DEV-2158, blocked by this change).

## Decisions

### D1. Structure reflection is dialect-owned (DuckDB reads its own catalog)

1.1 Typed results (Pydantic, in `slayer/sql/dialects`):
- `ReflectedColumn(name, type, is_float, db_type, comment)` — the shape the column path carries today
  (`type` is either a SQLAlchemy `TypeEngine` from the Inspector or an already-mapped `DataType`).
- `ReflectedForeignKey(columns: list[str], referred_schema: str | None, referred_table: str,
  referred_columns: list[str])`.
- `TableKeys(primary_key: list[str], foreign_keys: list[ReflectedForeignKey], unique_key_sets: list[list[str]])`
  — `unique_key_sets` = UNIQUE constraints + unique indexes constraining the whole table (no partial,
  no expression members), never including the PK set itself.

1.2 Hooks on `SqlDialect`:
- `reflect_columns(*, sa_engine, inspector, table, schema, catalog) -> list[ReflectedColumn]`.
  Base: today's behaviour — Inspector `get_columns` with the catalog-qualified token; on exception the
  information_schema columns query scoped by schema + catalog, plus the dialect's column-comment query.
  The per-dialect comment queries (`_COMMENT_FALLBACK_SQL`: MySQL/MariaDB, Snowflake, ClickHouse,
  Postgres) become each dialect's `column_comments_query(...)` override; BigQuery's dataset-scoped
  INFORMATION_SCHEMA source becomes the BigQuery override. DuckDB override: `duckdb_columns()` only
  (types + comments, scoped by `database_name` / `schema_name`, ordered by `column_index`), never the
  Inspector.
- `reflect_table_keys(*, sa_engine, inspector, table, schema, catalog) -> TableKeys`.
  Base: Inspector PK (falling back to the information_schema PK query when the Inspector yields no
  PK — today's behaviour for every dialect outside `_PK_AUTHORITATIVE_DIALECTS`), Inspector FKs,
  UNIQUE constraints and whole-table unique indexes. SQLite and BigQuery overrides: Inspector only for
  the PK (today's `_PK_AUTHORITATIVE_DIALECTS`). DuckDB override: `duckdb_constraints()` for PRIMARY
  KEY / FOREIGN KEY / UNIQUE (scoped by `database_name` + `schema_name`; `referred_schema` is the
  constraining table's schema) and `duckdb_indexes()` for unique indexes — never the Inspector. A
  unique index counts only when its `sql` parses (sqlglot, duckdb) to an index over plain column
  references; quoted names are taken from the AST, and any expression member or unparseable index is
  ignored (never split the rendered `expressions` text).
- Each sub-read is independently best-effort: a failing PK/FK/UNIQUE/index read yields "no evidence"
  for that kind only and logs a WARNING naming the table, the read, and the error (raised from today's
  DEBUG). A table is never skipped for a key-read failure (models/fk-ingestion "FK reflection failure
  loses only that table's joins" holds).
- SQLite's integer-affinity probe gate (`sa_engine.dialect.name != "sqlite"` in
  `_sqlite_probe_integer_columns`) becomes a dialect flag (`probes_integer_affinity`), so
  `slayer/engine/ingestion.py` carries no dialect-name branch.

1.3 One door per read, used everywhere:
- `slayer/engine` resolves the `SqlDialect` of an `sa.Engine` through one resolver in
  `slayer.sql.dialects`: every dialect declares the SQLAlchemy backend names it serves (`postgresql`,
  `mssql`, `duckdb`, `sqlite`, …), the registry built from them rejects a duplicate at import, and only
  a backend no dialect declares (a third-party SQLAlchemy dialect) gets the Postgres-shaped base. The
  engine then calls the two hooks. Deleted: `_get_fk_constraint_groups`'
  inspector call, `_pk_key_sets` / `_unique_constraint_key_sets` / `_unique_index_key_sets` /
  `_is_partial_index` (moved into the base hook), `_safe_get_pk_constraint`, `_get_pk_constraint_fallback`,
  `_normalized_pk`, `_PK_AUTHORITATIVE_DIALECTS`, `_safe_get_columns` / `_get_columns_fallback` /
  `_get_column_comments_fallback` / `_COMMENT_FALLBACK_SQL` / `_info_schema_columns_query` (moved into
  the base/dialect hooks).
- Readers: ingestion (columns, PK flags, joins + cardinality, `Column.unique`), schema drift
  (`_introspect_one_table`: columns only, via `reflect_columns`; `LiveTable.pk_columns` and
  `LiveTable.fk_relationships` — written, never read — are deleted with their PK/FK reads; a key-drift
  check is DEV-2158), the forced-filter
  column-presence probe in `query_engine.py`, and every importer path that goes through
  `introspect_table_to_model`.
- `_safe_introspect` stays only if still needed by non-key reads; otherwise deleted.
- `_resolve_fallback_ref` (DuckDB default-catalog resolution for `ref=None`) stays engine-side: the
  engine always passes a resolved schema + catalog to the hooks.

### D2. One CLI error door

- `main()` wraps command dispatch (including the `SLAYER_NOW` `host_clock()` check and the
  usage-reporting command) in one `except Exception` (SystemExit / KeyboardInterrupt are BaseException
  and pass through, keeping today's MCP stdio exit path).
- Output on stderr, exit code 1:
  - `SlayerError`: `Error: <message>`.
  - any other exception: `Error: <ExceptionClass>: <message> (rerun with --debug for the traceback)`.
- `--debug` is accepted on the root parser and on every leaf command, before or after the subcommand
  (including nested leaves such as `datasources refresh-samples`); with it, the exception re-raises
  with its full traceback. The leaf copies come from a shared parent parser with
  `default=argparse.SUPPRESS`, so a leaf never resets a root-level `--debug` to False.
- Deleted: `_exit_with_error` and the per-command `except (SlayerError, ValueError)` catches that only
  converted to a message. Every remaining `print("Error: …")` + `sys.exit(1)` site in `cli.py` writes to
  stderr (they currently write to stdout).
- `slayer search --verbose` keeps its current meaning (non-compact hits).

### D3. `slayer --version`

- Root parser `--version` (argparse `action="version"`), printing `slayer <version>` where the version
  is `importlib.metadata.version("motley-slayer")`; exit 0.

### D4. Default result order

- When the outermost query of a request (a single query, or the final entry of a query list,
  including a saved query run by name and its refinement) has no `order` (absent or empty), keeps
  `distinct_dimension_values` true, and projects at least one dimension or time dimension, it is planned
  exactly as if its `order` listed every projected dimension and time-dimension column ascending, left
  to right in result-column order. It goes through the same order binding and `build_ordered` door as
  an explicit order, so NULLs sort last on every dialect (SQL Server: native), per
  `docs/concepts/queries.md`.
- Mechanism: binding is per stage and has no notion of "outermost", so the orchestrator passes an
  explicit `apply_default_order` flag only for the request's final stage; the default `OrderSpec`s are
  built from that stage's already-bound dimension / time-dimension slots (never re-derived from
  names), so computed, joined and time-bucket dimensions sort exactly as projected. The default order is
  applied where an explicit order is — before `LIMIT` / `OFFSET`.
- Never applied to: inner stages of a query list, a query-backed model's backing query or
  `source_queries` consumed as a source, raw-rows queries (`distinct_dimension_values: false`),
  measure-only queries (no dimensions).
- An explicit non-empty `order` is used as written — no tiebreakers appended.
- Applies on every surface that executes queries (CLI, MCP, REST, client, pg/flight facades), since it
  is engine behaviour.

### D5. Exact demo money + formatted CLI table

5.1 Demo data:
- Bundled schema: `products.price`, `orders.subtotal`, `orders.tax_paid`, `orders.order_total`,
  `supplies.cost` become `DECIMAL(12,2)` (`stores.tax_rate` stays `DOUBLE` — a rate, not money).
- `load_data` converts jafgen's integer cents to `DECIMAL(12,2)` exactly (no DOUBLE intermediate
  that survives into storage).
- Curated enrichment gains an optional `db_type` on `_ColumnEnrichment` and declares
  `"DECIMAL(12,2)"` on those money columns; it is copied only when the stored column's `db_type` is
  empty, so a user-set non-empty `db_type` survives and upgraded installs reach native-precision sums
  (aggregations/native-type-preservation). Ingestion's additive merge is not changed.
- Results: money aggregates come back as exact decimals (`193647.33`); JSON output stays raw (Decimal
  serializes as a string, as Postgres NUMERIC sums already do) — no formatting, no rounding in SQL.

5.2 Demo healing (`ensure_demo_datasource`):
- The demo DuckDB is rebuilt (data regenerated) when its live schema differs from the bundled one:
  expected = the bundled `JAFFLE_SCHEMA_SQL` created in an in-memory DuckDB; both sides are compared
  as the canonical projection `(table_name, column_index, column_name, data_type)` of
  `duckdb_columns()` for the demo tables — database names, OIDs and other catalog metadata excluded;
  any difference (missing table/column, different type) → rebuild. Single source of truth:
  `JAFFLE_SCHEMA_SQL`. An up-to-date DB is never rebuilt.
- A rebuild never deletes the live file first: it builds a temporary sibling file, releases any
  `engine_factory`-cached engine for the demo datasource through the factory (the factory owns
  disposal, `python.sql` P14), then `os.replace`s the sibling over the file. If another process holds
  the file, setup fails with a one-line error naming the path.
- Every demo setup (CLI `--demo` on serve/mcp/flight-serve/pg-serve, `slayer datasources create demo`,
  MCP `create_datasource` demo path — whatever calls `ensure_demo_datasource`) ingests models only via
  `ingest_datasource_idempotent` (creates missing models, additively merges existing ones — missing FK
  joins are reconciled in), then applies the curated enrichment and saves models it changed.
- Deleted: the "DB reused and all models stored" fast path and the `assume_yes` model-overwrite path.
  `assume_yes` keeps only its datasource-record meaning.

5.3 CLI table output:
- `slayer query` table cells render through the one existing renderer `SlayerResponse._format_value`
  (`format_number` with the column/measure `NumberFormat` from `result.attributes`, else `str`) — the
  same rendering as MCP markdown (e.g. `$193.65K`, `9.2%`). `--format json` is unchanged (raw values).

### D6. `slayer search <question>`; `slayer datasources refresh-samples`

- `search` is no longer a subcommand group: `question` is an optional positional; `--question` stays
  as an alias. Both given → error (one-line, via the door).
- A positional question exactly equal to `refresh-samples` → `Error: 'refresh-samples' moved: run
  'slayer datasources refresh-samples'` (exit 1). Searching the literal text needs `--question`.
- `slayer datasources refresh-samples` with today's flags (`--data-source`, repeatable `--model`) plus
  `--format {text,json}` (today it honoured `search --format`); behaviour and exit codes unchanged
  (unresolved `--model` → exit 1). It never seeds help memories (stays write-free as today).
- Usage key `cli:search.refresh-samples` → `cli:datasources.refresh-samples`.

### D7. Retired usage keys fold into `other`

- `Batch` normalises its raw `usage` mapping in a `model_validator(mode="before")` (so it runs before
  the `UsageKey` literal check): every key not in `UsageKey` maps to its surface's `<surface>:other`
  when that key exists (the recorder's existing unknown → other rule, shared as one function), summing
  its `ok` and per-token `errors` counts into that bucket; a retired key whose surface has no `other`
  token (`flight:*`, `pg:*`) is dropped on its own. A batch is never dropped for a retired usage key.
  No new usage keys are added.

### D8. CI: latest SQLAlchemy + DuckDB leg

- New unit-test job in `.github/workflows/ci.yml` (Python 3.11): after `poetry install`, upgrade
  `sqlalchemy` (latest `>=2.0,<2.1`), `duckdb` and `duckdb-engine` (latest allowed by pyproject) with
  pip, print the resolved versions, then run the full non-integration suite with the main leg's
  invocation. Blocking like the main leg.

### D9. Docs (each change one sentence where possible)

- `docs/interfaces/cli.md`, `docs/reference/cli.md`: `refresh-samples` under `datasources`;
  `slayer search <question>`; `--version`; `--debug`; one-line errors; table output uses NumberFormat.
- `docs/concepts/ingestion.md`, `docs/concepts/search.md`, `docs/concepts/models.md`: the new
  `refresh-samples` spelling.
- `docs/concepts/queries.md` (ordering section): the default order.
- `docs/concepts/ingestion.md` (or `docs/database-support.md`, whichever documents DuckDB
  ingestion): DuckDB keys (PK/FK/UNIQUE) are read from DuckDB's own catalog.
- Demo docs (where the demo dataset's columns/types are described): money is `DECIMAL(12,2)`.
- README quickstart example still runs (no change unless it shows output).
- grep docs for `search refresh-samples`, `--verbose` (CLI traceback context), money `DOUBLE` in demo.

### Decisions log

1. Reflection scope — keys door only vs keys + columns/comments behind dialect hooks → **keys +
   columns/comments** (B): same bug class (`pg_collation` is the next breakage), code move not new
   logic. (user)
2. Default order NULL placement — dialect-native vs the explicit-order door (NULLs last everywhere,
   SQL Server native) → **explicit-order door**: reduction to an existing concept, matches the
   documented cross-backend promise. (user) Overrides the issue's "dialect-native NULL placement".
3. Traceback flag — `--verbose` (clashes with `search --verbose`) / `--debug` / rename search's flag →
   **`--debug` on every command**. (user) Overrides the issue's `--verbose`.
4. Error-door width — SlayerError only / + ValueError / every Exception → **every Exception**, typed
   errors print their message, others class + message + `--debug` hint. (user) Extends the issue.
5. Old `slayer search refresh-samples` — silent search vs loud refusal → **refusal pointing at the new
   command**. (user)
6. CI leg — SA only / SA + duckdb + duckdb-engine / all deps latest → **SA + duckdb + duckdb-engine**.
   (user) Extends the issue.
7. CLI table rendering — existing `format_number` (K/M abbreviation) vs a second full-precision mode →
   **existing renderer**, identical to MCP markdown. (user)
8. Retired usage keys in spooled batches — rename only vs fold into `<surface>:other` on load →
   **fold into other**. (user) Extends the issue.
9. Demo healing — rebuild-and-overwrite vs one idempotent door (+ enrichment fills `db_type`) →
   **idempotent door**; schema-mismatch rebuild for the data. (user) Extends the issue.
10. Spec attachment — `architecture/model.c4` `surfaces` specs `['mcp']` → `['mcp', 'cli', 'demo']`.
    (user, approved wording):
    ```
    -        specs ['mcp']
    +        specs ['mcp', 'cli', 'demo']
    ```
11. Structure reflection as `SqlDialect` hooks with typed Pydantic results and primitive schema/catalog
    arguments (layering). (default)
12. DuckDB keys from `duckdb_constraints()`; DuckDB unique indexes from `duckdb_indexes()` (parity with
    the base hook's whole-table unique-index evidence, which duckdb-engine never provided). (default)
13. Base hooks preserve today's non-DuckDB behaviour (information_schema PK fallback; SQLite/BigQuery
    PK Inspector-authoritative). (default)
14. Catalog queries as sqlglot AST + escaped literals, executed via `exec_driver_sql` (system P6).
    (default)
15. Key-read failures logged at WARNING per table and per read kind (issue). (default)
16. Default order: result-column order (dimensions, then time dimensions); outermost query only;
    absent or empty `order` both mean "no order"; no tiebreakers after an explicit order; applies to
    every surface. (default)
17. Demo money columns: price, subtotal, tax_paid, order_total, cost → `DECIMAL(12,2)`; `tax_rate`
    stays DOUBLE. (default)
18. Demo schema check derives the expected schema from `JAFFLE_SCHEMA_SQL` itself (no version stamp).
    (default)
19. `slayer --version` output `slayer <version>`. (default)
20. All CLI `Error:` lines go to stderr. (default — consistency)
21. `datasources refresh-samples` gains `--format {text,json}` to keep the JSON envelope it had under
    `search`. (default)
22. The issue's "aborted transaction poisons later reflection" is not reproduced; no handling added.
    (default)
23. Demo schema comparison uses the canonical projection `(table_name, column_index, column_name,
    data_type)` — raw catalog rows always differ (database names, OIDs); second-run no-rebuild test.
    (codex)
24. Retired usage keys: normalised in a `Batch` before-validator; folded into `<surface>:other` where
    it exists, else that key alone dropped (`flight`/`pg` have no `other`); no new keys. (codex)
25. `--debug` leaf copies use `default=argparse.SUPPRESS` so they never reset a root `--debug`; tests in
    root, leaf and nested-leaf positions. (codex)
26. Default order via an explicit `apply_default_order` flag for the final stage only, `OrderSpec`s built
    from the bound dimension slots, applied before LIMIT/OFFSET. (codex)
27. Default-order tests cover LIMIT/OFFSET, transforms, hidden order slots, time spine, pg/Flight
    facades, and unordered inner CTEs. (codex)
28. DuckDB unique indexes accepted only when their `sql` parses to plain columns (sqlglot); expression
    or unparseable indexes ignored; quoted / expression negative tests. (codex)
29. Demo rebuild builds a temporary sibling, releases the factory-cached engine, `os.replace`s; a file
    held by another process → one-line error. (codex)
30. Dialect resolver keyed by declared SQLAlchemy backend names with duplicate detection; only an
    undeclared backend gets the base. (codex)
31. `_ColumnEnrichment.db_type` optional, copied only into an empty stored `db_type`; user value
    survives. (codex)
32. Golden baselines that gain the default ORDER BY are enumerated and re-blessed individually.
    (codex)
33. Schema drift: `LiveTable.pk_columns` / `fk_relationships` are write-only → deleted with their
    reads, instead of routing them through the key door; no `models/schema-drift` delta. (user)
34. Key drift (stale uniqueness claims) is a separate issue, DEV-2158, created with the user's yes,
    blocked by DEV-2157. (user)

## Risks / Trade-offs

- [Moving every dialect's column/comment reflection changes paths only CI integration covers (MySQL, ClickHouse, Snowflake, BigQuery)] → a pure code move behind the base hook, emission tests for each moved catalog query, and the integration legs before merge.
- [The default ORDER BY changes emitted SQL for every dimensioned query] → baselines re-blessed one by one, each diff checked to be only the outermost ORDER BY; inner CTEs asserted unordered.
- [An ORDER BY costs a sort on large results] → accepted: the same cost an explicit order has; users wanting another order declare it.
- [Demo setup now reflects 7 tables on every start] → well under a second; it is what heals broken installs.
- [Demo money aggregates turn from floats into Decimals, which JSON output prints as strings] → accepted per the issue (JSON stays raw), same as Postgres NUMERIC today.
- [`slayer search refresh-samples` is a breaking CLI change] → the old spelling fails loudly with the new command; release notes list it.

## Migration Plan

- No stored-model migration. Existing demo installs heal on their next demo setup (stale DB rebuilt; join-less models reconciled by idempotent ingestion; empty `db_type` filled).
- Non-demo DuckDB datasources regain lost joins / gain UNIQUE keys on their next `slayer ingest` (idempotent re-ingest reconciles joins datasource-wide).
- Spooled usage batches from older versions load unchanged (retired keys fold into `other`).
- Rollback: revert the release; nothing persisted by this change is unreadable by 1.4.1 (DECIMAL columns and reconciled joins are valid there).
