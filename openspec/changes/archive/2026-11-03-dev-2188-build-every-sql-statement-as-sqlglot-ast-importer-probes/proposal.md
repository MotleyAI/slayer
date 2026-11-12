## Why

Importer type probes, the drift probe, the client's probe wrappers, the facade catalog, dialect session settings
and several other statements are composed with f-strings: a mixed-case, reserved-word, quoted or
`catalog.schema.table` source name breaks import typing, and nothing guards system §3.6 ("SQL is built as sqlglot
AST"), so the class recurs.

## What Changes

- Every SQL statement SLayer composes is built as sqlglot AST and rendered once: importer probes (dbt / cube / OSI),
  drift probe, sql-mode trial query, SQLite introspection probes, facade DuckDB catalog DDL/INSERT, dialect session
  settings (statement timeouts, ClickHouse `getSetting`, Snowflake `USE`), own-store SQLite queries, demo loader.
- Table names and user SQL enter through one parse door shared with the generator (`parse_table`, `parse_query`);
  a user `sql=` source must be a read-only query.
- Type probes are row-bounded by one dialect hook; the client's type-probe door takes a typed bounded probe and
  executes it verbatim instead of wrapping text. **BREAKING** (internal API): `SlayerSQLClient.get_column_types`
  and the sync probe functions take a `TypeProbe`, not a SQL string.
- OSI expression probing reuses the importers' `LiveSource`; `catalog.schema.table` reflection resolves through
  `SchemaRef`.
- A static law test fails on composed text that starts a SQL statement; system §3.6 gains its `enforced` tag.

## Capabilities

### New Capabilities

### Modified Capabilities
- `importers/meaning-preservation`: "Column types come from the database first" gains scenarios for mixed-case,
  schema-qualified, reserved-word and catalog-qualified sources and for user SQL the parser cannot read.

## Impact

- Code: `slayer/importers/column_types.py`, `slayer/osi/converter.py`, `slayer/engine/{schema_drift,query_engine,ingestion}.py`,
  `slayer/sql/{client,generator,sqlite_introspect}.py`, `slayer/sql/render/parse.py`,
  `slayer/sql/dialects/{base,sqlite,clickhouse,postgres,mysql,snowflake}.py`, `slayer/facade/catalog_sql.py`,
  `slayer/pg_facade/{connection,protocol}.py`, `slayer/storage/{sqlite_storage,sidecar_embedding_store}.py`,
  `slayer/demo/jaffle_shop.py`.
- Architecture: `system.arc42.md` §3.6 tag; no model or claims change.
- Tests: new `tests/test_law_sql_built_as_ast.py`; probe/importer tests; goldens re-blessed only for quoting.
