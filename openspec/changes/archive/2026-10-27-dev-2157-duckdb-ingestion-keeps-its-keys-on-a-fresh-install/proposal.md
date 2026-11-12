## Why

A user on SLayer 1.4.1 (`slayer mcp --demo`, fresh install) got demo models with no joins: SQLAlchemy
2.0.55 changed the Postgres catalog queries duckdb-engine inherits, so every DuckDB key read fails and
ingestion swallows it. DuckDB UNIQUE keys were never read on any SQLAlchemy version. The same report
hit five more rough edges: CLI tracebacks, no `--version`, unsorted rows, float noise in money sums,
and `slayer search <question>` failing.

## What Changes

- Reading a table's columns and keys becomes a `SqlDialect` hook with typed results; DuckDB reads its
  own catalog (`duckdb_columns()`, `duckdb_constraints()`, `duckdb_indexes()`) and never the inherited
  Postgres queries. Ingestion, schema drift and the forced-filter column probe share the one reader;
  the engine-side dialect-name branches and the DuckDB-only fallbacks are deleted, as are schema
  drift's write-only live-key reads. A failing key read logs a WARNING.
- One CLI error door: every exception prints one `Error:` line on stderr and exits 1; `--debug` on any
  command shows the traceback.
- `slayer --version`.
- A query with dimensions and no `order` is ordered by its projected dimensions, ascending, exactly as
  the explicit order would be — outermost query only.
- Demo money is `DECIMAL(12,2)` (exact sums); the demo DB is rebuilt when its schema is stale; every
  demo setup ingests through the idempotent door, healing join-less models without discarding user
  edits. CLI table output renders through each column's `NumberFormat`.
- **BREAKING** `slayer search refresh-samples` moves to `slayer datasources refresh-samples`;
  `slayer search <question>` takes the question positionally (`--question` kept); the old spelling
  fails with a pointer to the new command.
- Spooled usage batches carrying a retired usage key load, folding it into `<surface>:other`.
- CI gains a unit-test leg on the latest allowed SQLAlchemy, duckdb and duckdb-engine.

## Capabilities

### New Capabilities
- `queries/result-order`: the default order of a query that declares none.
- `cli/commands`: CLI error reporting, `--debug`, `--version`, search and refresh-samples command
  shapes, formatted table output.
- `demo/jaffle-shop`: the bundled demo's exact money, stale-schema rebuild and self-healing setup.

### Modified Capabilities
- `models/fk-ingestion`: every declared key (PK, FK, UNIQUE, whole-table unique index) is read on
  every supported SQLAlchemy version, DuckDB from its own catalog; a key-read failure is logged.
- `models/sample-profiling`: the forced refresh is spelled `slayer datasources refresh-samples`.
- `usage-reporting/collection`: a retired usage key counts as its surface's `other`.

## Impact

- Code: `slayer/sql/dialects/*` (reflection hooks, backend-name resolver), `slayer/engine/ingestion.py`,
  `slayer/engine/introspect_utils.py`, `slayer/engine/schema_drift.py`, `slayer/engine/query_engine.py`,
  binding/planning of the final stage, `slayer/cli.py`, `slayer/demo/jaffle_shop.py`,
  `slayer/usage_reporting/payload.py` / `recorder.py`.
- Architecture: `architecture/model.c4` `surfaces` specs gain `cli` and `demo`.
- CI: `.github/workflows/ci.yml` new leg.
- Docs: CLI interface/reference, ingestion, search, models, queries (ordering).
- Output: emitted SQL of dimensioned queries gains an outermost ORDER BY (golden baselines re-blessed);
  demo money aggregates return exact decimals.
