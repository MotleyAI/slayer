# Flight SQL

SLayer exposes an [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
endpoint on port **5144** by default (one above the REST API's 5143). It is wire-compatible
with the upstream Apache `flight-sql-jdbc-driver`, which makes SLayer accessible from
JDBC-based BI tools (Power BI / Sigma / Looker / Tableau / Hex / DBeaver / dbt Semantic
Layer connectors) without any extra glue.

The endpoint is **read-only**: catalog introspection plus a constrained SQL subset that
translates to a `SlayerQuery` and executes against the engine. SQL `INSERT` / `UPDATE` /
`DELETE` / `CREATE` / `ALTER` / `DROP` are refused with a `read-only` error.

> Prefer a no-Java option? The [Postgres facade](pg-facade.md) (`slayer pg-serve`) exposes
> the same query surface over the Postgres wire protocol, so any Postgres-connector BI
> tool — or `psql` / `asyncpg` — can connect without a JDBC/Arrow driver.

## Start the Server

```bash
# Local dev — loopback, no auth needed
slayer flight-serve --demo

# Production-ish — non-loopback bind requires a bearer token
slayer flight-serve --host 0.0.0.0 --token "$(pass slayer-token)"

# TLS-enabled
slayer flight-serve --host 0.0.0.0 --token TOK \
    --tls-cert /etc/ssl/slayer.crt --tls-key /etc/ssl/slayer.key
```

Flags:

| Flag | Description |
|---|---|
| `--host HOST` | Bind address. Default `0.0.0.0`. With `--demo` and no token, defaults to `127.0.0.1` for the loopback fallback. |
| `--port PORT` | Default `5144`. |
| `--token T` | Bearer token. Falls back to `$SLAYER_FLIGHT_TOKEN`. Required for non-loopback binds. |
| `--tls-cert C` / `--tls-key K` | TLS certificate + key pair (must be supplied together). |
| `--demo` | Generate + ingest the bundled Jaffle Shop dataset before starting. |
| `--storage PATH` | Storage path (same as the REST + MCP servers). |

## Connection URL

The JDBC driver's connection URL follows the upstream Apache `flight-sql-jdbc-driver`
syntax:

```
jdbc:arrow-flight-sql://<host>:<port>/?useEncryption=<bool>[&token=<bearer>][&environmentId=<id>]
```

* `useEncryption=true` requires a TLS-enabled server (`--tls-cert` / `--tls-key`).
* `token=<bearer>` adds an `Authorization: Bearer <bearer>` header. **Phase 1 caveat:**
  the Apache JDBC driver calls `handshake()` before its first real RPC to exchange the
  token. SLayer's Phase 1 facade validates bearer tokens via header-based middleware on
  every RPC, not via a handshake handler — so JDBC clients using `token=` will get an
  `UNIMPLEMENTED` error during the handshake step. Use the pyarrow-flight Python client
  (which honours per-call `Authorization` headers) until the handshake handler lands;
  it is tracked as a Phase 2 follow-up.
* `environmentId=<id>` is logged at INFO on each request and otherwise ignored.

## Authentication

* No token configured → the server accepts unauthenticated requests **only** from a
  loopback peer (`127.0.0.0/8` or `::1`). Non-loopback binds without a token are
  refused at startup time.
* Token configured → every RPC must carry `Authorization: Bearer <token>`. Mismatched
  or missing headers raise `UNAUTHENTICATED`.

## TLS

Pass `--tls-cert` and `--tls-key` together to enable TLS. The server advertises
`grpc+tls://<host>:<port>` and clients must connect with `useEncryption=true`. Supplying
only one of the pair is rejected at startup.

## Catalog Layout

SLayer exposes a single Flight catalog named **`slayer`** with one **schema per
datasource** and one **table per non-hidden `SlayerModel`** in that datasource. Each
table carries two fan-outs:

* **Metrics** — derived from each model's `columns` × eligible aggregations, plus saved
  `ModelMeasure` formulas, plus custom aggregations on the model, plus a synthetic
  `row_count` metric (`*:count`).
* **Dimensions** — every non-hidden column of the model, plus reachable join targets
  walked up to depth 3.

Cross-model dimensions use **dotted** path syntax — `customers.regions.name` is a
multi-hop dimension on `orders` when `orders → customers → regions`. The same dotted
form is used in `INFORMATION_SCHEMA.*`, in the BI-tool projection list, in `WHERE`, and
in the SLayer DSL.

`*:count` is exposed as a column literally named `row_count`. If a user-defined column
is also named `row_count`, SLayer renames the synthetic to `_row_count` and logs a
warning at catalog-build time.

## SQL Subset

SLayer accepts a single-`FROM` `SELECT` that translates to a `SlayerQuery`:

| Feature | Notes |
|---|---|
| `SELECT <metric> [, ...]` | Each item must be a metric, dimension, or time-grain expression on the resolved table. |
| `month(<col>)`, `quarter(...)`, etc. | Time-grain wrappers on time-typed columns. Equivalent to `date_trunc('month', <col>)`. |
| `WHERE <col> BETWEEN '...' AND '...'` | On time-typed columns, lifts to `time_dimensions[*].date_range`. |
| `WHERE <col> >= '...'` / `<=` / `>` / `<` | Same lift for time bounds. |
| `WHERE ...` (everything else) | Passed verbatim into `SlayerQuery.filters`. |
| `GROUP BY` | Strict on extras, lenient on omissions. User items must be in the derived dimension set; missing ones are silently filled in from the projection. |
| `ORDER BY <col> [DESC \| ASC]` | Resolved against projected names. |
| `LIMIT N OFFSET M` | Integer literals only. |

**`SELECT *` is rejected** on Flight tables; the error includes a pointer to
`SELECT * FROM INFORMATION_SCHEMA.METRICS WHERE table_name=...` for discovery. `SELECT *`
**is** accepted on `INFORMATION_SCHEMA.*` itself.

### Probe-query whitelist

Four canned probes return canned results (used by interactive clients to test the
connection):

* `SELECT 1`
* `SELECT NULL WHERE 1=0`
* `SELECT version()` (also `SELECT @@version`)
* `SELECT current_database()`

### Bare-name table resolution

`SELECT ... FROM orders` searches every schema:

* Exactly one match → use it.
* Multiple matches → error naming each `<schema>.<table>` candidate.
* Zero matches → `Unknown table`.

Or qualify explicitly as `<schema>.<table>` or `slayer.<schema>.<table>`.

## INFORMATION_SCHEMA

The catalog exposes the following well-known introspection tables:

* `INFORMATION_SCHEMA.METRICS` — every metric in the catalog, keyed by table.
* `INFORMATION_SCHEMA.DIMENSIONS` — every dimension (including joined paths).
* `INFORMATION_SCHEMA.TABLES`, `COLUMNS`, `SCHEMATA` — JDBC-shaped equivalents of the
  per-command Flight SQL RPCs.

## Prepared Statements

The Apache JDBC driver routes **every** `Statement.executeQuery` through the
prepared-statement triplet (`CreatePreparedStatement` → `GetFlightInfo` →
`do_get(<prepared-statement ticket>)`), not via `CommandStatementQuery`. SLayer's
implementation is stateless: the `prepared_statement_handle` is **the original
UTF-8 SQL bytes**, so `Close` is a no-op (nothing to free).

This means three translator runs per BI query (create-prepared + flight-info + do_get).
The database round-trip count is two: a `LIMIT 0` for schema validation on the
create-prepared step, then the full execution on `do_get`.

## DML / DDL behaviour

Any `INSERT` / `UPDATE` / `DELETE` / `MERGE` / `TRUNCATE` / `CREATE` / `ALTER` / `DROP`
raises a Flight `INVALID_ARGUMENT` whose message contains `SLayer Flight SQL endpoint
is read-only`. `BEGIN` / `COMMIT` / `ROLLBACK` / `START TRANSACTION` / `SET ...` /
`SHOW ...` / `USE ...` / `RESET ...` succeed as no-ops (empty result, no side effects).

## Error Taxonomy

Translator errors → Flight `INVALID_ARGUMENT`. Auth failures → `UNAUTHENTICATED`.
Unhandled commands → `INVALID_ARGUMENT`. Engine errors propagate as the underlying
gRPC status.

## Wire-Format Schema (Phase 1)

The wire schema for a `SELECT ... FROM <flight-table>` is derived from the
**catalog-declared** `DataType` of each projected item (`Column.type` for dimensions,
`ModelMeasure.type` for measures). A `LIMIT 0` is still executed for engine-side query
validation, but `SlayerResponse.attributes` does not yet expose per-column Arrow types
so the catalog-declared types are the wire-schema source. Phase 2 will tighten this to
a real `LIMIT 0`-derived schema.

If a `ModelMeasure` has an incorrect or absent declared `type`, the wire-schema /
data-row type mismatch surfaces as `ArrowTypeError`. Set `ModelMeasure.type` on custom
formulas that surface over Flight SQL.

## Unobserved Commands

The Apache JDBC driver did not exercise these commands during the Phase 1.0 wire
capture; SLayer implements them with well-typed empty (or canned) responses for
compatibility:

* `CommandStatementQuery` `[unobserved]` (driver uses prepared statements instead)
* `CommandGetSqlInfo` `[unobserved]` (catalog introspection goes through other RPCs)
* `CommandGetXdbcTypeInfo` `[unobserved]` — stub returns 6 entries
* `CommandPreparedStatementQuery` round-trips were partially captured against the
  Phase 1.0 capture-stub; the production handlers fill in the rest
* `ActionClosePreparedStatementRequest` is a no-op (stateless handle = SQL bytes)
