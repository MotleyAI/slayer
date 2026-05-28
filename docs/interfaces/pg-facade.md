# Postgres Facade

SLayer speaks the [Postgres wire protocol](https://www.postgresql.org/docs/current/protocol.html)
on port **5145** by default (REST is 5143, Flight SQL is 5144). Any tool that ships a
Postgres connector — Metabase, Superset, Tableau, Power BI, Looker, `psql`, `asyncpg`,
`psycopg` — can connect to SLayer as if it were a Postgres database, with no Java or
Arrow driver needed.

The endpoint is **read-only**: catalog introspection plus a constrained SQL subset that
translates to a `SlayerQuery` and executes against the engine. `INSERT` / `UPDATE` /
`DELETE` / `CREATE` / `ALTER` / `DROP` are refused with a read-only error.

## Start the Server

```bash
# Local dev — loopback, no auth needed
slayer pg-serve --demo

# Production-ish — non-loopback bind requires a password token
slayer pg-serve --host 0.0.0.0 --token "$(pass slayer-token)"

# TLS-enabled
slayer pg-serve --host 0.0.0.0 --token TOK \
    --tls-cert /etc/ssl/slayer.crt --tls-key /etc/ssl/slayer.key
```

Flags:

| Flag | Description |
|---|---|
| `--host HOST` | Bind address. Default `0.0.0.0`. With `--demo` and no token, defaults to `127.0.0.1` for the loopback fallback. |
| `--port PORT` | Default `5145`. |
| `--token T` | Password token. Falls back to `$SLAYER_PG_TOKEN`. Required for non-loopback binds. |
| `--tls-cert C` / `--tls-key K` | TLS certificate + key pair (must be supplied together). |
| `--demo` | Generate + ingest the bundled Jaffle Shop dataset before starting. |
| `--storage PATH` | Storage path (same as the REST + MCP servers). |

## The `database` selects a datasource

A SLayer datasource maps to a Postgres **database**. The `database` you connect with
scopes the whole connection to that one datasource; its models appear under the
Postgres schema `public`.

```bash
# `dbname` picks the SLayer datasource:
psql "host=127.0.0.1 port=5145 dbname=jaffle_shop"
```

* `current_database()` returns the connected datasource name.
* `current_schema()` returns `public`.
* Connecting with an unknown (or missing) `database` is rejected at startup with
  `FATAL: database "<name>" does not exist` (SQLSTATE `3D000`).

Cross-datasource queries are not supported — one connection sees exactly one datasource.

## Authentication

* No token configured → the server accepts unauthenticated requests **only** from a
  loopback bind (`127.0.0.0/8` or `::1`). Non-loopback binds without a token are refused
  at startup.
* With a token, the server requests a cleartext password
  (`AuthenticationCleartextPassword`); the client's password must equal the token.
  Combine with TLS (or a loopback bind) so the password is not sent in the clear.

## SQL Surface

The same translator the [Flight SQL facade](flight-sql.md) uses powers this endpoint, so
the query surface is identical:

* Project **named metrics** and **dimensions** the catalog advertises, e.g.
  `SELECT revenue_sum, status FROM orders`.
* Project **raw SQL aggregates over base columns** — `SUM(amount)`, `AVG(price)`,
  `MIN`/`MAX`, `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)` — which map to the
  matching metric. (Aggregating over a *saved measure* or a non-column expression is not
  supported yet; project the saved measure by name instead.)
* Wrap a time dimension in a grain: `date_trunc('month', ordered_at)` or `month(ordered_at)`.
* `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT` / `OFFSET`.
* `SELECT *` is rejected on models (project named columns), but allowed on
  `INFORMATION_SCHEMA.*` and `pg_catalog.*`.

Postgres-specific predicates that aren't valid SLayer DSL (`ILIKE`, `::cast`, regex `~`,
`ANY`/`ALL`) parse but are rejected at execution — use the standard comparison / `IN` /
`BETWEEN` forms.

## Introspection

* `INFORMATION_SCHEMA.METRICS` / `DIMENSIONS` / `SCHEMATA` / `TABLES` / `COLUMNS`.
* A minimum-viable `pg_catalog`: `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`,
  `pg_proc`, `pg_settings`. (Phase 1 ignores `WHERE` on these — the client filters the
  returned rows.)
* `version()` reports `PostgreSQL 14.0 (SLayer Postgres facade <version>) on
  slayer-semantic-layer`.

## Parameterised queries

Bound parameters (`$1`, `$2`, …) are supported: each value is decoded and substituted as a
properly-quoted SQL literal before translation, so BI-tool filter widgets and
`conn.fetch("… WHERE x = $1", value)` work. The connection's wire format is honoured
per column — `asyncpg` (which requests binary results) and `psql` (text) both work.

## Install

The facade is pure-stdlib; the extra exists only to keep the install path consistent:

```bash
pip install "motley-slayer[pg_facade]"
```
