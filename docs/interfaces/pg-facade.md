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

## View your models from a BI dashboard

Any tool with a PostgreSQL connector works. End-to-end with the bundled demo and
[Metabase](https://www.metabase.com/):

```bash
# 1. Start SLayer speaking Postgres, with the Jaffle Shop demo preloaded.
#    The BI tool connects over the network (e.g. from a Docker container), so
#    bind all interfaces — a non-loopback bind requires a token.
slayer pg-serve --demo --host 0.0.0.0 --token pick-a-secret

# 2. Run Metabase (any BI tool works — Superset, Tableau, Power BI, Grafana, …).
#    --add-host makes `host.docker.internal` resolve to the Docker host on
#    every platform (built into Docker Desktop; required on Linux, Docker ≥ 20.10).
#    The volume keeps Metabase's own settings/dashboards across container re-creates.
docker run -d -p 3000:3000 --name metabase \
  --add-host=host.docker.internal:host-gateway \
  -e MB_DB_FILE=/metabase.data/metabase.db \
  -v metabase-data:/metabase.data \
  metabase/metabase
```

In Metabase: **Admin → Databases → Add database → PostgreSQL** and fill in:

| Field | Value |
|---|---|
| Host | `host.docker.internal` |
| Port | `5145` |
| Database name | the SLayer **datasource** (e.g. `jaffle_shop`) |
| Username | anything non-empty (ignored) |
| Password | the `--token` value (`pick-a-secret`) |
| SSL | off (unless you started with `--tls-cert`/`--tls-key`) |

Or as a single JDBC connection string:

```text
jdbc:postgresql://host.docker.internal:5145/jaffle_shop?user=metabase&password=pick-a-secret&sslmode=disable
```

> **Connection refused / name not resolving?** Two common causes:
>
> 1. The server was started without `--host 0.0.0.0` — the default demo bind is
>    `127.0.0.1`, which containers cannot reach.
> 2. The BI container runs on Linux Docker without the `--add-host` mapping —
>    `host.docker.internal` only exists out of the box on Docker Desktop. Either
>    re-create the container with the flag (compose: `extra_hosts:
>    ["host.docker.internal:host-gateway"]`), or use the container's default
>    gateway IP as Host instead — find it with
>    `docker exec <container> ip route | awk '/default/ {print $3}'`
>    (typically `172.17.0.1` on the default bridge network, but it differs per
>    compose network and daemon config, so don't hard-code it).

Metabase introspects the schema (via `INFORMATION_SCHEMA` + `pg_catalog`), lists each
SLayer model as a table under schema `public`, and lets you build questions/dashboards
against them. Project named metrics (`revenue_sum`) or write `SUM(amount)` /
`COUNT(*)` — both map to SLayer measures.

> Phase-1 note: BI tools may issue `pg_catalog` queries beyond the six tables the facade
> implements; if a tool trips on one, that's the set to extend.

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
