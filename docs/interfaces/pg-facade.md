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

### `CAST(<column> AS <type>)` in projection

A projection of the shape `CAST(<column> AS <type>)` (and the equivalent `col::type`
sugar) is accepted when the inner expression is a bare or qualified column reference
**and** the (source, target) pair is in the allowlist below. The engine still executes
the bare column — the cast is a pure wire-layer type rewrite. The projected column's
Postgres OID is overridden to match the casted type.

Common BI shapes covered: `SELECT CAST(ordered_at AS TIMESTAMP) FROM orders` (DATE
column promoted for a TIMESTAMP-aware client), `SELECT CAST(amount AS TEXT) AS s
FROM orders` (stringification), `SELECT CAST(customers.region AS TEXT) FROM orders`
(joined column).

Out of scope: `CAST` around aggregates (`CAST(SUM(amount) AS DOUBLE)`), `TRY_CAST`,
and `CAST` around expressions that aren't a bare column (`CAST(SUBSTRING(...) AS T)`).
`CAST` wrapping a `DATE_TRUNC(...)` continues to route through the time-grain unwrap.

`CAST(...)` in `ORDER BY` and `GROUP BY` has two layers of admission:

1. **Unaliased canonical-form** (e.g. `ORDER BY CAST(c AS T)` repeating the
   projection's CAST verbatim): **never admitted.** The translator raises
   `ORDER BY column '...' is not in the projection list` / the GROUP BY
   strict-on-extras error. Workaround: alias and reference the alias.
2. **Aliased reference** (`SELECT CAST(c AS T) AS x ... ORDER BY x` /
   `... GROUP BY x`): admitted **only** when the `(source, target)` pair
   preserves sort/group semantics under the bare-column engine projection.

Pairs that **fail** the aliased-reference admission and raise
`ORDER BY on CAST projection '...' with lossy pair X→T is unsupported`
(symmetric message for GROUP BY):

| Path     | Lossy pairs                                                              |
|----------|--------------------------------------------------------------------------|
| ORDER BY | `X → TEXT` for every `X` (lex sort ≠ engine's natural sort)              |
| GROUP BY | `TIMESTAMP → DATE` (many-to-one rollup); `INT → DOUBLE` (IEEE 754 collapse beyond ±2^53) |

Every other admitted pair — identity (`X → X`), `DATE → TIMESTAMP`,
`TIMESTAMP → DATE` for ORDER BY, `INT → DOUBLE` — preserves the casted
semantics under the bare-column engine projection, so the alias path stays
open.

```sql
-- Always rejected (canonical form):
SELECT CAST(delivered_at AS TIMESTAMP) FROM orders
ORDER BY CAST(delivered_at AS TIMESTAMP);

-- Aliased reference, safe pair → works:
SELECT CAST(delivered_at AS TIMESTAMP) AS dt FROM orders
ORDER BY dt;

-- Aliased reference, lossy pair → rejected:
SELECT CAST(id AS TEXT) AS s FROM orders ORDER BY s;
SELECT CAST(ordered_at AS DATE) AS d, COUNT(*) FROM orders GROUP BY d;
```

The wire-type override still applies in the safe-pair case — `dt` is
wire-typed `TIMESTAMP` even though the engine sorts the underlying `DATE`.
A future ticket can lift the remaining restrictions by pushing the CAST
into the engine SQL.

Admitted (source, target) coercions:

| Source type   | Admitted target types        |
|---------------|------------------------------|
| `DATE`        | `DATE`, `TIMESTAMP`, `TEXT`  |
| `TIMESTAMP`   | `TIMESTAMP`, `DATE`, `TEXT`  |
| `INT`         | `INT`, `DOUBLE`, `TEXT`      |
| `DOUBLE`      | `DOUBLE`, `TEXT`             |
| `BOOLEAN`     | `BOOLEAN`, `TEXT`            |
| `TEXT`        | `TEXT`                       |
| *(unknown)*   | `TEXT`                       |

Pairs outside the allowlist (e.g. `CAST(name AS INT)`, `CAST(amount AS BOOLEAN)`)
raise `Unsupported CAST: cannot project <SOURCE> column as <TARGET> (...). Admitted
coercions: see docs/interfaces/pg-facade.md.` Unsupported target types (`UUID`, `JSON`,
`ARRAY`, `STRUCT`, …) raise the standard `Unsupported projection expression` error.

`DOUBLE → INT` is intentionally excluded: Python's `int(<float>)` truncates toward zero
while Postgres rounds half-to-even, so silently admitting the pair would diverge from
`psql` semantics. Pre-aggregate or pre-round on your side when an integer-typed result
is required.

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

## Testing your changes

For wire-level / translator changes, the unit suite under `tests/test_pg_facade*.py` covers
each component in isolation. Behaviour at the *interaction boundary* with a real BI client
is covered by the live-Metabase end-to-end suite (DEV-1562):

```bash
poetry run pytest -m metabase_e2e tests/integration/test_metabase_e2e.py -v
```

The suite needs Docker; it boots `metabase/metabase:v0.62.1.5` alongside two
token-protected pg-serve processes (per-session random tokens, both bound on `0.0.0.0`
so the container reaches them via `host.docker.internal`; the second backs the L.2 / L.3
bad-password tests) and drives ~62 cases through the real `pgjdbc` protocol — bootstrap +
sync, MBQL aggregations and time-grain breakouts, native-SQL probes, wire-format
round-trips, transactions, concurrency, and error envelopes. Skips cleanly when Docker is
unavailable. CI fires automatically on PRs touching `slayer/pg_facade/`, `slayer/facade/`,
`slayer/demo/`, the
e2e test files, or `pyproject.toml` / `poetry.lock`.
