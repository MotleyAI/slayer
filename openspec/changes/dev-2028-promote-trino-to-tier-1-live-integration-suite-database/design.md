## Context

A planning probe against `trinodb/trino:483` (built-in `memory` catalog, trino-python-client 0.340)
established the facts the decisions below rest on:

- `memory` supports `CREATE` / `INSERT` and `COMMENT ON TABLE/COLUMN` (the Inspector returns both);
  it rejects `PRIMARY KEY` and has no foreign keys. `INSERT` does not coerce varchar literals into
  DATE / TIMESTAMP columns.
- Emission failures today: unquoted `INTERVAL 1 DAY`; `EXTRACT(ISOYEAR|ISODOW …)`; the day gap
  `CAST(b AS DATE) - CAST(a AS DATE)` is an interval; `EXTRACT(EPOCH FROM interval)` is invalid;
  `DATE + INTERVAL 'n' HOUR` is rejected. With those patched the shared date matrix passed 107/116,
  the remainder being DATE plus a sub-day unit.
- `SET SESSION` is absorbed into the driver's client-session properties and persists on a pooled
  connection; writing those properties directly takes effect on the next statement.
- The driver chooses HTTPS only on port 443 or via the `http_scheme` connect argument, which the
  SQLAlchemy URL cannot express, and refuses credentials over HTTP.
- `timestamp with time zone` ingests as TIMESTAMP; truncation keeps the zone, and the session zone
  is the client's local zone.

Normative constraints: sql.arc42 §3 item 2 (one file per Tier-1 dialect, data-shaped Tier-2
table), item 1 (AST hooks), item 13 (rendered SQL executes verbatim through one door), item 14
(`create_engine` only in the factory and dialect build hooks). No arc42 or model change is needed:
`trino.py` lives inside the existing `sql.dialects` child.

## Goals / Non-Goals

**Goals:** Trino passes the full shared live coverage with no Trino-specific skips or xfails; Presto
and Athena get the same grammar fixes.

**Non-Goals:** live verification of Presto/Athena; an exact Trino percentile; a live TLS Trino.

## Decisions

### D1 One Presto-family grammar base

`PrestoFamilyDialect(SqlDialect)` in `trino.py` carries the grammar fixes and today's shared scalar
config; `TrinoDialect` adds server- and driver-level behaviour (timeout, engine hook, driver facts);
`PrestoDialect` in `_tier2.py` sets only its name and aliases. Grammar facts live once, so Trino and
Presto cannot drift; Tier 2 stays data-shaped because `PrestoDialect` itself declares no logic.
Alternative — Trino only — rejected by the user: it leaves known-broken Presto/Athena SQL.

Interval counts: an `INTERVAL` node's count is always a literal (the shifted amount, or the
per-unit `1` multiplied by a computed count, `(count) * INTERVAL '1' DAY`), so quoting the literal
never quotes an expression. Gaps use `date_diff('day', CAST(a AS DATE), CAST(b AS DATE))` and
`date_diff('second', a, b)` over the already-truncated operands, keeping the base's
boundaries-crossed semantics. A DATE operand is promoted to TIMESTAMP before a sub-day `date_add`.

### D2 Approximate median / percentile

Kept as sqlglot's `APPROX_PERCENTILE` (user decision) and documented, as `count_distinct_approx`
is; an exact array-sort emulation was probed and works but was not chosen.

### D3 Timeout on the driver connection

`set_connection_timeout` writes `query_max_run_time` (wall clock from submission, like the other
dialects' statement timeouts; `query_max_execution_time` excludes queueing and planning) into
`dbapi_connection._client_session.properties` and returns the prior value or an absent sentinel;
`restore_connection_timeout` puts it back. No extra statement, no pooled leak, a user-configured
value survives. Alternative `SET SESSION` + `RESET SESSION` costs two extra round trips per query
and clobbers a configured value. The private attribute is the risk; the live timeout tests catch
a driver change.

### D4 HTTPS selection in `build_engine`

`TrinoDialect.build_engine` parses the URL with SQLAlchemy, pops `http_scheme` (`https`/`http`,
else a typed error), else selects `https` exactly when the driver would configure authentication
(its own predicates: password present; `access_token` key; `cert` and `key` both present;
`externalAuthentication` key), else returns `None` so the factory's default engine is built. When
it builds, it calls `sa.create_engine(url, pool_pre_ping=True, connect_args={"http_scheme": …})`,
the URL rebuilt by SQLAlchemy (never string surgery) so every other component survives.

### D5 Live suite and fixtures

`TrinoContainer` pinned to `trinodb/trino:483`; each module seeds its own `memory` schema. Shared
fixture seeders gain a Trino column-type map and a typed-temporal-literal mode used only by Trino,
so every other backend's seed SQL is byte-identical. The approximate-percentile tests compare
against a direct `APPROX_PERCENTILE` query over the same rows. A federation test adds Postgres and
MySQL containers on a Docker network shared with Trino, each mounted as a catalog, and joins models
from both through one Trino datasource.

### D6 CI path gates

The Trino workflow and the MySQL, ClickHouse and SQL Server workflows gate on the shared code
their suites exercise (client, engine factory, datasource config, dialect base/registry,
generator, shared fixtures, example seed/verify helpers, `pyproject.toml`, `poetry.lock`), not only
their own dialect files.

## Risks / Trade-offs

- [Driver refactor moves `_client_session`] → the live timeout tests fail loudly.
- [Presto/Athena behaviour changes without a live check] → unit emission tests on both dialects;
  the grammar facts are shared by the Presto family.
- [HTTPS path not live-tested] → unit tests over a stubbed `create_engine`; documented.
- [`APPROX_PERCENTILE` determinism] → tests compare with Trino's own result on the same rows; the
  example checks membership and ordering only.
- [`timestamp with time zone` follows the client's local zone] → documented in Trino caveats.
- [Wider path gates run Docker suites more often] → accepted for coverage.
