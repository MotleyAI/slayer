## Why

On a warm connection one SLayer statement costs about five database requests: a pre-ping, BEGIN,
a per-statement timeout, the statement, and ROLLBACK, plus `USE …` on every Snowflake checkout. A
synchronous call on an async-capable driver (psycopg, the Postgres default) opens a new connection
and re-runs SQLAlchemy's eight-statement dialect init every time, and every synchronous workflow
drains networked pools. Cost scales with latency: Snowflake and remote cloud databases pay several
extra round trips per query, and the Snowflake integration job runs into its 15-minute timeout.

## What Changes

- A statement on a warm connection costs the database one request: session state (statement
  timeout, Snowflake role/warehouse/database/schema) is set once per connection and re-set only
  when it changes; liveness is pinged only after 60 s idle; connections run in autocommit where the
  driver allows, so no BEGIN/ROLLBACK wrapper is sent.
- Postgres statement timeouts become session-level, set once per connection.
- New datasource field `transaction_pooler` (Postgres only): a datasource reached through a
  transaction-mode pooler keeps today's transaction-local timeout.
- Synchronous entry points run on one long-lived bridge event loop, so connections persist across
  synchronous calls; the end of a synchronous workflow releases only idle connections holding a
  local database file.
- **BREAKING** (SQL client contract): `execute` refuses statements that change session state or
  control transactions (`USE`, `SET`/`RESET`, `ALTER SESSION`, `SET ROLE`, `DISCARD`, `BEGIN`,
  `START TRANSACTION`, `COMMIT`, `ROLLBACK`) with a typed error.
- SQLAlchemy minimum raised to 2.0.43.
- New sql principle "One request per statement" in `architecture/python.sql.arc42.md`, enforced by
  a statement-count law test.
- Snowflake integration suite: the reserved-alias sweep's concurrency fits the pool; job time is
  measured against a 5-minute target.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/execution`: statement-timeout scope (Postgres session-level, pooler opt-in); new
  requirements for one request per warm statement, connection reuse across synchronous calls,
  after-idle liveness checks, and the pooler opt-in; release at the end of a synchronous workflow
  narrowed to local-file connections.
- `sql/client-contract`: the client refuses session-changing and transaction-control statements;
  Snowflake session overrides apply once per connection.

## Impact

- Code: `slayer/async_utils.py`, `slayer/sql/client.py`, `slayer/sql/engine_factory.py`,
  `slayer/sql/dialects/*` (autocommit, timeout scope, session setup and local-file hooks),
  `slayer/engine/query_engine.py` (`run_sync`, `close`), `slayer/core/models.py`
  (`DatasourceConfig.transaction_pooler`), `slayer/storage/base.py` (save-time check).
- Dependencies: `sqlalchemy >=2.0.43,<2.1`; lock regenerated.
- Docs: `docs/configuration/datasources.md`, `docs/configuration/sql-clients.md`.
- Architecture: `architecture/python.sql.arc42.md` §3 item 17 (new);
  `architecture/system.arc42.md` §3 duplicate item 18 renumbered to 24.
- Tests: new law test and per-backend statement-count tests; existing tests pinning
  `pool_pre_ping`, the checkout listener, the per-statement timeout log and Postgres `SET LOCAL`
  are aligned.
- CI: `.github/workflows/integration-snowflake.yml` keeps its 15-minute step timeout.
