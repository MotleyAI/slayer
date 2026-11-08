## Why

Trino is Tier 2: SQL generation is unit-tested only, and a live probe against Trino 483 shows
the emitted SQL fails on every date shift, relative date range, `week_sunday` bucket and most
date functions (unquoted `INTERVAL` counts, Postgres-only `EXTRACT` fields, interval-typed date
gaps, sub-day arithmetic on a DATE). It also has no timeout, no install extra, and cannot connect
with credentials on any port but 443. Presto and Athena share the same grammar and the same bugs.

## What Changes

- `TrinoDialect` moves out of `_tier2.py` into `slayer/sql/dialects/trino.py`, over a new
  `PrestoFamilyDialect` base that carries the Presto-family grammar fixes; `PrestoDialect`
  (`presto`, `athena`) stays Tier 2 in `_tier2.py` as plain config over that base.
- Grammar fixes (Trino and Presto/Athena): quoted interval counts, `YEAR_OF_WEEK` /
  `DAY_OF_WEEK` / `DAY_OF_YEAR` date parts, `date_diff`-based day and second gaps, and DATE
  promoted to TIMESTAMP before hour/minute/second arithmetic.
- Trino statement timeout: `query_max_run_time` set on the driver connection for one statement
  and restored afterwards.
- Trino connects over HTTPS whenever the URL carries credentials; `?http_scheme=` overrides.
- New `trino` install extra (in `all`); a missing driver names `motley-slayer[trino]`.
- `median` / `percentile` on Trino stay `APPROX_PERCENTILE` and are documented as approximate.
- Live suite `tests/integration/test_integration_trino.py` (testcontainers, `memory` catalog),
  path-gated `integration-trino.yml` workflow, and `examples/trino/`. The MySQL, ClickHouse and
  SQL Server workflows' path gates widen to the shared code their suites depend on.
- Docs: Trino moves to Tier 1 with a caveats section; Presto/Athena stay Tier 2.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/execution`: Trino timeout scenarios, Trino in the extra-backed missing-driver list, and a
  new requirement that Trino authenticates over HTTPS.
- `queries/date-functions`: Trino joins the Tier-1 list whose date functions must agree.
- `aggregations/formula-templates`: a new requirement that Trino `median` / `percentile` are
  approximate.

## Impact

- New `slayer/sql/dialects/trino.py`; `slayer/sql/dialects/_tier2.py`, `__init__.py`.
- `pyproject.toml` / `poetry.lock` (`trino` extra, `testcontainers[trino]`).
- Shared test fixtures gain a Trino seeding variant; existing dialect tests re-point imports and
  the Trino driver-facts expectation.
- `.github/workflows/` (new Trino workflow; widened MySQL/ClickHouse/SQL Server gates), `ci.yml`,
  `CLAUDE.md`, `examples/seed.py`, new `examples/trino/`.
- `docs/database-support.md`, `docs/getting-started/index.md`, `docs/configuration/datasources.md`.
