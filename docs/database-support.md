# Database support

SLayer uses [sqlglot](https://github.com/tobymao/sqlglot) for dialect-aware
SQL generation. Databases are supported at two tiers.

## Tier 1 — fully tested

Integration tests and/or Docker examples; must not regress.

| Engine | Coverage |
|---|---|
| **SQLite** | Integration tests in `tests/integration/test_integration.py`; embedded example. |
| **Postgres** | Integration tests in `tests/integration/test_integration_postgres.py`; Docker example. |
| **DuckDB** | Integration tests in `tests/integration/test_integration_duckdb.py` (in-process, no Docker). |
| **MySQL** | Docker example with `verify.py`. |
| **ClickHouse** | Docker example with `verify.py`. |

## Tier 2 — code-covered

Unit tests for SQL generation; no live-instance verification.

Snowflake, BigQuery, Redshift, Trino/Presto, Databricks/Spark,
MS SQL Server, Oracle.

## Aggregation support

Most aggregations (`sum`, `avg`, `min`, `max`, `count`, `count_distinct`,
`first`, `last`, `weighted_avg`) work on every supported database.
`median` and `percentile` need dialect-specific handling because no standard
syntax works everywhere:

| Engine | `median` | `percentile(p=...)` | How |
|---|---|---|---|
| Postgres | yes | yes | Native `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`. |
| DuckDB | yes | yes | sqlglot rewrites ordered-set percentiles to DuckDB's `QUANTILE_CONT(x, p ORDER BY x)` syntax. |
| SQLite | yes | yes | Python aggregate UDFs registered on every connection — see "SQLite caveats" below. |
| ClickHouse | yes | yes | Native `median(x)` and parametric `quantile(p)(x)`. |
| MySQL | **no** | **no** | No native function and no Python-UDF mechanism — SLayer raises `NotImplementedError`. Use MariaDB or compute client-side. |

### SQLite caveats

SQLite has no native `MEDIAN`, `PERCENTILE_CONT`, or `PERCENTILE_DISC`. SLayer
registers Python aggregate UDFs on every new SQLite connection via
SQLAlchemy's `connect` event (see `slayer/sql/sqlite_udfs.py`). The UDFs are:

- `median(x)` — 1-arg, average of the two middle values for even N.
- `percentile_cont(x, p)` — 2-arg, linear interpolation (matches Postgres).
- `percentile_disc(x, p)` — 2-arg, smallest value v with `cume_dist(v) >= p`.

These are registered automatically as long as connections go through
`SlayerSQLClient` (which uses the cached SQLAlchemy engine). If you open a
SQLite connection directly outside SLayer, the UDFs will not be available —
call `register_sqlite_udfs(connection)` manually if you need them.

### MySQL caveats

MySQL has no native `PERCENTILE_CONT`, no `MEDIAN`, and no Python-UDF
mechanism (UDFs are loadable C `.so` files requiring server-side install).
Workarounds (`GROUP_CONCAT` + `SUBSTRING_INDEX`, or windowed CTE rewrites)
have material downsides — silent truncation past `group_concat_max_len`,
or major restructuring of the generated query that interacts poorly with
multi-measure `GROUP BY`. SLayer raises `NotImplementedError` at SQL
generation time so the failure is loud and the message is actionable.

If you need percentiles on MySQL, the recommended options are:

- Switch to MariaDB, which has `MEDIAN()`.
- Pull the raw values and compute the percentile in your application.
- Define a custom `Aggregation` on the model with whatever `GROUP_CONCAT`-
  based or windowed expression suits your data shape and group sizes.

## Adding a new dialect

1. Add the mapping to `slayer/engine/query_engine.py:_dialect_for_type()`.
2. If the dialect doesn't accept Postgres-style `INTERVAL` for date arithmetic,
   add a branch in `_build_time_offset_expr` in `slayer/sql/generator.py`.
3. Add parameterized tests in `TestMultiDialectGeneration` in
   `tests/test_sql_generator.py`.
4. For median/percentile, decide whether the native syntax already works
   (sqlglot may handle it) or whether a branch in `_build_median` /
   `_build_percentile` is needed.
