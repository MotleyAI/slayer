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
`median`, `percentile`, the variance/stddev family (`stddev_samp`,
`stddev_pop`, `var_samp`, `var_pop`), and the paired statistics
(`corr`, `covar_samp`, `covar_pop`) need dialect-specific handling
because no standard syntax works everywhere:

| Engine | `median` | `percentile(p=...)` | `stddev_*` / `var_*` | `corr` / `covar_*` (`other=...`) | How |
|---|---|---|---|---|---|
| Postgres | yes | yes | yes | yes | Native `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`, native `STDDEV_*`/`VAR_*`/`CORR`/`COVAR_*`. |
| DuckDB | yes | yes | yes | yes | sqlglot rewrites ordered-set percentiles to `QUANTILE_CONT`. Native `STDDEV_*`/`VAR_*`/`CORR`/`COVAR_*` (sqlglot may emit `VARIANCE` for `var_samp`). |
| SQLite | yes | yes | yes | yes | Python aggregate UDFs registered on every connection — see "SQLite caveats" below. |
| ClickHouse | yes | yes | yes | yes | Native `median(x)`, parametric `quantile(p)(x)`, native `stddev_*`/`var_*`/`corr`/`covar*` (camelCase variants emitted by sqlglot for `var_samp`). |
| MySQL | **no** | **no** | yes | **no** | No native `MEDIAN`/`PERCENTILE_CONT`/`CORR`/`COVAR_*` and no Python-UDF mechanism — SLayer raises `NotImplementedError` for those. `STDDEV_SAMP`/`STDDEV_POP`/`VAR_SAMP`/`VAR_POP` are native on MySQL. Use MariaDB or compute the unsupported aggregations client-side. |

### SQLite caveats

SQLite has a much smaller built-in math/stat catalog than the other supported
engines. SLayer registers Python aggregate and scalar UDFs on every new SQLite
connection via SQLAlchemy's `connect` event (see
`slayer/sql/sqlite_udfs.py`).

**Aggregate UDFs:**

- `median(x)` — 1-arg, average of the two middle values for even N.
- `percentile_cont(x, p)` — 2-arg, linear interpolation (matches Postgres).
- `percentile_disc(x, p)` — 2-arg, smallest value v with `cume_dist(v) >= p`.
- `stddev_samp(x)` — sample stddev; NULL when N ≤ 1 (matches Postgres).
- `stddev_pop(x)` — population stddev; NULL at N=0, 0 at N=1.
- `var_samp(x)` — sample variance; NULL when N ≤ 1. Also registered as
  `variance(x)` because sqlglot rewrites `var_samp` → `VARIANCE` on SQLite.
- `var_pop(x)` — population variance; NULL at N=0, 0 at N=1. Also registered
  as `variance_pop(x)` (same sqlglot rewrite reason).
- `corr(x, y)` — Pearson correlation. NULL when fewer than 2 non-null pairs
  OR either side has zero variance. NULL pairs are skipped entirely.
- `covar_samp(x, y)` — sample covariance (Bessel-corrected); NULL when N ≤ 1.
- `covar_pop(x, y)` — population covariance; NULL at N=0, 0 at N=1. NULL
  pairs are skipped for both covariance variants.

**Scalar UDFs:**

- `ln(x)`, `log10(x)`, `exp(x)`, `sqrt(x)` — single-arg.
- `log(B, X)` — base-first 2-arg logarithm. Returns log_B(X). Matches SQLite
  ≥3.35 built-in (we register the UDF only on older SQLite). Matches Postgres
  `LOG(b, x)`.
- `pow(x, n)` and `power(x, n)` — both spellings registered (sqlglot may emit
  either).

NULL inputs return NULL on every UDF (matching cross-dialect SQL semantics).
Math-domain errors (`ln(0)`, `sqrt(-1)`, `pow(0, -1)`) propagate as
`sqlite3.OperationalError` — matching Postgres's strict error semantics rather
than SQLite ≥3.35's silent-NULL built-in `log()`.

These are registered automatically as long as connections go through
`SlayerSQLClient` (which uses the cached SQLAlchemy engine). If you open a
SQLite connection directly outside SLayer, the UDFs will not be available —
call `register_sqlite_udfs(connection)` manually if you need them.

### MySQL caveats

MySQL has no native `PERCENTILE_CONT`, no `MEDIAN`, no `CORR`, no
`COVAR_SAMP` / `COVAR_POP`, and no Python-UDF mechanism (UDFs are loadable C
`.so` files requiring server-side install).
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
