# Database support

SLayer uses [sqlglot](https://github.com/tobymao/sqlglot) for dialect-aware
SQL generation. Databases are supported at two tiers.

## Tier 1 — fully tested

Live-instance integration tests must not regress. Where Docker images exist,
the suites spin up the engine via `testcontainers`; the cloud-only engines
(BigQuery, Snowflake) skip cleanly when credentials aren't available and run
against the live service in CI when they are.

| Engine | Live test | Docker example |
|---|---|---|
| **SQLite** | `tests/integration/test_integration.py` (in-process) | `examples/embedded/` |
| **Postgres** | `tests/integration/test_integration_postgres.py` (pytest-postgresql, spawned temp instance) | `examples/postgres/` |
| **DuckDB** | `tests/integration/test_integration_duckdb.py` (in-process) | `examples/embedded/` (DuckDB mode) |
| **MySQL** | `tests/integration/test_integration_mysql.py` (`testcontainers[mysql]`) | `examples/mysql/` |
| **ClickHouse** | `tests/integration/test_integration_clickhouse.py` (`testcontainers[clickhouse]`) | `examples/clickhouse/` |
| **SQL Server** | `tests/integration/test_integration_sqlserver.py` (`testcontainers`, `msodbcsql18` + `unixodbc-dev` on the runner) | `examples/sqlserver/` |
| **Snowflake** | `tests/integration/test_integration_snowflake.py` (skips without `~/.snowflake/connections.toml`; profile name overridable via `$SLAYER_SNOWFLAKE_CONNECTION`) | `examples/snowflake/` (no Docker) |
| **BigQuery** | `examples/bigquery/verify.py` driven by CI against `bigquery-public-data.thelook_ecommerce` (gated on `GCP_PROJECT_ID` / `GCP_SA_KEY_B64` repo secrets) | `examples/bigquery/` (no Docker — managed service) |

BigQuery does not yet have a pytest-style integration suite; its CI coverage
runs the example's `verify.py` directly via `.github/workflows/ci.yml`. That
exercises auto-ingestion, basic projection, joins, time-grain dimensions, and
the cardinality / sum-of-grouped-equals-total invariants — enough to catch
emitted-SQL regressions, but the verify-script tier is shallower than the
testcontainers suites.

## Tier 2 — code-covered

Unit tests for SQL generation; no live-instance verification.

Redshift, Trino/Presto (Athena uses the Presto dialect), Databricks/Spark,
Oracle.

## Aggregation support

Most aggregations (`sum`, `avg`, `min`, `max`, `count`, `count_distinct`,
`count_distinct_approx`, `first`, `last`, `weighted_avg`) work on every
supported database. `count_distinct_approx` is dialect-aware (see
[below](#count_distinct_approx-by-dialect)) but always available — it falls
back to an exact `COUNT(DISTINCT)` where there's no native function.
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
| Snowflake | yes | yes | yes | yes | Native `MEDIAN`, `PERCENTILE_CONT(p) WITHIN GROUP`, `STDDEV_*`/`VAR_*`/`CORR`/`COVAR_*`. `LOG10` native; no native `LOG2` (falls through to `LOG(2, x)`). |
| MySQL | **no** | **no** | yes | **no** | No native `MEDIAN`/`PERCENTILE_CONT`/`CORR`/`COVAR_*` and no Python-UDF mechanism — SLayer raises `NotImplementedError` for those. `STDDEV_SAMP`/`STDDEV_POP`/`VAR_SAMP`/`VAR_POP` are native on MySQL. Use MariaDB or compute the unsupported aggregations client-side. |
| SQL Server (T-SQL) | **no** | **no** | yes | yes (decomposed) | `MEDIAN` doesn't exist and T-SQL's `PERCENTILE_CONT` is window-only (no `WITHIN GROUP` aggregate form) — SLayer raises `NotImplementedError`. Native `STDEV`/`STDEVP`/`VAR`/`VARP` (slayer renames the canonical `STDDEV_*`/`VAR_*` names at emit time). `CORR`/`COVAR_*` use the same variance-decomposition formula as MySQL (`cov(x,y) = (var(x+y) − var(x) − var(y)) / 2`, `corr = cov / (stddev(x) · stddev(y))`). |
| BigQuery | **no** | **no** | yes | yes | BigQuery has no `MEDIAN` aggregate, and its `PERCENTILE_CONT` is analytic-only (no `WITHIN GROUP` syntax) — the base class emit `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` fails at runtime. If you need percentile on BigQuery, define a custom `Aggregation` using `APPROX_QUANTILES(x, 100)[OFFSET(N)]`. Native `STDDEV_SAMP`/`STDDEV_POP`/`VAR_SAMP`/`VAR_POP`/`CORR`/`COVAR_SAMP`/`COVAR_POP` (sqlglot may emit `VARIANCE` for `var_samp`). |

### `count_distinct_approx` by dialect

`count_distinct_approx` emits each database's native approximate-distinct
function where one exists, and falls back to an **exact** `COUNT(DISTINCT)`
where it does not. The fallback is exact (more accurate, never approximate),
so results are always at least as precise as requested. The per-dialect
mapping lives in `SqlDialect.build_approx_count_distinct`.

| Engine | Emitted SQL |
|---|---|
| DuckDB / Spark / Databricks | `approx_count_distinct(x)` |
| ClickHouse | `uniq(x)` |
| BigQuery / Snowflake / SQL Server (T-SQL) / Oracle | `APPROX_COUNT_DISTINCT(x)` |
| Trino / Presto | `approx_distinct(x)` |
| Redshift | `APPROXIMATE COUNT(DISTINCT x)` |
| Postgres / SQLite / MySQL | `COUNT(DISTINCT x)` (exact fallback) |

### SQLite caveats

SQLite has a much smaller built-in math/stat catalog than the other supported
engines. SLayer registers Python aggregate and scalar UDFs on every new SQLite
connection via SQLAlchemy's `connect` event (see
`slayer/sql/dialects/sqlite.py`).

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

- `ln(x)`, `log10(x)`, `log2(x)`, `exp(x)`, `sqrt(x)` — single-arg. `log2(x)` is registered on **every** SQLite version (overriding ≥3.35's silent-NULL built-in) for the same strict-error reason as `log(B, X)` below.
- `log(B, X)` — base-first 2-arg logarithm. Returns log_B(X). Registered on **every** SQLite version, including ≥3.35 where it overrides the built-in (the built-in silently returns NULL on math-domain inputs; the UDF raises, matching the strict-Postgres semantics SLayer promises). Same B-first arg order as SQLite ≥3.35's built-in and Postgres's `LOG(b, x)`.
- `pow(x, n)` and `power(x, n)` — both spellings registered (sqlglot may emit
  either).

NULL inputs return NULL on every UDF (matching cross-dialect SQL semantics).
Math-domain errors (`ln(0)`, `sqrt(-1)`, `pow(0, -1)`) propagate as
`sqlite3.OperationalError` — matching Postgres's strict error semantics rather
than SQLite ≥3.35's silent-NULL built-in `log()`.

These are registered automatically as long as connections go through
`SlayerSQLClient` (which uses the cached SQLAlchemy engine). If you open a
SQLite connection directly outside SLayer, the UDFs will not be available —
import and call the registration helper manually if you need them:

```python
from slayer.sql.dialects.sqlite import register_sqlite_udfs
register_sqlite_udfs(connection)
```

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

### SQL Server (T-SQL) caveats

T-SQL has `STDEV`/`STDEVP`/`VAR`/`VARP` (not `STDDEV_SAMP`/`STDDEV_POP`/
`VAR_SAMP`/`VAR_POP`); sqlglot's tsql transpiler emits incorrect names like
`VAR_SAMP` and `VARIANCE_POP`, so the T-SQL dialect overrides the canonical
spellings via `Anonymous` rewrites in `slayer/sql/dialects/tsql.py`.

`CORR`/`COVAR_SAMP`/`COVAR_POP` are derived from variance:
`cov(x, y) = (var(x + y) − var(x) − var(y)) / 2`,
`corr = cov / (stddev(x) · stddev(y))`. The decomposition is shared with
MySQL via `_build_covar_decomposition` in `slayer/sql/dialects/base.py`.

`MEDIAN` doesn't exist, and `PERCENTILE_CONT` in T-SQL is a window function
only — there is no `WITHIN GROUP` aggregate form. SLayer raises
`NotImplementedError` for both at SQL generation time. Use the windowed form
as a custom `Aggregation` if you need it, or compute client-side.

Other T-SQL specifics surfaced by the dialect:

- `DATETRUNC(unit, col)` for time-grain dimensions (SQL Server 2022+ —
  earlier versions don't have `DATETRUNC` and aren't supported).
- `DATETRUNC(iso_week, col)` for Monday-aligned week truncation —
  `@@DATEFIRST`-independent so the bucketing is deterministic.
- `DATEADD(unit, n, col)` for time-shift arithmetic — T-SQL has no
  `INTERVAL` literal.
- Bracketed `[ident]` quoting — `<model>.<column>` SLayer aliases get
  mangled to `<model>___<column>` at emit and decoded back on result-row
  keys (mirror of the BigQuery `___` mangling; see DEV-1571).
- Native `LOG10`, no native `LOG2` (`log2(x)` falls through to the
  canonical 2-arg `LOG(2, x)` form).

### Snowflake caveats

Snowflake is a fully managed cloud warehouse — no Docker, no local instance.
The integration suite skips by default unless `~/.snowflake/connections.toml`
contains a profile named `slayer_test` (override with
`$SLAYER_SNOWFLAKE_CONNECTION`). See [Datasources →
Snowflake](configuration/datasources.md#snowflake) for connection setup.

- **`LIMIT 0` type probes still compile.** SLayer infers column types via
  `LIMIT 0` wrapper queries. Snowflake compiles those — consuming a small
  amount of warehouse compute — even though no rows are returned. A future
  `DESCRIBE QUERY`-based probe would skip this; not yet implemented.
- **Identifier casing.** Snowflake stores unquoted identifiers in uppercase
  but resolves them case-insensitively. sqlglot's snowflake dialect emits
  bare lowercase identifiers, which therefore resolve correctly against
  uppercase storage. **Mixed-case** names like `"Revenue"` get double-quoted
  by sqlglot and become case-sensitive — they must match the stored case
  exactly.
- **Declarative FK constraints are surfaced.** Unlike ClickHouse / BigQuery,
  Snowflake exposes its (non-enforced) FK metadata via the Inspector. Auto-
  ingestion discovers joins like Postgres / MySQL / SQLite.
- **No native LOG2.** `log2(x)` in a `Column.sql` falls through to the
  canonical 2-arg `LOG(2, x)` form. `LOG10` and the rest of the math /
  statistical functions are native.

### BigQuery caveats

BigQuery is a fully managed cloud warehouse — no Docker, no local instance.
CI runs the example's `verify.py` against `bigquery-public-data.thelook_ecommerce`,
gated on `GCP_PROJECT_ID` and `GCP_SA_KEY_B64` repo secrets (forks without
them skip cleanly). Auth via Google Application Default Credentials
(`$GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account JSON key,
plus `$GCP_PROJECT_ID` for billing). The `bigquery://` driver requires the
`sqlalchemy-bigquery` extra.

- **No FK introspection.** BigQuery exposes no foreign-key metadata via
  `INFORMATION_SCHEMA`, so auto-ingestion cannot discover joins. Hand-declare
  `ModelJoin`s on the model.
- **Dotted alias mangling.** BigQuery rejects column names containing `.`
  (output schema names must match `[A-Za-z_][A-Za-z0-9_]*`), so SLayer
  rewrites `<model>.<column>` aliases (`orders._count`,
  `orders.products.category`) to `<model>___<column>` at emit time and
  reverses the mapping on result rows. The triple-underscore separator is
  distinct from `__` (used by `_query_as_model` for cross-model leaf
  flattening), so the two encodings never collide. In `Column.sql`,
  fully-qualified table paths must be backticked per-segment
  (`` `project`.`dataset`.`table` ``) — a single backticked dotted path of
  word-only segments (`` `my_dataset.my_table` ``) would false-positive
  mangle.
- **No `MEDIAN` aggregate; `PERCENTILE_CONT` is analytic-only.** Both
  raise at SQL generation time (sqlglot doesn't transpile the base class's
  `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` to BigQuery's analytic
  form). Use a custom `Aggregation` with `APPROX_QUANTILES(x, 100)[OFFSET(N)]`
  when you need it.
- **No native EXPLAIN.** BigQuery has no SQL-level `EXPLAIN`. The
  `BigqueryDialect.explain_prefix` is `None`, so `engine.execute(...,
  explain=True)` returns the dry-run SQL unchanged rather than an execution
  plan.

## Adding a new dialect

1. Add the mapping to `slayer/engine/query_engine.py:_dialect_for_type()`.
2. If the dialect doesn't accept Postgres-style `INTERVAL` for date arithmetic,
   add a branch in `_build_time_offset_expr` in `slayer/sql/generator.py`.
3. Add parameterized tests in `TestMultiDialectGeneration` in
   `tests/test_sql_generator.py`.
4. For median/percentile, decide whether the native syntax already works
   (sqlglot may handle it) or whether a branch in `_build_median` /
   `_build_percentile` is needed.
