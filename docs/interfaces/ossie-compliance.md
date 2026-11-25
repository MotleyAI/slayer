# Ossie compliance

This page states how far SLayer 2.0 complies with [Ossie](https://github.com/apache/ossie), layer by layer. Every
claim is measured against the Ossie specification vendored in SLayer's test suite, apache/ossie revision `d4dca4e9`
(22 November 2026), and names the tests that back it. The Tier-1 databases are SQLite, DuckDB, PostgreSQL, MySQL,
ClickHouse, SQL Server, Trino, Snowflake and BigQuery. The compliance gate runs on SQLite and DuckDB on every pull
request (`tests/test_ossie_gate.py`), on the others nightly (`tests/integration/test_integration_ossie_gate.py`), and on
all nine weekly against upstream's `main`.

| Layer | Claim | Certified on |
|---|---|---|
| 1. Expression language | sound: every expression SLayer accepts has Ossie's meaning; some constructs are refused or planned | SQLite, DuckDB, PostgreSQL, MySQL, ClickHouse, SQL Server, Trino, Snowflake, BigQuery |
| 2. SQL queries | not claimed | — |
| 3. Wide-table queries | conformant under `conformance="ossie-l3"` | SQLite, DuckDB, PostgreSQL, MySQL, ClickHouse, SQL Server, Trino, Snowflake, BigQuery |
| Semantic-model format | the vendored corpus loads, round-trips and grades portable | DuckDB |

Rows marked `planned` below are constructs SLayer does not support yet. Each is a test asserting Ossie's result,
expected to fail today; the test starts passing, and the row leaves this page, when SLayer supports it. Rows marked
`refused` are constructs SLayer rejects with a typed error and does not plan to add on the databases named.

## Layer 1: expression language

SLayer's expressions are a superset of `OSSIE_SQL_2026`. Where SLayer accepts an Ossie construct, it evaluates it
with Ossie's meaning on every Tier-1 database. The gate holds one case for every REQUIRED and RECOMMENDED construct of
`expression_language.md` (`tests/_dev2446_required_manifest.py`), checks that the specification lists no construct the
gate misses (`tests/test_dev2446_required_constructs.py`), and runs each case on every database. Every library function
also has executed cases with one meaning (`tests/test_dev2120_library_matrix.py`).

SLayer does not claim full Layer 1 compliance: it refuses or plans the constructs below. Per database, the gate's
cases are:

| Database | Pass | Refused | Gap |
|---|---|---|---|
| SQLite | 155 | 4 | 45 |
| DuckDB | 157 | 4 | 43 |
| PostgreSQL | 157 | 4 | 43 |
| MySQL | 157 | 4 | 43 |
| ClickHouse | 155 | 4 | 45 |
| SQL Server | 154 | 7 | 43 |
| Trino | 157 | 4 | 43 |
| Snowflake | 157 | 4 | 43 |
| BigQuery | 155 | 4 | 45 |

| Construct | Example | Status |
|---|---|---|
| `APPROX_PERCENTILE` | `APPROX_PERCENTILE(orders.amount, 0.5)` (RECOMMENDED) | refused |
| `RADIANS` | `RADIANS(180)` (RECOMMENDED) | refused |
| `DEGREES` | `DEGREES(0)` (RECOMMENDED) | refused |
| `REGEXP_COUNT` | `REGEXP_COUNT(orders.status, 'n')` (RECOMMENDED) | refused |
| `REGEXP_LIKE` | `REGEXP_LIKE(orders.status, '^pe')`, on SQL Server before 2025 | refused |
| `REGEXP_EXTRACT` | `REGEXP_EXTRACT(orders.status, 'e.d')`, on SQL Server before 2025 | refused |
| `REGEXP_REPLACE` | `REGEXP_REPLACE(orders.status, 'n', 'N')`, on SQL Server before 2025 | refused |
| `PERCENTILE_DISC` | `PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY orders.amount)` | planned |
| `DISTINCT MODIFIER` | `SUM(DISTINCT orders.qty)` (only `COUNT(DISTINCT …)` is supported) | planned |
| `CURRENT_TIME` | `CURRENT_TIME`, `CURRENT_TIME()` | planned |
| `EXTRACT(DAYOFWEEK)` | `EXTRACT(DAYOFWEEK FROM orders.created_at)` | planned |
| `DATE_PART(DAYOFWEEK)` | `DATE_PART('dayofweek', orders.created_at)` | planned |
| `EXTRACT(MILLISECOND)` | `EXTRACT(MILLISECOND FROM orders.created_at)` | planned |
| `DATE_PART(MILLISECOND)` | `DATE_PART('millisecond', orders.created_at)` | planned |
| `TIMESTAMP_NTZ '2024-01-15 10:30:00'` | a `TIMESTAMP_NTZ` literal | planned |
| `TIME '10:30:00'` | a time-of-day literal | planned |
| `CAST('10:30:00' AS TIME)` | a cast to a time of day (answers outside strict on DuckDB, PostgreSQL, MySQL, SQL Server, Trino and Snowflake) | planned |
| `CAST AS TIME` | `CAST('10:30:00' AS TIME)` (answers outside strict on DuckDB, PostgreSQL, MySQL, SQL Server, Trino and Snowflake) | planned |
| `TO_DATE` | `TO_DATE('2024-01-15')` | planned |
| `TO_TIMESTAMP` | `TO_TIMESTAMP('2024-01-15 10:30:00')` | planned |
| `CHARINDEX` | `CHARINDEX('d', orders.status)` | planned |
| `STARTSWITH` | `STARTSWITH(orders.status, 'pe')` | planned |
| `ENDSWITH` | `ENDSWITH(orders.status, 'ing')` | planned |
| `TRUNC` | `TRUNC(2.789, 1)` (the one-argument form is SLayer's) | planned |
| `TRUNCATE` | `TRUNCATE(2.789, 1)` | planned |
| `IF` | `IF(orders.amount > 20, 1, 0)` (`IFF` is supported) | planned |
| `NVL` | `NVL(orders.note, 'x')` (`COALESCE` and `IFNULL` are supported) | planned |
| `NVL2` | `NVL2(orders.status, 'y', 'n')` | planned |
| `ZEROIFNULL` | `ZEROIFNULL(orders.amount)` | planned |
| `NULLIFZERO` | `NULLIFZERO(orders.amount)` | planned |
| `ROW_NUMBER` | `ROW_NUMBER() OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `RANK` | `RANK() OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `DENSE_RANK` | `DENSE_RANK() OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `NTILE` | `NTILE(2) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `PERCENT_RANK` | `PERCENT_RANK() OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `CUME_DIST` | `CUME_DIST() OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `LAG` | `LAG(orders.amount, 1, 0) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `LEAD` | `LEAD(orders.amount, 1, 0) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `FIRST_VALUE` | `FIRST_VALUE(orders.amount) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `LAST_VALUE` | `LAST_VALUE(orders.amount) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `NTH_VALUE` | `NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount)` in a query dimension | planned |
| `RUNNING TOTAL` | `SUM(orders.amount) OVER (ORDER BY orders.order_date)` in a query dimension | planned |
| `RUNNING AVERAGE` | `AVG(…) OVER (ORDER BY … ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` in a query dimension | planned |
| `PARTITION TOTALS` | `SUM(orders.amount) OVER (PARTITION BY orders.region)` in a query dimension | planned |
| `PERCENT OF TOTAL` | `orders.amount / SUM(orders.amount) OVER () * 100` in a query dimension | planned |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | a window frame in a query dimension | planned |
| `ROWS BETWEEN N PRECEDING AND N FOLLOWING` | a window frame in a query dimension | planned |
| `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | a window frame in a query dimension | planned |
| `NULLS FIRST` | `NULLS FIRST` in a window's `ORDER BY` in a query dimension | planned |
| `NULLS LAST` | `NULLS LAST` in a window's `ORDER BY` in a query dimension | planned |
| `DEFAULT NULL PLACEMENT` | Ossie's default NULL placement in a window's `ORDER BY` | planned |

Ossie's window functions are refused in a query's dimensions; a dataset field's expression may hold one (see
[Datasets](../concepts/datasets.md#window-functions-in-a-fields-expression)), and SLayer's own query language
computes ranks, offsets and running totals with its transforms (see
[Expressions](../concepts/expressions.md#transform-functions)). Ossie does not fix `DAYOFWEEK`'s numbering or
`MILLISECOND`'s meaning; SLayer's own `date_part('day_of_week', x)` is ISO (Monday 1, Sunday 7).

## Layer 2: SQL queries

SLayer does not claim Ossie Layer 2. Its SQL interfaces ([SQL API](pg-facade.md), [Flight SQL](flight-sql.md)) are
SLayer's own.

## Layer 3: wide-table queries

Under `conformance="ossie-l3"`, SLayer answers Ossie's wide-table queries with Ossie's rows, output names and error
codes on every Tier-1 database: see [Ossie Layer 3](ossie-layer-3.md). Every case of upstream's
`compliance/wide_table/cases` runs verbatim under the strict binding on its fixture, seeded into each database, each
answer and each error code compared with upstream's expected result. SLayer knows of no deviation.
Expressions inside a query are subject to the listed Layer 1 gaps.

Under the default binding (`conformance="slayer"`), SLayer's own query language is a superset: every Ossie query
answers with Ossie's rows (each row case of upstream's corpus runs under the default too), and SLayer also answers
queries Ossie refuses, with warnings. Result columns carry Ossie's output names only under the strict binding; by
default they keep SLayer's dotted names. A model-scoped metric is referenced by its bare name.

## Semantic-model format

SLayer stores Ossie semantic-model documents natively. What passes, against the vendored revision:

- every document of the vendored corpus passes upstream's validator, stores and loads, binds and compiles every
  metric, and exports back to the same document (`tests/test_dev2243_corpus.py`);
- upstream's portability grader grades `slayer ossie-roundtrip <in> <out>` portable on every rung of its ladder
  (`tests/test_dev2243_portability_grader.py`).

SLayer intends to accept any valid Ossie document; beyond the corpus that is not proven. The behaviours below are
known not to follow Ossie yet:

| Case | Behaviour | Status |
|---|---|---|
| `format-relationship-ai-context` | a relationship's `ai_context` is not shown when inspecting the dataset | planned |
| `format-is-time-role` | a field's `dimension.is_time` does not set whether it is a time dimension | planned |
| `format-is-time-axis` | a temporal field marked `is_time: false` is still taken as a time axis | planned |
| `format-metric-dialect` | a metric's dialect-specific expression is not used on that dialect | planned |
| `format-field-name-dot` | a field name containing a dot cannot be referenced | planned |
| `format-dataset-name-dot` | a dataset name containing a dot cannot be referenced | planned |
| `format-metric-name-punctuation` | a metric name containing a space or punctuation cannot be referenced | planned |
| `format-quoted-names-distinct` | names differing only by quoting (`"unit price"`, `unit_price`) collide | planned |
| `format-delimited-dataset-name` | a delimited dataset name (`"Order Lines"`) cannot be referenced | planned |
| `format-quoted-column-expression` | a field expression naming a quoted column (`"Order Amount"`) is refused | planned |
| `format-decimal-exact` | `Decimal` aggregates are not exact | planned |
| `format-time-dimension` | a `Time` field cannot be a dimension | planned |
| `format-time-aggregate` | a `Time` field cannot be aggregated (`MIN`, `MAX`) | planned |
| `format-datetimetz-truncation` | truncating a `DateTimeTz` field drops its time zone | planned |
| `format-datetimetz-offset-literal` | a timestamp literal with a UTC offset cannot be compared with a `DateTimeTz` field | planned |
