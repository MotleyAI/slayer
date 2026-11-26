# Ossie compliance

This page states how far SLayer 2.0 complies with [Ossie](https://github.com/apache/ossie), layer by layer. Every
claim is measured against the Ossie specification vendored in SLayer's test suite, apache/ossie revision `c2980498`
(26 November 2026), and names the tests that back it. The Tier-1 databases are SQLite, DuckDB, PostgreSQL, MySQL,
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
| SQLite | 199 | 0 | 5 |
| DuckDB | 201 | 0 | 3 |
| PostgreSQL | 201 | 0 | 3 |
| MySQL | 200 | 1 | 3 |
| ClickHouse | 199 | 0 | 5 |
| SQL Server | 197 | 4 | 3 |
| Trino | 201 | 0 | 3 |
| Snowflake | 201 | 0 | 3 |
| BigQuery | 201 | 0 | 3 |

| Construct | Example | Status |
|---|---|---|
| `REGEXP_COUNT` | `REGEXP_COUNT(orders.status, 'n')` (RECOMMENDED), on MySQL and on SQL Server before 2025 | refused |
| `REGEXP_LIKE` | `REGEXP_LIKE(orders.status, '^pe')`, on SQL Server before 2025 | refused |
| `REGEXP_EXTRACT` | `REGEXP_EXTRACT(orders.status, 'e.d')`, on SQL Server before 2025 | refused |
| `REGEXP_REPLACE` | `REGEXP_REPLACE(orders.status, 'n', 'N')`, on SQL Server before 2025 | refused |
| `CURRENT_TIME` | `CURRENT_TIME`, `CURRENT_TIME()` | planned |
| `TIME '10:30:00'` | a time-of-day literal | planned |
| `CAST('10:30:00' AS TIME)` | a cast to a time of day (answers outside strict on DuckDB, PostgreSQL, MySQL, SQL Server, Trino, Snowflake and BigQuery) | planned |
| `CAST AS TIME` | `CAST('10:30:00' AS TIME)` (answers outside strict on DuckDB, PostgreSQL, MySQL, SQL Server, Trino, Snowflake and BigQuery) | planned |

Ossie's window functions run in every position (see
[SQL window functions](../concepts/expressions.md#sql-window-functions-over)); `COUNT(DISTINCT …)`, `MEDIAN` and
`PERCENTILE_CONT` under `OVER` run only where the database windows them (see
[Database support](../database-support.md#window-functions)). Ossie does not fix `DAYOFWEEK`'s numbering or
`MILLISECOND`'s meaning, so SLayer rules them: `EXTRACT(DAYOFWEEK FROM x)` is ISO (Monday 1, Sunday 7), as
`date_part('day_of_week', x)` is, and `EXTRACT(MILLISECOND FROM x)` is the millisecond component (0 to 999, truncated).
Both are parts only: `DAYOFWEEK(x)` and `MILLISECOND(x)` are refused, as those function names mean other numberings
on several databases.

## Layer 2: SQL queries

SLayer does not claim Ossie Layer 2. Its SQL interfaces ([SQL API](pg-facade.md), [Flight SQL](flight-sql.md)) are
SLayer's own.

## Layer 3: wide-table queries

Under `conformance="ossie-l3"`, SLayer answers Ossie's wide-table queries with Ossie's rows, output names and error
codes on every Tier-1 database: see [Ossie Layer 3](ossie-layer-3.md). Every case script of upstream's
`compliance/wide_table` runs verbatim under the strict binding on its data and model, seeded into each database, each
answer and each error code compared with the script's expected result. SLayer knows of no deviation.
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
