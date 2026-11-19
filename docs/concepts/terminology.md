# Terminology

Key terms used throughout SLayer documentation and code.

## Data Structure

**Dataset** — A semantic layer definition that maps a database table, SQL subquery, or saved query to queryable fields. Defined as YAML files or auto-generated via ingestion.

**Field** — The unified row-level building block of a dataset (`SlayerDataset.fields`). Each field has a `name`, `sql` expression, and `datatype` (one of Ossie's datatype names — `String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque` — or `Array` with an `element_type`). At query time a field can be used as a group-by key (a dimension), as the input to an aggregation (a metric), or both; a list field has rules of its own, see [List fields](queries.md#list-fields). Fields can also carry `primary_key`, `allowed_aggregations` (whitelist), `filter` (a `CASE WHEN` value mask that fires in every position), `granularity` (the time bucket a temporal field is already truncated to), `format`, `label`, and `meta`. `DatasetMetric` (named expression) and `Aggregation` carry the same `meta` field with identical semantics — opaque JSON metadata for caller bookkeeping, persisted with the entity.

**Dimension** — How a field is *used* in a query when it's a GROUP BY key. The field itself isn't a dimension or metric intrinsically — that role is decided per query. In SLayer's query DSL, the `dimensions` list names the fields to group/filter by.

**Metric (in a query)** — An expression entry in `SlayerQuery.metrics`. Examples: `"sum(revenue)"`, `"count(*)"`, `{"metric": "sum(revenue) / count(*)", "alias": "aov"}`, `"cumsum(sum(revenue))"`. Each entry compiles to one output column.

**Metric (named expression)** — A saved expression stored on a dataset (`SlayerDataset.metrics: List[DatasetMetric]`). Shape `{name, expression, label, description, datatype, meta}`; a query's inline `metrics` entry is `{metric, alias, label, description, datatype, meta}`. Queries reference saved metrics by bare name in any expression context (`"aov"`).

**Aggregation** — How a field is rolled up. Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `count_distinct_approx`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`, `collect`. Custom aggregations can be defined at dataset level. Applied at query time as a function call: `sum(revenue)`, `count(*)`, `weighted_avg(price, weight=quantity)`, `corr(price, other=quantity)`.

**Relationship** — A LEFT JOIN edge between two datasets. Defined by a target dataset name (`to`) and join key pairs (from the dataset's own foreign keys). Each dataset only stores direct relationships — multi-hop paths like `customers.regions.name` are resolved at query time by walking each intermediate dataset's own relationships.

**Cross-dataset metric** — An aggregation over a joined dataset's field, referenced with a dotted path inside the aggregation (`avg(customers.score)`, or multi-hop: `sum(customers.regions.population)`). Computed as a sub-query to avoid row multiplication. Transforms work on cross-dataset metrics: `cumsum(avg(customers.score))`.

**Query extension** (an `extensions` entry, a `DatasetExtension`) — Adds `metrics` (named expressions) and `relationships` to any dataset a query object reads, visible only in that query object, without modifying the stored dataset. Used for derived buckets or one-off relationships; a per-query field is an expression in the position that uses it.

**Dataset filter** — A WHERE filter defined on a dataset, applied wherever the dataset's rows are read, including as a relationship target (e.g., `"deleted_at IS NULL"`).

**Query-as-dataset** — Using a query's result as the source for another query, or saving it as a permanent dataset. Useful for materializing complex aggregations.

**Datasource** — A database connection configuration: host, port, credentials, database type. SLayer supports Postgres, MySQL/MariaDB, ClickHouse, SQLite, BigQuery, and Snowflake.

## Queries

**Metric entry** — An expression entry in `SlayerQuery.metrics`. Defined by an expression string. Examples: aggregated field reference (`"sum(revenue)"`), `*`-count (`"count(*)"`), arithmetic on aggregated metrics (`"sum(revenue) / count(*)"`), transform function (`"cumsum(sum(revenue))"`), or bare named-expression reference (`"aov"`). Supports an optional `label` for human-readable display. See [Expressions](expressions.md).

**Label** — An optional human-readable display name for a metric entry, dimension, or time dimension. Separate from the technical `alias`, which is used as the result column key. Example: `{"metric": "sum(revenue) / count(*)", "alias": "aov", "label": "Average Order Value"}`.

**Filter** — A condition that restricts which rows are included. Defined as an expression string: `"status = 'completed'"`, `"amount > 100"`. See [Filters](queries.md#filters).

**Time dimension** — A dimension of type `time` or `date`, used for time-based grouping. When specified in `time_dimensions`, SLayer truncates it to the given granularity (e.g., monthly buckets). The same field can also be used as a regular dimension (without truncation).

**Granularity** — The level of time truncation applied to a time dimension, to determine the size of each time bucket (one row's time span) in the result, or as an argument in time related functions. Available granularities: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`. (`week` is Monday-anchored; `week_sunday` is Sunday-anchored.)

**Time bucket** — A single unit of the granularity. If granularity is `month`, each time bucket is one calendar month (e.g., January 2024, February 2024). Each time bucket becomes one row in the query result.

**Date range** — The start and end bounds for a time dimension filter. Specified as `["2024-01-01", "2024-12-31"]` in the `time_dimensions` parameter. Limits which time buckets are included.

**Period** — Refers to either **time bucket** of a single row or **date range** of the whole query. Due to this ambiguity, we tend to avoid using this term.

## Expressions

**Transform function** — A function applied to a metric, computing values across time buckets or partitions. Examples: `cumsum` (running total), `time_shift`/`change`/`change_pct` (self-join-based), `lag`/`lead` (window-function-based), and the rank family `rank`/`percent_rank`/`dense_rank`/`ntile` (timeless ranking; optional `partition_by=`).

**Self-join vs window-function transforms:**

- `time_shift`, `change`, and `change_pct` use self-join CTEs — they can reach outside the current result set (no edge NULLs) and handle gaps in data correctly. The join matches on all non-time dimensions as well as the shifted time column, so grouped comparisons are partition-safe and reset per group. `time_shift(revenue, -1, 'year')` (with granularity) joins on calendar date arithmetic for comparisons like year-over-year.
- `lag(revenue, 1)` / `lead(revenue, 1)` use SQL `LAG`/`LEAD` window functions directly — more efficient, but produce NULLs at the edges and are sensitive to gaps in data.

**Nesting** — Expressions can be nested: `change(cumsum(revenue))` applies `change` to the result of `cumsum`. Each level of nesting generates an additional CTE layer in the SQL.

## Ingestion

**Rollup** — During auto-ingestion, SLayer follows foreign key relationships and creates datasets with direct relationships (one per FK on the source table). Fields from transitively reachable tables appear as dotted references (e.g., `customers.name`, `customers.regions.name`) usable as dimensions or in expressions. Multi-hop JOINs are resolved dynamically at query time by walking each intermediate dataset's own relationships.

**Default time dimension** — An optional dataset-level setting (`default_time_dimension`) that specifies which dimension to use for time ordering in transform functions, when no time dimension is explicitly provided in the query.
