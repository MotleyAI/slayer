# Terminology

Key terms used throughout SLayer documentation and code.

## Data Structure

**Model** — A semantic layer definition that maps a database table, SQL subquery, or saved query to queryable columns. Defined as YAML files or auto-generated via ingestion.

**Column** — The unified row-level building block of a model (`SlayerModel.columns`). Each column has a `name`, `sql` expression, and `type` (`string`/`number`/`boolean`/`time`/`date`). At query time a column can be used as a group-by key (a dimension), as the input to an aggregation (a measure), or both. Columns can also carry `primary_key`, `allowed_aggregations` (whitelist), `filter` (CASE-WHEN at aggregation time), `format`, `label`, and `meta`.

**Dimension** — How a column is *used* in a query when it's a GROUP BY key. The column itself isn't a dimension or measure intrinsically — that role is decided per query. In SLayer's query DSL, the `dimensions` list names the columns to group/filter by.

**Measure (in a query)** — A formula entry in `SlayerQuery.measures`. Examples: `"revenue:sum"`, `"*:count"`, `{"formula": "revenue:sum / *:count", "name": "aov"}`, `"cumsum(revenue:sum)"`. Each entry compiles to one output column.

**Measure (named formula)** — A saved formula stored on a model (`SlayerModel.measures: List[ModelMeasure]`). Shape `{formula, name, label, description}` — same as a query's inline `measures` entry. Queries reference saved measures by bare name in any formula context (`{"formula": "aov"}`).

**Aggregation** — How a column is rolled up. Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`. Custom aggregations can be defined at model level. Applied at query time via colon syntax: `revenue:sum`, `*:count`, `price:weighted_avg(weight=quantity)`, `price:corr(other=quantity)`.

**Join** — A LEFT JOIN relationship between two models. Defined by a target model name and join key pairs (from the model's own foreign keys). Each model only stores direct joins — multi-hop paths like `customers.regions.name` are resolved at query time by walking each intermediate model's own joins.

**Cross-model measure** — An aggregation over a joined model's column, referenced with dotted syntax and colon aggregation (`customers.score:avg`, or multi-hop: `customers.regions.population:sum`). Computed as a sub-query to avoid row multiplication. Transforms work on cross-model measures: `cumsum(customers.score:avg)`.

**ModelExtension** — Extends a model inline on a query with extra `columns`, `measures` (named formulas), `joins`, or `filters` — without modifying the stored model. Used for ad-hoc expression columns, derived buckets, or one-off joins.

**Model filter** — A WHERE filter defined on a model, always applied to every query on that model (e.g., `"deleted_at IS NULL"`).

**Query-as-model** — Using a query's result as the source for another query, or saving it as a permanent model. Useful for materializing complex aggregations.

**Datasource** — A database connection configuration: host, port, credentials, database type. SLayer supports Postgres, MySQL/MariaDB, ClickHouse, SQLite, BigQuery, and Snowflake.

## Queries

**Measure entry** — A formula entry in `SlayerQuery.measures` (called *Field* in v1, before the v2 schema rename). Defined by a formula string. Examples: aggregated column reference (`"revenue:sum"`), `*`-count (`"*:count"`), arithmetic on aggregated measures (`"revenue:sum / *:count"`), transform function (`"cumsum(revenue:sum)"`), or bare named-formula reference (`{"formula": "aov"}`). Supports an optional `label` for human-readable display. See [Formulas](formulas.md).

**Label** — An optional human-readable display name for a measure entry, dimension, or time dimension. Separate from the technical `name`, which is used as the result column key. Example: `{"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"}`.

**Filter** — A condition that restricts which rows are included. Defined as a formula string: `"status = 'completed'"`, `"amount > 100"`. See [Filter Formulas](formulas.md#filter-formulas).

**Time dimension** — A dimension of type `time` or `date`, used for time-based grouping. When specified in `time_dimensions`, SLayer truncates it to the given granularity (e.g., monthly buckets). The same column can also be used as a regular dimension (without truncation).

**Granularity** — The level of time truncation applied to a time dimension, to determine the size of each time bucket (one row's time span) in the result, or as an argument in time related functions. Available granularities: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`.

**Time bucket** — A single unit of the granularity. If granularity is `month`, each time bucket is one calendar month (e.g., January 2024, February 2024). Each time bucket becomes one row in the query result.

**Date range** — The start and end bounds for a time dimension filter. Specified as `["2024-01-01", "2024-12-31"]` in the `time_dimensions` parameter. Limits which time buckets are included.

**Period** — Refers to either **time bucket** of a single row or **date range** of the whole query. Due to this ambiguity, we tend to avoid using this term.

## Formulas

**Transform function** — A function applied to a measure, computing values across time buckets. Examples: `cumsum` (running total), `time_shift`/`change`/`change_pct` (self-join-based), `lag`/`lead` (window-function-based), `rank` (ordering).

**Self-join vs window-function transforms:**

- `time_shift`, `change`, and `change_pct` use self-join CTEs — they can reach outside the current result set (no edge NULLs) and handle gaps in data correctly. `time_shift(revenue, -1, 'year')` (with granularity) joins on calendar date arithmetic for comparisons like year-over-year.
- `lag(revenue, 1)` / `lead(revenue, 1)` use SQL `LAG`/`LEAD` window functions directly — more efficient, but produce NULLs at the edges and are sensitive to gaps in data.

**Nesting** — Formulas can be nested: `change(cumsum(revenue))` applies `change` to the result of `cumsum`. Each level of nesting generates an additional CTE layer in the SQL.

## Ingestion

**Rollup** — During auto-ingestion, SLayer follows foreign key relationships and creates models with direct joins (one per FK on the source table). Columns from transitively reachable tables appear as dotted references (e.g., `customers.name`, `customers.regions.name`) usable as dimensions or in formulas. Multi-hop JOINs are resolved dynamically at query time by walking each intermediate model's own joins.

**Transitive closure** — The set of all tables reachable from a source table via foreign key chains. For `orders → customers → regions`, the transitive closure of `orders` includes both `customers` and `regions`. Used during ingestion for FK graph analysis and column introspection (determining which dotted dimensions to create), but not baked into model joins — each model only stores direct joins from its own FKs.

**Default time dimension** — An optional model-level setting (`default_time_dimension`) that specifies which dimension to use for time ordering in transform functions, when no time dimension is explicitly provided in the query.
