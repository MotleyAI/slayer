# Terminology

Key terms used throughout SLayer documentation and code.

## Data Structure

**Model** — A semantic layer definition that maps a database table (or SQL subquery) to queryable dimensions and measures. Defined as YAML files or auto-generated via ingestion.

**Dimension** — A column used for grouping and filtering. Examples: `status`, `region`, `customer_name`. Dimensions are not aggregated — they appear in GROUP BY clauses.

**Measure** — A model-defined aggregation. Each measure has a name, a SQL expression, and an aggregation type (`count`, `sum`, `avg`, `min`, `max`, `count_distinct`). Examples: `count` (COUNT(*)), `revenue_sum` (SUM(amount)).

**Datasource** — A database connection configuration: host, port, credentials, database type. SLayer supports Postgres, MySQL/MariaDB, ClickHouse, SQLite, BigQuery, and Snowflake.

## Queries

**Field** — A data column returned by a query. Defined by a formula string. A field can be a plain measure reference (`"count"`), arithmetic on measures (`"revenue / count"`), or a transform function (`"cumsum(revenue)"`). Fields support an optional `label` for human-readable display. See [Formulas](formulas.md).

**Label** — An optional human-readable display name for a field, dimension, or time dimension. Separate from the technical `name`, which is used as the result column key. Example: `{"formula": "revenue / count", "name": "aov", "label": "Average Order Value"}`.

**Filter** — A condition that restricts which rows are included. Defined as a formula string: `"status == 'completed'"`, `"amount > 100"`. See [Filter Formulas](formulas.md#filter-formulas).

**Time dimension** — A dimension of type `time` or `date`, used for time-based grouping. When specified in `time_dimensions`, SLayer truncates it to the given granularity (e.g., monthly buckets). The same column can also be used as a regular dimension (without truncation).

**Granularity** — The level of time truncation applied to a time dimension, to determine the size of each time bucket (one row's time span) in the result, or as an argument in time related functions. Available granularities: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`.

**Time bucket** — A single unit of the granularity. If granularity is `month`, each time bucket is one calendar month (e.g., January 2024, February 2024). Each time bucket becomes one row in the query result.

**Date range** — The start and end bounds for a time dimension filter. Specified as `["2024-01-01", "2024-12-31"]` in the `time_dimensions` parameter. Limits which time buckets are included.

**Period** — Refers to either **time bucket** of a single row or **date range** of the whole query. Due to this ambiguity, we tend to avoid using this term.

## Formulas

**Transform function** — A function applied to a measure, computing values across time buckets. Examples: `cumsum` (running total), `time_shift` (previous/next value via self-join), `lag`/`lead` (window-function-based row access), `change` (difference from previous bucket), `rank` (ordering).

**`time_shift` vs `lag`/`lead`:**

- `time_shift(revenue, -1)` uses a self-join CTE to fetch the previous period's value — it can reach outside the current result set (no edge NULLs) and handles gaps correctly.
- `time_shift(revenue, -1, 'year')` (with granularity) joins on calendar date arithmetic for comparisons like year-over-year.
- `lag(revenue, 1)` / `lead(revenue, 1)` use SQL `LAG`/`LEAD` window functions directly — more efficient, but produce NULLs at the edges and are sensitive to gaps in data.

**Nesting** — Formulas can be nested: `change(cumsum(revenue))` applies `change` to the result of `cumsum`. Each level of nesting generates an additional CTE layer in the SQL.

## Ingestion

**Rollup** — During auto-ingestion, SLayer follows foreign key relationships and creates denormalized models with LEFT JOINs baked into the SQL. Columns from joined tables appear as `table__column` dimensions (e.g., `customers__name`).

**Transitive closure** — The set of all tables reachable from a source table via foreign key chains. For `orders → customers → regions`, the transitive closure of `orders` includes both `customers` and `regions`.

**Default time dimension** — An optional model-level setting (`default_time_dimension`) that specifies which dimension to use for time ordering in transform functions, when no time dimension is explicitly provided in the query.
