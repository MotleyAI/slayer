# Models

A model maps a database table (or SQL subquery) to queryable columns and measures.
Field-level schemas are in the `create_model` / `edit_model` tool docs; this page
covers the semantics.

## Source

Exactly one of `sql_table: "public.orders"` (a named table) or `sql: "SELECT ..."`
(an inline subquery) — either becomes the FROM clause at query time.

## Columns

Row-level SQL expressions; the role (group-by key or aggregation source) is decided
per query. A column's `sql` is Mode-A SQL: any dialect expression, dotted join paths
allowed, but no aggregations or {{product}} transforms. A column `filter` becomes
CASE WHEN inside aggregates — `sum(active_revenue)` counts only matching rows,
without affecting sibling measures. `allowed_aggregations` whitelists what may
aggregate the column; `primary_key: true` restricts it to count / count_distinct.

Advanced escape hatch: a `Column.sql` MAY be a raw window expression
(`row_number() over (...)`); filtering on such a column auto-promotes to a
post-aggregation WHERE. Prefer the built-in rank-family transforms when they cover
the need.

## Saved measures and custom aggregations

A model's `measures` list is a library of named formulas (same grammar as query
measures); queries reference them by bare name, and saved formulas may reference
each other (cycles raise). `aggregations` defines custom aggregation functions
(`{value}` placeholder plus params) callable like built-ins:
`trimmed_mean(score, lo=10, hi=90)`.

## Joins

Declare each edge once — joins are symmetric: a declared join traverses in both
directions with flipped cardinality. Parallel edges between the same pair need a
join `name` to disambiguate, or the hop fails closed. When the same model is
reachable via two paths (a diamond), each path becomes a separate sub-query alias;
equate them with a model filter if the diamond should collapse. Auto-ingestion
creates joins from foreign keys.

## Model filters

`filters` are always-applied Mode-A WHERE conditions (`"deleted_at IS NULL"`,
dotted paths allowed). They never reference measures or transforms.

## Other fields

`default_time_dimension` names the fallback time axis for transforms and
`first`/`last`. `hidden: true` excludes the model from discovery while keeping it
queryable by name.

## Result keys and query-backed models

Result columns are `model.column`: `sum(revenue)` → `orders.revenue_sum`,
`count(*)` → `orders._count`; joined dimensions keep the full path
(`orders.customers.regions.name`); expression keys are sanitized
(`sum(amount - cost)` → `amount_cost_sum`); `name=` overrides. Saving a query as a
model (`create_model` with `query=`) makes it query-backed: its stages are stored
SlayerQuery dicts, dotted paths flatten into `__` column names (`stores.name` →
`stores__name`), and the cached columns refresh only when the model is saved again.
