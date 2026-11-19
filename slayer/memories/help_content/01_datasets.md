# Datasets

A dataset maps a database table (or SQL subquery) to queryable fields and metrics.
Field-level schemas are in the `create_dataset` / `edit_dataset` tool docs; this page
covers the semantics.

## Source

Exactly one of `sql_table: "public.orders"` (a named table), `sql: "SELECT ..."`
(an inline subquery), or `source_queries` (a query-backed dataset, see below) — the
first two become the FROM clause at query time.

## Fields

Row-level SQL expressions; the role (group-by key or aggregation source) is decided
per query. A list field (`datatype: ARRAY`) groups and compares as the set of its distinct elements and
takes only `count` / `count_distinct`; aggregate its elements through `<field>.value` (e.g. `tags.value`).
A field's `sql` is Mode-A SQL: any dialect expression, dotted relationship paths allowed, but no aggregations or {{product}} transforms. A field `filter` becomes
CASE WHEN inside aggregates — `sum(active_revenue)` counts only matching rows,
without affecting sibling metrics. `allowed_aggregations` whitelists what may
aggregate the field; a sole `primary_key: true` field is restricted to count / count_distinct / count_distinct_approx / min / max / collect.

Advanced escape hatch: a `DatasetField.sql` MAY be a raw window expression
(`row_number() over (...)`); filtering on such a field auto-promotes to a
post-aggregation WHERE. Prefer the built-in rank-family transforms when they cover
the need.

## Saved metrics and custom aggregations

A dataset's `metrics` list is a library of named expressions (same grammar as query
metrics); queries reference them by bare name, and saved expressions may reference
each other (cycles raise). `aggregations` defines custom aggregation functions
(`{value}` placeholder plus params) callable like built-ins:
`trimmed_mean(score, lo=10, hi=90)`.

## Relationships

Declare each edge once — relationships are symmetric: a declared relationship traverses in both
directions with flipped cardinality. Parallel edges between the same pair need a
relationship `name` to disambiguate, or the hop fails closed. When the same dataset is
reachable via two paths (a diamond), each path becomes a separate sub-query alias;
equate them with a dataset filter if the diamond should collapse. Auto-ingestion
creates relationships from foreign keys.

## Dataset filters

`filters` are always-applied Mode-A WHERE conditions (`"deleted_at IS NULL"`,
dotted paths allowed). They never reference metrics or transforms.

## Other settings

`default_time_dimension` (else the only date/time field) names the fallback time
axis for transforms and `first`/`last`, and the dataset's `time_spine` axis. `hidden: true` excludes the dataset from discovery while keeping it
queryable by name.

## Result keys and query-backed datasets

Result columns are `dataset.field`: `sum(revenue)` → `orders.revenue_sum`,
`count(*)` → `orders._count`; joined dimensions keep the full path
(`orders.customers.regions.name`); expression keys are sanitized
(`sum(amount - cost)` → `amount_cost_sum`); `name=` overrides. Saving a query as a
dataset (`create_dataset` with `query=`) makes it query-backed: its stages are stored
SlayerQuery dicts, dotted paths flatten into `__` field names (`stores.name` →
`stores__name`), and the cached fields refresh only when the dataset is saved again.
Run one by name with `query(query="monthly_revenue", refine={"dimensions": ["region"]})` to
merge extra dimensions, metrics, filters, order or limit into its final stage.
