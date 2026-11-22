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
A field's `expression` is portable SQL (or `{sql, dialects: [{dialect, expression}]}`, the entry for the datasource's dialect running first); a bare name reads the source column and `dataset.field` reads a field, with no aggregations or {{product}} transforms. A field `filter` becomes
CASE WHEN inside aggregates — `sum(orders.active_revenue)` counts only matching rows,
without affecting sibling metrics. `allowed_aggregations` whitelists what may
aggregate the field; a sole `primary_key: true` field is restricted to count / count_distinct / count_distinct_approx / min / max / collect.

Advanced escape hatch: a field's dialect entry MAY be a raw window expression
(`row_number() over (...)`); filtering on such a field auto-promotes to a
post-aggregation WHERE. Prefer the built-in rank-family transforms when they cover
the need.

## Saved metrics and custom aggregations

A dataset's `metrics` list is a library of named expressions (same grammar as query
metrics, read from the dataset itself: a definition names its own fields bare); queries
reference them as `<dataset>.<metric>` (`orders.aov`), and saved expressions may reference
each other (cycles raise). `aggregations` defines custom aggregation functions
(`{value}` placeholder plus params) callable like built-ins:
`trimmed_mean(orders.score, lo=10, hi=90)`.

## Semantic models and model-scoped metrics

A semantic model is an Ossie document of one datasource: its datasets, the relationships
between them and its own top-level metrics. Its datasets query like any other. A
model-scoped metric belongs to the document, not to one dataset, so a query names it bare:
`{"metrics": ["revenue_per_customer"]}`. Its definition qualifies every reference and
reaches a dataset metric only through `AGG(dataset.metric)`:
`AGG(orders.total_revenue) / COUNT(DISTINCT customers.customer_id)` is 650 of revenue over
4 customers, one row of 162.5 without dimensions. Each aggregate in it runs over its own
dataset's rows. A query alias of the same name shadows it. `AGG` takes a single dataset
metric: `AGG` of a field, of a model-scoped metric, of any other expression or with a
`FILTER` clause is refused. Create and change documents with `create_semantic_model` /
`edit_semantic_model`; `inspect(reference="revenue_per_customer", entity_type="metric")`
shows one.

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

Result columns key by the reference as written, never by the population:
`sum(orders.revenue)` → `orders.revenue_sum`, `count(orders.*)` → `orders._count`,
`customers.regions.name` keeps its path; expression keys are sanitized
(`sum(orders.amount - orders.cost)` → `orders.amount_cost_sum`); an alias keys as itself. Saving a query as a
dataset (`create_dataset` with `query=`) makes it query-backed: its stages are stored
SlayerQuery dicts, dotted paths flatten into `__` field names (`stores.name` →
`stores__name`), and the cached fields refresh only when the dataset is saved again.
Run one by name with `query(query="monthly_revenue", refine={"dimensions": ["orders.status"]})` to
merge extra dimensions, metrics, filters, order or limit into its final stage.
