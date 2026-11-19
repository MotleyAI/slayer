# Queries

A `SlayerQuery` specifies what data to retrieve from a dataset.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | No | Name for this query — used to reference it from other queries in a list |
| `population` | string | No | The population (query root): a stored dataset or stage name. Omit it to infer the population — see [Population](#population). |
| `extensions` | list[DatasetExtension] | No | Query-scoped metrics and relationships added to any dataset the query reads — see [Query extensions](#query-extensions). |
| `metrics` | list[str \| QueryMetric] | No | Computed/aggregated values — expressions, arithmetic, transforms. Aggregations are function calls (`sum(revenue)`, incl. expressions like `sum(amount - cost)`) — see [Expressions](formulas.md) and [Aggregation syntax](references.md#aggregation-syntax). |
| `dimensions` | list[str \| QueryDimension] | No | Fields to group by — bare strings (`"status"`), dotted names for joined datasets (`customers.name`), row-level expressions, or `{"field": …, "alias": …, "label": …}` for a result name or label (a [computed expression](#expression-dimensions)). |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `main_time_dimension` | string | No | Explicit time dimension name for transforms (overrides auto-detection) |
| `filter` | str \| list[str] | No | A condition, or a list of conditions all of which must hold. Supports `{variable}` placeholders. See [Filters](#filters). |
| `variables` | dict[str, Any] | No | Values for `{variable}` placeholders. See [Variables](#variables). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `whole_periods_only` | bool | No | Snap time bounds to bucket boundaries and exclude the current incomplete bucket — see [Time](time.md#whole_periods_only) |
| `to_many_handling` | str | No | How an aggregate resolves query dimensions [unattributable from its root](#cross-dataset-metrics). Omitted (the default): per-cell value over the distinct associated rows, falling back to broadcast with a warning where association is unsupported; `associate` (the same, refusing what it cannot associate), `broadcast` (repeat the safe-grain value across the cells and warn), or `error` (refuse). Semi-join-pushed filters are always applied and never error. |

You can pass a single query or a **list of queries** to `execute()`. When passing a list, earlier queries are named sub-queries that later queries can reference. The last query in the list is the main one whose results are returned. See [Query Lists](#query-lists) for examples.

A query carries no tenant scoping of its own. To force every query through an engine to one tenant's rows — joins and sub-queries included — configure a policy at engine construction; see [Row-Level Security](row-level-security.md).

## Dimensions

Each entry in `dimensions` is either a string (a field reference or an expression) or an object with `field` and an optional `alias` and `label`. Both styles support dotted paths for joined datasets, auto-resolved via the relationship graph. An entry without `alias` or `label` serializes as its string.

```json
"dimensions": [
  "status",
  "customers.name",
  "customers.regions.name",
  {"field": "status", "label": "Order Status"}
]
```

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | A field reference (dotted paths for joined datasets) or a row-level expression. A double-quoted segment (`"\"Revenue\""`, `orders."status"`) matches its declared name exactly. |
| `alias` | string | Optional result key |
| `label` | string | Optional human-readable display name |

### Expression dimensions

To group by a computed expression (rather than a bare field), give its text as
`field`, with an optional `alias`:

```json
"dimensions": [
  "region",
  {"field": "lower(city)", "alias": "city_lc"},
  {"field": "CASE WHEN amount > 2000 THEN 'big' ELSE 'small' END", "alias": "size"}
]
```

The expression is a [Mode-B expression](references.md) (scalar functions,
arithmetic, `CASE` / `iif`) over the dataset's fields; it is projected **and**
grouped. A bare-string entry that is not a valid identifier / dotted path is
also parsed as an expression (`"round(amount)"`), auto-named from its text —
alias it to control the result key (`dataset.<alias>`). An alias that collides with an
existing field or metric is rejected. The alias is resolvable from `filter`
and `order`. The alias keys the result even when the field is a bare joined
path: `{"field": "customers.region_id", "alias": "region_id"}` returns
`orders.region_id`, and a downstream stage reads it as `region_id`.

#### Grouping by an expression over an aggregate

A computed dimension may also group by a value derived from an aggregate at a
**finer grain** than the query — e.g. band cities by whether each city's total
crosses a threshold, then group by `(region, band)`:

```json
{
  "population": "orders",
  "dimensions": [
    "region",
    {"field": "CASE WHEN sum(amount, partition_by=city) > 5000 THEN 1 ELSE 0 END", "alias": "band"}
  ],
  "metrics": [{"metric": "sum(amount)", "alias": "band_total"}]
}
```

The aggregate inside the expression **must** carry
[`partition_by=`](formulas.md#aggregate-at-a-coarser-grain-partition_by), which
names the grain it aggregates over (here, `city`). SLayer computes that
aggregate in a synthesized internal stage, attaches it to the raw rows on the
partition grain, evaluates the expression per row, then regroups by the query's
dimensions — so every metric still aggregates the raw rows once (non-additive
metrics like `avg` / `count_distinct` reconcile correctly, and a `NULL`
partition value keeps its own group). The `partition_by` grain may be any
groupable key: a query dimension, a finer local field (`city`), a joined path
(`customers.region_id`), a time bucket, or `[]` for the grand total. A base-row
filter (`status = 'ok'`) also constrains the partition aggregate; a filter on
the computed dimension name (`band == 1`) applies after regrouping.

A dimension expression may band a windowed partitioned aggregate
(`sum(amount, window='90d', partition_by=region)`), a `first` / `last`, or a
transform over a grained aggregate — `rank(sum(revenue, partition_by=region), direction='desc')` as
a DIMENSION ranks the partitions (it evaluates at the producer grain), whereas
the same expression as a METRIC ranks the result rows (query grain).

An aggregation-derived dimension combines with transform metrics: alongside
`band` you can declare `time_shift(sum(amount), -1)`, `change` / `change_pct`,
`cumsum`, `lag` / `lead`, `consecutive_periods(...)`, or `rank(sum(amount), direction='desc')`,
with or without plain and `partition_by=` metrics in the same query. Every
transform treats the computed dimension as an ordinary grouping dimension (a
running total accumulates within each `(region, band)` group; a time shift
compares each group only against itself).

A transform over aggregates at **different** partition grains unions the grains
and broadcasts each aggregate to the union —
`rank(sum(amount, partition_by=region) - sum(amount, partition_by=city), direction='desc')` ranks
the `(region, city)` rows, each region
total broadcast across its cities and each city total against its region. The
same holds as a bare metric (`sum(a, partition_by=region) - sum(b, partition_by=city)`
evaluated at the query grain) and recursively for a nested transform, which
accumulates within its **own** grain before broadcasting into the outer union.

A cross-dataset aggregate source inside a dimension expression is legal: it
compiles through the same target-rooted producer as a
[cross-dataset metric](#cross-dataset-metrics), with the same exact-grain vs
broadcast semantics.

An aggregate partitioned by another computed dimension — even one carrying an
attached aggregate — compiles as a nested producer
([re-aggregation](formulas.md#re-aggregation-aggregate-over-an-attached-value)).

Ill-typed shapes (raise a clear error naming the remedy, by design): a bare
aggregate without `partition_by=` — the ungrained default would include the
dimension being defined — and a time-ordered transform (`cumsum`, `lag`, …)
whose evaluation grain lacks its time-ordering key — include that key in
`partition_by=` so the transform accumulates within its own grain.

### Dim-only queries deduplicate

A query with no metrics and at least one dimension or time-dimension returns the **distinct combinations** of those dimensions, not the raw underlying-row stream. SLayer emits `GROUP BY` over all dim/time-dim aliases before applying `LIMIT`, so a row cap can't silently drop unique tuples that only appear past `limit` rows.

```json
{"population": "orders", "dimensions": ["status"], "limit": 100}
```

Emits `SELECT orders.status FROM orders GROUP BY orders.status ORDER BY orders.status LIMIT 100`.

### Raw rows (`distinct_dimension_values`)

Set `distinct_dimension_values: false` on a query to opt out of the auto-dedup and project raw rows instead. No top-level `GROUP BY`; the usual `WHERE` / `ORDER BY` / `LIMIT` / `OFFSET` still apply.

```json
{
  "population": "orders",
  "dimensions": ["status", "amount"],
  "filter": ["amount > 100"],
  "order": [{"field": "amount", "direction": "desc"}],
  "limit": 100,
  "distinct_dimension_values": false
}
```

Emits roughly `SELECT orders.status, orders.amount FROM orders WHERE orders.amount > 100 ORDER BY orders.amount DESC LIMIT 100` — one row per source row.

**Rules** when `distinct_dimension_values=False`:

- `metrics` must be empty — `DistinctDimensionValuesError` otherwise.
- At least one of `dimensions` / `time_dimensions` must be non-empty (nothing to project otherwise).
- Filters / order items must not reference any metric — neither an aggregation (`sum(amount) > 100`, `count(*) > 0`), transform calls (`rank(sum(amount), direction='desc') <= 5`), nor a bare saved-`DatasetMetric` name.

**Time dimensions** are allowed: each one emits its `DATE_TRUNC` truncation as a projected column without aggregating. For raw field values (no truncation), put the time field in `dimensions` instead.

**Multi-stage**: the flag is per-stage in DAG queries — an inner stage with `false` produces a flat raw-row sub-query that the outer stage can aggregate over.

## TimeDimension

A time dimension with a required granularity and an optional date range. Supports an optional `label` for human-readable output. To use a time field without truncation, add it as a regular dimension instead. `field` is text; an object there is refused.

`field` may be any row-level expression typed DATE or TIMESTAMP (`"coalesce(shipped_at, created_at)"`, `"CAST(created_txt AS DATE)"`, also as `date_trunc('month', coalesce(shipped_at, created_at))`), and an optional `alias` sets its result key and lets `main_time_dimension` and `partition_by=` refer to it (an unaliased expression is named like a computed dimension). Names are unique across a query's dimensions and time dimensions. An aliased time dimension's key never carries a granularity suffix; unaliased time dimensions sharing one source at different granularities get one each (`orders.created_at.month`, `orders.created_at.year`).

```json
{
  "field": "created_at",
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"],
  "label": "Order Month"
}
```

**Granularities**: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`

A `dimensions` or `time_dimensions` entry may also be written as the string `date_trunc('<unit>', expr)` (e.g. `"date_trunc('month', created_at)"`) — equivalent to a `TimeDimension` with that field and granularity (use the dict form above to add `date_range` or `label`), and usable as an `order` key too, where it must match a projected time dimension on the same expression at that granularity or the query fails. A `time_dimensions` string in any other form is an error naming its `date_trunc` spelling.

A downstream stage of a multi-stage query may declare a time dimension on any DATE/TIMESTAMP column of its upstream stage — re-bucketing a column the upstream already truncated only at the same or a nesting-coarser granularity (e.g. `month` → `year`). The same rule applies to a dataset field that carries a `granularity` (a query-backed dataset's bucketed cache field, or a hand-set one): a finer or non-nesting time dimension over it is the same typed error, judged for every bucketed field an expression time dimension reads (`CAST(month_col AS DATE)` at `day` fails).

`week` is Monday-anchored (ISO-8601); `week_sunday` is Sunday-anchored (weeks start Sunday, end Saturday) for tools that use Sunday weeks. Both are dataset granularities you set on a `TimeDimension` — `week_sunday` is the SLayer value, not a wire keyword sent by a BI tool.

`date_range` is one time point (`"2025-Q1"`, `"last month"`) or a `[lower, upper]` pair with either bound `null`, read with the [time-point semantics](time.md#date_range) — a date-only bound there covers its whole day.

## OrderItem

A sort specification: `field` is text naming a dimension (`status`, `customers.region`), a declared metric's short alias (`revenue_sum`), or an expression (`count(*)`); `direction` is `asc` or `desc`.
An optional `nulls` (`first` or `last`) places NULL values first or last on every dialect; without it the database's default placement applies.

```json
{"field": "count(*)", "direction": "desc"}
```

Via MCP: `{"field": "count(*)", "direction": "desc"}`

A query (or the final stage of a query list) with no `order` that projects dimensions is ordered by its dimensions and time dimensions ascending, in result-column order, exactly as if it listed them in `order`; raw-row queries and inner stages are left unordered.

### Ordering by something you don't project

`order` may reference a field or aggregate that is **not** declared as a dimension/metric — the classic "top-N by metric X, display only Y, Z" pattern:

```json
{"population": "orders", "dimensions": ["status"], "metrics": ["count(*)"],
 "order": [{"field": "sum(amount)", "direction": "desc"}], "limit": 10}
```

The `sum(amount)` aggregate is computed as a hidden column, sorted on, and **stripped from the result** — the response projects only `status` and `_count`. This works for local aggregates, cross-dataset aggregates (`sum(customers.revenue)`), and inner-stage columns re-aggregated in a later DAG stage (`max(customers__revenue_sum)`).

What each shape of an *undeclared* order target does:

| Order target | Behavior |
| --- | --- |
| An aggregate (`sum(amount)`, `sum(customers.revenue)`) | Computed hidden, sorted on, stripped from the result. Always allowed. |
| An inline **transform** (`rank(sum(amount), direction='desc')`, `cumsum(...)`, `change(...)`, `lag`/`lead`/`ntile`) | Computed hidden, sorted on, stripped. |
| An inline **composite** (`sum(revenue) / sum(cnt)`, `abs(sum(amount))`, `change(sum(amount)) / 2`) | Computed hidden, sorted on, stripped. |
| A **windowed** aggregate (`sum(amount, window='90d')`), alone or inside a composite | Computed hidden in its own rolling-window CTE, sorted on, stripped. |
| A raw row field, in a **raw-rows** query (`distinct_dimension_values: false`, no metrics) | Sorted on directly (`ORDER BY orders.created_at`). Applies to a **joined** field (`customers.regions.name`) and to a derived field whose `sql` reaches through a relationship — the join is pulled in for the sort. |
| A raw row field or row-level expression (`amount - fee`), in an **aggregated / dedup** query | Sorted on **per group**: ASC by each group's minimum, DESC by each group's maximum. The wrap is implicit — you do not write `min(created_at)`. |
| A **joined** row field (`customers.regions.name`) in an aggregated query | Same per-group wrap, computed in a CTE rooted at the population with the join pulled inside, so each group gets its own extreme rather than one global value. |

Ordering by an undeclared row field in a grouped query is **not** the same as
ordering by the field itself — there is no single value per group to sort by.
SLayer picks the extreme the direction puts first: `asc` sorts each group by its
`min`, `desc` by its `max`. Write `{"field": "max(created_at)", "direction": "asc"}`
explicitly if you want the other one.

NULLs sort **last** in both directions, on every database, so the same query
returns the same row order regardless of backend. (SQL Server is the one
exception: its native ordering is used because the portable emulation makes the
statement fail there.)
An explicit `nulls` (`first` or `last`) places NULLs there on every database, SQL Server included.

An order target that names nothing SLayer can resolve is an error, never a
silently unsorted result.

Transform and composite order targets accept the full expression syntax, so
`{"field": "sum(revenue) / sum(cnt)"}` and `{"field": "change(sum(revenue))"}` both
work without declaring a metric, and their operands may also be the names of metrics declared in the same query (`{"field": "rev / cnt"}`), as in filters.

Windowed metrics work in declared composites and transforms too — see [expressions](formulas.md#windowed-aggregations).

## Response

Query results are returned as a `SlayerResponse`:

| Field | Type | Description |
|-------|------|-------------|
| `data` | list[dict] | Rows as dictionaries |
| `columns` | list[str] | Column names in `dataset_name.field_name` format (e.g., `"orders._count"`, `"orders.customers.regions.name"` for multi-hop), **in the order you declared them** |
| `row_count` | int | Number of rows |
| `sql` | string | The generated SQL (useful for debugging) |
| `attributes` | ResponseAttributes | Field metadata split by type: `attributes.dimensions` and `attributes.metrics`, each a dict of column alias → FieldMetadata (label, format) |
| `warnings` | list[SlayerWarning] | Advisories, discriminated by `kind`: input normalizations (`"normalization"`), a [cross-dataset metric broadcast](#cross-dataset-metrics) (`"broadcast"` — `metric`, `location`, per-dimension `dimensions[].reason`, and `association_unsupported` when the default fell back to it), a distinct-entity attribution over an unattributable dimension (`"associated"` — `metric`, `location`, `dimensions`; cells overlap and are not additive), an aggregate over a dataset no relationship path reaches (`"unrelated_dataset"` — `dataset`, `population`, `metrics`, `location`), a semi-join-pushed filter (`"semi_join_pushed"` — `metric` (`null` for a population-level push), `location`, `filter_text`), a query run without its statement timeout (`"statement_timeout_skipped"` — `datasource`, `timeout_seconds`, `reason`: `readonly_user` or `timeout_rejected`), [`whole_periods_only`](time.md#whole_periods_only) over non-nesting granularities (`"whole_periods_non_nesting"` — `field`, `granularities`, `location`), a [list](#list-fields) read as its element set (`"whole_list"` — `operation`: `group` or `count_distinct`, `list`), and a deprecated input form that was converted (`"deprecated_input"` — `old`, `new`, `removal`; see [Upgrading to 2.0](../upgrading/2.0.md)) |

`columns` — and the key order of each row in `data` — follows the order you
declared fields in the query: dimensions, then time dimensions, then metrics,
each in the order given. This holds regardless of how a metric is computed, so
a metric on a joined dataset appears where you declared it rather than after the
local ones. Fields used only for ordering are computed but not returned.

```json
{
  "data": [
    {"orders.status": "completed", "orders._count": 42},
    {"orders.status": "pending", "orders._count": 15}
  ],
  "columns": ["orders.status", "orders._count"],
  "row_count": 2,
  "sql": "SELECT ..."
}
```

Result keys are always this canonical dotted form, on every backend. Where a
database's own rules force a different spelling in the emitted SQL — BigQuery
and SQL Server reject dotted column aliases, and most engines cap identifier
length — the alias is rewritten on the way out and restored on the way back, so
`data` and `columns` do not vary by dialect. Only `sql` shows the rewritten
form, since that is what actually ran. See
[Database support](../database-support.md#identifier-length-limits).

An ill-typed query fails with a `QueryTypeError` subclass (a `ValueError`, e.g. `PartitionKeyError`) whose text is a `<ClassName>: <summary>` first line, then an optional `  at <subject>` line naming the offending metric, filter, dimension or transform, then an optional `  suggestion: <remedy>` line.

---

## Filters

Filter expressions define conditions for the query. `filter` is one condition string, or a list of them, all of which must hold; it is serialized in the shape written, and omitted, `null` and `[]` all mean no filter:

```json
{"filter": "status = 'active'"}
{"filter": ["status = 'active'", "amount > 100"]}
```

### Comparison Operators

| Operator | Example |
|----------|---------|
| `=` | `"status = 'active'"` |
| `<>` | `"status <> 'cancelled'"` |
| `>` | `"amount > 100"` |
| `>=` | `"amount >= 100"` |
| `<` | `"amount < 1000"` |
| `<=` | `"amount <= 1000"` |
| `in` | `"status in ('active', 'pending')"` |
| `not in` | `"status not in ('cancelled', 'expired')"` |
| `IS NULL` | `"discount IS NULL"` |
| `IS NOT NULL` | `"discount IS NOT NULL"` |
| `like` | `"name like '%acme%'"` |
| `not like` | `"name not like '%test%'"` |
| `ilike` | `"name ilike '%acme%'"` |
| `between` | `"amount between 100 and 1000"` |
| `is distinct from` | `"discount is distinct from 0"` (NULL-safe `<>`) |
| `is not distinct from` | `"discount is not distinct from 0"` (NULL-safe `=`) |
| `IS TRUE` / `IS FALSE` | `"is_paid IS TRUE"` (NULL is neither) |
| `IS NOT TRUE` / `IS NOT FALSE` | `"is_paid IS NOT TRUE"` (keeps false and NULL) |

The right-hand side of `in` / `not in` must be a non-empty tuple of literal
values (strings, numbers, or booleans) — references and expressions on the
RHS are not supported. Both `(...)` and `[...]` syntax are accepted.

A string compared with a DATE / TIMESTAMP expression is a [time point](time.md#time-points):
`"created_at >= 'last month'"`, `"created_at in '2025-Q1'"` and `"date_trunc('month', created_at) = '2025-03'"`
all work; a date-only string is its midnight (`created_at in '2025-03-01'` is the whole day).

### Boolean Logic

Use `and`, `or`, `not` within a single filter string:

```json
"filter": [
    "status = 'completed' or status = 'pending'",
    "amount > 100 and amount < 1000"
]
```

Multiple entries in the `filter` list are combined with AND.

### Field vs metric filters

Each filter conjunct (a top-level `and` splits) types as either a **field**
(aggregate-free — masks rows before aggregation, so every metric sees only
passing rows) or a **metric** (any expression legal as a metric in the same
query, including partitioned, windowed, and cross-dataset aggregates — evaluated
at query grain, it prunes result rows without changing surviving values). An
expression valid as both (a plain dimension) is a field; one valid as neither
— e.g. an `or` mixing an aggregate with a row field that isn't a query
dimension — fails with an error naming both failed typings. The same typing
applies to `order` targets, so anything you can compute as a metric you can filter or sort
by.

### Scalar Functions in Filters

Filters in `SlayerQuery.filter` accept the
[portable function library](references.md#the-portable-function-library) — strings
(`lower`, `left`, `split_part`, `concat_ws`, …), math (`round`, `div`, `atan2`, …), null
handling, `greatest` / `least`, regex (`regexp_like`, `x ~ 'p'`), JSON (`json_value`,
`payload ->> 'key'`), the [list functions](#list-fields) `contains`, `contains_any`,
`contains_all` and `length`, and [date functions](references.md#date-and-time-functions) — each with
one meaning on every database that translates it. The SQL `||` concat operator is rewritten to `concat(...)`
automatically.

```json
"filter": [
  "lower(status) = 'active'",
  "trim(name) = 'Smith'",
  "replace(category, ',', '') = 'books'",
  "substr(s, 1, instr(s, ',') - 1) = 'first_token'",
  "coalesce(nickname, name) = 'Ada'",
  "first || ' ' || last = 'jane doe'",
  "regexp_like(sku, '^[A-Z]{3}-[0-9]+$')",
  "json_value(payload, '$.plan') = 'pro'"
]
```

Names are matched case-insensitively — `LOWER(...)` and `lower(...)` both bind — and raw SQL
functions outside the library (`initcap`, `to_date`, …) belong in `DatasetField.sql` /
`DatasetField.filter` / `SlayerDataset.filters` (Mode A SQL).

### Filtering on Computed Metrics

Filters can reference names of computed metrics — transforms and arithmetic expressions defined in `metrics`. These are applied as post-filters on the outer query, after all transforms are computed. In a query with any transform, a plain aggregate filter such as `sum(amount) > 200` is applied there too, so it never changes the series a transform reads.

When a query metric is renamed via `{"metric": "agg(col)", "alias": "alias"}`, the filter in the same node may reference EITHER form — the raw aggregation expression `agg(col)` OR the user alias `alias`. Both resolve to the user alias, and an aggregation filter is classified as HAVING on the underlying aggregate. Renaming never changes the legal filter form. Two enrichment-time validations apply: (1) a query metric `alias` that collides with a source field on the population is rejected (alias-form filters would otherwise silently bind to the source field); (2) a rename whose canonical alias literally shadows a source field on the same dataset is also rejected (the aggregation filter would otherwise be ambiguous).

Renaming also works for *cross-dataset* aggregated metrics (`{"metric": "sum(customers.revenue)", "alias": "cust_rev"}`). The result-column key is `<dataset>.<alias>` (`orders.cust_rev`), however many hops the metric crosses. In any *downstream* stage of a multi-stage query list, the column is exposed under the BARE user name — type `max(cust_rev)` (or `max(region_pop)`) in stage 2 to consume the value, not the dotted hop-path form. Filters referencing a renamed cross-dataset metric in the SAME stage resolve in both forms — the bare user alias (`filter=["cust_rev > 100"]`) and the raw aggregation form (`"sum(customers.revenue) > 100"`) — and restrict the result rows on the attached value, as does ORDER BY via the bare user alias (`order=[{"field": "cust_rev"}]`).

```json
{
  "metrics": [
    "sum(revenue)",
    {"metric": "change(sum(revenue))", "alias": "rev_change"}
  ],
  "filter": ["rev_change < 0"]
}
```

Transform expressions can also be used **directly in filters** without defining them as fields first:

```json
{
  "filter": ["last(change(sum(revenue))) < 0"]
}
```

Post-filters can be combined with regular filters — base filters (on dimensions/metrics) are applied in the inner query, post-filters on the outer wrapper:

```json
{
  "filter": ["status = 'completed'", "change(sum(revenue)) > 0"]
}
```

### Filters and Auto-Joins

Filters can reference fields from joined datasets, and the planner adds the implied joins automatically — no need to also list the field in `dimensions`. This works for three reference shapes:

- Dotted refs: `"customers.region = 'EU'"` — direct relationship target.
- Multi-hop dotted refs: `"customers.regions.name = 'US'"` — every prefix on the path is added.
- Bare-named local derived fields whose own SQL crosses a relationship: e.g. a field `{"name": "is_eu", "sql": "CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END"}` referenced as `"filter": ["is_eu = 1"]`. The planner walks the field's `sql` (recursively, through any local derived-field chain) to find the cross-table aliases and adds the corresponding joins.

The same auto-join logic applies to dataset-level `filters` (always-applied WHERE) and to field-level `filter=` attributes (a `CASE WHEN` value mask that fires in every position).

A query filter that reaches the population root only across a fanning (not provably to-one) hop restricts the population *by association* — a correlated `EXISTS` that counts each population row once, surfaced as a `semi_join_pushed` warning rather than multiplying rows through the join.

A negated condition on a joined dataset does not mean "has none": `not orders.status = 'bad'` keeps a customer with at least one non-bad order, while a customer with no orders fails it (the comparison is NULL) — include those with `or orders.id is null`.

### List fields

A list field `skills` (datatype `ARRAY`, see [Data types](models.md#data-types)) is also a relationship hop to its elements: one row per non-NULL element, with the single field `skills.value`. So `skills.value = 'go'` keeps the contractors having a `go` element, `skills.value` groups by element with `count(skills.*)` counting occurrences, and `count(*)` by `skills.value` is the number of contractors per skill (no key needed). Every `skills.value` condition in one query reads the same element, so require several with `contains_all(skills, ['go', 'sql'])`, not `AND`; absence is `NOT contains(skills, 'go')`, since `NOT (skills.value = 'go')` keeps rows with some other element.

```json
{"population": "contractors", "dimensions": ["skills.value"], "metrics": ["count(*)"]}
```

With `to_many_handling: "broadcast"`, a parent aggregate sliced by `skills.value` repeats its total on every element with a `broadcast` warning instead.

A whole list groups (a dimension or `partition_by=`), compares (`=`, `<>`, `IS [NOT] DISTINCT FROM`, `skills IN (['go'], ['sql'])`) and counts distinctly (`count_distinct(skills)`) as its element set, its distinct non-NULL elements in ascending order (`['go', 'sql', 'go']` and `['sql', 'go']` are one group, shown as `['go', 'sql']`, and `[NULL]` equals `[]`), with a `whole_list` warning naming `skills.value` for grouping and distinct counting, while a raw-rows query shows each list as stored.

What a list can't do, each a typed error naming the remedy:

- be compared with a scalar (use `contains` / `contains_any`, or compare `skills.value`), or ordered (`<`, `>`, `BETWEEN`, an `order` key); `IS NULL` / `IS NOT NULL` work;
- take an aggregation that [Aggregation eligibility](models.md#aggregation-eligibility) does not allow a list, `collect` and custom aggregations included;
- hold a NULL element in a list literal (`['go', NULL]`);
- have `Opaque` elements and still be grouped, compared, counted distinctly, measured by `length` or read element by element: such a list supports only NULL tests and `count`.

A text value against a numeric list is converted like any mixed-type comparison, so `contains(scores, '5')` tests for the element `5`.

Element access composes like any relationship hop: through a to-one relationship (`contractors.skills.value` from a dataset joined to `contractors`), on a derived list field, and on a list column of a query stage, including a stage's `collect` result. One query may aggregate the parent's rows (`count(*)`) and the elements (`count(skills.value)`), each over its own rows. Saving a dataset whose list field has the name of a hop reachable from it (an edge or joined-dataset name) fails.

`inspect`, `search` and `datasets_summary` show a list field's datatype as `Array<String>` with its operations, and its element samples once profiled.

`collect(x)` goes the other way: the distinct non-NULL values of `x` in each cell, sorted, and `[]` for a cell with none. `x` is a field, a list's elements or an expression, of the query's dataset or a joined one (`collect(orders.status)` by customer is each customer's order statuses), and a collected list stays a list in a later stage, with the same functions and elements.

The [list functions](references.md#lists) test a whole list, and [Database support](../database-support.md) names the databases that run lists.

### Window functions in filters

Window functions (`OVER (...)`) are not allowed inside the inner WHERE on SQLite or most dialects. Query filters reject them in two ways:

* **Raw `OVER (...)` in filter strings is rejected at SlayerQuery construction.** Inline window-function SQL inside a query filter or `DatasetMetric.expression` is not parseable by SLayer's expression grammar.
* **Filtering on a `DatasetField` whose `sql` contains a window function is rejected at enrichment.** A query filter `"rn <= 3"` against a field whose `sql` is `row_number() over (...)` raises with a suggestion to use a rank-family transform.

Use one of:

* `rank(<metric>, direction='desc') <= N` (or `dense_rank` / `percent_rank` / `ntile(<metric>, n=N)`) for ranking — simpler and dialect-portable. Pass `partition_by=` to rank within groups.
* `first(x)` / `last(x)` / `lag(x, n)` / `lead(x, n)` for time-based window transforms.
* A multi-stage `source_queries` dataset where the window computation lives in an earlier stage.

### Variables

Filters support `{variable_name}` placeholders, substituted from the query's `variables` dict. This keeps filter templates reusable and avoids string concatenation in client code.

The same placeholders work in metric expressions, computed- and time-dimension expressions, `order` expressions and `date_range` bounds, where a placeholder is written unquoted (`"date_range": ["{start}", "{end}"]`; `"metrics": ["sum(amount) * {k} / 100"]`), and an unnamed entry is named from its template (`amount_sum_k_100`), so result keys never depend on the values; `{? ... ?}` blocks and a placeholder naming a time dimension's granularity or standing for its whole dimension is rejected.

```json
{
  "population": "orders",
  "metrics": ["count(*)"],
  "filter": ["status = '{status}' AND amount > {min_amount}"],
  "variables": {"status": "completed", "min_amount": 100}
}
```

This produces the filter `status = 'completed' AND amount > 100`.

- Variable names must be alphanumeric + underscore (`[a-zA-Z_][a-zA-Z0-9_]*`)
- Values must be strings, numbers, booleans or **lists**, and every value binds as a literal: **you write the surrounding quotes** for a string (`status = '{status}'`, an embedded `'` is doubled), an unquoted placeholder takes a number (a numeric string included) or a boolean, and a placeholder in a name position (`sum({col})`, `customers.{c}`, `{f}(x)`, `partition_by={x}`) is rejected, so no value can change the expression's structure; non-finite floats (`nan`/`inf`) are rejected.
- **List values render an `IN`-list.** A list variable powers an `in` / `not in` filter — write the parentheses and omit per-element quotes (each string element is auto-quoted):

    ```json
    {
      "population": "orders",
      "metrics": ["count(*)"],
      "filter": ["region in ({regions})"],
      "variables": {"regions": ["US", "CA"]}
    }
    ```

    An empty list there raises, and a literal `x IN []` is rejected; an empty list binds only as a list-function argument (`contains_all(tags, {wanted})`).
- `{{` and `}}` produce literal `{` and `}`; a brace pair holding only digits, commas and spaces (`'{1,2}'`, `'{}'`, `'[0-9]{3}'`) is literal text in any string
- Undefined variables raise an error
- The same `{variable}` mechanism also works in the **raw-SQL (Mode A) surfaces** of a dataset — see [Variables in dataset SQL](models.md#variables-in-dataset-sql).

!!! note "Literals only"
    A query carries no raw SQL: every value it supplies, in query fields and in a dataset's raw-SQL (Mode A) surfaces alike, binds as a literal, and a value that would change the SQL's structure is rejected.

#### Variables passed as a runtime kwarg

Every execution entry point also accepts a `variables=` runtime kwarg that **always wins**, even over a stage's own `variables` dict and a query-backed dataset's `query_variables` defaults:

```python
# Python
await engine.execute(slayer_query, variables={"region": "EU"})
await engine.execute("monthly_revenue", variables={"region": "EU"})  # run-by-name
```

```bash
# CLI
slayer query @query.json --variables region=EU --variables threshold=100
slayer query monthly_revenue --variables region=EU
```

```json
// REST POST /query
{"population": "orders", "metrics": ["count(*)"], "variables": {"region": "EU"}}
{"name": "monthly_revenue", "variables": {"region": "EU"}}  // run-by-name
```

Precedence (highest first):

1. Runtime kwarg (`variables=`)
2. Stage `SlayerQuery.variables`
3. Outer-query `.variables` (when a query-backed dataset is used as `population`)
4. Dataset defaults (`dataset.query_variables`)

Unknown kwarg variables (not referenced in any filter) are silently ignored.

## Run a saved query by name

If a dataset is **query-backed** (created via `create_dataset_from_query` or saved with `source_queries`), you can run its stored backing query directly:

```python
await engine.execute("monthly_revenue", variables={"region": "EU"})
```

This loads the dataset, runs its `source_queries` stages with the merged variables, and returns the final-stage result. Calling `execute(str)` on a non-query-backed dataset raises a clear error directing the user to wrap it in a `SlayerQuery` instead.

REST equivalent: `POST /query` with `{"name": "<dataset>", "variables": {...}}`. Run-by-name also accepts `dry_run`, `explain` and `refine`; query-defining fields (`population`, `metrics`, `dimensions`, `filter`, `time_dimensions`, `order`, `limit`, `offset`) are not allowed next to `name` — put them in `refine`.

CLI equivalent: `slayer query <dataset_name> [--refine JSON|@file] [--variables k=v ...] [--dry-run] [--explain]` — when the positional argument doesn't look like JSON (doesn't start with `{` or `[`) and isn't a `@file` reference, it's interpreted as a dataset name.

MCP equivalent: `query(query="<dataset>", refine={...}, variables={...}, dry_run=True/False, explain=True/False)` — a bare dataset-name string is run-by-name execution (a non-query-backed name raises the same error as `execute(str)`).

### Refining a saved query

`refine` merges extra clauses into the saved query's **final stage**, which then runs exactly as if you had written that merged stage by hand:

```python
await engine.execute("monthly_revenue", refine={"dimensions": ["region"]})         # extra group-by
await engine.execute("monthly_revenue", refine={"filter": "amount >= 50"})        # ANDed with the saved filter
await engine.execute("monthly_revenue", refine={"order": [{"field": "revenue", "direction": "desc"}], "limit": 1})
```

- `dimensions` and `metrics` are appended; an entry identical to a saved one (`region` ≡ `orders.region`) collapses, and a same-named entry that differs raises `RefinementConflictError`.
- `time_dimensions` merge per (field, granularity): a refinement may add a `date_range` or `label` the saved one lacks, but a different value conflicts — narrow a saved window with a filter on the time field instead.
- `filter` conditions are appended to the saved ones, skipping any the saved filter already has (the merged filter is then a list); `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`, `distinct_dimension_values` and `to_many_handling` replace the saved value (`{"limit": null}` clears a saved limit; a null `to_many_handling` keeps the saved mode).
- `population`, `name` and `variables` are not refinable; `refine` with a query object or list is an error.

For a multi-stage saved query only the final stage is refined, so it can use only what the earlier stages project: refining `[{"name": "per_customer", ...}, {"population": "per_customer", "metrics": [{"metric": "avg(revenue)", "alias": "avg_revenue"}]}]` with `{"dimensions": ["region"]}` averages per region, provided `per_customer` projects `region`.

Adding a second granularity of a saved time field (`{"time_dimensions": ["date_trunc('year', ordered_at)"]}` on a monthly query) renames the saved key `orders.ordered_at` to `orders.ordered_at.month`, as in any query with two granularities. A saved query's population is fixed when it is saved (see [Population](#population)), so a refinement never changes it.

---

## Population

`population` declares the query's **population** — the dataset whose rows the result is quantified over (one result row per distinct combination of its dimensions). It is optional: omit it and the population is inferred as the smallest dataset (fewest relationship hops) that **determines every queried dimension** along provably to-one relationship paths, read from the dimensions and field-typed (aggregate-free) filters **only — never metrics**. Adding or removing a metric therefore never changes which rows come back.

```json
{"dimensions": ["customers.region"], "metrics": ["sum(orders.amount)"]}
```

infers population `customers` (one row per region present among customers, order totals attached, NULL where a region has no orders) — not "regions that happen to have orders". The Python client and REST responses always carry the effective population as `population` and whether it was inferred as `population_inferred`; MCP output reports them only when the population was inferred.

Inference fails closed with a `PopulationInferenceError` naming the candidates when no single dataset determines everything, several minimal candidates tie, a dimension's relationship path is ambiguous, or the referenced datasets don't scope to exactly one datasource. Name `population` explicitly (any dataset — including a bridge that owns none of the queried items) to override inference.

A time dimension on `time_spine.timestamp` makes the population `time_spine × P`, P inferred from the remaining dimensions and filters — see [Time spine](time.md#time-spine).

Saving a query-backed dataset writes each inferred population into its stored stage as `population`, so later refinements and dataset edits never move it.

Inference is routing-aware: a short-form cross-dataset dimension (bare `regions.name`) is probed per candidate through the same auto-routing binding applies, so it infers the same population as its full dotted path (`customers.regions.name`) — or fails closed identically.

## Choosing a root dataset

`recommend_population` is the explain surface for the population rule: give it the `dataset.field` / `dataset.metric` items you want (aggregation suffixes allowed) and it returns the population it would pick plus each item's relationship-qualified reference path from it — ready to paste into a query.

```python
rec = engine.recommend_population_sync(["customers.name", "products.category"])
rec.population          # "orders"  (the only dataset that determines both to-one)
{ip.input_item: ip.path for ip in rec.item_paths}
# {"customers.name": "customers.name", "products.category": "products.category"}
```

A root is valid when it **determines** every requested field along provably to-one relationship paths; saved metrics and aggregation-suffixed items are attachments that only need to be reachable and never steer the choice (each `ItemPath` flags `attachment`). Among valid roots, the one with the fewest total relationship hops wins — the same rule as population inference, so the two surfaces cannot disagree. Root-owned items come back as a bare leaf (`status`); joined items as a dotted path (`customers.regions.name`), each hop spelled by its edge name when it has one (`customers.hr.name` for an edge named `hr`); aggregation suffixes are preserved (`sum(revenue)`). With at least one field item, an attachment on a dataset no relationship path reaches never blocks a root: it comes back spelled from the root (`count(events.id)`) and flagged `unrelated`.

When no single dataset reaches everything, `population` is `None`, `reachable` is `False`, and `coverage` lists the best partial roots (each with its reachable / unreachable items) so you can split the request into a multi-stage [`source_queries`](models.md#query-backed-datasets) query.

### Forcing a root with `root_hint`

Sometimes you already know the host you want — often a **bridge** dataset that owns none of the requested items but matches the grain you're building on. Pass `root_hint` (a bare dataset name or `<data_source>.<dataset>`) to force it:

```python
rec = engine.recommend_population_sync(
    ["customers.name", "regions.name"], root_hint="orders"
)
rec.population   # "orders"  (honored — it reaches both, overriding the closer auto-pick)
```

When the hint determines every item it's honored outright, overriding the fewest-hops pick. When it can't, the auto-pick is used instead and `warnings` explains which owning datasets the hint missed and which root was chosen. If no dataset reaches everything (`reachable` is `False`), the hint's own row is included in `coverage` too, so you can see exactly what it reaches. `root_hint` is resolved after the datasource is fixed from the items, so it names a dataset *within* that datasource — it can't choose the datasource. A hint that isn't a dataset in the resolved datasource raises.

Surfaces: MCP `recommend_population(items, data_source=None, root_hint=None, format="markdown")`, REST `POST /recommend-population` (`{"items": [...], "data_source": null, "root_hint": null}`), CLI `slayer recommend-population ITEM... [--data-source X] [--root-hint M] [--format json|text]`, and `SlayerClient.recommend_population(_sync)`. The optional `data_source` scopes name resolution to one datasource; all items must resolve to a single datasource.

---

## Examples

### Count by status

```json
{
  "population": "orders",
  "metrics": ["count(*)"],
  "dimensions": ["status"]
}
```

### Monthly revenue with date range

```json
{
  "population": "orders",
  "metrics": ["sum(revenue)"],
  "time_dimensions": [{
    "field": "created_at",
    "granularity": "month",
    "date_range": ["2024-01-01", "2024-12-31"]
  }]
}
```

### Top 5 customers by revenue

```json
{
  "population": "orders",
  "metrics": ["sum(revenue)"],
  "dimensions": ["customer_name"],
  "order": [{"field": "sum(revenue)", "direction": "desc"}],
  "limit": 5
}
```

### Filtered count with OR logic

```json
{
  "population": "orders",
  "metrics": ["count(*)"],
  "filter": ["status = 'completed' or status = 'pending'"]
}
```

### Derived metrics with transforms

```json
{
  "population": "orders",
  "metrics": [
    "count(*)",
    "sum(revenue)",
    {"metric": "sum(revenue) / count(*)", "alias": "aov", "label": "Average Order Value"},
    {"metric": "cumsum(sum(revenue))", "alias": "running"},
    {"metric": "change(sum(revenue))", "alias": "mom_change"}
  ],
  "time_dimensions": [{"field": "created_at", "granularity": "month"}]
}
```

### Statistical aggregations

The `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, and `covar_pop` aggregations behave like the rest of the aggregations. `corr` / `covar_samp` / `covar_pop` take two fields — the second rides as a named `other` parameter, the same way `weighted_avg` takes `weight`:

```json
{
  "population": "orders",
  "metrics": [
    {"metric": "stddev_samp(latency)", "alias": "latency_sd"},
    {"metric": "var_pop(latency)", "alias": "latency_var_pop"},
    {"metric": "corr(price, other=quantity)", "alias": "price_qty_corr"},
    {"metric": "covar_samp(price, other=quantity)", "alias": "price_qty_cov"}
  ],
  "dimensions": ["status"]
}
```

Edge cases match Postgres exactly:
- sample stddev/variance/covariance return NULL when N ≤ 1
- population stddev/variance/covariance return 0 at N = 1 and NULL at N = 0
- `corr` additionally returns NULL when either side has zero variance (covariance is well-defined in that case and just returns 0)

See [database-support.md](../database-support.md#aggregation-support) for the per-engine support matrix.

### Share of parent (`partition_by=`)

`partition_by=` on an aggregation computes it over a subset of the query's
dimensions and repeats the value across the finer rows — the share-of-parent /
percent-of-total shape. See [Expressions](formulas.md#aggregate-at-a-coarser-grain-partition_by).

```json
{
  "population": "orders",
  "dimensions": ["region", "city"],
  "metrics": [
    {"metric": "sum(revenue)", "alias": "city_rev"},
    {"metric": "sum(revenue) / sum(revenue, partition_by=region)", "alias": "share_of_region"},
    {"metric": "sum(revenue) / sum(revenue, partition_by=[])", "alias": "share_of_total"}
  ]
}
```

`share_of_region` sums to 1.0 within each region; `share_of_total` sums to 1.0
over the whole result. An unnamed partitioned metric's result key carries the
partition in its suffix (`orders.revenue_sum_partition_by_region`,
`orders.revenue_sum_partition_by` for the grand total) — name the metric to
control the key.

A `partition_by` element may also be a time bucket the query groups by, so with time dimension `ordered_at` at month, `partition_by=[date_trunc('month', ordered_at)]` means `partition_by=[ordered_at]`.

### Cross-dataset metrics

When datasets have [relationships](models.md#relationships), you can reference metrics from joined datasets using a dotted path inside the aggregation — `aggregation(dataset_name.metric_name)`:

```json
{
  "population": "orders",
  "metrics": [
    "count(*)",
    "avg(customers.score)"
  ],
  "time_dimensions": [{"field": "created_at", "granularity": "month"}]
}
```

SLayer computes the joined metric in its own sub-query **rooted at the metric's
dataset** (here `customers`) and LEFT JOINs it back to the main query — so the
aggregate runs over the target's rows once, never through a 1:N join that would
multiply them.

The method-call spelling `order_items.count(*)` or `order_items.sum(price * qty)` (further arguments literals only) means `count(order_items.*)` / `sum(order_items.price * order_items.qty)`.

The sub-query groups by the subset of your dimensions the engine can **prove
safe** — those reachable from the metric's dataset over relationship hops that are
provably many-to-one (a primary key on the far side, or a declared
[relationship `cardinality`](models.md#relationship-cardinality)). Each remaining dimension is
**associated**: each cell aggregates over the distinct rows of the metric's dataset
reaching it, and the response carries a `warnings` entry (`kind: "associated"`)
because the cells may overlap and are not additive. Declare the missing relationship
cardinality (or primary key) to make such a dimension partition the rows.

Query **filters** still restrict the metric: a conjunct the sub-query can only
reach across an unproven hop is pushed down as a correlated `EXISTS` semi-join —
the metric counts exactly the target rows related to at least one row passing
the filter (each row once, never multiplied through the join), surfaced as an
informational `kind: "semi_join_pushed"` entry in `.warnings` (never an error,
in any mode). The correlation path resolves through the same
[bidirectional traversal](models.md#bidirectional-traversal) as every other hop
— no declared reverse relationship is needed, and a hop spanned by two or more edges
fails the whole query with the ambiguous-hop error (in every mode) rather than
guessing. Pushdown is total over the conjunct's boolean shape: a row survives
when the predicate holds on at least one row of its join product over the
referenced branches, each hop joined as declared (LEFT by default), so
`tier = 'gold' or orders.status = 'ok'` keeps a gold customer with no orders and
`orders.id is null` reads as "no orders"; a reference with no resolvable relationship path
is refused with a typed error in every mode. On ClickHouse the semi-join needs
server ≥ 25.4 (the required setting is attached automatically), and older servers
fail with a clear error, as does a `readonly = 1` user whose profile leaves
`allow_experimental_correlated_subqueries` off (the 25.4–25.7 default) — enable it
in the user's profile, use `readonly = 2`, or upgrade to 25.8+.

`to_many_handling` chooses how such a dimension resolves — omitted (association,
the default above), `associate`, `broadcast` (the total over the safe grain,
repeated across the dimension, with a `kind: "broadcast"` warning naming the
metric and each broadcast dimension with the reason), or `error` (refuse) — where
a stored query saved with the old `"broadcast"` default loads with the mode
omitted, and a semi-join-pushed filter is applied, never erroring, in every mode.
Example: `{"population": "orders", "dimensions": ["status"], "metrics": ["sum(customers.spend)"]}` gives each status the spend of the distinct customers with an order in it.

Association serves every plain scalar aggregate. A dataset without a primary key uses a
per-query row identity. This includes datasets with a standalone unique field that may
contain NULLs. Each source row counts separately, and copies made by a join count once.

`window=` and `first`/`last` cannot associate yet: by default they broadcast with a `broadcast` warning whose `association_unsupported` names the reason, while an explicit `associate` (or an explicit `partition_by=` needing association) returns a typed error. An input crossing an unproven hop fails with a typed error in every mode. Each cell aggregates over the metric's own home rows by the home's relationship path, so an entity with no population row still counts in the cells its path reaches. An aggregate reads like a field of a dataset keyed by its grain, so an entity with no related row counts in the NULL cell whenever the result has one, however the query is rooted. A pushed filter binds to the same related row as a dimension it shares a hop with. An attached (aggregate- or transform-valued) parameter or source constituent is computed at its own home and attached per home row in every mode, so `weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))` weights each customer by its region's order total under `broadcast`, `associate` and `error` alike.

Every input of an aggregate — its source, arguments (`weight=`), definition defaults, and its field-level `filter=` — is traced recursively through derived-field definitions, and an input whose expansion crosses a fanning (not provably to-one) hop fails closed with a typed error instead of silently multiplying rows.

A dataset (or query stage) that no relationship path reaches is read only through aggregates: rooted at `customers`, `"count(events.id) / count(*)"` computes `count(events.id)` over every event, repeats it on every row (per bucket of a `time_spine` time dimension) with a `kind: "unrelated_dataset"` warning, and a filter on it restricts its rows, while grouping, ordering or filtering by its fields alongside other datasets' is a typed error.

A filter **on** the cross-dataset value itself (`"avg(customers.score) > 4"`)
restricts the result rows, uniformly with local aggregate filters — groups that
fail the predicate are dropped, not returned with `NULL`.

Cross-dataset aggregates also support `window=`, `partition_by=`, and `first` /
`last` (broadcast, as above, where a dimension would need association), inside
[dimension expressions](#expression-dimensions), and as hidden
[order-only fields](#ordering-by-something-you-dont-project).

An [aggregated expression](formulas.md#expression-aggregation) may mix host and joined-dataset fields (`sum(amount - customers.discount)`); it is homed at the deepest dataset that determines every operand, so the relationship hop's cardinality decides exact grain vs association exactly as for a single cross-dataset metric.

A cross-dataset **parametric** aggregate keeps its kwarg signature in the result key, so two variants on the same target field do not collide:

```json
{
  "population": "orders",
  "dimensions": ["customers.region"],
  "metrics": [
    "percentile(customers.revenue, p=0.5)",
    "percentile(customers.revenue, p=0.95)"
  ]
}
```

surfaces two distinct result keys — `orders.customers.revenue_percentile_p_0_5` and `orders.customers.revenue_percentile_p_0_95`. (A non-parametric `sum(customers.revenue)` surfaces as `orders.customers.revenue_sum`.)

### Query lists

Pass a list of queries to `execute()`. Earlier queries are named sub-queries, the last is the main query. Named queries can be referenced by `population` name or joined via `relationships`:

```json
[
  {
    "name": "monthly",
    "population": "orders",
    "metrics": ["count(*)", "sum(amount)"],
    "time_dimensions": [{"field": "created_at", "granularity": "month"}]
  },
  {
    "population": "monthly",
    "metrics": ["count(*)"]
  }
]
```

This counts how many months exist in the monthly summary. The main query references `"monthly"` by name — a stage name overrides a same-named dataset only within its own query list, never inside stored definitions such as a dataset's stored relationships.

You can also join named queries to datasets:

```json
[
  {
    "name": "customer_scores",
    "population": "customers",
    "dimensions": ["id"],
    "metrics": ["avg(score)"]
  },
  {
    "population": "orders",
    "extensions": [{"dataset": "orders", "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
    "metrics": ["count(*)", "avg(customer_scores.score_avg)"],
    "time_dimensions": [{"field": "created_at", "granularity": "month"}]
  }
]
```

The main query's `extensions` entry adds a relationship from `orders` to the named sub-query. Queries can also be saved as permanent datasets — see [Query-backed datasets](models.md#query-backed-datasets). `population` names a stored dataset or stage; an inline dataset is rejected — write a query-backed dataset's queries as named stages of the list instead.

Sibling stages can also reference each other — any non-final stage may use a *prior* named stage as `population`, as `relationships.to`, or in a reference such as `max(z_ev.n)`, so a query list forms a DAG, not just a chain; only the siblings a stage names join its relationship graph. For example, two parallel rollups feeding a single final stage:

```json
[
  {
    "name": "customer_scores",
    "population": "customers",
    "dimensions": ["id"],
    "metrics": ["avg(score)"]
  },
  {
    "name": "tagged_orders",
    "population": "orders",
    "extensions": [{"dataset": "orders", "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
    "dimensions": ["customer_scores.score_avg"],
    "metrics": ["count(*)"]
  },
  {
    "population": "tagged_orders",
    "metrics": ["max(_count)"]
  }
]
```

A grouped stage is unique on its dimensions, so a relationship onto a sibling stage that covers all of them (here `customer_scores` on `id`) is provably many-to-one and keeps exact per-dimension values. A joined stage's columns keep their flat names: a stage `s` with the dimension `customers.name` is read as `s.customers__name`, and the dotted `s.customers.name` is refused with that flat name as its suggestion.

**Stages inherit edges on pass-through keys.** A stage keeps every relationship of a dataset it reads, in either direction, whose key columns it outputs unchanged (a plain dimension, renamed by an alias or not; never a time bucket, a cast, a computed expression or a metric), so a later stage reaches the related dataset without declaring the relationship again:

```json
[
  {"name": "per_customer", "population": "orders", "dimensions": ["customer_id"], "metrics": [{"metric": "sum(amount)", "alias": "revenue"}]},
  {"population": "per_customer", "dimensions": ["customers.region"], "metrics": [{"metric": "sum(revenue)", "alias": "total"}]}
]
```

An inherited edge keeps its name, its target and (for a stored relationship) its join type; its stage side is "one" exactly when the stage's grain is covered by the key, and its target side keeps the source edge's arity, so `per_customer` above is one-to-one with `customers`. A relationship the consuming query declares on the stage wins over an inherited one: an identical one is applied once, a same-named one on other keys replaces it, and differently named edges on the same keys stay parallel.

**Order doesn't matter for runtime lists.** Stages can be submitted in any order — the engine auto-sorts them topologically so every stage appears after the siblings it references. The **last entry** of the input list is always the entry point / DAG root (its result is what's returned); only the non-final entries are reordered. Cycles and self-references are rejected with a clear error naming the offending stages. A non-final stage may not reference the root (the root must be the dependency sink). Stages that aren't reachable from the root are accepted as utility sub-queries — they're silently dropped from the emitted SQL.

`SlayerDataset.source_queries` (stored, YAML-defined) keeps stricter top-to-bottom rules: any reference must point to a stage defined *earlier* in the list, so the file reads top-to-bottom as the execution order.

**Surface coverage.** Query lists work via every surface:

- Python SDK: `engine.execute(query=[...])` and `SlayerClient.query`/`query_sync`/`sql`/`sql_sync`/`explain`/`explain_sync`/`query_df` all accept `SlayerQuery | dict | list[SlayerQuery | dict] | str` (str = run-by-name).
- CLI: `slayer query @file.json` — accepts both a single object and a top-level list.
- MCP: the `query` tool with `query=[...]` (a list of stage objects); the same tool also accepts a dataset name or a single query object.
- REST: `POST /query` with body `{"queries": [...], "variables": {...}, "dry_run": ..., "explain": ...}` (the single-query body shape is also still accepted).

### Query extensions

`extensions` adds metrics and relationships to any dataset a query object reads — its population, a joined dataset, or a stage — without modifying the stored dataset:

```json
{
  "population": "orders",
  "extensions": [
    {
      "dataset": "orders",
      "relationships": [{"to": "customer_scores", "from_columns": ["customer_id"], "to_columns": ["id"]}]
    },
    {"dataset": "customers", "metrics": [{"name": "credit_total", "expression": "sum(credit)"}]}
  ],
  "dimensions": [{"field": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END", "alias": "tier"}],
  "metrics": ["count(*)", "customers.credit_total"]
}
```

Each entry's keys are `dataset` (required — the dataset or stage to extend; `source_name` is its pre-2.0 spelling), `metrics` and `relationships` (both optional — merged with the dataset's own); any other key, or a second entry for one dataset, is rejected when the query is constructed. A per-query field is an expression in the position that uses it (a computed dimension, a filter, an aggregation's argument); `fields` is rejected.

An extension is visible only in the query object that declares it: other stages see that stage's output, and stored relationships, fields, metrics and query-backed datasets never see it.

A relationship repeating an edge between its two datasets — declared on either dataset, with the same key pairs (`from_columns` / `to_columns`) in either direction, whatever their order — is that edge, and its `name` becomes a local alias; a name, pairs or cardinality that conflicts with an existing edge is an error.

`"join_type": "inner"` keeps only the population rows with a match along the extended dataset's path, without multiplying rows; a `left` join (the default) never restricts, and a new edge is always traversed as LEFT.

An extension object in `population` (`{"source_name": ...}`) still works with a deprecation warning, and is removed in SLayer 3.0 — see [Upgrading to 2.0](../upgrading/2.0.md#part-2-deprecated-in-20-removed-in-30).

### Multi-hop dimensions

Dimensions from joined datasets can be referenced with dotted paths. SLayer auto-resolves multi-hop relationship chains by walking each intermediate dataset's own relationships:

```json
{
  "population": "orders",
  "dimensions": ["customers.regions.name"],
  "metrics": ["count(*)"]
}
```

This walks `orders → customers → regions` via the relationship graph and resolves `name` from the `regions` dataset. Works with both ingested rollup datasets and explicit relationships.

A dotted reference may target a *derived* field on the joined dataset — i.e., a field whose own `sql` is an expression rather than a base table column. The engine recursively inlines the derivation at query time, and the same chaining works whether the reference appears in a query's `dimensions` / `metrics` / `filter` or inside another dataset's `DatasetField.sql`. The planner also walks the chain to discover the joins each derived field's SQL implies, so a filter that names only a bare local derived field (no dimension entry) still triggers the right LEFT JOINs. See [Datasets → Derived Fields Referencing Other Derived Fields](models.md#derived-fields-referencing-other-derived-fields) and [Filters and Auto-Joins](#filters-and-auto-joins).

SQL dimensions can be mixed with regular dimensions. The expression goes directly into SELECT and GROUP BY.

