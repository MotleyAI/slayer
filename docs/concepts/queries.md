# Queries

A `SlayerQuery` specifies what data to retrieve from a model.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | No | Name for this query — used to reference it from other queries in a list |
| `model` | string, SlayerModel, or ModelExtension | Yes | Target model name, inline model, or model extension (adds dimensions/measures/joins) |
| `fields` | list[Field] | No | Data columns — measures, arithmetic, transforms. See [Field Formulas](formulas.md#field-formulas). |
| `dimensions` | list[ColumnRef] | No | Dimensions to group by. Supports dotted names for joined models (`customers.name`, `customers.regions.name`). |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `main_time_dimension` | string | No | Explicit time dimension name for transforms (overrides auto-detection) |
| `filters` | list[str] | No | Conditions as formula strings. See [Filters](#filters). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `whole_periods_only` | bool | No | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |

You can pass a single query or a **list of queries** to `execute()`. When passing a list, earlier queries are named sub-queries that later queries can reference. The last query in the list is the main one whose results are returned. See [Query Lists](#query-lists) for examples.

## ColumnRef

A reference to a dimension. Three modes:

```json
{"name": "status"}
{"name": "status", "label": "Order Status"}
{"sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END", "name": "tier"}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Required. Result column name. For existing dimensions, also the column to look up. |
| `sql` | string | Raw SQL expression — used as a computed grouping dimension |
| `formula` | string | Formula expression referencing measures (e.g., `"revenue / count"`) |
| `label` | string | Human-readable display name |

Via MCP, simple dimensions are passed as strings: `dimensions=["status"]`

## TimeDimension

A time dimension with a required granularity and an optional date range. Supports an optional `label` for human-readable output. To use a time column without truncation, add it as a regular dimension instead.

```json
{
  "dimension": {"name": "created_at"},
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"],
  "label": "Order Month"
}
```

**Granularities**: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`

## OrderItem

```json
{"column": {"name": "count"}, "direction": "desc"}
```

Via MCP: `{"column": "count", "direction": "desc"}`

## Response

Query results are returned as a `SlayerResponse`:

| Field | Type | Description |
|-------|------|-------------|
| `data` | list[dict] | Rows as dictionaries |
| `columns` | list[str] | Column names in `model_name.column_name` format (e.g., `"orders.count"`) |
| `row_count` | int | Number of rows |
| `sql` | string | The generated SQL (useful for debugging) |
| `labels` | dict | Column alias → human-readable label (from `label` on fields/dimensions) |

```json
{
  "data": [
    {"orders.status": "completed", "orders.count": 42},
    {"orders.status": "pending", "orders.count": 15}
  ],
  "columns": ["orders.status", "orders.count"],
  "row_count": 2,
  "sql": "SELECT ..."
}
```

---

## Filters

Filter formulas define conditions for the query. They go in the `filters` parameter as plain strings:

```json
"filters": ["status == 'active'", "amount > 100"]
```

### Comparison Operators

| Operator | Example |
|----------|---------|
| `==` | `"status == 'active'"` |
| `!=` | `"status != 'cancelled'"` |
| `>` | `"amount > 100"` |
| `>=` | `"amount >= 100"` |
| `<` | `"amount < 1000"` |
| `<=` | `"amount <= 1000"` |
| `in` | `"status in ('active', 'pending')"` |
| `is None` | `"discount is None"` (IS NULL) |
| `is not None` | `"discount is not None"` (IS NOT NULL) |
| `like` | `"name like '%acme%'"` |
| `not like` | `"name not like '%test%'"` |

### Boolean Logic

Use `and`, `or`, `not` within a single filter string:

```json
"filters": [
    "status == 'completed' or status == 'pending'",
    "amount > 100 and amount < 1000"
]
```

Multiple entries in the `filters` list are combined with AND.

### Filtering on Computed Columns

Filters can reference names of computed fields — transforms and arithmetic expressions defined in `fields`. These are applied as post-filters on the outer query, after all transforms are computed. Note: bare measure renames (e.g., `{"formula": "count", "name": "n"}`) are not post-filterable by name; use the original measure name instead.

```json
{
  "fields": [
    {"formula": "revenue"},
    {"formula": "change(revenue)", "name": "rev_change"}
  ],
  "filters": ["rev_change < 0"]
}
```

Transform expressions can also be used **directly in filters** without defining them as fields first:

```json
{
  "filters": ["last(change(revenue)) < 0"]
}
```

Post-filters can be combined with regular filters — base filters (on dimensions/measures) are applied in the inner query, post-filters on the outer wrapper:

```json
{
  "filters": ["status == 'completed'", "change(revenue) > 0"]
}
```

---

## Examples

### Count by status

```json
{
  "model": "orders",
  "fields": [{"formula": "count"}],
  "dimensions": [{"name": "status"}]
}
```

### Monthly revenue with date range

```json
{
  "model": "orders",
  "fields": [{"formula": "revenue_sum"}],
  "time_dimensions": [{
    "dimension": {"name": "created_at"},
    "granularity": "month",
    "date_range": ["2024-01-01", "2024-12-31"]
  }]
}
```

### Top 5 customers by revenue

```json
{
  "model": "orders",
  "fields": [{"formula": "revenue_sum"}],
  "dimensions": [{"name": "customer_name"}],
  "order": [{"column": {"name": "revenue_sum"}, "direction": "desc"}],
  "limit": 5
}
```

### Filtered count with OR logic

```json
{
  "model": "orders",
  "fields": [{"formula": "count"}],
  "filters": ["status == 'completed' or status == 'pending'"]
}
```

### Derived columns with transforms

```json
{
  "model": "orders",
  "fields": [
    {"formula": "count"},
    {"formula": "revenue_sum"},
    {"formula": "revenue_sum / count", "name": "aov", "label": "Average Order Value"},
    {"formula": "cumsum(revenue_sum)", "name": "running"},
    {"formula": "change(revenue_sum)", "name": "mom_change"}
  ],
  "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
}
```

### Cross-model measures

When models have [joins](models.md#joins), you can reference measures from joined models using dotted syntax `model_name.measure_name`:

```json
{
  "model": "orders",
  "fields": [
    {"formula": "count"},
    {"formula": "customers.avg_score"}
  ],
  "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
}
```

This generates a sub-query for the joined measure, scoped to shared dimensions, and LEFT JOINs it to the main query — avoiding aggregation errors from row multiplication.

### Query lists

Pass a list of queries to `execute()`. Earlier queries are named sub-queries, the last is the main query. Named queries can be referenced by `model` name or joined via `joins`:

```json
[
  {
    "name": "monthly",
    "model": "orders",
    "fields": [{"formula": "count"}, {"formula": "total_amount"}],
    "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
  },
  {
    "model": "monthly",
    "fields": [{"formula": "count"}]
  }
]
```

This counts how many months exist in the monthly summary. The main query references `"monthly"` by name — if a named query and a stored model share a name, the query takes precedence.

You can also join named queries to models:

```json
[
  {
    "name": "customer_scores",
    "model": "customers",
    "dimensions": [{"name": "id"}],
    "fields": [{"formula": "avg_score"}]
  },
  {
    "model": {"source_name": "orders", "joins": [{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}]},
    "fields": [{"formula": "count"}, {"formula": "customer_scores.avg_score_avg"}],
    "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
  }
]
```

The main query uses a `ModelExtension` to add a join to the named sub-query. Queries can also be saved as permanent models — see [Creating Models from Queries](models.md#creating-models-from-queries).

### ModelExtension

Extend a model inline with extra dimensions, measures, or joins — without modifying the stored model:

```json
{
  "model": {
    "source_name": "orders",
    "dimensions": [{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
    "joins": [{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}]
  },
  "dimensions": [{"name": "tier"}],
  "fields": [{"formula": "count"}]
}
```

`ModelExtension` fields: `source_name` (required — model to extend), `dimensions`, `measures`, `joins` (all optional — merged with the source model's).

### Multi-hop dimensions

Dimensions from transitively joined models can be referenced with dotted paths. SLayer auto-resolves the join chain:

```json
{
  "model": "orders",
  "dimensions": [{"name": "customers.regions.name"}],
  "fields": [{"formula": "count"}]
}
```

This walks `orders → customers → regions` via the join graph and resolves `name` from the `regions` model. Works with both ingested rollup models and explicit joins.

SQL dimensions can be mixed with regular dimensions. The expression goes directly into SELECT and GROUP BY.

