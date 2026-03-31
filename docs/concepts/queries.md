# Queries

A `SlayerQuery` specifies what data to retrieve from a model.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | string | Yes | Target model name |
| `fields` | list[Field] | No | Data columns — measures, arithmetic, transforms. Each field has a `formula` (required), optional `name`, and optional `label` (human-readable display name). See [Field Formulas](formulas.md#field-formulas). |
| `dimensions` | list[ColumnRef] | No | Dimensions to group by |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `main_time_dimension` | string | No | Explicit time dimension name for transforms (overrides auto-detection) |
| `filters` | list[str] | No | Conditions as formula strings. See [Filters](#filters). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `whole_periods_only` | bool | No | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |

## ColumnRef

A reference to a dimension. Supports an optional `label` for human-readable output.

```json
{"name": "status"}
{"name": "status", "label": "Order Status"}
```

Via MCP, dimensions are passed as simple strings: `dimensions=["status"]`

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

