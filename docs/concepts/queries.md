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
| `filters` | list[str] | No | Conditions as formula strings. See [Filter Formulas](formulas.md#filter-formulas). |
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

