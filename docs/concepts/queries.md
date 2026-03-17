# Queries

A `SlayerQuery` specifies what data to retrieve from a model.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `model` | string | Yes | Target model name |
| `fields` | list[Field] | No | Data columns — measures, arithmetic, transforms. See [Field Formulas](formulas.md#field-formulas). |
| `dimensions` | list[ColumnRef] | No | Dimensions to group by |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `filters` | list[str] | No | Conditions as formula strings. See [Filter Formulas](formulas.md#filter-formulas). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `variables` | dict | No | Template variables for `{var}` placeholders in filters |
| `whole_periods_only` | bool | No | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |

## ColumnRef

A reference to a dimension.

```json
{"name": "status"}
```

Via MCP, dimensions are passed as simple strings: `dimensions=["status"]`

## TimeDimension

A time dimension with a required granularity and an optional date range. To use a time column without truncation, add it as a regular dimension instead.

```json
{
  "dimension": {"name": "created_at"},
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"]
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
    {"formula": "revenue_sum / count", "name": "aov"},
    {"formula": "cumsum(revenue_sum)", "name": "running"},
    {"formula": "change(revenue_sum)", "name": "mom_change"}
  ],
  "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
}
```

### Variables in filters

```json
{
  "model": "orders",
  "fields": [{"formula": "count"}],
  "filters": ["status == '{status}'"],
  "variables": {"status": "completed"}
}
```
