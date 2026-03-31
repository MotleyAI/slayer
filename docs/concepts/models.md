# Models

A model maps a database table (or SQL subquery) to queryable **dimensions** and **measures**. Models are defined as YAML files or created via the API/MCP.

## YAML Structure

```yaml
name: orders                    # Required: unique model name
description: "Order data"       # Optional — helps agents and users understand the model
sql_table: public.orders        # One of: sql_table or sql
# sql: "SELECT * FROM ..."     # Alternative: custom SQL subquery
data_source: my_postgres        # Required: datasource name
hidden: false                   # Optional: hide from listings
default_time_dimension: created_at  # Optional: default for time-dependent formulas

dimensions:
  - name: id                    # Required
    description: "Order ID"     # Optional — clarifies meaning when column names are technical
    sql: "id"                   # SQL expression (bare column name)
    type: number                # Required: string, number, boolean, time, date
    primary_key: true           # Optional
    hidden: false               # Optional

measures:
  - name: count                 # Required
    description: "Row count"    # Optional — explains what this measure computes
    type: count                 # Required: count, count_distinct, sum, avg, min, max, last
    # sql: not needed for count

  - name: revenue_sum
    sql: "amount"               # Required for non-count types
    type: sum
```

## Dimensions

Dimensions are the columns you group by and filter on.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique dimension name |
| `description` | string | No | — | Clarifies meaning for agents and users, especially for technical column names |
| `sql` | string | No | — | SQL expression |
| `type` | string | No | `string` | Data type |
| `primary_key` | bool | No | `false` | Is this a primary key? |
| `hidden` | bool | No | `false` | Hide from listings |

### Dimension Types

| Type | Description | SQL Examples |
|------|-------------|--------------|
| `string` | Text values | VARCHAR, TEXT, CHAR |
| `number` | Numeric values | INTEGER, FLOAT, NUMERIC |
| `boolean` | True/false | BOOLEAN |
| `time` | Timestamp | TIMESTAMP, DATETIME |
| `date` | Date only | DATE |

## Measures

Measures are aggregated values — counts, sums, averages.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique measure name |
| `description` | string | No | — | Explains what this measure computes, shown in datasource_summary and inspect_model |
| `sql` | string | No* | — | SQL expression to aggregate |
| `type` | string | No | `count` | Aggregation type |
| `hidden` | bool | No | `false` | Hide from listings |

*Not required for `count` type (uses `COUNT(*)`).

### Aggregation Types

| Type | SQL Function | Notes |
|------|-------------|-------|
| `count` | `COUNT(*)` | No `sql` needed |
| `count_distinct` | `COUNT(DISTINCT expr)` | Unique count |
| `sum` | `SUM(expr)` | Sum aggregation |
| `avg` | `AVG(expr)` | Average |
| `min` | `MIN(expr)` | Minimum |
| `max` | `MAX(expr)` | Maximum |
| `last` | Latest record's value | See below |

### The `last` Aggregation Type

`type: last` returns the value from the **most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state.

```yaml
measures:
  - name: balance
    sql: balance
    type: last
```

When grouped by month, each month returns the `balance` value from the latest record in that month. The time column for ordering is resolved via: query's `main_time_dimension` → first time/date dimension in the query → first time dimension in filters → model's `default_time_dimension`.

Not to be confused with the [`last()` formula function](formulas.md#last-function), which is a window-function transform that broadcasts a single value across all rows.

## SQL Expressions

### In Dimensions and Measures

Use **bare column names** (e.g., `"amount"`) — SLayer automatically qualifies them with the model's table reference at query time.

For complex expressions, use the model name as a table prefix: `"orders.amount * orders.quantity"`.

## Model Fields Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique model name |
| `sql_table` | string | One of | — | Database table (e.g. `public.orders`) |
| `sql` | string | these | — | Custom SQL subquery |
| `data_source` | string | Yes | — | Datasource name |
| `dimensions` | list | No | `[]` | Dimension definitions |
| `measures` | list | No | `[]` | Measure definitions |
| `description` | string | No | — | Helps agents and users understand the model |
| `hidden` | bool | No | `false` | Hide from model listings |
| `default_time_dimension` | string | No | — | Default time dimension name for time-dependent formulas (e.g. `"created_at"`) |

## Result Column Format

Query results use `model_name.column_name` format for column keys:

```json
{"orders.status": "completed", "orders.count": 42}
```
