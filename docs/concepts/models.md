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

## Joins

Models can declare explicit LEFT JOIN relationships to other models:

```yaml
name: orders
sql_table: public.orders
joins:
  - target_model: customers
    join_pairs: [["customer_id", "id"]]
  - target_model: products
    join_pairs: [["product_id", "id"]]
```

Joins enable **cross-model measures** — querying a measure from a joined model alongside the main model's data. See [Cross-Model Measures](queries.md#cross-model-measures).

During [auto-ingestion](ingestion.md), joins are generated automatically from foreign key relationships (including transitive joins like `orders → customers → regions`). Multi-hop dimensions are auto-resolved by walking the join graph — `customers.regions.name` in a query on `orders` follows `orders → customers → regions` automatically.

### Path-Based Table Aliases

Joined tables use `__`-delimited path aliases in generated SQL to disambiguate **diamond joins** — when the same table is reachable via multiple paths. For example, if `orders` joins both `customers` and `warehouses`, each referencing `regions`:

- `customers.regions.name` → table alias `customers__regions`
- `warehouses.regions.name` → table alias `warehouses__regions`

In queries, use dots to denote paths (`customers.regions.name`). In model SQL definitions (dimension/measure `sql` fields), use the `__` alias convention (`customers__regions.name`). See [Diamond Joins](ingestion.md#diamond-joins) for details.

## Model Filters

Models can have always-applied WHERE filters on the underlying table:

```yaml
name: active_orders
sql_table: public.orders
filters:
  - "deleted_at IS NULL"
  - "status <> 'test'"
```

Model filters only support conditions on underlying table columns (WHERE). For measure-based conditions, use query-level filters instead.

Since model filters are SQL snippets, multi-hop joined column references should use the `__` alias syntax (e.g., `customers__regions.name`), not dots. Single-dot references like `customers.name` (table.column) are fine. Multi-dot references like `customers.regions.name` are auto-converted to `customers__regions.name` with a warning. The same auto-conversion applies to dimension and measure `sql` fields.

## Creating Models from Queries

You can save a query's result as a permanent model. The query structure is preserved, and dimensions and measures are auto-introspected:

```python
engine.create_model_from_query(
    query=SlayerQuery(
        source_model="orders",
        time_dimensions=[...],
        fields=[{"formula": "count"}, {"formula": "total_amount"}],
    ),
    name="monthly_summary",
)
```

The saved model can then be queried by name like any other model — useful for materializing complex aggregations.

Via MCP, use the `create_model_from_query` tool. Via API, `POST /models/from_query`.

### Column naming in query-derived models

A query result is a self-contained table — it no longer has the joins that the source model may have had. Dimensions and measures that came from joined models use `__` to encode the original join path in their name:

| Inner query dimension | Virtual model column name |
|----------------------|--------------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `customer_id` | `customer_id` |
| `count` (measure) | `count` |

This uses the same `__` convention as SQL-level join path aliases. When referencing these columns in an outer query, use the `__` name directly (e.g., `{"name": "stores__name"}`), not dot syntax — dots would imply a join to a model that doesn't exist on the virtual table.

See the [multistage queries example](../examples/multistage_queries/multistage_queries.md) for working examples.

## Model Fields Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique model name |
| `sql_table` | string | One of | — | Database table (e.g. `public.orders`) |
| `sql` | string | these | — | Custom SQL subquery |
| `data_source` | string | Yes | — | Datasource name |
| `dimensions` | list | No | `[]` | Dimension definitions |
| `measures` | list | No | `[]` | Measure definitions |
| `joins` | list | No | `[]` | JOIN relationships to other models |
| `filters` | list[str] | No | `[]` | Model-level WHERE filters (always applied, e.g., `"deleted_at IS NULL"`) |
| `description` | string | No | — | Helps agents and users understand the model |
| `hidden` | bool | No | `false` | Hide from model listings |
| `default_time_dimension` | string | No | — | Default time dimension name for time-dependent formulas (e.g. `"created_at"`) |

## Result Column Format

Query results use `model_name.column_name` format for column keys. For multi-hop joined dimensions, the full path is included:

```json
{"orders.status": "completed", "orders.count": 42}
{"orders.customers.regions.name": "US", "orders.count": 3}
```
