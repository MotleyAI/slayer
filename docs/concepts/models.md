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
  - name: revenue               # Required
    description: "Order amount" # Optional — explains what this measure computes
    sql: "amount"               # SQL expression (bare column name or expression)

  - name: quantity
    sql: "qty"
    allowed_aggregations: [sum, avg, min, max]  # Optional whitelist

aggregations:                   # Optional: custom aggregation definitions
  - name: weighted_avg
    formula: "sum({expr} * {weight}) / sum({weight})"
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

Measures are named row-level SQL expressions. They define *what* to compute, not *how* to aggregate — aggregation is specified at query time using colon syntax.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | — | Unique measure name |
| `description` | string | No | — | Explains what this measure computes, shown in datasource_summary and inspect_model |
| `sql` | string | Yes | — | SQL expression (bare column name or expression) |
| `allowed_aggregations` | list[str] | No | — | Whitelist of allowed aggregation types (validated at model creation and query time) |
| `hidden` | bool | No | `false` | Hide from listings |

### Built-in Aggregations

Aggregation is applied at query time via colon syntax: `measure_name:aggregation`. For example, `revenue:sum` means "SUM the `revenue` measure."

| Aggregation | Colon syntax | SQL Generated |
|-------------|-------------|---------------|
| `count` | `*:count` | `COUNT(*)` — counts all rows |
| `count` | `col:count` | `COUNT(col)` — counts non-null values |
| `count_distinct` | `col:count_distinct` | `COUNT(DISTINCT col)` |
| `sum` | `revenue:sum` | `SUM(revenue)` |
| `avg` | `revenue:avg` | `AVG(revenue)` |
| `min` | `revenue:min` | `MIN(revenue)` |
| `max` | `revenue:max` | `MAX(revenue)` |
| `first` | `col:first(time_col)` | Earliest record's value (ordered by `time_col`) |
| `last` | `col:last(time_col)` | Latest record's value (ordered by `time_col`) |
| `weighted_avg` | `price:weighted_avg(weight=quantity)` | `SUM(price * quantity) / SUM(quantity)` |
| `median` | `revenue:median` | Median value |
| `percentile` | `revenue:percentile(p=0.95)` | 95th percentile |

`*:count` is always available — no measure definition needed. `*` means "all rows", `count` is a regular aggregation.

### The `first` and `last` Aggregations

`first` and `last` return the value from the **earliest or most recent record** within each grouped bucket — like `min`/`max`, but ordered by time instead of value. Useful for snapshot metrics like balances, inventory counts, or status fields where you want the latest state.

```yaml
measures:
  - name: balance
    sql: balance
```

At query time, use `balance:last(updated_at)` to get the most recent balance per group, or `balance:first(updated_at)` for the earliest. When grouped by month, each month returns the `balance` value from the latest (or earliest) record in that month. If no time column is specified, the time column for ordering is resolved via: query's `main_time_dimension` → first time/date dimension in the query → first time dimension in filters → model's `default_time_dimension`.

Not to be confused with the [`last()` formula function](formulas.md#last-function), which is a window-function transform that broadcasts a single value across all rows.

### Custom Aggregations

Models can define custom aggregations in the `aggregations` list. Each custom aggregation has a name and a formula template using `{expr}` for the measure expression and named placeholders for kwargs:

```yaml
aggregations:
  - name: weighted_avg
    formula: "sum({expr} * {weight}) / sum({weight})"
  - name: trimmed_mean
    formula: "avg(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)"
```

Use at query time: `price:weighted_avg(weight=quantity)`, `revenue:trimmed_mean(low=10, high=1000)`.

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
        fields=["*:count", "amount:sum"],
    ),
    name="monthly_summary",
)
```

The saved model can then be queried by name like any other model — useful for materializing complex aggregations.

Via MCP, use the `create_model_from_query` tool. Via API, `POST /models/from_query`.

### Column naming in query-derived models

A query result is a self-contained table — it no longer has the joins that the source model may have had. Dimensions and measures that came from joined models use `__` to encode the original join path in their name:

| Inner query field | Virtual model column name |
|----------------------|--------------------------|
| `stores.name` | `stores__name` |
| `customers.regions.name` | `customers__regions__name` |
| `customer_id` | `customer_id` |
| `*:count` (measure) | `count` |
| `revenue:sum` (measure) | `revenue_sum` |

This uses the same `__` convention as SQL-level join path aliases. When referencing these columns in an outer query, use the `__` name directly (e.g., `{"name": "stores__name"}`), not dot syntax — dots would imply a join to a model that doesn't exist on the virtual table.

See the [multistage queries example](../examples/06_multistage_queries/multistage_queries.md) for working examples.

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

Query results use `model_name.column_name` format for column keys. Colon syntax in field names is converted: `revenue:sum` becomes `orders.revenue_sum`, and `*:count` becomes `orders.count`. For multi-hop joined dimensions, the full path is included:

```json
{"orders.status": "completed", "orders.count": 42, "orders.revenue_sum": 1500}
{"orders.customers.regions.name": "US", "orders.count": 3}
```
