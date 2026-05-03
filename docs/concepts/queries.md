# Queries

A `SlayerQuery` specifies what data to retrieve from a model.

## Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | string | No | Name for this query — used to reference it from other queries in a list |
| `source_model` | string, SlayerModel, or ModelExtension | Yes | Source model name, inline model, or model extension (adds columns/measures/joins) |
| `measures` | list[ModelMeasure] | No | Computed/aggregated values — formulas, arithmetic, transforms. See [Formulas](formulas.md). |
| `dimensions` | list[ColumnRef] | No | Columns to group by. Supports dotted names for joined models (`customers.name`, `customers.regions.name`). |
| `time_dimensions` | list[TimeDimension] | No | Time dimensions with granularity |
| `main_time_dimension` | string | No | Explicit time dimension name for transforms (overrides auto-detection) |
| `filters` | list[str] | No | Conditions as formula strings. Supports `{variable}` placeholders. See [Filters](#filters). |
| `variables` | dict[str, Any] | No | Variable values for filter substitution. See [Filter Variables](#filter-variables). |
| `order` | list[OrderItem] | No | Sort specifications |
| `limit` | int | No | Maximum rows to return |
| `offset` | int | No | Number of rows to skip |
| `whole_periods_only` | bool | No | Snap date filters to time bucket boundaries, exclude the current incomplete time bucket |

You can pass a single query or a **list of queries** to `execute()`. When passing a list, earlier queries are named sub-queries that later queries can reference. The last query in the list is the main one whose results are returned. See [Query Lists](#query-lists) for examples.

## ColumnRef

A reference to a model dimension. Supports dotted names for joined models.

```json
{"name": "status"}
{"name": "status", "label": "Order Status"}
{"name": "customers.name"}
{"name": "customers.regions.name"}
```

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Dimension name. Supports dotted paths for joined models (auto-resolved via join graph). |
| `label` | string | Optional human-readable display name |

For computed columns (SQL expressions like CASE), use [ModelExtension](#modelextension) on the query's `source_model` field. For derived metrics, use [formulas](formulas.md) in `measures`.

Via MCP, simple dimensions are passed as strings: `dimensions=["status"]`

## TimeDimension

A time dimension with a required granularity and an optional date range. Supports an optional `label` for human-readable output. To use a time column without truncation, add it as a regular dimension instead.

```json
{
  "dimension": "created_at",
  "granularity": "month",
  "date_range": ["2024-01-01", "2024-12-31"],
  "label": "Order Month"
}
```

**Granularities**: `second`, `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`

## OrderItem

```json
{"column": "*:count", "direction": "desc"}
```

Via MCP: `{"column": "*:count", "direction": "desc"}`

## Response

Query results are returned as a `SlayerResponse`:

| Field | Type | Description |
|-------|------|-------------|
| `data` | list[dict] | Rows as dictionaries |
| `columns` | list[str] | Column names in `model_name.column_name` format (e.g., `"orders.count"`, `"orders.customers.regions.name"` for multi-hop) |
| `row_count` | int | Number of rows |
| `sql` | string | The generated SQL (useful for debugging) |
| `attributes` | ResponseAttributes | Field metadata split by type: `attributes.dimensions` and `attributes.measures`, each a dict of column alias → FieldMetadata (label, format) |

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
"filters": ["status = 'active'", "amount > 100"]
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
| `IS NULL` | `"discount IS NULL"` |
| `IS NOT NULL` | `"discount IS NOT NULL"` |
| `like` | `"name like '%acme%'"` |
| `not like` | `"name not like '%test%'"` |

### Boolean Logic

Use `and`, `or`, `not` within a single filter string:

```json
"filters": [
    "status = 'completed' or status = 'pending'",
    "amount > 100 and amount < 1000"
]
```

Multiple entries in the `filters` list are combined with AND.

### Filtering on Computed Columns

Filters can reference names of computed measures — transforms and arithmetic expressions defined in `measures`. These are applied as post-filters on the outer query, after all transforms are computed. Note: bare measure renames (e.g., `{"formula": "*:count", "name": "n"}`) are not post-filterable by name; use the original measure name instead.

```json
{
  "measures": [
    "revenue:sum",
    {"formula": "change(revenue:sum)", "name": "rev_change"}
  ],
  "filters": ["rev_change < 0"]
}
```

Transform expressions can also be used **directly in filters** without defining them as fields first:

```json
{
  "filters": ["last(change(revenue:sum)) < 0"]
}
```

Post-filters can be combined with regular filters — base filters (on dimensions/measures) are applied in the inner query, post-filters on the outer wrapper:

```json
{
  "filters": ["status = 'completed'", "change(revenue:sum) > 0"]
}
```

### Filter Variables

Filters support `{variable_name}` placeholders, substituted from the query's `variables` dict. This keeps filter templates reusable and avoids string concatenation in client code.

```json
{
  "source_model": "orders",
  "measures": ["*:count"],
  "filters": ["status = '{status}' AND amount > {min_amount}"],
  "variables": {"status": "completed", "min_amount": 100}
}
```

This produces the filter `status = 'completed' AND amount > 100`.

- Variable names must be alphanumeric + underscore (`[a-zA-Z_][a-zA-Z0-9_]*`)
- Values must be strings or numbers (inserted as-is — strings should be quoted in the filter template)
- `{{` and `}}` produce literal `{` and `}`
- Undefined variables raise an error

#### Variables passed as a runtime kwarg

Every execution entry point also accepts a `variables=` runtime kwarg that **always wins**, even over a stage's own `variables` dict and a query-backed model's `query_variables` defaults:

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
{"source_model": "orders", "measures": [{"formula": "*:count"}], "variables": {"region": "EU"}}
{"name": "monthly_revenue", "variables": {"region": "EU"}}  // run-by-name
```

Precedence (highest first):

1. Runtime kwarg (`variables=`)
2. Stage `SlayerQuery.variables`
3. Outer-query `.variables` (when a query-backed model is used as `source_model`)
4. Model defaults (`model.query_variables`)

Unknown kwarg variables (not referenced in any filter) are silently ignored.

## Run a saved query by name

If a model is **query-backed** (created via `create_model_from_query` or saved with `source_queries`), you can run its stored backing query directly:

```python
await engine.execute("monthly_revenue", variables={"region": "EU"})
```

This loads the model, runs its `source_queries` stages with the merged variables, and returns the final-stage result. Calling `execute(str)` on a non-query-backed model raises a clear error directing the user to wrap it in a `SlayerQuery` instead.

REST equivalent: `POST /query` with `{"name": "<model>", "variables": {...}}`. Run-by-name also accepts `dry_run` and `explain`; query-defining fields (`source_model`, `measures`, `dimensions`, `filters`, `time_dimensions`, `order`, `limit`, `offset`) are not allowed in this body shape.

CLI equivalent: `slayer query <model_name> [--variables k=v ...] [--dry-run] [--explain]` — when the positional argument doesn't look like JSON (doesn't start with `{` or `[`) and isn't a `@file` reference, it's interpreted as a model name.

MCP equivalent: `query(source_model="<model>", variables={...}, dry_run=True/False, explain=True/False)` — when only `source_model` (and optional flags) is supplied, the call dispatches through the run-by-name shortcut.

---

## Examples

### Count by status

```json
{
  "source_model": "orders",
  "measures": ["*:count"],
  "dimensions": ["status"]
}
```

### Monthly revenue with date range

```json
{
  "source_model": "orders",
  "measures": ["revenue:sum"],
  "time_dimensions": [{
    "dimension": "created_at",
    "granularity": "month",
    "date_range": ["2024-01-01", "2024-12-31"]
  }]
}
```

### Top 5 customers by revenue

```json
{
  "source_model": "orders",
  "measures": ["revenue:sum"],
  "dimensions": ["customer_name"],
  "order": [{"column": "revenue:sum", "direction": "desc"}],
  "limit": 5
}
```

### Filtered count with OR logic

```json
{
  "source_model": "orders",
  "measures": ["*:count"],
  "filters": ["status = 'completed' or status = 'pending'"]
}
```

### Derived columns with transforms

```json
{
  "source_model": "orders",
  "measures": [
    "*:count",
    "revenue:sum",
    {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
    {"formula": "cumsum(revenue:sum)", "name": "running"},
    {"formula": "change(revenue:sum)", "name": "mom_change"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

### Statistical aggregations

The `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, and `covar_pop` aggregations behave like the rest of the colon-syntax measures. `corr` / `covar_samp` / `covar_pop` are two-column — the second column rides as a named `other` parameter, the same way `weighted_avg` takes `weight`:

```json
{
  "source_model": "orders",
  "measures": [
    {"formula": "latency:stddev_samp", "name": "latency_sd"},
    {"formula": "latency:var_pop", "name": "latency_var_pop"},
    {"formula": "price:corr(other=quantity)", "name": "price_qty_corr"},
    {"formula": "price:covar_samp(other=quantity)", "name": "price_qty_cov"}
  ],
  "dimensions": [{"name": "status"}]
}
```

Edge cases match Postgres exactly:
- sample stddev/variance/covariance return NULL when N ≤ 1
- population stddev/variance/covariance return 0 at N = 1 and NULL at N = 0
- `corr` additionally returns NULL when either side has zero variance (covariance is well-defined in that case and just returns 0)

See [database-support.md](../database-support.md#aggregation-support) for the per-engine support matrix.

### Cross-model measures

When models have [joins](models.md#joins), you can reference measures from joined models using dotted syntax with colon aggregation — `model_name.measure_name:aggregation`:

```json
{
  "source_model": "orders",
  "measures": [
    "*:count",
    "customers.score:avg"
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

This generates a sub-query for the joined measure, scoped to shared dimensions, and LEFT JOINs it to the main query — avoiding aggregation errors from row multiplication.

### Query lists

Pass a list of queries to `execute()`. Earlier queries are named sub-queries, the last is the main query. Named queries can be referenced by `source_model` name or joined via `joins`:

```json
[
  {
    "name": "monthly",
    "source_model": "orders",
    "measures": ["*:count", "amount:sum"],
    "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
  },
  {
    "source_model": "monthly",
    "measures": ["*:count"]
  }
]
```

This counts how many months exist in the monthly summary. The main query references `"monthly"` by name — if a named query and a stored model share a name, the query takes precedence.

You can also join named queries to models:

```json
[
  {
    "name": "customer_scores",
    "source_model": "customers",
    "dimensions": ["id"],
    "measures": ["score:avg"]
  },
  {
    "source_model": {"source_name": "orders", "joins": [{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}]},
    "measures": ["*:count", "customer_scores.score_avg:avg"],
    "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
  }
]
```

The main query uses a `ModelExtension` to add a join to the named sub-query. Queries can also be saved as permanent models — see [Creating Models from Queries](models.md#creating-models-from-queries).

### ModelExtension

Extend a model inline with extra columns, measures, or joins — without modifying the stored model:

```json
{
  "source_model": {
    "source_name": "orders",
    "columns": [{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
    "joins": [{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}]
  },
  "dimensions": ["tier"],
  "measures": ["*:count"]
}
```

`ModelExtension` fields: `source_name` (required — model to extend), `columns`, `measures`, `joins` (all optional — merged with the source model's).

### Multi-hop dimensions

Dimensions from joined models can be referenced with dotted paths. SLayer auto-resolves multi-hop join chains by walking each intermediate model's own joins:

```json
{
  "source_model": "orders",
  "dimensions": ["customers.regions.name"],
  "measures": ["*:count"]
}
```

This walks `orders → customers → regions` via the join graph and resolves `name` from the `regions` model. Works with both ingested rollup models and explicit joins.

SQL dimensions can be mixed with regular dimensions. The expression goes directly into SELECT and GROUP BY.

