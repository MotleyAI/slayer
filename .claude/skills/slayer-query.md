---
description: How to construct and execute SLayer queries. Use when building queries with measures, dimensions, filters, time dimensions.
---

# Querying with SLayer

A `SlayerQuery` is a JSON/dict object. The same shape works across the REST API, MCP tools, the CLI, and the Python SDK — pick whichever matches your interface.

## Query Structure

```json
{
  "source_model": "orders",
  "measures": ["*:count", "revenue:sum"],
  "dimensions": ["status"],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
  "filters": ["status = 'active'"],
  "order": [{"column": "count", "direction": "desc"}],
  "limit": 10
}
```

`order[].column` is the short alias (`count`, `revenue_sum`) — not the colon form.

## Measures — colon aggregation

Each entry in `measures` is either a bare formula string or a `{"formula": ..., "name": ..., "label": ...}` dict. Aggregation is chosen at query time using **colon syntax**:

```json
"measures": [
  "*:count",
  "revenue:sum",
  "revenue:avg",
  "price:weighted_avg(weight=quantity)",
  {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
  "cumsum(revenue:sum)",
  "change_pct(revenue:sum)",
  "last(revenue:sum)",
  "time_shift(revenue:sum, -1, 'year')",
  "lag(revenue:sum, 1)",
  "rank(revenue:sum)"
]
```

Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`. Two-column `corr`/`covar_samp`/`covar_pop` take the second column as a named param: `price:corr(other=quantity)`. `sum` and `avg` accept an optional trailing-window: `revenue:sum(window='30d')`.

`*:count` is always available — no column definition needed. `col:count` counts non-nulls.

Saved named formulas (`SlayerModel.measures`) can be referenced by bare name in any formula context: `{"formula": "aov"}`.

Result column naming: `revenue:sum` → `orders.revenue_sum` (colon becomes underscore). `*:count` → `orders._count` (the leading `_` distinguishes it from any user-defined column literally named `count`). An explicit `name` on the measure spec overrides the canonical form: `{"formula": "amount:sum", "name": "rev"}` → `orders.rev`. Multi-stage `source_queries` rely on this — downstream stages reference inner-stage outputs by the chosen name.

## Filters

```json
"filters": [
  "status = 'active'",
  "amount > 100",
  "status = 'completed' OR status = 'pending'"
]
```

**Operators**: `=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`

**Boolean logic**: `AND`, `OR`, `NOT`

**Filtering on computed measures**: `"change(revenue:sum) > 0"`, `"last(change(revenue:sum)) < 0"`. Applied as post-filters on the outer query.

**Variable substitution**: `{var}` placeholders in filter strings are substituted from the query's `variables` dict (or per-model defaults). Use `{{`/`}}` for literal braces.

## Executing

`SlayerQueryEngine.execute(...)` is **async**. Use `await` from async code, or call `execute_sync(...)` from CLIs / notebooks / scripts.

```python
engine = SlayerQueryEngine(storage=storage)

# Async (most callers — REST/MCP):
result = await engine.execute(query=query)  # SlayerResponse with .data, .columns, .row_count, .sql, .attributes

# With runtime variables (highest precedence — wins over query.variables / model defaults):
result = await engine.execute(query=query, variables={"region": "US"})

# Plan-only modes are engine kwargs (v3) — no longer fields on the query body:
result = await engine.execute(query=query, dry_run=True)
result = await engine.execute(query=query, explain=True)

# Run-by-name: execute the stored backing query of a query-backed model.
result = await engine.execute("monthly_revenue", variables={"region": "US"})
result = await engine.execute("monthly_revenue", dry_run=True)

# Sync wrapper (use from CLIs / notebooks; not from running event loops):
result = engine.execute_sync(query=query)
```

Variable precedence (highest first): `runtime kwarg > stage.variables > outer query.variables > model.query_variables`. Runtime kwargs are merged into the available variable set; extra keys simply remain unused if the query does not reference them. Unresolved `{var}` placeholders raise at execute time, naming the model and stage.

## Cross-model measures

Reference measures from joined models with dotted syntax + colon aggregation:

```json
"measures": [
  "*:count",
  "customers.score:avg",
  "cumsum(customers.score:avg)",
  "customers.regions.population:sum"
]
```

## ModelExtension

Extend a model inline with extra columns, named-formula measures, joins, or filters. The stored model is not modified:

```json
{
  "source_model": {
    "source_name": "orders",
    "columns": [
      {"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END", "type": "string"}
    ]
  },
  "dimensions": ["tier"],
  "measures": ["*:count"]
}
```

Allowed `ModelExtension` keys: `source_name` (required), `columns`, `measures`, `joins`, `filters`.

## Query lists

Pass a list of queries — earlier queries are named sub-queries; the last is the main one whose result is returned:

```json
[
  {
    "name": "monthly",
    "source_model": "orders",
    "measures": ["*:count", "revenue:sum"],
    "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
  },
  {
    "source_model": "monthly",
    "measures": ["*:count"]
  }
]
```

## Result format

Column keys use `model_name.column_name` format: `"orders._count"`, `"orders.revenue_sum"`. For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`. The response also includes `attributes` — a `ResponseAttributes` object with `.dimensions` and `.measures` dicts, each mapping column alias → `FieldMetadata` (label, format).

## Strict validation (v3)

`SlayerQuery` v3 sets `extra="forbid"`. Misspelled field names raise a `ValidationError` instead of being silently dropped — typo `dimensios` will not become an empty `dimensions` list.
