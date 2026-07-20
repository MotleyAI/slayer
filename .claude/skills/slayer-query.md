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

**Dim-only queries deduplicate.** A query with no measures and at least one dimension or time-dimension auto-emits `GROUP BY <dim/td aliases>` and returns the distinct combinations. The `GROUP BY` is applied before `LIMIT`, so a row cap can't silently drop unique tuples. To opt out, set `"distinct_dimension_values": false` on the query — emits raw rows (no top-level `GROUP BY`), with WHERE / ORDER BY / LIMIT applied as usual. Any measure reference in `measures` / `filters` / `order` raises `DistinctDimensionValuesError` in this mode.

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
  "rank(revenue:sum)",
  "round(revenue:sum, 2)",
  "abs(revenue:sum - cost:sum)"
]
```

Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `count_distinct_approx`, `first`, `last`, `weighted_avg`, `median`, `percentile`, `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`. `count_distinct_approx` is dialect-aware (native approximate-distinct where available, exact `COUNT(DISTINCT)` fallback otherwise). Two-column `corr`/`covar_samp`/`covar_pop` take the second column as a named param: `price:corr(other=quantity)`. `sum` and `avg` accept an optional trailing-window: `revenue:sum(window='30d')`.

For month-over-month / period-over-period growth use `change_pct(x)` (absolute delta: `change(x)`) — both are calendar-aware and partition-safe (the underlying self-join matches on all non-time dimensions, so per-group series reset cleanly). Reach for `time_shift` only when you need the shifted value itself as a term in custom arithmetic or at a different grain (`time_shift(revenue:sum, -1, 'year')` for year-over-year).

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

**String-hygiene scalars** (DEV-1378, lowercase only): `lower`, `upper`, `trim`, `replace`, `substr`, `instr`, `length`, `concat`. Plus the SQL `||` operator (folded into `concat(...)`). Examples: `"lower(status) = 'active'"`, `"length(replace(x, ',', '')) > 0"`, `"substr(s, 1, instr(s, ',') - 1) = 'first'"`, `"first || ' ' || last = 'jane doe'"`. Calls outside this allowlist (`json_extract`, `coalesce`, …) belong in `Column.sql` / `Column.filter` / `SlayerModel.filters` (Mode A SQL), not query filters.

**Filtering on computed measures**: `"change(revenue:sum) > 0"`, `"last(change(revenue:sum)) < 0"`. Applied as post-filters on the outer query.

**Top-N filtering**: use `"rank(<measure>) <= N"` (e.g. `"rank(revenue:sum) <= 10"`) — dialect-portable and auto-promoted to a post-filter on the outer query. Raw `OVER (...)` SQL inside a filter or `ModelMeasure.formula` is rejected with an actionable error. Filtering on a `Column` whose `sql` contains a window function is also rejected (DEV-1369): use `rank()` / `dense_rank()` / `percent_rank()` / `ntile(n=<N>)` for top-N, or factor the windowed expression into an earlier stage of a multi-stage `source_queries` model.

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

A dotted reference may target a *derived* column on the joined model (a column whose own `sql` is itself an expression). The engine recursively inlines the chain at query time — `"B.foo_normalized:sum"` where `B.foo_normalized.sql = "foo_raw / 100.0"` emits `SUM(B.foo_raw / 100.0)`. The same chaining works inside `Column.sql`, `filters`, and `dimensions`. When a filter names a *bare* local derived column whose SQL crosses a join (e.g. `Column(name="is_eu", sql="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END")` referenced as `"filters": ["is_eu = 1"]`), the planner walks the column's chain and adds the joins the chain implies — no need to also list the column in `dimensions`.

## Picking the root model

Not sure which model to use as `source_model` for a set of columns/metrics? Call `recommend_root_model` with the `model.column` / `model.metric` items you want; it introspects the join graph and returns the recommended root plus each item's join-qualified path from it (aggregation suffixes preserved), ready to drop into a query.

```python
rec = client.recommend_root_model_sync(["customers.name", "products.category"])
rec.root_model  # "orders"
[ip.path for ip in rec.item_paths]  # ["customers.name", "products.category"]
```

Pass `root_hint` (a bare model name or `<data_source>.<model>`) to force an intended root — useful when the host is a bridge model that owns none of the items but matches your grain. It's honored when it reaches every item; otherwise the auto-pick is used and `warnings` says why.

MCP: `recommend_root_model(items, data_source=None, root_hint=None, format="markdown")`. If no single model reaches every item, `root_model` is `None` and `coverage` lists the best partial roots — a hint to split into a multi-stage `source_queries` query.

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

Order doesn't matter for runtime lists — the engine auto-sorts so every stage appears after the siblings it references. The **last entry stays last** as the entry point. Cycles, self-references, and a non-final stage referencing the root are rejected; unreachable utility stages are accepted (silently dropped from the emitted SQL).

Surfaces: Python SDK `engine.execute(query=[...])`; CLI `slayer query @file.json` (accepts both single object and top-level list); MCP `query_nested(queries=[...])`; REST `POST /query` with body `{"queries": [...], "variables": {...}, "dry_run": ..., "explain": ...}` (the single-query body shape is also still accepted). The single-stage MCP `query` tool stays single-query only — use it when the typed per-field schema fits a one-shot query. `SlayerModel.source_queries` itself keeps strict top-to-bottom order; runtime lists are the only DAG-auto-sort surface.

## Result format

Column keys use `model_name.column_name` format: `"orders._count"`, `"orders.revenue_sum"`. For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`. An explicit `name` on a measure spec swaps the canonical leaf — local (`{"formula": "amount:sum", "name": "rev"}` → `"orders.rev"`) or cross-model (`{"formula": "customers.revenue:sum", "name": "cust_rev"}` → `"orders.customers.cust_rev"`, hop path preserved). In any downstream stage of a `query_nested` DAG the column is exposed under the bare `name` (e.g. `cust_rev`) — that's what you type in stage 2's `formula` to reference the value. The response also includes `attributes` — a `ResponseAttributes` object with `.dimensions` and `.measures` dicts, each mapping column alias → `FieldMetadata` (label, format).

## Strict validation (v3)

`SlayerQuery` v3 sets `extra="forbid"`. Misspelled field names raise a `ValidationError` instead of being silently dropped — typo `dimensios` will not become an empty `dimensions` list.
