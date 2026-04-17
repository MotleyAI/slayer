---
description: How to construct and execute SLayer queries. Use when building queries with fields, dimensions, filters, time dimensions.
---

# Querying with SLayer

## SlayerQuery Structure

```python
from slayer.core.query import SlayerQuery

query = SlayerQuery(
    source_model="orders",
    fields=["*:count", "revenue:sum"],
    dimensions=["status"],
    time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
    filters=["status = 'active'"],
    order=[{"column": "*:count", "direction": "desc"}],
    limit=10,
)
```

## Fields — Measures with Colon Aggregation

Measures are row-level expressions; aggregation is chosen at query time with **colon syntax**:

```python
fields=[
    "*:count",                          # COUNT(*)
    "revenue:sum",                      # SUM(revenue)
    "revenue:avg",                      # AVG(revenue)
    "price:weighted_avg(weight=quantity)",  # weighted average
    {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
    "cumsum(revenue:sum)",              # running total
    "change_pct(revenue:sum)",          # month-over-month growth
    "last(revenue:sum)",               # most recent period's value
    "time_shift(revenue:sum, -1, 'year')",  # year-over-year
    "lag(revenue:sum, 1)",             # previous row (window function)
    "rank(revenue:sum)",               # ranking
]
```

Built-in aggregations: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `first`, `last`, `weighted_avg`, `median`, `percentile`.

`*:count` is always available — no measure definition needed. `col:count` counts non-nulls.

Result column naming: `revenue:sum` → `orders.revenue_sum` (colon becomes underscore). `*:count` → `orders.count`.

## Filters

```python
filters=[
    "status = 'active'",
    "amount > 100",
    "status = 'completed' OR status = 'pending'",
]
```

**Operators**: `=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`

**Boolean logic**: `AND`, `OR`, `NOT`

**Filtering on computed columns**: `"change(revenue:sum) > 0"`, `"last(change(revenue:sum)) < 0"`. Applied as post-filters on the outer query.

## Executing

```python
engine = SlayerQueryEngine(storage=storage)
result = engine.execute(query=query)  # SlayerResponse with .data, .columns, .row_count, .sql, .attributes
```

## Cross-Model Measures

Reference measures from joined models with dotted syntax + colon aggregation:

```python
fields=[
    "*:count",
    "customers.score:avg",                  # single hop
    "cumsum(customers.score:avg)",          # transforms work too
    "customers.regions.population:sum",    # multi-hop
]
```

## ModelExtension

Extend a model inline with extra dimensions, measures, or joins:

```python
query = SlayerQuery(
    source_model=ModelExtension(
        source_name="orders",
        dimensions=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
    ),
    dimensions=["tier"],
    fields=["*:count"],
)
```

## Query Lists

Pass a list of queries — earlier queries are named sub-queries, last is the main:

```python
inner = SlayerQuery(name="monthly", source_model="orders", fields=["*:count", "revenue:sum"], time_dimensions=[...])
outer = SlayerQuery(source_model="monthly", fields=["*:count"])
engine.execute(query=[inner, outer])
```

## Result Format

Column keys use `model_name.column_name` format: `"orders.count"`, `"orders.revenue_sum"`. For multi-hop joined dimensions, the full path is included: `"orders.customers.regions.name"`. Response includes `attributes` with nested `dimensions` and `measures` dicts, each mapping column aliases to `FieldMetadata` objects (label, format).
