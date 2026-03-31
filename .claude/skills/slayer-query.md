---
description: How to construct and execute SLayer queries. Use when building queries with fields, dimensions, filters, time dimensions.
---

# Querying with SLayer

## SlayerQuery Structure

```python
from slayer.core.query import SlayerQuery, ColumnRef, TimeDimension, OrderItem

query = SlayerQuery(
    model="orders",                                    # Target model name
    fields=[{"formula": "count"}, {"formula": "revenue"}],
    dimensions=[ColumnRef(name="status")],
    time_dimensions=[
        TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,         # Required
            date_range=["2024-01-01", "2024-12-31"],   # Optional
        )
    ],
    filters=[
        "status == 'active'",
    ],
    order=[OrderItem(column=ColumnRef(name="count", model="orders"), direction="desc")],
    limit=10,
    offset=0,

    whole_periods_only=False,                       # Optional: snap date filters to time bucket boundaries
)
```

## ColumnRef

- `ColumnRef(name="status")` — column in the query's model
- `ColumnRef(name="status", label="Order Status")` — with optional human-readable label
- `ColumnRef.from_string("orders.status")` — parse from dotted string

## Filters

Filters are simple formula strings passed as `List[str]`:

```python
filters=[
    "status == 'active'",
    "amount > 100",
    "status == 'completed' or status == 'pending'",
]
```

**Operators**: `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `is None`, `is not None`

**Boolean logic**: `and`, `or`, `not` within a single string

**Pattern matching**: `like` and `not like` operators (e.g., `"name like '%acme%'"`, `"name not like '%test%'"`). Filters on measures are automatically routed to HAVING.

**Filtering on computed columns**: filters can reference field names from `fields` (e.g., `"rev_change < 0"`) or contain inline transform expressions (e.g., `"last(change(revenue)) < 0"`). These are applied as post-filters on the outer query.

## Executing

```python
# Via engine directly
engine = SlayerQueryEngine(storage=storage)
result = engine.execute(query=query)  # SlayerResponse with .data, .columns, .row_count, .sql

# Via client (remote)
client = SlayerClient(url="http://localhost:5143")
df = client.query_df(query)

# Via client (local, no server)
client = SlayerClient(storage=YAMLStorage(base_dir="./models"))
data = client.query(query)
```

## Fields

The `fields` parameter specifies what data columns to return. Each field has a `formula` (required), optional `name`, and optional `label` (human-readable display name). Formulas are parsed by `slayer/core/formula.py`.

```python
query = SlayerQuery(
    model="orders",
    time_dimensions=[TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH)],
    fields=[
        {"formula": "count"},
        {"formula": "revenue_sum"},
        {"formula": "revenue_sum / count", "name": "aov", "label": "Average Order Value"},
        {"formula": "cumsum(revenue_sum)"},
        {"formula": "change_pct(revenue_sum)"},
        {"formula": "last(revenue_sum)", "name": "latest_rev"},
        {"formula": "time_shift(revenue_sum, -1, 'year')", "name": "rev_last_year"},
        {"formula": "time_shift(revenue_sum, -2)", "name": "rev_2_ago"},
        {"formula": "lag(revenue_sum, 1)", "name": "rev_prev_row"},
        {"formula": "rank(revenue_sum)"},
    ],
)
```

Available formula functions: `cumsum`, `time_shift`, `change`, `change_pct`, `rank`, `last`, `lag`, `lead`. `time_shift` always uses a self-join CTE — no edge NULLs, handles data gaps correctly. `lag(x, n)` / `lead(x, n)` use SQL window functions directly (more efficient, but NULLs at edges).

Time dimension resolution: single `time_dimensions` entry is used automatically. With 2+, `main_time_dimension` disambiguates (or model's `default_time_dimension` if among query's time dims). With none, falls back to model default.

## Result Format

Column keys use `model_name.column_name` format: `"orders.count"`, `"orders.status"`.
