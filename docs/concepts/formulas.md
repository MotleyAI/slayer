# Formulas

SLayer uses formula strings in two places: **fields** (data columns) and **filters** (conditions). Both are compiled to SQL — everything runs in the database. Field formulas are documented below; filter formulas are in [Queries — Filters](queries.md#filters).

---

## Colon Syntax

Measures and aggregations are separate concepts in SLayer. Measures are named row-level expressions defined on a model. Aggregation is specified at query time using **colon syntax**: `measure_name:aggregation`.

```
revenue:sum          — SUM the "revenue" measure
*:count              — COUNT(*), always available, no measure definition needed
revenue:avg          — AVG the "revenue" measure
price:weighted_avg(weight=quantity)  — weighted average with kwargs
customers.score:avg  — cross-model: AVG of "score" from the joined "customers" model
```

Colon syntax is used everywhere measures appear: in `fields`, in arithmetic expressions, in transform function arguments, and in filters.

---

## Field Formulas

Field formulas define what data columns a query returns. They go in the `fields` parameter:

```json
"fields": [
  "*:count",
  {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
  "cumsum(revenue:sum)",
  ...
]
```

The `name` is optional — if omitted, it's auto-generated from the formula. The `label` is an optional human-readable display name for the field.

### Arithmetic Operators

| Operator | Example | SQL |
|----------|---------|-----|
| `+` | `"revenue:sum + bonus:sum"` | `SUM(revenue) + SUM(bonus)` |
| `-` | `"revenue:sum - cost:sum"` | `SUM(revenue) - SUM(cost)` |
| `*` | `"price:avg * quantity:sum"` | `AVG(price) * SUM(quantity)` |
| `/` | `"revenue:sum / *:count"` | `SUM(revenue) / COUNT(*)` |
| `**` | `"value:sum ** 2"` | `SUM(value) ** 2` |

Parentheses work as expected: `"(revenue:sum - cost:sum) / *:count"`.

All measure names referenced in the formula must exist in the model (except `*` which is always available). For measures from joined models, use dotted syntax with colon aggregation: `"customers.score:avg"` or multi-hop: `"customers.regions.population:sum"`. Joins are auto-resolved by walking the join graph. See [Cross-Model Measures](queries.md#cross-model-measures).

Transforms work on cross-model measures: `"cumsum(customers.score:avg)"`, `"first(customers.score:avg)"`, `"last(customers.score:avg)"`. The cross-model measure is computed first (as a sub-query CTE), then the transform is applied on the joined result.

### Transform Functions

Functions apply window operations to measures:

| Function | Description | SQL Generated |
|----------|-------------|---------------|
| `cumsum(x)` | Running total over time | `SUM(x) OVER (ORDER BY time)` |
| `time_shift(x, n)` | Value N periods back/ahead | Self-join CTE with INTERVAL offset |
| `time_shift(x, offset, gran)` | Value from a different time bucket | Self-join CTE with INTERVAL offset |
| `lag(x, n)` | Value N rows back (window function) | `LAG(x, n) OVER (ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead (window function) | `LEAD(x, n) OVER (ORDER BY time)` |
| `change(x)` | Difference from previous period | Desugars to `x - time_shift(x, -1)` |
| `change_pct(x)` | Percentage change from previous | Desugars to `(x - ts) / ts` where `ts = time_shift(x, -1)` |
| `rank(x)` | Ranking by value (descending) | `RANK() OVER (ORDER BY x DESC)` |
| `first(x)` | Earliest time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time ASC ...)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** All time-ordered transforms (`cumsum`, `time_shift`, `change`, `change_pct`, `first`, `last`, `lag`, `lead`) require an explicit `time_dimensions` entry in the query. With a single entry, it's used automatically. With 2+ time dimensions, specify the query's `main_time_dimension` to disambiguate, or the model's `default_time_dimension` is used if it's among the query's time dimensions. `rank` does not need a time dimension.

**Self-join transforms vs window-function transforms:**

`time_shift` uses a **self-join CTE** with an INTERVAL-shifted time column. `change` and `change_pct` are desugared into a hidden `time_shift` + arithmetic expression at query enrichment time. The shifted sub-query applies the time offset everywhere (WHERE, GROUP BY, SELECT), so it can reach outside the current result set — no edge NULLs when the database has the data, and correct handling of gaps in time series.

`lag(x, n)` and `lead(x, n)` use SQL `LAG`/`LEAD` window functions directly. They are more efficient but have two trade-offs:

- **Edge NULLs**: the first/last N rows always return NULL since window functions can only see rows within the current result set.
- **Gap sensitivity**: if there are missing time periods in your data, `lag` shifts by row position, not by logical period — so the "previous row" might not be the previous calendar period.

### Nesting

Field formulas support nesting — window transforms can wrap self-join transforms (but not vice versa):

```json
"fields": [
  {"formula": "cumsum(change(revenue:sum))", "name": "cumsum_delta"},
  "last(change(revenue:sum))",
  {"formula": "cumsum(revenue:sum / *:count)", "name": "running_aov"},
  {"formula": "cumsum(revenue:sum) / *:count", "name": "cumsum_div_count"}
]
```

Use `show_sql=True` on the query to see what SQL is generated for complex formulas.

**Mathematical identity:** `cumsum(change(x)) == x - x[0]` for all rows after the first.

### Rank

`rank(x)` assigns a ranking to each row based on the measure value (highest = rank 1), using `RANK() OVER (ORDER BY x DESC)`. It does not need a time dimension and does not partition — it ranks across the **entire result set**.

The ranking granularity depends on the query's dimensions and time dimensions. Each unique combination of dimension values becomes one row, and rank orders those rows by the measure:

```json
{
  "source_model": "orders",
  "dimensions": ["customer_name"],
  "fields": [
    "revenue:sum",
    {"formula": "rank(revenue:sum)", "name": "rnk"}
  ],
  "order": [{"column": "revenue:sum", "direction": "desc"}]
}
```

This ranks customers by total revenue. Combine with `limit` to get "top N":

```json
{
  ...
  "filters": ["rank(revenue:sum) <= 10"]
}
```

With multiple dimensions (e.g., `status` + `month`), each status/month combination is ranked together — there is no automatic partitioning by dimension.

Ties receive the same rank (standard SQL `RANK` behavior): if two rows tie at rank 2, the next row is rank 4.

### First and Last Functions

`first(x)` and `last(x)` are window-function transforms that take an aggregated measure and **broadcast a single time bucket's value to every row** in the result. `first()` broadcasts the **earliest** bucket's value; `last()` broadcasts the **most recent** bucket's value.

```json
{
  "source_model": "orders",
  "fields": [
    "revenue:sum",
    {"formula": "first(revenue:sum)", "name": "initial_revenue"},
    {"formula": "last(revenue:sum)", "name": "latest_revenue"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

This returns monthly revenue with extra columns showing the first and last month's revenue on every row — useful for comparisons like "this month vs initial/latest" or for filtering: `"last(change(revenue:sum)) < 0"` keeps rows only if the trend is negative.

Both `first()` and `last()` require a time dimension with granularity in the query (same resolution as `time_shift`).

Not to be confused with the [`first`/`last` aggregation types](models.md#the-last-aggregation-type), which are per-group aggregates returning the earliest/latest *record's* value within each bucket.

---

## Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **AggregatedMeasureRef** — measure with colon aggregation (`"revenue:sum"`, `"*:count"`)
- **ArithmeticField** — arithmetic on aggregated measures (`"revenue:sum / *:count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue:sum)"`)
- **MixedArithmeticField** — arithmetic containing function calls (`"cumsum(revenue:sum) / *:count"`)

The query engine's `_enrich()` method processes field formulas into ordered enrichment steps, and the SQL generator translates them into stacked CTEs.
