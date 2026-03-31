# Formulas

SLayer uses formula strings in two places: **fields** (data columns) and **filters** (conditions). Both are compiled to SQL — everything runs in the database. Field formulas are documented below; filter formulas are in [Queries — Filters](queries.md#filters).

---

## Field Formulas

Field formulas define what data columns a query returns. They go in the `fields` parameter:

```json
{"formula": "count"}
{"formula": "revenue / count", "name": "aov", "label": "Average Order Value"}
{"formula": "cumsum(revenue)"}
```

The `name` is optional — if omitted, it's auto-generated from the formula. The `label` is an optional human-readable display name for the field.

### Arithmetic Operators

| Operator | Example | SQL |
|----------|---------|-----|
| `+` | `"revenue + bonus"` | `revenue + bonus` |
| `-` | `"revenue - cost"` | `revenue - cost` |
| `*` | `"price * quantity"` | `price * quantity` |
| `/` | `"revenue / count"` | `revenue / count` |
| `**` | `"value ** 2"` | `value ** 2` |

Parentheses work as expected: `"(revenue - cost) / count"`.

All measure names referenced in the formula must exist in the model.

### Transform Functions

Functions apply window operations to measures:

| Function | Description | SQL Generated |
|----------|-------------|---------------|
| `cumsum(x)` | Running total over time | `SUM(x) OVER (ORDER BY time)` |
| `time_shift(x, n)` | Value N periods next or back | Self-join CTE on query granularity |
| `time_shift(x, offset, gran)` | Value from a different time bucket | Self-join CTE on given granularity |
| `lag(x, n)` | Value N rows back (window function) | `LAG(x, n) OVER (ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead (window function) | `LEAD(x, n) OVER (ORDER BY time)` |
| `change(x)` | Difference from previous period | Self-join CTE, `current - previous` |
| `change_pct(x)` | Percentage change from previous period | Self-join CTE, `(current - previous) / previous` |
| `rank(x)` | Ranking by value (descending) | `RANK() OVER (ORDER BY x DESC)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** Functions that order over time (`cumsum`, `time_shift`, `change`, `change_pct`, `last`, `lag`, `lead`) need a time dimension. With a single `time_dimensions` entry, it's used automatically. With 2+ time dimensions, specify query's `main_time_dimension` to disambiguate, or the model's `default_time_dimension` is used if it's among the query's time dimensions. `rank` does not need a time dimension.

**Self-join transforms vs window-function transforms:**

`time_shift`, `change`, and `change_pct` all use **self-join CTEs** internally. This means they can reach outside the current result set to fetch previous/next values — no edge NULLs when the database has the data, and correct handling of gaps in time series.

`lag(x, n)` and `lead(x, n)` use SQL `LAG`/`LEAD` window functions directly. They are more efficient but have two trade-offs:

- **Edge NULLs**: the first/last N rows always return NULL since window functions can only see rows within the current result set.
- **Gap sensitivity**: if there are missing time periods in your data, `lag` shifts by row position, not by logical period — so the "previous row" might not be the previous calendar period.

### Nesting

Field formulas support arbitrary nesting — functions can wrap other functions or arithmetic:

```json
{"formula": "change(cumsum(revenue))", "name": "cumsum_delta"}
{"formula": "last(change(cumsum(revenue)))"}
{"formula": "cumsum(revenue / count)", "name": "running_aov"}
{"formula": "cumsum(revenue) / count", "name": "cumsum_div_count"}
```

Use `show_sql=True` on the query to see what SQL is generated for complex formulas.

**Mathematical identity:** `cumsum(change(x)) == x - x[0]` for all rows after the first.

### Rank

`rank(x)` assigns a ranking to each row based on the measure value (highest = rank 1), using `RANK() OVER (ORDER BY x DESC)`. It does not need a time dimension and does not partition — it ranks across the **entire result set**.

The ranking granularity depends on the query's dimensions and time dimensions. Each unique combination of dimension values becomes one row, and rank orders those rows by the measure:

```json
{
  "model": "orders",
  "dimensions": [{"name": "customer_name"}],
  "fields": [
    {"formula": "revenue_sum"},
    {"formula": "rank(revenue_sum)", "name": "rnk"}
  ],
  "order": [{"column": {"name": "revenue_sum"}, "direction": "desc"}]
}
```

This ranks customers by total revenue. Combine with `limit` to get "top N":

```json
{
  "filters": ["rank(revenue_sum) <= 10"]
}
```

With multiple dimensions (e.g., `status` + `month`), each status/month combination is ranked together — there is no automatic partitioning by dimension.

Ties receive the same rank (standard SQL `RANK` behavior): if two rows tie at rank 2, the next row is rank 4.

### Last Function

`last(x)` is a window-function transform that takes an aggregated measure and **broadcasts the most recent time bucket's value to every row** in the result.

```json
{
  "fields": [
    {"formula": "revenue_sum"},
    {"formula": "last(revenue_sum)", "name": "latest_revenue"}
  ],
  "time_dimensions": [{"dimension": {"name": "created_at"}, "granularity": "month"}]
}
```

This returns monthly revenue with an extra column showing the most recent month's revenue on every row — useful for comparisons like "this month vs latest" or for filtering: `"last(change(revenue)) < 0"` keeps rows only if the trend is negative.

`last()` requires a time dimension with granularity in the query (same resolution as `time_shift`).

Not to be confused with the [`last` aggregation type](models.md#the-last-aggregation-type), which is a per-group aggregate returning the latest *record's* value within each bucket.

---

## Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **MeasureRef** — bare measure name (`"count"`)
- **ArithmeticField** — arithmetic on measures (`"revenue / count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue)"`)
- **MixedArithmeticField** — arithmetic containing function calls (`"cumsum(revenue) / count"`)

The query engine's `_enrich()` method processes field formulas into ordered enrichment steps, and the SQL generator translates them into stacked CTEs.
