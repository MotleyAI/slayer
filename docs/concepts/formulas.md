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
| `time_shift(x, -1)` | Previous period's value | Self-join CTE on row number |
| `time_shift(x, -n)` | Value N periods back | Self-join CTE on row number |
| `time_shift(x, 1)` | Next period's value | Self-join CTE on row number |
| `time_shift(x, offset, gran)` | Value from a different calendar time bucket | Self-join CTE on date arithmetic |
| `lag(x, n)` | Value N rows back (window function) | `LAG(x, n) OVER (ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead (window function) | `LEAD(x, n) OVER (ORDER BY time)` |
| `change(x)` | Difference from previous period | Self-join CTE (current - previous) |
| `change_pct(x)` | Percentage change from previous period | Self-join CTE ((current - previous) / previous) |
| `rank(x)` | Ranking by value (descending) | `RANK() OVER (ORDER BY x DESC)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** Functions that order over time (`cumsum`, `time_shift`, `change`, `change_pct`, `last`, `lag`, `lead`) need a time dimension, resolved via: query's `main_time_dimension` → query's `time_dimensions` (if exactly one) → model's `default_time_dimension` → error. `rank` does not need a time dimension.

**Self-join transforms vs window-function transforms:**

`time_shift`, `change`, and `change_pct` all use self-join CTEs internally. This means they can reach outside the current result set to fetch previous/next values — no edge NULLs when the database has the data, and correct handling of gaps in time series.

- `time_shift(revenue, -1)` — previous period's value (ROW_NUMBER-based self-join)
- `time_shift(revenue, -1, 'year')` — value from a different calendar time bucket (date-arithmetic self-join). Supported granularities: `year`, `month`, `quarter`, `week`, `day`.
- `change(revenue)` — difference from previous period (`current - previous`, self-join)
- `change_pct(revenue)` — percentage change from previous period (`(current - previous) / previous`, self-join)

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

---

## Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **MeasureRef** — bare measure name (`"count"`)
- **ArithmeticField** — arithmetic on measures (`"revenue / count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue)"`)
- **MixedArithmeticField** — arithmetic containing function calls (`"cumsum(revenue) / count"`)

The query engine's `_enrich()` method processes field formulas into ordered enrichment steps, and the SQL generator translates them into stacked CTEs.
