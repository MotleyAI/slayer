# Formulas

SLayer uses formula strings in two places: **fields** (data columns) and **filters** (conditions). Both are compiled to SQL — everything runs in the database.

---

## Field Formulas

Field formulas define what data columns a query returns. They go in the `fields` parameter:

```json
{"formula": "count"}
{"formula": "revenue / count", "name": "aov"}
{"formula": "cumsum(revenue)"}
```

The `name` is optional — if omitted, it's auto-generated from the formula.

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
| `lag(x)` | Previous row's value | `LAG(x, 1) OVER (ORDER BY time)` |
| `lag(x, n)` | Value N rows back | `LAG(x, n) OVER (ORDER BY time)` |
| `lead(x)` | Next row's value | `LEAD(x, 1) OVER (ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead | `LEAD(x, n) OVER (ORDER BY time)` |
| `change(x)` | Difference from previous row | `x - LAG(x) OVER (ORDER BY time)` |
| `change_pct(x)` | Percentage change from previous row | `CASE WHEN LAG(x) != 0 THEN (x - LAG(x)) / LAG(x) END` |
| `rank(x)` | Ranking by value (descending) | `RANK() OVER (ORDER BY x DESC)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |
| `time_shift(x, offset, gran)` | Value from a different calendar time bucket | Self-join CTE |

**Time dimension requirement:** Functions that order over time (`cumsum`, `lag`, `lead`, `change`, `change_pct`, `last`, `time_shift`) need a time dimension, resolved via: query's `time_dimensions` (if exactly one) → model's `default_time_dimension` → error. `rank` does not need a time dimension.

**`time_shift` details:** `time_shift(revenue, -1, 'year')` compares the current time bucket to the matching time bucket in a different year (e.g., January 2024 → January 2023). Supported granularities: `year`, `month`, `quarter`, `week`, `day`. If the matching time bucket doesn't exist (gaps in data), the result is NULL.

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

## Filter Formulas

Filter formulas define conditions for the query. They go in the `filters` parameter as plain strings:

```json
"filters": ["status == 'active'", "amount > 100"]
```

### Comparison Operators

| Operator | Example |
|----------|---------|
| `==` | `"status == 'active'"` |
| `!=` | `"status != 'cancelled'"` |
| `>` | `"amount > 100"` |
| `>=` | `"amount >= 100"` |
| `<` | `"amount < 1000"` |
| `<=` | `"amount <= 1000"` |
| `in` | `"status in ('active', 'pending')"` |
| `is None` | `"discount is None"` (IS NULL) |
| `is not None` | `"discount is not None"` (IS NOT NULL) |

### Boolean Logic

Use `and`, `or`, `not` within a single filter string:

```json
"filters": [
    "status == 'completed' or status == 'pending'",
    "amount > 100 and amount < 1000"
]
```

Multiple entries in the `filters` list are combined with AND.

### Filter Functions

| Function | Example | SQL |
|----------|---------|-----|
| `contains(col, val)` | `"contains(name, 'acme')"` | `name LIKE '%acme%'` |
| `starts_with(col, val)` | `"starts_with(name, 'A')"` | `name LIKE 'A%'` |
| `ends_with(col, val)` | `"ends_with(email, '.com')"` | `email LIKE '%.com'` |
| `between(col, low, high)` | `"between(amount, 100, 500)"` | `amount BETWEEN 100 AND 500` |
| `having(expr)` | `"having(count > 10)"` | Filters on aggregated values (HAVING clause) |

### Variables

Use `{var_name}` placeholders in filter strings, resolved from `variables`:

```json
{
    "filters": ["status == '{status}'"],
    "variables": {"status": "completed"}
}
```

---

## Shared: Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **MeasureRef** — bare measure name (`"count"`)
- **ArithmeticField** — arithmetic on measures (`"revenue / count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue)"`)
- **MixedArithmeticField** — arithmetic containing function calls (`"cumsum(revenue) / count"`)

**Filter formulas** are classified into:

- **ParsedFilter** — a SQL-ready condition string with column references and a `is_having` flag

The query engine's `_enrich()` method processes field formulas into ordered enrichment steps, and the SQL generator translates them into stacked CTEs. Filter formulas are parsed into `ParsedFilter` objects and column names are qualified with the model name during SQL generation.
