# Formulas

SLayer uses formula strings in two places: **measures** (data columns returned by a query) and **filters** (conditions). Both are compiled to SQL — everything runs in the database. Measure formulas are documented below; filter formulas are in [Queries — Filters](queries.md#filters).

---

## Colon Syntax

Measures and aggregations are separate concepts in SLayer. Measures are named row-level expressions defined on a model. Aggregation is specified at query time using **colon syntax**: `measure_name:aggregation`.

```
revenue:sum          — SUM the "revenue" measure
*:count              — COUNT(*), always available, no measure definition needed
revenue:avg          — AVG the "revenue" measure
revenue:sum(window='90d')  — trailing 90-day SUM ending at each output bucket
price:weighted_avg(weight=quantity)  — weighted average with kwargs
latency:stddev_samp  — sample standard deviation
latency:var_pop      — population variance
price:corr(other=quantity)  — Pearson correlation between two columns (named `other` kwarg)
price:covar_samp(other=quantity)  — sample covariance (Bessel-corrected)
price:covar_pop(other=quantity)   — population covariance
customers.score:avg  — cross-model: AVG of "score" from the joined "customers" model
```

Colon syntax is used everywhere measures appear: in `measures`, in arithmetic expressions, in transform function arguments, and in filters.

### Windowed sum and average

`sum` and `avg` accept an optional `window` parameter for trailing time-window
aggregations:

```json
{
  "measures": [
    {"formula": "revenue:sum(window='30d')", "name": "revenue_30d"},
    {"formula": "price:avg(window='1y')", "name": "avg_price_1y"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

The window is measured against the raw time dimension, not just the output
rows. For each output bucket, SLayer aggregates source rows in the trailing
interval ending at that bucket's end. This means the window can be larger than
the query granularity (overlapping windows), equal to it (equivalent to normal
`sum`/`avg` for that bucket), or smaller than it (only the trailing part of each
bucket is included).

Window sizes use compact duration syntax:

| Unit | Meaning |
|------|---------|
| `y` | years |
| `m` | months |
| `w` | weeks |
| `d` | days |
| `h` | hours |
| `min` | minutes |
| `s` | seconds |

Units can be combined in descending or practical order, for example
`'1y2m3w5d6h7min8s'`, `'90d'`, `'6h'`, or `'15min'`. Quote the duration value
inside the formula.

---

## Field Formulas

Measure formulas define what aggregated values a query returns. They go in the `measures` parameter:

```json
"measures": [
  "*:count",
  {"formula": "revenue:sum / *:count", "name": "aov", "label": "Average Order Value"},
  "cumsum(revenue:sum)",
  ...
]
```

The `name` is optional — if omitted, it's auto-generated from the formula. The `label` is an optional human-readable display name for the field.

When a measure is renamed via `name`, query filters and ORDER BY entries in the same node accept either form — the raw colon formula or the user alias. See [Filters → Filtering on Computed Columns](queries.md#filtering-on-computed-columns).

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

### Saved Formulas (Named Measures)

A model can carry a library of named formulas in `model.measures`. Queries can reference these by **bare name** in their own measure formulas — root, inside transforms, or inside arithmetic:

```yaml
# model definition
measures:
  - {name: aov, formula: "revenue:sum / *:count", label: "Average Order Value"}
  - {name: aov_pct_change, formula: "change_pct(aov)"}
```

All three forms below — bare name, transform, arithmetic — work as query measures:

```json
"measures": [
  {"formula": "aov"},
  {"formula": "cumsum(aov)"},
  {"formula": "aov * 1.1", "name": "aov_with_markup"}
]
```

Bare-name references are inline-expanded at parse time into the saved formula's text, so queries that use the saved name produce the same SQL as queries with the formula written out longhand. Saved formulas can reference other saved formulas (transitively) — cycles like `a → b → a` are detected and rejected with the chain in the error message.

The bare name resolves only when it appears as a standalone identifier — a name that's part of colon syntax (`revenue:sum`), preceded by `.` (cross-model: `customers.aov`), or followed by `(` (a transform call) is not expanded. Saved-measure names that would shadow built-in transform names (`cumsum`, `change`, `time_shift`, `lag`, `lead`, `rank`, `percent_rank`, `dense_rank`, `ntile`, `first`, `last`, `change_pct`, `consecutive_periods`) are rejected at model construction time.

Transforms work on cross-model measures: `"cumsum(customers.score:avg)"`, `"first(customers.score:avg)"`, `"last(customers.score:avg)"`. The cross-model measure is computed first (as a sub-query CTE), then the transform is applied on the joined result.

Inside any formula or `Column.sql`, dotted references to columns on joined models can target *derived* columns (columns whose own `sql` is itself an expression). The engine recursively inlines those references at query time, so `"B.foo_normalized:sum"` — where `B.foo_normalized.sql = "foo_raw / 100.0"` — emits `SUM(B.foo_raw / 100.0)`. See [Models → Derived Columns Referencing Other Derived Columns](models.md#derived-columns-referencing-other-derived-columns) for the full chaining behaviour and cycle-detection semantics.

### Transform Functions

Functions apply window operations to measures:

| Function | Description | SQL Generated |
|----------|-------------|---------------|
| `cumsum(x)` | Running total over time | `SUM(x) OVER (PARTITION BY dims ORDER BY time)` |
| `time_shift(x, n)` | Value N periods back/ahead | Self-join CTE with INTERVAL offset |
| `time_shift(x, offset, gran)` | Value from a different time bucket | Self-join CTE with INTERVAL offset |
| `lag(x, n)` | Value N rows back (window function) | `LAG(x, n) OVER (PARTITION BY dims ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead (window function) | `LEAD(x, n) OVER (PARTITION BY dims ORDER BY time)` |
| `change(x)` | Difference from previous period | Desugars to `x - time_shift(x, -1)` |
| `change_pct(x)` | Percentage change from previous | Desugars to `(x - ts) / ts` where `ts = time_shift(x, -1)` |
| `consecutive_periods(predicate)` | Current trailing run length where predicate is true | Staged window CTEs with reset groups |
| `rank(x[, partition_by=...])` | Ranking by value (descending) | `RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `percent_rank(x[, partition_by=...])` | Relative rank in [0, 1] (descending) | `PERCENT_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `dense_rank(x[, partition_by=...])` | Ranking with no gaps after ties (descending) | `DENSE_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `ntile(x, n=N[, partition_by=...])` | Bucket the rows into N equal groups (descending) | `NTILE(N) OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `first(x)` | Earliest time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time ASC ...)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** All time-ordered transforms (`cumsum`, `time_shift`, `change`, `change_pct`, `first`, `last`, `lag`, `lead`, `consecutive_periods`) require an explicit `time_dimensions` entry in the query. With a single entry, it's used automatically. With 2+ time dimensions, specify the query's `main_time_dimension` to disambiguate, or the model's `default_time_dimension` is used if it's among the query's time dimensions. The rank-family transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) do not need a time dimension.

Time-ordered window transforms partition by the query's non-time dimensions.
For example, `cumsum(revenue:sum)` grouped by `status` computes one running
total per status, not one running total across the whole result set.

**Self-join transforms vs window-function transforms:**

`time_shift` uses a **self-join CTE** with an INTERVAL-shifted time column. `change` and `change_pct` are desugared into a hidden `time_shift` + arithmetic expression at query enrichment time. The shifted sub-query applies the time offset everywhere (WHERE, GROUP BY, SELECT), so it can reach outside the current result set — no edge NULLs when the database has the data, and correct handling of gaps in time series.

`lag(x, n)` and `lead(x, n)` use SQL `LAG`/`LEAD` window functions directly. They are more efficient but have two trade-offs:

- **Edge NULLs**: the first/last N rows always return NULL since window functions can only see rows within the current result set.
- **Gap sensitivity**: if there are missing time periods in your data, `lag` shifts by row position, not by logical period — so the "previous row" might not be the previous calendar period.

`consecutive_periods(predicate)` evaluates a predicate at the query grain and
returns an integer streak length for the current row. False or NULL breaks the
run and returns 0. The result composes with normal comparisons:

```json
{
  "measures": [
    {"formula": "consecutive_periods(revenue:sum > 0)", "name": "positive_run"},
    {"formula": "consecutive_periods(revenue:sum > 0) >= 3", "name": "positive_3_periods"}
  ],
  "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]
}
```

### Nesting

Field formulas support nesting — window transforms can wrap self-join transforms (but not vice versa):

```json
"measures": [
  {"formula": "cumsum(change(revenue:sum))", "name": "cumsum_delta"},
  "last(change(revenue:sum))",
  {"formula": "cumsum(revenue:sum / *:count)", "name": "running_aov"},
  {"formula": "cumsum(revenue:sum) / *:count", "name": "cumsum_div_count"}
]
```

Use `show_sql=True` on the query to see what SQL is generated for complex formulas.

**Mathematical identity:** `cumsum(change(x)) == x - x[0]` for all rows after the first.

### Rank-family transforms

The rank family — `rank`, `percent_rank`, `dense_rank`, `ntile` — are timeless window-function transforms that order rows by the inner measure descending and emit a per-row rank value. They do not need a time dimension and, unlike the time-ordered transforms (`cumsum`, `lag`, `lead`, `first`, `last`, …), they default to **no `PARTITION BY`** — every row in the result set is ranked against every other row.

```json
{
  "source_model": "orders",
  "dimensions": ["customer_name"],
  "measures": [
    "revenue:sum",
    {"formula": "rank(revenue:sum)", "name": "rnk"}
  ],
  "order": [{"column": "revenue:sum", "direction": "desc"}]
}
```

Combine with a filter to get "top N":

```json
{"filters": ["rank(revenue:sum) <= 10"]}
```

**Choosing between the four:**

- `rank(x)` — ties share a rank, then the next rank is skipped (`1, 1, 3, 4`). Use for top-N rows.
- `dense_rank(x)` — ties share a rank, no gaps after (`1, 1, 2, 3`). Use for "top N distinct values" / tier counting.
- `percent_rank(x)` — relative position in `[0, 1]` (`(rank - 1) / (count - 1)`). Use for normalized rankings comparable across queries with different result-set sizes.
- `ntile(x, n=N)` — bucket every row into one of `N` equal-sized groups (`1` is the top bucket; required `n=` kwarg is a positive integer). Use for quartiles / deciles.

**Ranking within a partition (`partition_by=`):**

To rank within groups instead of across the whole result set, pass `partition_by=` referencing one or more **query dimensions** (or time dimensions). The columns must already be grouped on — partitioning by a column that's not a dimension errors at enrichment time.

```json
{
  "source_model": "orders",
  "dimensions": ["region", "customer_name"],
  "measures": [
    "revenue:sum",
    {"formula": "dense_rank(revenue:sum, partition_by=region)", "name": "rev_rank_within_region"},
    {"formula": "ntile(revenue:sum, n=4, partition_by=region)", "name": "rev_quartile_within_region"}
  ]
}
```

Multiple partition columns: `partition_by=[region, channel]`. Cross-model dotted paths work too: `partition_by=customers.region`.

> **Note:** SLayer's formula parser is Python-AST-based and rejects raw `OVER (...)` SQL in `ModelMeasure.formula` and filter strings. Use the rank-family transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) for ranking instead of `row_number() over (...) <= N`. If you need a non-standard window expression, define it on a `Column.sql` (e.g., `{"name": "rn", "sql": "row_number() over (order by mass desc)", "type": "NUMBER"}`) and filter on the column — SLayer auto-promotes the predicate to a post-aggregation outer `WHERE`.

### First and Last Functions

`first(x)` and `last(x)` are window-function transforms that take an aggregated measure and **broadcast a single time bucket's value to every row** in the result. `first()` broadcasts the **earliest** bucket's value; `last()` broadcasts the **most recent** bucket's value.

```json
{
  "source_model": "orders",
  "measures": [
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

## Scalar Math Functions

Inside `Column.sql`, `ModelMeasure.formula`, or any `Aggregation.formula`, you can call standard scalar math functions. They pass through to the underlying database via sqlglot — the formula parser does not need to know about them.

| Function | Args | Behaviour |
|----------|------|-----------|
| `ln(x)` | 1 | Natural logarithm |
| `log10(x)` | 1 | Base-10 logarithm |
| `log2(x)` | 1 | Base-2 logarithm |
| `log(B, X)` | 2 | log base B of X — **base first, value second**. Matches SQLite ≥3.35 built-in `log(B, X)`, Postgres `LOG(b, x)`, and sqlglot transpilation. |
| `exp(x)` | 1 | `e^x` |
| `sqrt(x)` | 1 | Square root |
| `pow(x, n)` / `power(x, n)` | 2 | `x^n`. Both spellings are accepted (sqlglot may emit either depending on origin dialect). |

These are native on Postgres / DuckDB / MySQL / ClickHouse. SQLite doesn't have most of them in the standard build, so SLayer registers Python implementations on every connection (see `slayer/sql/dialects/sqlite.py`). NULL inputs always return NULL. Math-domain errors (`ln(0)`, `sqrt(-1)`, `pow(0, -1)`) propagate as `sqlite3.OperationalError` — matching Postgres's strict semantics rather than SQLite ≥3.35's silent-NULL built-in `log()`.

The 2-arg `log(B, X)` UDF is registered on **every** SQLite version, including ≥3.35 where it overrides the built-in's silent-NULL behaviour to match Postgres's strict error semantics. `ln`, `log10`, and `log2` also always register; the `log2` UDF overrides SQLite ≥3.35's silent-NULL built-in to keep the same strict semantics.

The single-arg aliases `log10(x)` and `log2(x)` round-trip verbatim in emitted SQL on every supported backend (SQLite, Postgres, DuckDB, MySQL, ClickHouse, Snowflake, BigQuery, Redshift, Trino/Presto, Databricks/Spark, T-SQL). Backends that lack a native single-arg form fall back to the canonical 2-arg `LOG(base, x)`: Oracle for both, T-SQL for `log2`. Other 2-arg `log(B, X)` calls — including non-literal bases like `log(some_col, x)` — always emit as `LOG(B, X)`.

```python
# Examples (in Column.sql):
Column(name="ln_amount", sql="ln(amount)", type=DataType.DOUBLE)
Column(name="rms", sql="sqrt(pow(x, 2) + pow(y, 2))", type=DataType.DOUBLE)
```

---

## Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **AggregatedMeasureRef** — measure with colon aggregation (`"revenue:sum"`, `"*:count"`)
- **ArithmeticField** — arithmetic on aggregated measures (`"revenue:sum / *:count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue:sum)"`)
- **MixedArithmeticField** — arithmetic containing function calls. Covers both transform calls (`"cumsum(revenue:sum) / *:count"`) and non-transform SQL function calls wrapping aggregated refs, e.g. `"*:count / nullif(revenue:max, 0)"` or `"coalesce(revenue:sum, 0) + amount:avg"`. Aggregated refs nested inside non-transform calls are resolved as their own measure aliases; the call passes through to emitted SQL unchanged.

The query engine's `_enrich()` method processes field formulas into ordered enrichment steps, and the SQL generator translates them into stacked CTEs.
