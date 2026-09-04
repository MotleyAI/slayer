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
revenue:sum(partition_by=region)  — the region total, repeated on every finer row
price:weighted_avg(weight=quantity)  — weighted average with kwargs
latency:stddev_samp  — sample standard deviation
latency:var_pop      — population variance
price:corr(other=quantity)  — Pearson correlation between two columns (named `other` kwarg)
price:covar_samp(other=quantity)  — sample covariance (Bessel-corrected)
price:covar_pop(other=quantity)   — population covariance
customers.score:avg  — cross-model: AVG of "score" from the joined "customers" model
```

Colon syntax is used everywhere measures appear: in `measures`, in arithmetic expressions, in transform function arguments, and in filters.

### Functional spelling

Every colon aggregation may equally be written as a function call —
`sum(revenue)` ≡ `revenue:sum`, `count(*)` ≡ `*:count`,
`percentile(price, p=0.9)` ≡ `price:percentile(p=0.9)` — in every position,
with identical SQL, results, result-column keys, and errors. Neither spelling
is rewritten: a saved model keeps the author's text. The full mapping table
and the `first`/`last` disambiguation rules are in
[Reference semantics → Aggregation spelling equivalence](references.md#aggregation-spelling-equivalence).

### Expression aggregation

The functional spelling additionally accepts a same-model scalar
**expression** as the aggregated value — something the colon form cannot
spell:

```text
sum(amount - cost)                     — aggregate a row-level expression
count_distinct(upper(email))           — scalar-allowlist calls compose
percentile(price * quantity, p=0.5)    — parametric aggs work too
my_agg(price * quantity)               — as do model-defined custom aggs
sum(amount - cost, partition_by=region)  — reserved kwargs compose
count(1)                               — a constant is a valid expression
```

The expression may use bare host-model columns (derived `Column.sql` columns
included), scalar-allowlist functions, arithmetic, and literals. It works in
every functional-aggregation position — measures, post-aggregation filters
(`sum(amount - cost) > 0` routes to HAVING), order, computed-dimension
expressions — and composes with `rename`.

**Naming.** The result key derives from the expression via the same sanitizer
used for computed dimensions: `sum(amount - cost)` on `orders` →
`orders.amount_cost_sum`, formatting-insensitively (`sum(amount-cost)` is the
same key). Very long expressions fold to a stable-hash key. An explicit
`name` overrides the derived key; two *different* expressions whose derived
keys collide (`sum(amount - cost)` and `sum(amount + cost)`) fail with a
duplicate-key error asking for a rename.

**Boundaries** (rejected with clear errors):

* joined-model refs / dotted paths inside the expression
  (`sum(amount - customers.discount)`) — cross-model expression aggregation
  is not yet supported;
* operands whose column carries a column-level `filter` — define a derived
  model column instead;
* nested aggregations or transforms (`sum(sum(x))`, `sum(cumsum(x) - 1)`).

**Gates.** Per-column `allowed_aggregations` / primary-key / type-default
gates apply to *columns*, not expressions — `sum(price * quantity)` succeeds
even when `quantity` whitelists only `min`/`max`, because the expression is a
new derived quantity owned by the query author. Global validation still
applies: the aggregation name must be known, and numeric-only aggregations
reject a confidently non-numeric expression (`sum(lower(name))`).

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

Windowed measures need exactly one resolvable time dimension (a single
`time_dimensions` entry, or `main_time_dimension` to disambiguate). Filtering on
a windowed measure (`{"formula": "revenue:sum(window='90d') > 100"}`) applies
after aggregation, and the windowed measure must also be selected.

A group whose dimension value is NULL gets its real windowed value, like any
other group. (Earlier versions returned NULL for such groups: the rolling
aggregate was matched to its group with a plain equality, and `NULL = NULL` is
not true. Grouping keys now compare null-safely everywhere, so a NULL region or
an unmatched outer join no longer silently blanks the measure.)

A windowed measure may also be used purely as an **order** target without being
selected — `{"order": [{"column": "revenue:sum(window='90d')", "direction": "desc"}]}`
ranks by the rolling value and keeps it out of the result. That works both for a
bare windowed measure and for one inside an order-only composite
(`{"column": "revenue:sum(window='90d') / cnt:sum"}`).

Note the deliberate asymmetry: a windowed measure inside a composite is allowed
in `order` but not yet in `measures`. Ordering needs only a single scalar
comparison, whereas projecting the composite surfaces the rolling value's NULLs
(a grain bucket with no matching source rows yields NULL) as user-visible
result values — settling those semantics is part of the follow-up below.

#### Time bounds do not clip the window

A trailing window has to read rows from *before* the earliest bucket you asked
for — otherwise that bucket silently under-counts. So a **time bound narrows
which buckets come back, not which rows the window may reach**. These two
queries return identical numbers:

```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month",
                      "date_range": ["2025-01-01", "2025-12-31"]}]}
```
```json
{"time_dimensions": [{"dimension": "created_at", "granularity": "month"}],
 "filters": ["created_at >= '2025-01-01' and created_at <= '2025-12-31'"]}
```

A bound counts as a *frame* bound when it compares a **time dimension's own
column** against a **literal** using `<`, `<=`, `>`, or `>=`. Everything else is
an ordinary row filter and does restrict the window's input, including:

- other operators on that column — `created_at == '2025-01-01'`, `IN (…)`,
  `IS NOT NULL`;
- a bound on a time column that is not one of the query's time dimensions;
- a comparison against another column rather than a literal;
- a bound wrapped in `or` or `not`, which cannot be separated out safely;
- `filters` declared on the **model** — those define which rows exist at all, so
  a model scoped to `created_at >= '2024-01-01'` does clip the window there.

Mixed filters are split, so only the time part is set aside:
`"created_at >= '2025-01-01' and status = 'paid'"` restricts the window's input
to paid rows while still reaching back before January.

The same rule applies to [`time_shift`](#transform-functions) — the earliest
visible bucket still gets its prior-period value under either spelling.

If you genuinely want to clip the underlying rows, apply the bound in an inner
stage of a multi-stage query so the windowed stage never sees the raw column.

A cross-model windowed measure (`customers.revenue:sum(window=…)`) works when
the query's active time dimension is *attributable* from the measure's own
model — reachable from it over provably many-to-one join hops (see
[cross-model measures](queries.md#cross-model-measures)); the window buckets by
that time dimension inside the measure's sub-query. When it is not
attributable, the query errors naming the time dimension and the remedy.

The following windowed-measure shapes raise a clear error rather than returning
wrong numbers, and are planned follow-ups: a windowed aggregation other than
`sum`/`avg`; a windowed measure combined with a transform (`cumsum`,
`time_shift`, …) in any position; a windowed measure nested in an
arithmetic/composite expression in `measures`
(`{"formula": "revenue:sum(window='90d') / 2"}`); or one compared
against a plain aggregate inside one filter
(`revenue:sum(window='90d') > 100 and revenue:sum > 50`).

### Aggregate at a coarser grain (`partition_by=`)

Any aggregation accepts an optional `partition_by=` to compute it over a
**subset** of the query's dimensions, repeated across the finer rows — the
share-of-parent shape (`SUM(revenue) OVER (PARTITION BY region)`):

```json
{
  "dimensions": ["region", "city"],
  "measures": [
    {"formula": "revenue:sum", "name": "city_rev"},
    {"formula": "revenue:sum(partition_by=region)", "name": "region_rev"},
    {"formula": "revenue:sum / revenue:sum(partition_by=region)", "name": "share_of_region"}
  ]
}
```

`region_rev` repeats the region total on every city row, so `share_of_region`
sums to 1.0 within each region. `partition_by` takes one query dimension, a list
(`partition_by=[region, channel]`), a dotted path (`partition_by=customers.region`),
or `[]` for the grand total (share of total). A time dimension partitions by its
truncated bucket. The coarser aggregate is computed over the rows passing the
query's row-level filters — `having`-phase filters and pagination change which
rows you see, never the parent total. NULL-valued partition dimensions keep their
group.

A `partition_by` aggregate can also drive a
[computed dimension](queries.md#grouping-by-an-expression-over-an-aggregate) —
grouping by a value derived from an aggregate at a finer grain than the query.
There, the `partition_by` grain is unconstrained (it may be finer than the query
dimensions), since it defines the grain of a synthesized internal stage.

A `partition_by` aggregate composes with the rest of the query: combined with
`window=` (a rolling total at the partition grain), on `first`/`last`, nested
inside a transform (`cumsum(revenue:sum(partition_by=region))`), and referenced
in a filter (`revenue:sum(partition_by=region) > 5000`) — a filter's top-level
`AND` conjuncts route independently to the earliest scope where their
references resolve, and a predicate whose references share no scope raises a
"split the filter" error. This includes cross-model sources
(`customers.spend:sum(partition_by=…)`), which compute in a sub-query rooted at
the measure's model. Consumed in a **combined position** — a query measure, an
arithmetic/scalar composite, a transform input, or a raw `order` target — every
explicit partition key must be a query dimension (or a query time dimension's
bucket), for local and cross-model sources alike, else it errors at plan time
naming the key and the remedy. Only the computed-dimension consumer keeps the
finer-grain freedom; a filter over, or `order` by the *name* of, that dimension's
own aggregate is a row-scope reference that stays legal at any partition grain.
For a cross-model source, every explicit partition key must additionally be
*attributable* from the measure's own model (see
[cross-model measures](queries.md#cross-model-measures)) — an unprovable key
errors naming the join remedy rather than fanning the value.

Combining aggregates at **different** grains in one expression is well-defined:
`amount:sum(partition_by=region) - amount:sum(partition_by=city)` (as a measure,
a computed dimension, a filter, or wrapped in a transform such as
`rank(...)`) unions the two grains and broadcasts each aggregate to the union —
never an error. See [grain-union broadcasting](queries.md#grouping-by-an-expression-over-an-aggregate).

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

A saved measure reused by name — bare `aov` or another model's `customers.aov` — expands to the same SQL as its longhand formula and is legal only in a measure formula or computed-dimension expression (see [Models → Reusing another model's saved measure](models.md#reusing-another-models-saved-measure-customersaov)).

Transforms work on cross-model measures: `"cumsum(customers.score:avg)"`, `"first(customers.score:avg)"`, `"last(customers.score:avg)"`. The cross-model measure is computed first (as a sub-query CTE), then the transform is applied on the joined result.

Inside any formula or `Column.sql`, dotted references to columns on joined models can target *derived* columns (columns whose own `sql` is itself an expression). The engine recursively inlines those references at query time, so `"B.foo_normalized:sum"` — where `B.foo_normalized.sql = "foo_raw / 100.0"` — emits `SUM(B.foo_raw / 100.0)`. See [Models → Derived Columns Referencing Other Derived Columns](models.md#derived-columns-referencing-other-derived-columns) for the full chaining behaviour and cycle-detection semantics.

### Transform Functions

Functions apply window operations to measures:

| Function | Description | SQL Generated |
|----------|-------------|---------------|
| `cumsum(x)` | Running total over time | `SUM(x) OVER (PARTITION BY dims ORDER BY time)` |
| `time_shift(x, n)` | Value N time buckets back/ahead (calendar-aware) | Self-join CTE with INTERVAL offset |
| `time_shift(x, offset, gran)` | Value from a different time bucket | Self-join CTE with INTERVAL offset |
| `lag(x, n)` | Value N rows back (window function) | `LAG(x, n) OVER (PARTITION BY dims ORDER BY time)` |
| `lead(x, n)` | Value N rows ahead (window function) | `LEAD(x, n) OVER (PARTITION BY dims ORDER BY time)` |
| `change(x)` | Period-over-period difference (partition-safe, resets per group) | Desugars to `x - time_shift(x, -1)` |
| `change_pct(x)` | Period-over-period % change, e.g. month-over-month growth (partition-safe, resets per group; NULL when the prior period's value is 0 or missing) | Desugars to `CASE WHEN ts != 0 THEN (x - ts) / ts END` where `ts = time_shift(x, -1)` |
| `consecutive_periods(predicate)` | Current trailing run length where predicate is true | Staged window CTEs with reset groups |
| `rank(x[, partition_by=...])` | Ranking by value (descending) | `RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `percent_rank(x[, partition_by=...])` | Relative rank in [0, 1] (descending) | `PERCENT_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `dense_rank(x[, partition_by=...])` | Ranking with no gaps after ties (descending) | `DENSE_RANK() OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `ntile(x, n=N[, partition_by=...])` | Bucket the rows into N equal groups (descending) | `NTILE(N) OVER ([PARTITION BY ...] ORDER BY x DESC)` |
| `first(x)` | Earliest time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time ASC ...)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** All time-ordered transforms (`cumsum`, `time_shift`, `change`, `change_pct`, `first`, `last`, `lag`, `lead`, `consecutive_periods`) require an explicit `time_dimensions` entry in the query. With a single entry, it's used automatically. With 2+ time dimensions, specify the query's `main_time_dimension` to disambiguate, or the model's `default_time_dimension` is used if it's among the query's time dimensions. The rank-family transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) do not need a time dimension.

Time-ordered window transforms partition by **every** projected non-time
dimension — plain columns, joined and derived columns, and
[computed (expression) dimensions](queries.md#grouping-by-an-expression-over-an-aggregate),
aggregation-derived ones included. For example, `cumsum(revenue:sum)` grouped
by `status` computes one running total per status, not one running total
across the whole result set; grouped by a computed `band`, one per
`(…, band)` group. An attached `partition_by=` *measure* value never joins the
partition (it is a value, not a grouping dimension). An explicit
`partition_by=` is accepted only on the rank family; on other transforms it
errors (their partition is fixed to the query's dimensions). To coarsen the
*measure* itself, put `partition_by=` on the aggregation
([above](#aggregate-at-a-coarser-grain-partition_by)).

**Self-join transforms vs window-function transforms:**

`time_shift` uses a **self-join CTE** with an INTERVAL-shifted time column. `change` and `change_pct` are desugared into a hidden `time_shift` + arithmetic expression when the query is compiled. The shifted sub-query applies the time offset everywhere (WHERE, GROUP BY, SELECT), so it can reach outside the current result set — no edge NULLs when the database has the data, and correct handling of gaps in time series.

The self-join matches on **every projected dimension as well as the shifted time column** — plain columns, joined columns (`stores.name`), derived columns, and any secondary time dimension all take part in the join grain (e.g. `ON base.month IS NOT DISTINCT FROM shifted.month AND base.store IS NOT DISTINCT FROM shifted.store`). So these transforms are partition-safe: each group's series is compared only against itself, and per-group series reset cleanly. One store's first month is never diffed against another store's last month. The grain match is **null-safe** (`IS NOT DISTINCT FROM`, or the dialect equivalent), so a group with a NULL dimension value — for example rows with no matching row across a LEFT join — still lines up against its own prior period instead of dropping to a NULL shifted value.

`time_shift` (and `change` / `change_pct`) also accepts a composite input whose leaves are all aggregates (e.g. `time_shift(revenue:sum / qty:sum, -1)`), re-aggregating each leaf in the shifted period, while a nested transform, a row-level column, or a cross-model leaf *inside the composite* is rejected (a bare cross-model input like `time_shift(customers.spend:sum, -1)` renders).

**Intent recipes:**

- Month-over-month / period-over-period growth → `change_pct(revenue:sum)` with a `time_dimensions` entry at the desired granularity. Prefer this over hand-building the ratio from `time_shift`.
- Absolute period-over-period delta → `change(revenue:sum)`.
- Comparing against a *different* grain than the query's (e.g. year-over-year on a monthly series), or using the shifted value as a term in custom arithmetic → `time_shift(revenue:sum, -1, 'year')`.

`lag(x, n)` and `lead(x, n)` use SQL `LAG`/`LEAD` window functions directly. They are more efficient but have two trade-offs:

- **Edge NULLs**: the first/last N rows always return NULL since window functions can only see rows within the current result set.
- **Gap sensitivity**: if there are missing time periods in your data, `lag` shifts by row position, not by logical period — so the "previous row" might not be the previous calendar period.

`consecutive_periods(predicate)` evaluates a predicate at the query grain and
returns an integer streak length for the current row. False or NULL breaks the
run and returns 0. The input is a Mode-B predicate or numeric value — a
comparison, a null test (`is None` / `is not None`), `BETWEEN`, `IN`, a boolean
connective, a nested transform, or a bare value
(truthy when non-NULL and non-zero) — with a boolean-shaped node legal only at
the predicate top level or an `iif` condition. The result composes with normal
comparisons:

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

Field formulas support nesting — window transforms can wrap self-join transforms (but not vice versa, though `consecutive_periods` may nest a transform in its predicate):

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

To rank within groups instead of across the whole result set, pass `partition_by=` referencing one or more **query dimensions** (or time dimensions). The columns must already be grouped on — partitioning by a column that's not a dimension errors at plan time (HTTP 400). Naming a query time-dimension partitions by its truncated bucket, not the raw timestamp.

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
| `round(x[, ndigits])` | 1–2 | Round to `ndigits` decimal places (default 0). `ndigits` must be an integer literal. |
| `abs(x)` | 1 | Absolute value. |

Unlike the other scalar functions above — which pass through only when embedded in a larger `Column.sql` or arithmetic expression — `round` and `abs` are also valid as the **top-level** form of a query measure or `ModelMeasure.formula`:

```python
{"formula": "round(revenue:sum, 2)"}        # round an aggregate
{"formula": "abs(revenue:sum - cost:sum)"}  # absolute difference
{"formula": "round(revenue:sum / *:count, 2)"}
```

On Postgres, 2-argument `round` over a floating-point value is automatically cast to `numeric` so it executes (Postgres has no `round(double precision, integer)` overload). SQLite and DuckDB round `DOUBLE` natively.

These are native on Postgres / DuckDB / MySQL / ClickHouse. SQLite doesn't have most of them in the standard build, so SLayer registers Python implementations on every connection (see `slayer/sql/dialects/sqlite.py`). NULL inputs always return NULL. Math-domain errors (`ln(0)`, `sqrt(-1)`, `pow(0, -1)`) propagate as `sqlite3.OperationalError` — matching Postgres's strict semantics rather than SQLite ≥3.35's silent-NULL built-in `log()`.

The 2-arg `log(B, X)` UDF is registered on **every** SQLite version, including ≥3.35 where it overrides the built-in's silent-NULL behaviour to match Postgres's strict error semantics. `ln`, `log10`, and `log2` also always register; the `log2` UDF overrides SQLite ≥3.35's silent-NULL built-in to keep the same strict semantics.

The single-arg aliases `log10(x)` and `log2(x)` round-trip verbatim in emitted SQL on every supported backend (SQLite, Postgres, DuckDB, MySQL, ClickHouse, Snowflake, BigQuery, Redshift, Trino/Presto, Databricks/Spark, T-SQL). Backends that lack a native single-arg form fall back to the canonical 2-arg `LOG(base, x)`: Oracle for both, T-SQL for `log2`. Other 2-arg `log(B, X)` calls — including non-literal bases like `log(some_col, x)` — always emit as `LOG(B, X)`.

```python
# Examples (in Column.sql):
Column(name="ln_amount", sql="ln(amount)", type=DataType.DOUBLE)
Column(name="rms", sql="sqrt(pow(x, 2) + pow(y, 2))", type=DataType.DOUBLE)
```

## Conditionals (`CASE WHEN` / `iif`)

Any formula, filter, or field expression can branch with SQL `CASE`:

```json
{"formula": "CASE WHEN revenue:sum >= 10000 THEN 1 ELSE 0 END", "name": "big"}
```

- **Searched** (`CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 …] [ELSE d] END`) and
  **simple** (`CASE x WHEN v1 THEN r1 … END`, lowered to `x = v1`) forms are both
  accepted; keywords are case-insensitive and CASE nests anywhere.
- A missing `ELSE` yields `NULL`. `iif(cond, then, otherwise)` is an equivalent
  spelling — an allowlisted scalar function taking exactly three arguments.
  Everything renders to a portable SQL `CASE` on every Tier-1 dialect.
- Inside a `WHEN` condition you may use SQL operators (`=`, `<>`, `AND`, `OR`,
  `NOT`, `LIKE`) even in a measure formula; `THEN` / `ELSE` values are taken
  as-is (a string literal like `'a AND b'` is never rewritten).
- The result **type is the join of the branches** (Postgres semantics):
  identical types pass through, a numeric mix widens to `DOUBLE`, a `NULL`
  branch is absorbed by the other, and any other mix is a plan-time error
  naming both types. The Python conditional `x if c else y` is not supported —
  use `CASE` / `iif`.

---

## Parsing Internals

Both field and filter formulas are parsed by `slayer/core/formula.py` using Python's `ast` module.

**Field formulas** are classified into:

- **AggregatedMeasureRef** — measure with colon aggregation (`"revenue:sum"`, `"*:count"`)
- **ArithmeticField** — arithmetic on aggregated measures (`"revenue:sum / *:count"`)
- **TransformField** — function call, possibly nested (`"cumsum(revenue:sum)"`)
- **MixedArithmeticField** — arithmetic containing function calls. Covers both transform calls (`"cumsum(revenue:sum) / *:count"`) and non-transform SQL function calls wrapping aggregated refs, e.g. `"*:count / nullif(revenue:max, 0)"` or `"coalesce(revenue:sum, 0) + amount:avg"`. Aggregated refs nested inside non-transform calls are resolved as their own measure aliases; the call passes through to emitted SQL unchanged.

The query engine binds and expands field formulas into ordered planned stages, and the SQL generator translates them into stacked CTEs.
