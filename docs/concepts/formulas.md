# Expressions

SLayer uses expression strings in two places: **metrics** (data columns returned by a query) and **filters** (conditions). Both are compiled to SQL — everything runs in the database. Metric expressions are documented below; filter expressions are in [Queries — Filters](queries.md#filters).

---

## Aggregations

Metrics and aggregations are separate concepts in SLayer. Metrics are named row-level expressions defined on a dataset. Aggregation is specified at query time as a function call: `aggregation(metric_name)`.

```
sum(orders.revenue)          — SUM the "revenue" metric
count(orders.*)              — COUNT(*), always available, no metric definition needed
avg(orders.revenue)          — AVG the "revenue" metric
sum(orders.revenue, window='90d')  — trailing 90-day SUM ending at each output bucket
sum(orders.revenue, partition_by=orders.region)  — the region total, repeated on every finer row
weighted_avg(orders.price, weight=orders.quantity)  — weighted average with kwargs
stddev_samp(orders.latency)  — sample standard deviation
var_pop(orders.latency)      — population variance
corr(orders.price, other=orders.quantity)  — Pearson correlation between two fields (named `other` kwarg)
covar_samp(orders.price, other=orders.quantity)  — sample covariance (Bessel-corrected)
covar_pop(orders.price, other=orders.quantity)   — population covariance
avg(customers.score)  — cross-dataset: AVG of "score" from the joined "customers" dataset
collect(customers.name)      — the names as a list
```

Aggregations are used everywhere metrics appear: in `metrics`, in arithmetic expressions, in transform function arguments, and in filters. The same call is valid in every position, with the author's text preserved on save. The `first`/`last` disambiguation rules are in [Reference semantics → Aggregation syntax](references.md#aggregation-syntax). `count(distinct x)` is the same as `count_distinct(x)`. `collect` is specified in [List fields](queries.md#list-fields). The pre-2.0 colon spelling of aggregations and granularities is refused with its functional twin; stored text written in it is upgraded on load.

A metric may combine query-dimension values with aggregates (`orders.quantity * count(orders.*)` by `orders.quantity`, `iif(orders.region == 'North', sum(orders.amount), 0)` by `orders.region`), reading each dimension's value per result row; a row-level field that is not a query dimension (`orders.amount + sum(orders.amount)` by `orders.region`) is a typing error.

### Expression aggregation

An aggregation additionally accepts a row-level **expression** over its dataset and
its to-one relationships as its aggregated value, not just a bare field:

```text
sum(orders.amount - orders.cost)                     — aggregate a row-level expression
count_distinct(upper(orders.email))                  — scalar-allowlist calls compose
percentile(orders.price * orders.quantity, p=0.5)    — parametric aggs work too
my_agg(orders.price * orders.quantity)               — as do dataset-defined custom aggs
sum(orders.amount - orders.cost, partition_by=orders.region)  — reserved kwargs compose
count(1)                                             — a constant is a valid expression
```

The expression may use bare host-dataset fields (derived `DatasetField.sql` fields
included), scalar-allowlist functions, arithmetic, and literals. It works in
every functional-aggregation position — metrics, post-aggregation filters
(`sum(orders.amount - orders.cost) > 0` routes to HAVING), order, computed-dimension
expressions — and composes with `rename`.
Comparisons, `and` / `or` / `not` and `in` aggregate as booleans, so `sum(orders.amount > 15)` counts the rows whose `amount` exceeds 15.
Wherever a number is needed — an arithmetic operand, a math function's argument, a comparison with a number — a boolean reads as 1 / 0 (`orders.flag * orders.amount`, `orders.flag = 1`).
A numeric aggregation reads a boolean parameter the same way (`weighted_avg(orders.amount, weight=orders.status = 'paid')`, `corr(orders.amount, other=orders.flag)`), while a custom aggregation's `{value}` and placeholders receive the raw boolean.

**Naming.** The result key derives from the expression via the same sanitizer
used for computed dimensions: `sum(orders.amount - orders.cost)` →
`orders.amount_cost_sum`, formatting-insensitively (`sum(orders.amount-orders.cost)` is the
same key). Every operand is named by its expression text — date comparisons, nested aggregates and transforms included — spelled from the operands' common dataset, which the key carries (`sum(customers.spend - 1)` → `customers.spend_1_sum`, under every population). Very long expressions fold to a stable-hash key. An explicit
`name` overrides the derived key; two *different* expressions whose derived
keys collide (`sum(orders.amount - orders.cost)` and `sum(orders.amount + orders.cost)`) fail with a
duplicate-key error asking for a rename.

Expression sources also accept joined-dataset refs (`sum(orders.amount - customers.discount)`, homed at the deepest dataset that determines every operand — see [cross-dataset metrics](queries.md#cross-dataset-metrics)), operands whose field carries a `filter` (which masks that operand's value), and nested transforms (`sum(cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at])) - 1)`, aggregated over the transform's own cells). A re-aggregation constituent mixed with a row-level field (`sum(orders.amount * last(X))`) executes at its own grain and broadcasts per partition onto the rows, and one query may select the same re-aggregation both on its own and inside a mixed source; a windowed inner under a transform constituent (`sum(rank(sum(orders.revenue, window='90d', partition_by=orders.region), direction='desc'))`) is a second-order aggregation over the operand cells (COMBINED phase, not a row broadcast); a target-homed inner whose `partition_by=` names the host's time axis stays a typed error.

A source mixing row-level fields with attached values
(`sum(orders.quantity * avg(orders.price, partition_by=orders.product))`) is a row-grain aggregation
— the attached value broadcasts onto each base row, weighted per row — while a
fully-attached source is a
[re-aggregation](#re-aggregation-aggregate-over-an-attached-value).
`first` / `last` take such a source too, as a ranked pick: `first(orders.quantity * avg(orders.price, partition_by=orders.product))`
is the earliest row's `quantity` times its product's average price.
An attached value — an aggregate or a grained transform
(`weight=rank(sum(orders.amount, partition_by=orders.region), direction='desc')`) — may also arrive as a
*parameter* of a row-level aggregation
(`weighted_avg(orders.amount, weight=sum(orders.amount, partition_by=orders.region))`): it is
attached into the input relation, so each row is weighted by its cell's value.
A field-valued parameter also takes a row-level expression, positionally or by name
(`weighted_avg(orders.price, weight=orders.quantity * 2)`, `corr(orders.amount, orders.quantity + 1)`).

**Gates.** Per-field `allowed_aggregations` / primary-key / type-default
gates apply to *fields*, not expressions — `sum(orders.price * orders.quantity)` succeeds
even when `quantity` whitelists only `min`/`max`, because the expression is a
new derived quantity owned by the query author. Global validation still
applies: the aggregation name must be known, and numeric-only aggregations
reject a confidently non-numeric expression (`sum(lower(orders.name))`).

### Filtered aggregation (`FILTER (WHERE …)`)

Any aggregation takes a SQL `FILTER (WHERE c)` clause with a row-level condition `c` and aggregates only the rows passing it, every row-level input included (`count(orders.*) FILTER (WHERE orders.status = 'paid')`, `last(orders.amount) FILTER (WHERE orders.amount > 0)` picks the newest passing row).
An aggregate, transform or metric inside `c` is rejected, the clause composes with `partition_by=` and `window=` (`sum(orders.amount, window='3m') FILTER (WHERE orders.status = 'paid')`), and an unnamed filtered aggregation's result key derives from the whole expression, condition included, so it never collides with the unfiltered one.
On a custom aggregation the clause restricts the rows its expression reads (`COUNT(*)` included), and a grouped cell with no passing rows reads NULL.

An unnamed filtered aggregation's result key is its expression text (`count(orders.*) FILTER (WHERE orders.status = 'paid')` → `orders.count_filter_where_status_paid`), the same with or without a `{variable}` in the condition.

### Windowed aggregations

Every aggregation — built-in or custom — accepts an optional `window` parameter for
trailing time-window aggregations:

```json
{
  "metrics": [
    {"metric": "sum(orders.revenue, window='30d')", "alias": "revenue_30d"},
    {"metric": "avg(orders.price, window='1y')", "alias": "avg_price_1y"}
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

The window is measured against the raw time dimension, not just the output
rows. For each output bucket, SLayer aggregates source rows in the trailing
interval ending at that bucket's end. This means the window can be larger than
the query granularity (overlapping windows), equal to it (equivalent to normal
`sum`/`avg` for that bucket), or smaller than it (only the trailing part of each
bucket is included).

An aggregate over no rows — an empty trailing interval, or a parent with no rows across a
relationship (`count(orders.id)` for a customer without orders) — yields 0 for the `count` family, `[]` for `collect` and
NULL for every other aggregation, including custom and expression-overridden ones; `first`/`last` pick the earliest/latest interval row by their ranking time
field; and reference-bearing parameters (a field, an attached aggregate, a
definition-default field) are read on each interval row while literals pass through.

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
inside the expression.

Windowed metrics need exactly one resolvable time dimension (a single
`time_dimensions` entry, or `main_time_dimension` to disambiguate). Filtering on
a windowed metric (`"sum(orders.revenue, window='90d') > 100"`) applies
after aggregation and, like an order-only target, needs no matching selected
metric — a filter-only windowed value stays out of the result.

A group whose dimension value is NULL gets its real windowed value, like any
other group. (Earlier versions returned NULL for such groups: the rolling
aggregate was matched to its group with a plain equality, and `NULL = NULL` is
not true. Grouping keys now compare null-safely everywhere, so a NULL region or
an unmatched outer join no longer silently blanks the metric.)

A windowed metric may also be used purely as an **order** target without being
selected — `{"order": [{"field": "sum(orders.revenue, window='90d')", "direction": "desc"}]}`
ranks by the rolling value and keeps it out of the result. That works both for a
bare windowed metric and for one inside an order-only composite
(`{"field": "sum(orders.revenue, window='90d') / sum(orders.cnt)"}`).

#### Time bounds do not clip the window

A time bound narrows which buckets come back, not which rows a trailing window or `time_shift` may read — see [Time bounds do not clip the window](time.md#time-bounds-do-not-clip-the-window).

A windowed metric (`sum(orders.revenue, window=…)`, `sum(customers.revenue, window=…)`)
works when the query's active time dimension is *attributable* from the
aggregation's home dataset — reachable from it over provably many-to-one relationship hops (see
[cross-dataset metrics](queries.md#cross-dataset-metrics)); the window buckets by
that time dimension inside the metric's sub-query. When it is not
attributable, the query errors naming the time dimension and the remedy.

A windowed metric composes like any other aggregate: inside arithmetic (`sum(orders.revenue, window='90d') / sum(orders.revenue)`), under a transform (`cumsum(sum(orders.revenue, window='90d'))`, `time_shift(sum(orders.revenue, window='90d'), -1)`), and in filters or `order`, alone or mixed with plain aggregates.

### Aggregate at a coarser grain (`partition_by=`)

Any aggregation accepts an optional `partition_by=` to compute it over a
**subset** of the query's dimensions, repeated across the finer rows — the
share-of-parent shape (`SUM(revenue) OVER (PARTITION BY region)`):

```json
{
  "dimensions": ["orders.region", "orders.city"],
  "metrics": [
    {"metric": "sum(orders.revenue)", "alias": "city_rev"},
    {"metric": "sum(orders.revenue, partition_by=orders.region)", "alias": "region_rev"},
    {"metric": "sum(orders.revenue) / sum(orders.revenue, partition_by=orders.region)", "alias": "share_of_region"}
  ]
}
```

`region_rev` repeats the region total on every city row, so `share_of_region`
sums to 1.0 within each region. `partition_by` takes one query dimension, a list
(`partition_by=[orders.region, orders.channel]`), a joined dataset's field (`partition_by=customers.region`),
or `[]` for the grand total (share of total). A time dimension partitions by its
truncated bucket. A key may also be a row-level expression (`partition_by=upper(orders.city)`) or a
time dimension's name: an expression equal to a query computed dimension or time dimension
is that dimension. The coarser aggregate is computed over the rows passing the
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
inside a transform (`cumsum(sum(orders.revenue, partition_by=orders.region))`), and referenced
in a filter (`sum(orders.revenue, partition_by=orders.region) > 5000`) — a filter's top-level
`AND` conjuncts route independently to the earliest scope where their
references resolve, and a predicate whose references share no scope raises a
"split the filter" error. This includes cross-dataset sources
(`sum(customers.spend, partition_by=…)`), which compute in a sub-query rooted at
the metric's dataset. Consumed in a **combined position** — a query metric, an
arithmetic/scalar composite, a transform input, or a raw `order` target — every
explicit partition key must be a query dimension (or a query time dimension's
bucket), for local and cross-dataset sources alike, else it errors at plan time
naming the key and the remedy. Only the computed-dimension consumer and a
[re-aggregation](#re-aggregation-aggregate-over-an-attached-value) operand keep
the finer-grain freedom; a filter over, or `order` by the *name* of, that dimension's
own aggregate is a row-scope reference that stays legal at any partition grain.
For a cross-dataset source, every explicit partition key must additionally be
*attributable* from the metric's own dataset (see
[cross-dataset metrics](queries.md#cross-dataset-metrics)) — an unprovable key
errors naming the relationship remedy rather than fanning the value.

Combining aggregates at **different** grains in one expression is well-defined:
`sum(orders.amount, partition_by=orders.region) - sum(orders.amount, partition_by=orders.city)` (as a metric,
a computed dimension, a filter, or wrapped in a transform such as
`rank(...)`) unions the two grains and broadcasts each aggregate to the union —
never an error. See [grain-union broadcasting](queries.md#grouping-by-an-expression-over-an-aggregate).

### Re-aggregation (aggregate over an attached value)

Wrapping a partitioned aggregate in another aggregation re-aggregates its
**cells** — one input row per inner group, never the query's population rows:

```json
{
  "dimensions": ["orders.region"],
  "metrics": [
    {"metric": "avg(sum(orders.amount, partition_by=[orders.city, orders.region]))", "alias": "avg_city_total"}
  ]
}
```

`avg_city_total` is each region's unweighted average of its city totals (a
row-weighted average would be wrong, and is exactly what this shape avoids).
The operand may compose several attached aggregates (their grains union), and
`partition_by=` may name a computed dimension — including one carrying an
attached aggregate itself. A `first`/`last` or windowed aggregate is a legal operand too — `sum(last(accounts.balance, partition_by=[accounts.account_id, accounts.customer_id]))` by `accounts.customer_id` sums each account's latest balance per customer. The outer aggregation's parameters (`weight=` and friends) are typed by the operand grain — a cell of the operand dataset (`weighted_avg(sum(orders.amount, partition_by=[orders.city, orders.region]), weight=count(orders.id, partition_by=[orders.city, orders.region]))`), a grained transform over such cells, or a field that grain determines; anything else is a typed error naming the `partition_by=` remedy.
An outer dimension that is a row-level expression over fields the operand grain determines (`orders.city == 'Alpha'` over `[orders.city, orders.region]` cells) partitions the cells exactly.
An outer dimension not determined by the operand's
grain resolves per `to_many_handling` (associated + warning by default), and an
operand grain equal to the outer grain is the identity plus a degenerate
warning naming the `partition_by=` remedy.
The outer `partition_by=` follows the combined-position rule above: only inside a computed dimension may it be finer than the query dimensions, where the re-aggregation is computed at that grain and broadcast onto its rows.
An outer dimension or explicit outer `partition_by=` key that the operand's grain determines is attributed even when the query root reaches it only across a to-many relationship (e.g. a joined dataset's time bucket); an explicit outer key the grain does not determine is associated by default and under `associate`, and an error under `broadcast` or `error`.

---

## Field Expressions

Metric expressions define what aggregated values a query returns. They go in the `metrics` parameter:

```json
"metrics": [
  "count(orders.*)",
  {"metric": "sum(orders.revenue) / count(orders.*)", "alias": "aov", "label": "Average Order Value"},
  "cumsum(sum(orders.revenue))"
]
```

The `name` is optional — if omitted, an expression that is more than a plain aggregate reference derives it from its references' common dataset and its sanitized text (`"sum(orders.revenue) / count(orders.*)"` → `orders.revenue_sum_count`; very long names hash-fold), while plain references keep their usual keys (`count(orders.*)` → `orders._count`). The `label` is an optional human-readable display name for the field.

When a metric is renamed via `name`, query filters and ORDER BY entries in the same node accept either form — the raw aggregation expression or the user alias. See [Filters → Filtering on Computed Metrics](queries.md#filtering-on-computed-metrics).

### Arithmetic Operators

| Operator | Example | SQL |
|----------|---------|-----|
| `+` | `"sum(orders.revenue) + sum(orders.bonus)"` | `SUM(revenue) + SUM(bonus)` |
| `-` | `"sum(orders.revenue) - sum(orders.cost)"` | `SUM(revenue) - SUM(cost)` |
| `*` | `"avg(orders.price) * sum(orders.quantity)"` | `AVG(price) * SUM(quantity)` |
| `/` | `"sum(orders.revenue) / count(orders.*)"` | `SUM(revenue) / NULLIF(COUNT(*), 0)` |
| `**` | `"sum(orders.value) ** 2"` | `POWER(SUM(value), 2)` |
| `%` | `"sum(orders.quantity) % 12"` | `SUM(quantity) % 12` |

Parentheses work as expected: `"(sum(orders.revenue) - sum(orders.cost)) / count(orders.*)"`. Division by zero is NULL on every backend, and `/` is true division even between integers (`5 / 2` is `2.5`).

Precedence, tightest first: `::`, `**`, unary `-` / `+`, `*` / `/` / `%`, `+` / `-`, `||` / `->` / `->>`, comparisons (`=`, `<`, `IN`, `BETWEEN`, `LIKE`, `IS`, `~`, …), `NOT`, `AND`, `OR` — so `NOT a = b` is `NOT (a = b)` and `'a' || 'b' = 'ab'` is true.
`**` is right-associative and binds tighter than a sign (`2 ** 3 ** 2` is 512, `-2 ** 2` is -4), and `%` takes the sign of its left operand (`-7 % 3` is -1, `7 % -3` is 1).

Every reference in a query expression starts at its dataset (`count(orders.*)` counts rows), joined datasets included: `"avg(customers.score)"`, or with an explicit route: `"sum(customers.regions.population)"`. The population reaches each dataset by walking the relationship graph. See [Cross-Dataset Metrics](queries.md#cross-dataset-metrics).

### Saved Expressions (Named Metrics)

A dataset can carry a library of named expressions in `dataset.metrics`. Queries reference these by their dataset-qualified name (`orders.aov`) in their own metric expressions — root, inside transforms, or inside arithmetic:

```yaml
# dataset definition
metrics:
  - {name: aov, expression: "sum(revenue) / count(*)", label: "Average Order Value"}
  - {name: aov_pct_change, expression: "change_pct(aov)"}
```

All three forms below — plain reference, transform, arithmetic — work as query metrics:

```json
"metrics": [
  "orders.aov",
  "cumsum(orders.aov)",
  {"metric": "orders.aov * 1.1", "alias": "aov_with_markup"}
]
```

A saved metric reused by name — bare `aov` or another dataset's `customers.aov` — expands to the same SQL as its longhand expression and is legal only in a metric expression or computed-dimension expression (see [Datasets → Reusing another dataset's saved metric](models.md#reusing-another-datasets-saved-metric-customersaov)).

Transforms work on cross-dataset metrics: `"cumsum(avg(customers.score))"`, `"first(avg(customers.score))"`, `"last(avg(customers.score))"`. The cross-dataset metric is computed first (as a sub-query CTE), then the transform is applied on the joined result.

Inside any expression or `DatasetField.sql`, dotted references to fields on joined datasets can target *derived* fields (fields whose own `sql` is itself an expression). The engine recursively inlines those references at query time, so `"sum(B.foo_normalized)"` — where `B.foo_normalized.sql = "foo_raw / 100.0"` — emits `SUM(B.foo_raw / 100.0)`. See [Datasets → Derived Fields Referencing Other Derived Fields](models.md#derived-fields-referencing-other-derived-fields) for the full chaining behaviour and cycle-detection semantics.

### Transform Functions

Functions apply window operations to metrics:

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
| `rank(x, direction='asc'\|'desc'[, partition_by=...])` | Ranking by value in the required direction | `RANK() OVER ([PARTITION BY ...] ORDER BY x ASC\|DESC)` |
| `percent_rank(x[, partition_by=...])` | Relative rank in [0, 1] (ascending) | `PERCENT_RANK() OVER ([PARTITION BY ...] ORDER BY x ASC)` |
| `dense_rank(x, direction='asc'\|'desc'[, partition_by=...])` | Ranking with no gaps after ties, in the required direction | `DENSE_RANK() OVER ([PARTITION BY ...] ORDER BY x ASC\|DESC)` |
| `ntile(x, n=N[, partition_by=...])` | Bucket the rows into N equal groups (ascending) | `NTILE(N) OVER ([PARTITION BY ...] ORDER BY x ASC)` |
| `first(x)` | Earliest time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time ASC ...)` |
| `last(x)` | Most recent time bucket's value | `FIRST_VALUE(x) OVER (ORDER BY time DESC ...)` |

**Time dimension requirement:** All time-ordered transforms (`cumsum`, `time_shift`, `change`, `change_pct`, `first`, `last`, `lag`, `lead`, `consecutive_periods`) require an explicit `time_dimensions` entry in the query. With a single entry, it's used automatically. With 2+ time dimensions, specify the query's `main_time_dimension` to disambiguate, or the dataset's `default_time_dimension` is used if it's among the query's time dimensions. A downstream stage's own time dimension counts, so these transforms work over a re-bucketed stage column too. A stage has no dataset-level default, so set `main_time_dimension` when a stage has 2+ time dimensions. The rank-family transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) do not need a time dimension.

Time-ordered window transforms partition by **every** projected non-time
dimension — plain fields, joined and derived fields, and
[computed (expression) dimensions](queries.md#grouping-by-an-expression-over-an-aggregate),
aggregation-derived ones included. For example, `cumsum(sum(orders.revenue))` grouped
by `orders.status` computes one running total per status, not one running total
across the whole result set; grouped by a computed `band`, one per
`(…, band)` group. An attached `partition_by=` *metric* value never joins the
partition (it is a value, not a grouping dimension). An explicit
`partition_by=` is accepted only on the rank family; on other transforms it
errors (their partition is fixed to the query's dimensions). To coarsen the
*metric* itself, put `partition_by=` on the aggregation
([above](#aggregate-at-a-coarser-grain-partition_by)).

**Self-join transforms vs window-function transforms:**

`time_shift` uses a **self-join CTE**: each row reads the shifted relation at the bucket containing its own bucket offset by the shift (for an unaligned shift such as `time_shift(x, -1, 'day')` over month buckets, the bucket containing the offset instant). `change` and `change_pct` are desugared into a hidden `time_shift` + arithmetic expression when the query is compiled. For an all-local aggregate or composite input, the shifted evaluation ignores the query's date range and other time-frame bounds, so it can reach outside the current result set — no edge NULLs when the database has the data, and correct handling of gaps in time series; series-regime inputs (below) instead read the materialised series and are NULL where the shifted bucket falls outside it.

The self-join matches on **the shifted input's grain as well as the offset time bucket** — normally every projected dimension (an input whose leaves are all coarser, such as `sum(orders.amount, partition_by=[orders.ordered_at]) / 2`, matches on its own partition keys only): plain fields, joined fields (`stores.name`), derived fields, and any secondary time dimension all take part in the join grain (e.g. `ON shifted.month IS NOT DISTINCT FROM base.month - INTERVAL '1 month' AND base.store IS NOT DISTINCT FROM shifted.store`). So these transforms are partition-safe: each group's series is compared only against itself, and per-group series reset cleanly. One store's first month is never diffed against another store's last month. The grain match is **null-safe** (`IS NOT DISTINCT FROM`, or the dialect equivalent), so a group with a NULL dimension value — for example rows with no matching row across a LEFT join — still lines up against its own prior period instead of dropping to a NULL shifted value.

`time_shift` (and `change` / `change_pct`) also accepts a composite input whose leaves are all aggregates (e.g. `time_shift(sum(orders.revenue) / sum(orders.qty), -1)`), re-aggregating each leaf in the shifted period; an input containing a nested transform or a cross-dataset aggregate leaf instead shifts its materialised result series — NULL where the shifted bucket falls outside the series. A top-level predicate over aggregates (`time_shift(sum(orders.revenue) > 100, -1)`) also shifts as a series, but only under `time_shift` — `change` / `change_pct` reject boolean-shaped inputs (their desugared subtraction has no truth-value operands). Each leaf keeps its own grain when shifted (in `time_shift(sum(orders.amount) / sum(orders.amount, partition_by=[orders.ordered_at]), -1)` the denominator is the prior month's total), and a row-level field that is not a query dimension is rejected, as under any other transform.

A transform can also sit inside arithmetic or a scalar call beside other aggregates — local or cross-dataset, in any position (`change(sum(customers.spend)) + sum(orders.revenue)`, `iif(change(sum(customers.spend)) > 0, sum(customers.spend), sum(orders.revenue))`).

**Intent recipes:**

- Month-over-month / period-over-period growth → `change_pct(sum(orders.revenue))` with a `time_dimensions` entry at the desired granularity. Prefer this over hand-building the ratio from `time_shift`.
- Absolute period-over-period delta → `change(sum(orders.revenue))`.
- Comparing against a *different* grain than the query's (e.g. year-over-year on a monthly series), or using the shifted value as a term in custom arithmetic → `time_shift(sum(orders.revenue), -1, 'year')`.

`lag(x, n)` and `lead(x, n)` use SQL `LAG`/`LEAD` window functions directly. They are more efficient but have two trade-offs:

- **Edge NULLs**: the first/last N rows always return NULL since window functions can only see rows within the current result set.
- **Gap sensitivity**: if there are missing time periods in your data, `lag` shifts by row position, not by logical period — so the "previous row" might not be the previous calendar period.

`consecutive_periods(predicate)` evaluates a predicate at the query grain and
returns an integer streak length for the current row. False or NULL breaks the
run and returns 0. A time bucket missing from the query's rows (after the date
range and row filters) also breaks the run, so the next true bucket restarts at 1. The input is a Mode-B predicate or numeric value — a
comparison, a null test (`is None` / `is not None`), `IN`, a boolean
connective, a nested transform, or a bare value
(truthy when non-NULL and non-zero) — with a boolean in a numeric position read
as its integer (`(sum(orders.revenue) > 0) + (sum(orders.cost) > 0)`). The result composes with normal
comparisons:

```json
{
  "metrics": [
    {"metric": "consecutive_periods(sum(orders.revenue) > 0)", "alias": "positive_run"},
    {"metric": "consecutive_periods(sum(orders.revenue) > 0) >= 3", "alias": "positive_3_periods"}
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

### Nesting

Field expressions support nesting — window transforms can wrap self-join transforms and vice versa (`change(cumsum(x))` shifts the cumulative series; `consecutive_periods` may also nest a transform in its predicate):

```json
"metrics": [
  {"metric": "cumsum(change(sum(orders.revenue)))", "alias": "cumsum_delta"},
  "last(change(sum(orders.revenue)))",
  {"metric": "cumsum(sum(orders.revenue) / count(orders.*))", "alias": "running_aov"},
  {"metric": "cumsum(sum(orders.revenue)) / count(orders.*)", "alias": "cumsum_div_count"}
]
```

Use `show_sql=True` on the query to see what SQL is generated for complex expressions.

**Mathematical identity:** `cumsum(change(x)) == x - x[0]` for all rows after the first.

### Rank-family transforms

The rank family — `rank`, `percent_rank`, `dense_rank`, `ntile` — are timeless window-function transforms that order rows by the inner value and emit a per-row rank value. They do not need a time dimension and, unlike the time-ordered transforms (`cumsum`, `lag`, `lead`, `first`, `last`, …), they default to **no `PARTITION BY`** — every row in the result set is ranked against every other row.

`rank` and `dense_rank` take a **required** `direction=` keyword: `direction='desc'` ranks the highest value 1, `direction='asc'` the lowest (`ascending` / `descending` and any case are accepted too). Leaving it out is an error that shows both spellings. The inner value may be any orderable type, so `rank(min(orders.created_at), direction='asc')` ranks the earliest first. `ntile` and `percent_rank` take no `direction=` and always order ascending: bucket `1` holds the lowest values and the lowest value has `percent_rank` `0`.

```json
{
  "population": "orders",
  "dimensions": ["orders.customer_name"],
  "metrics": [
    "sum(orders.revenue)",
    {"metric": "rank(sum(orders.revenue), direction='desc')", "alias": "rnk"}
  ],
  "order": [{"field": "sum(orders.revenue)", "direction": "desc"}]
}
```

Combine with a filter to get "top N" (or `direction='asc'` for "bottom N"):

```json
{"filter": ["rank(sum(orders.revenue), direction='desc') <= 10"]}
```

A row whose inner value is NULL gets a NULL result from all four, takes no rank position or `ntile` bucket, and does not count in `percent_rank`'s denominator; the other rows rank as if it were absent, on every database. A `rank(...) <= N` filter therefore drops NULL-valued rows.

An unnamed rank metric's result key spells its direction as a bare value: `rank(sum(orders.revenue), direction='desc')` is returned as `orders.rank_revenue_sum_desc`.

**Choosing between the four:**

- `rank(x, direction=...)` — ties share a rank, then the next rank is skipped (`1, 1, 3, 4`). Use for top-N or bottom-N rows.
- `dense_rank(x, direction=...)` — ties share a rank, no gaps after (`1, 1, 2, 3`). Use for "top N distinct values" / tier counting.
- `percent_rank(x)` — relative position in `[0, 1]` (`(rank - 1) / (count - 1)`), lowest value `0`. Use for normalized rankings comparable across queries with different result-set sizes.
- `ntile(x, n=N)` — bucket every row into one of `N` equal-sized groups (`1` is the lowest bucket; required `n=` kwarg is a positive integer). Use for quartiles / deciles.

Datasets and queries saved before `direction=` existed load with `direction='desc'` filled in on every `rank` / `dense_rank` call, keeping their meaning; their stored `ntile` / `percent_rank` results now order ascending.

**Ranking within a partition (`partition_by=`):**

To rank within groups instead of across the whole result set, pass `partition_by=` referencing one or more **query dimensions** (or time dimensions). The fields must already be grouped on — partitioning by a field that's not a dimension errors at plan time (HTTP 400). Naming a query time-dimension partitions by its truncated bucket, not the raw timestamp. A key may also be a time dimension's `name` (`partition_by=yr`) or a row-level expression equal to a query computed dimension or time dimension (`partition_by=upper(customers.name)` beside that computed dimension).

```json
{
  "population": "orders",
  "dimensions": ["orders.region", "orders.customer_name"],
  "metrics": [
    "sum(orders.revenue)",
    {"metric": "dense_rank(sum(orders.revenue), partition_by=orders.region, direction='desc')", "alias": "rev_rank_within_region"},
    {"metric": "ntile(sum(orders.revenue), n=4, partition_by=orders.region)", "alias": "rev_quartile_within_region"}
  ]
}
```

Multiple partition fields: `partition_by=[orders.region, orders.channel]`. Joined datasets' fields work too: `partition_by=customers.region`.

The rank family's `partition_by=` may name a computed dimension, as an aggregation's can (one that itself wraps an aggregation is for now accepted only in filters and order). Each key must be a member of the transform's operand grain — the union of its inner aggregates' grains (an ungrained inner contributes the query's dimensions) — so `rank(sum(orders.revenue, partition_by=[orders.city, orders.region]), partition_by=orders.product, direction='desc')` is an error naming the remedy.

> **Note:** SLayer's expression grammar rejects raw `OVER (...)` SQL in `DatasetMetric.expression` and filter strings. Use the rank-family transforms (`rank`, `percent_rank`, `dense_rank`, `ntile`) for ranking instead of `row_number() over (...) <= N`. If you need a non-standard window expression, define it on a `DatasetField.sql` (e.g., `{"name": "rn", "sql": "row_number() over (order by mass desc)", "datatype": "NUMBER"}`) and filter on the field — SLayer auto-promotes the predicate to a post-aggregation outer `WHERE`.

### First and Last Functions

`first(x)` and `last(x)` are window-function transforms that take an aggregated metric and **broadcast a single time bucket's value to every row** in the result. `first()` broadcasts the **earliest** bucket's value; `last()` broadcasts the **most recent** bucket's value.

```json
{
  "population": "orders",
  "metrics": [
    "sum(orders.revenue)",
    {"metric": "first(sum(orders.revenue))", "alias": "initial_revenue"},
    {"metric": "last(sum(orders.revenue))", "alias": "latest_revenue"}
  ],
  "time_dimensions": [{"field": "orders.created_at", "granularity": "month"}]
}
```

This returns monthly revenue with extra columns showing the first and last month's revenue on every row — useful for comparisons like "this month vs initial/latest" or for filtering: `"last(change(sum(orders.revenue))) < 0"` keeps rows only if the trend is negative.

Both `first()` and `last()` require a time dimension with granularity in the query (same resolution as `time_shift`).

Not to be confused with the [`first`/`last` aggregation types](models.md#the-first-and-last-aggregations), which are per-group aggregates returning the earliest/latest *record's* value within each bucket.

---

## Scalar Math Functions

In Mode-A SQL (`DatasetField.sql`, `Aggregation.expression`) any database function passes through. In Mode-B expressions and query fields, scalar functions come from the closed [portable function library](references.md#the-portable-function-library), each translated per backend; a name outside it (`initcap`, `random`) is rejected. The math functions:

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
| `floor(x)` / `ceil(x)` | 1 | Round down / up to an integer value (`ceiling` is `ceil`). |
| `trunc(x)` | 1 | Round toward zero. |
| `sign(x)` | 1 | -1, 0 or 1. |
| `mod(a, b)` | 2 | Remainder, with the sign of `a` (as `a % b`). |

Query and metric expressions also take `div`, `pi`, `sin` … `atan2` and `cbrt` from the [portable function library](references.md#the-portable-function-library).

Any scalar function can wrap aggregates as the **top-level** form of a query metric or `DatasetMetric.expression` (`ln(sum(x))`, `coalesce(sum(a), 0)`, `greatest(sum(a), 1)`):

```python
"round(sum(orders.revenue), 2)"               # round an aggregate
"abs(sum(orders.revenue) - sum(orders.cost))"  # absolute difference
"round(sum(orders.revenue) / count(orders.*), 2)"
```

On Postgres, 2-argument `round` over a floating-point value is automatically cast to `numeric` so it executes (Postgres has no `round(double precision, integer)` overload). SQLite and DuckDB round `DOUBLE` natively. A DECIMAL tie rounds away from zero (`round(2.5)` is `3`, `round(-0.125, 2)` is `-0.13`); a floating-point tie is rounded as the database rounds it, since a binary value is rarely an exact tie.

These are native on Postgres / DuckDB / MySQL / ClickHouse. SQLite doesn't have most of them in the standard build, so SLayer registers Python implementations on every connection (see `slayer/sql/dialects/sqlite.py`). NULL inputs always return NULL. Math-domain errors (`ln(0)`, `sqrt(-1)`, `pow(0, -1)`) propagate as `sqlite3.OperationalError` — matching Postgres's strict semantics rather than SQLite ≥3.35's silent-NULL built-in `log()`.

The 2-arg `log(B, X)` UDF is registered on **every** SQLite version, including ≥3.35 where it overrides the built-in's silent-NULL behaviour to match Postgres's strict error semantics. `ln`, `log10`, and `log2` also always register; the `log2` UDF overrides SQLite ≥3.35's silent-NULL built-in to keep the same strict semantics.

The single-arg aliases `log10(x)` and `log2(x)` round-trip verbatim where the backend has them natively; elsewhere they emit the canonical 2-arg `LOG(base, x)`: Snowflake and Oracle for both, Postgres, BigQuery, Redshift and T-SQL for `log2`. Other 2-arg `log(B, X)` calls — including non-literal bases like `log(some_col, x)` — always emit as `LOG(B, X)`.

```python
# Examples (in DatasetField.sql):
DatasetField(name="ln_amount", sql="ln(amount)", datatype=DataType.FLOAT)
DatasetField(name="rms", sql="sqrt(pow(x, 2) + pow(y, 2))", datatype=DataType.FLOAT)
```

## Conditionals (`CASE WHEN` / `iif`)

Any expression, filter, or field expression can branch with SQL `CASE`:

```json
{"metric": "CASE WHEN sum(orders.revenue) >= 10000 THEN 1 ELSE 0 END", "alias": "big"}
```

- **Searched** (`CASE WHEN c1 THEN v1 [WHEN c2 THEN v2 …] [ELSE d] END`) and
  **simple** (`CASE x WHEN v1 THEN r1 … END`, lowered to `x = v1`) forms are both
  accepted; keywords are case-insensitive and CASE nests anywhere.
- Identifiers named after, containing, or qualified by SQL keywords (`case`,
  `customers.end`, `écase`) always parse as ordinary references — `CASE` starts
  a conditional only when a `WHEN` follows it.
- A missing `ELSE` yields `NULL`. `iif(cond, then, otherwise)` is an equivalent
  spelling — an allowlisted scalar function taking exactly three arguments.
  Everything renders to a portable SQL `CASE` on every Tier-1 dialect.
- Conditions and `THEN` / `ELSE` values are full expressions in the same grammar —
  SQL operators, function calls, aggregates (`THEN sum(orders.amount) * 2`) — and a string
  literal like `'a AND b'` is never rewritten.
- The result **type is the join of the branches** (Postgres semantics):
  identical types pass through, a numeric mix widens to `Float`, a `Boolean`
  beside a number reads as its integer, `Date` beside `DateTime` widens to
  `DateTime`, a `NULL` branch or a value whose type cannot be determined (an untyped or `Opaque` field, an unrecognised expression) is
  absorbed by the other, and any other mix is a plan-time error naming both types; `coalesce`, `ifnull`, `greatest` and `least` follow the
  same rule on every dialect. The Python conditional `x if c else y` is not supported —
  use `CASE` / `iif`.

---

## Parsing Internals

Every Mode-B field is parsed by one SQL-compatible grammar (`slayer/core/syntax.py`) into an expression tree that the query engine binds, checks and expands into planned stages, and the SQL generator translates into stacked CTEs.
