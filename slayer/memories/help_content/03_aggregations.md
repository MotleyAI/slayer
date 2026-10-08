# Aggregations

An aggregation is a function call over a column, a joined column (dotted path) or a
same-model row-level expression. It is valid in every expression position: measures,
filters, order, and computed dimensions.

## Catalogue

`count(*)`, `count(x)`, `count_distinct(x)` (never `count(distinct x)`),
`count_distinct_approx(x)`, `sum`, `avg`, `min`, `max`, `median(x)`,
`percentile(x, p=0.95)`, `weighted_avg(x, weight=w)`, `stddev_samp`, `stddev_pop`,
`var_samp`, `var_pop`, `corr(x, other=y)`, `covar_samp(x, other=y)`,
`covar_pop(x, other=y)`, `first(x[, time_col])` / `last(x[, time_col])` (the earliest /
latest record's value in each group), plus the model's custom aggregations
(`inspect` the model to list them).

A sole primary-key column only takes `count`, `count_distinct`,
`count_distinct_approx`, `min` and `max`; a column's `allowed_aggregations` may
restrict it further. Expressions are not gated: `sum(price * quantity)` works.

## Expressions and booleans

`sum(amount - cost)`, `count_distinct(upper(email))`, `percentile(price * qty, p=0.5)`.
A comparison aggregates as 1 / 0: `sum(amount > 15)` counts the rows above 15. A
conditional aggregate uses `iif`: `sum(iif(status == 'paid', amount, 0))`; always give
it a `name`.

## Empty input

An aggregate over no rows (a parent without joined rows, an empty window) is 0 for the
`count` family and NULL for everything else, `sum(iif(...))` included.

## Result names

`sum(amount)` on `orders` returns `orders.amount_sum`, `count(*)` returns
`orders._count`, an expression is sanitized (`sum(amount - cost)` → `amount_cost_sum`).
A `name` overrides it. A name equal to a column of the source model is refused with a
suggested free name.

## partition_by

`partition_by=` computes the aggregate at a coarser grain and repeats it on every row:
one key, a list, a dotted path for a joined column, or `[]` for the grand total. Keys
are spelled exactly like the query's dimensions. In a measure, filter, order or
transform input every key must be one of the query's dimensions (a time dimension
partitions by its bucket). The coarser total is computed over the rows that pass the
row-level conditions; aggregate conditions and `limit` never change it.

## window

`window='90d'` aggregates source rows in a trailing interval ending at each output
bucket's end. Units: `y`, `m` (months), `w`, `d`, `h`, `min`, `s`, combinable
(`'1y2m'`); only this compact form is accepted. The query needs exactly one time
dimension (or `main_time_dimension`). The window reads rows before the query's date
range. A window adds no dialect support: `median` and `percentile` stay unavailable on
MySQL and SQL Server.

## Nesting

`avg(sum(amount, partition_by=[region, city]), partition_by=[region])` averages the
per-city totals in each region: the outer aggregation runs over one row per inner
group, never over the base rows. The top-level `partition_by` must be a subset of the
query's dimensions; inner ones need not be. An outer aggregation's parameters must be
fixed by the inner grain, e.g. `weight=count(id, partition_by=[region, city])`.
