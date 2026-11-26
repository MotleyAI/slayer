# Aggregations

An aggregation is a function call over a field, a joined field or a row-level
expression, each reference starting at its dataset; it may reach across to-one
relationships (`sum(orders.amount * customers.credit)`). It is valid in every expression
position: metrics, filters, order, and computed dimensions.

## Catalogue

`count(<dataset>.*)`, `count(x)`, `count_distinct(x)` (or `count(distinct x)`),
`count_distinct_approx(x)`, `sum`, `avg`, `min`, `max`, `sum_distinct(x)` (or
`sum(distinct x)`), `avg_distinct(x)`, `median(x)`, `percentile(x, p=0.95)` (or
`PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY x)`), `percentile_disc(x, p=0.95)` (an observed value of any
ordered type; or `PERCENTILE_DISC(…) WITHIN GROUP (…)`), `percentile_approx(x, p=0.95)` (or
`APPROX_PERCENTILE(x, 0.95)`; exact where the database has no approximation), `weighted_avg(x, weight=w)`, `stddev_samp`, `stddev_pop`,
`var_samp`, `var_pop`, `corr(x, other=y)`, `covar_samp(x, other=y)`,
`covar_pop(x, other=y)`, `first(x[, time_col])` / `last(x[, time_col])` (the earliest /
latest record's value in each group), `collect(x)` (the distinct sorted values in each
group, as a list), plus the dataset's custom aggregations
(`inspect` the dataset to list them). Any field argument may be a row-level expression:
`weighted_avg(x, weight=orders.qty * 2)`, `last(x, coalesce(orders.updated_at, orders.created_at))`.
In an aggregation or transform call, `name = value` is a keyword argument when `name` is
one of its parameters (`weighted_avg(x, weight = orders.qty)`) and a comparison otherwise
(`sum(orders.status = 'paid')` counts the paid rows). In a scalar function
(`iif(orders.status = 'paid', …)`) `=` compares.

A sole primary-key field only takes `count`, `count_distinct`,
`count_distinct_approx`, `min`, `max` and `collect`; a field's `allowed_aggregations` may
restrict it further. Expressions are not gated: `sum(orders.price * orders.quantity)` works.

## Expressions and booleans

`sum(orders.amount - orders.cost)`, `count_distinct(upper(customers.email))`,
`percentile(orders.price * orders.qty, p=0.5)`. A comparison aggregates as 1 / 0:
`sum(orders.amount > 15)` counts the rows above 15. A conditional aggregate uses `iif`:
`sum(iif(orders.status = 'paid', orders.amount, 0))`; always give it an `alias`.
`agg(x) FILTER (WHERE c)` aggregates only the rows where the row-level condition `c`
holds: `sum(orders.amount) FILTER (WHERE orders.status = 'paid')`.

## Empty input

An aggregate over no rows (a parent without joined rows, an empty window) is 0 for the
`count` family, `[]` for `collect`, and NULL for everything else, `sum(iif(...))` included.

## Result names

`sum(orders.amount)` returns `orders.amount_sum` and `count(orders.*)` returns
`orders._count`, whatever the population; an expression is sanitized after its common
dataset (`sum(orders.amount - orders.cost)` → `orders.amount_cost_sum`). An `alias` is the
key itself. An alias equal to a field of the source dataset is refused with a suggested
free name.

## partition_by

`partition_by=` computes the aggregate at a coarser grain and repeats it on every row:
one key, a list, a dotted path for a joined field, or `[]` for the grand total. Keys
are spelled exactly like the query's dimensions, an expression key (`upper(city)`) being
the computed dimension with that expression. In a metric, filter, order or
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

`avg(sum(orders.amount, partition_by=[stores.region, stores.city]), partition_by=[stores.region])`
averages the per-city totals in each region: the outer aggregation runs over one row per
inner group, never over the base rows. The top-level `partition_by` must be a subset of
the query's dimensions; inner ones need not be. An outer aggregation's parameters must be
fixed by the inner grain, e.g. `weight=count(orders.id, partition_by=[stores.region, stores.city])`.
