# aggregations/trailing-window Specification

## Purpose
Defines the trailing `window=` parameter for every aggregation: which rows an aggregation
reads per output bucket, what an empty interval yields, how `first`/`last` pick within the
interval, how parameters are read, and that no aggregation is refused a window for
implementation reasons.

## Requirements

### Requirement: Every aggregation accepts a trailing window
Every aggregation — each built-in (`sum`, `avg`, `count`, `min`, `max`, `count_distinct`,
`count_distinct_approx`, `first`, `last`, `weighted_avg`, `median`, `percentile`,
`stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop`) and
every custom model-level aggregation — SHALL accept `window=<compact duration>` in every
position a windowed `sum` is accepted (measure, filter, order, computed dimension,
composite, transform input, combined with `partition_by=`, cross-model, and over an
expression source). The value per output cell SHALL be the aggregation evaluated over the
home rows whose window time lies in `[bucket_end − window, bucket_end)`, `bucket_end`
being the exclusive upper edge of the cell's bucket of the query's active time dimension.
The former "only supported for sum and avg" error SHALL NOT exist; duration validation
(compact duration string, non-empty, positive parts, well-formed) SHALL be unchanged.

#### Scenario: Rolling count, min and max by month
- **WHEN** a query over a month time dimension selects `amount:count(window='90d')`,
  `amount:min(window='90d')` and `amount:max(window='90d')` over rows dated 2024-01-01
  (100), 2024-01-15 (200), 2024-02-15 (300), 2024-03-15 (400) and 2024-03-20 (300)
- **THEN** the executed values are count `{Jan 2, Feb 3, Mar 4}`, min
  `{Jan 100, Feb 100, Mar 200}`, max `{Jan 200, Feb 300, Mar 400}` — the 2024-01-01 row
  falls outside March's interval, which starts 2024-01-02

#### Scenario: Rolling count_distinct counts distinct interval values
- **WHEN** the same query selects `amount:count_distinct(window='90d')`
- **THEN** the executed values are `{Jan 2, Feb 3, Mar 3}` — March's two 300s count once

#### Scenario: Statistics family and per-row parameters over the interval
- **WHEN** the same query selects `amount:median(window='90d')`,
  `amount:stddev_samp(window='90d')`, `amount:corr(other=qty, window='90d')` and
  `amount:weighted_avg(weight=qty, window='90d')`, `qty` being 1..5 in row order
- **THEN** each value is the statistic over the interval rows with `qty` read on each
  interval row: median `{150, 200, 300}`, sample stddev `{70.71, 100, 81.65}`, corr
  `{1.0, 1.0, 0.632}`, weighted average `{166.67, 233.33, 314.29}`

#### Scenario: Literal parameters pass through unchanged
- **WHEN** a query selects `amount:percentile(p=0.5, window='90d')`, or a custom
  aggregation whose definition default for a parameter is a numeric literal
- **THEN** the literal reaches the aggregation unchanged (the percentile equals the
  median above) and the literal-only rule for `percentile`'s `p` still raises on a
  non-numeric value

#### Scenario: Custom aggregation over the interval
- **WHEN** a model defines `trimmed_mean` as
  `AVG(CASE WHEN {value} BETWEEN {lo} AND {hi} THEN {value} END)` with defaults `lo=0`,
  `hi=1000` and a query selects `amount:trimmed_mean(lo=150, hi=350, window='90d')`
- **THEN** the formula runs over the interval rows with the overridden literals:
  `{Jan 200, Feb 250, Mar 266.67}`; without overrides it equals the rolling average

#### Scenario: Custom aggregation with a definition-default column parameter on a joined model
- **WHEN** a custom aggregation defined on a joined target model has a definition default
  naming one of that model's columns, and a query selects it windowed over a time
  dimension attributable from that model
- **THEN** the default column is read on each interval row of the target model and the
  query executes; it never fails with an unknown-aggregation or unknown-column error

#### Scenario: Attached aggregate as a windowed parameter
- **WHEN** a query over a month time dimension selects
  `amount:weighted_avg(weight=qty:sum(partition_by=region), window='90d')`
- **THEN** each interval row is weighted by its region's total `qty` and the executed
  values are the hand-derived weighted averages, never a value computed from the raw
  `qty` or a multiplied row set

#### Scenario: Windowed count with partition_by, in a filter, and as an order target
- **WHEN** a query selects `amount:count(window='90d', partition_by=region)` over
  `[region, month]`, another filters on `amount:count(window='90d') >= 3` without
  selecting it, and a third orders by `amount:max(window='90d')` without selecting it
- **THEN** the partitioned count is per (region, month) interval, the filter keeps
  exactly the buckets whose rolling count is at least 3, and the order follows the
  rolling max with no windowed column in the response

#### Scenario: Cross-model windowed count
- **WHEN** a query rooted at `orders` over the `customers.signup_at` month time dimension
  selects `customers.spend:count(window='1y')`
- **THEN** the count is over the customers whose signup falls in each bucket's trailing
  year, computed on the customers producer with no fan-out from orders

#### Scenario: Windowed aggregation over a stage time dimension
- **WHEN** a two-stage query's outer stage selects `rev:count(window='60d')` and
  `rev:first(window='60d')` over the stage's own month time dimension
- **THEN** each value is the aggregation over the stage rows in the trailing interval,
  identical to the same shapes evaluated over a model-backed dataset holding the same rows

#### Scenario: Dialect gaps are unchanged
- **WHEN** `amount:median(window='90d')` is rendered for MySQL or T-SQL
- **THEN** the query fails with the same dialect error the plain aggregation raises, and
  every other Tier-1 dialect renders SQL — including BigQuery median and MySQL
  `corr`/`covar`, which the plain path already emulates, so a window adds no new gap

### Requirement: An empty interval follows SQL empty-set semantics
For an output cell whose trailing interval contains no home rows, `count`,
`count_distinct` and `count_distinct_approx` SHALL yield 0 and every other aggregation
SHALL yield NULL. `*:count(window=)` SHALL count the interval rows: an empty interval
yields 0, never 1 for the cell's own grain row.

#### Scenario: One-day window on month buckets
- **WHEN** a query over month buckets selects `amount:count(window='1d')`,
  `*:count(window='1d')`, `amount:sum(window='1d')` and `amount:last(window='1d')` over
  rows none of which falls on the last day of its month
- **THEN** every bucket is still returned, the counts are 0, and the sum and last are
  NULL

### Requirement: Windowed first and last pick within the interval
`first(window=)` and `last(window=)` SHALL yield the value of the interval row that
ranks earliest or latest by the ranking time column, resolved as for plain `first`/`last`:
the explicit positional argument, else the query's temporal row dimension, else the time
dimension's raw column, else the model's `default_time_dimension`. An empty interval
SHALL yield NULL. NULL ranking keys and ties SHALL order per the dialect's native
ordering, as for plain `first`/`last`; a row whose ranking key is NULL is still an
interval member.

#### Scenario: Default ranking is the window axis
- **WHEN** the rows of the first scenario are queried with `amount:first(window='90d')`
  and `amount:last(window='90d')`
- **THEN** first is `{Jan 100, Feb 100, Mar 200}` and last is `{Jan 200, Feb 300, Mar 300}`
  — March's last is the 2024-03-20 row's 300, not the interval max 400

#### Scenario: Explicit ranking column with NULL keys
- **WHEN** the same rows carry `updated_at` = 2024-01-02, 2024-01-16, NULL, 2024-03-25,
  2024-03-16 and the query selects `amount:last(updated_at, window='90d')`
- **THEN** last is `{Jan 200, Feb 200, Mar 400}`: February skips the NULL-keyed row
  under descending native ordering on SQLite and DuckDB, and March picks the row with
  the latest `updated_at` rather than the latest `created_at`

#### Scenario: Interval whose only rows have NULL ranking keys
- **WHEN** the same query groups by `region` and the EU/February cell's interval holds
  only the NULL-keyed 300 row
- **THEN** `amount:last(updated_at, window='90d')` for that cell is 300, not NULL

#### Scenario: Cross-model windowed last
- **WHEN** a query rooted at `orders` over the `customers.signup_at` month time dimension
  selects `customers.spend:last(customers.signup_at, window='1y')` over customers signed
  up 2024-01-05 (100), 2024-02-10 (150), 2024-03-15 (60), 2024-03-20 (40)
- **THEN** the values are `{Jan 100, Feb 150, Mar 40}`

#### Scenario: Windowed first/last never renders as a plain aggregate
- **WHEN** any `first`/`last` with `window=` is rendered for a Tier-1 dialect
- **THEN** the emitted SQL ranks the interval rows and picks rank 1; it never raises the
  internal plain-renderer guard and never emits `MAX`/`MIN` over the value as a substitute
