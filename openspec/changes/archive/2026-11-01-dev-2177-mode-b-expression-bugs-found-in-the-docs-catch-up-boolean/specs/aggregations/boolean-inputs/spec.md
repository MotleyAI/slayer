## MODIFIED Requirements

### Requirement: Numeric aggregations lower a boolean input to an integer
Every numeric aggregation — `sum`, `avg`, `min`, `max`, `median`, `percentile`, `weighted_avg`,
`stddev_samp`, `stddev_pop`, `var_samp`, `var_pop`, `corr`, `covar_samp`, `covar_pop` — SHALL read
every boolean-valued input as its integer value — true as 1, false as 0, NULL as NULL — so NULL
inputs are ignored as for any aggregate. The inputs are the aggregated value and every parameter
the aggregation reads: `weighted_avg`'s `weight=`, the `other=` of `corr` / `covar_samp` /
`covar_pop`, `percentile`'s `p=` (a number literal, which the rule leaves unchanged), and every
placeholder of a model-level aggregation that reuses a built-in aggregation's name.
`min` and `max` SHALL return a BOOLEAN, converted back within the aggregate expression itself,
so the aggregate is boolean wherever it appears (projection, post-aggregation filter, ordering,
arithmetic). The count family (`count`, `count_distinct`, `count_distinct_approx`), `first`,
`last` (value and ranking key) and custom aggregations (`{value}` and every placeholder) SHALL
receive every boolean input unchanged. A boolean-valued input SHALL remain boolean in every
non-aggregate position (dimension, row filter).

#### Scenario: Sum counts the trues
- **WHEN** a query over rows whose `flag` values are `true, true, false, NULL, false` selects
  `sum(flag)`
- **THEN** it returns 2 on SQLite, DuckDB and Postgres — never `true`, and never a database error

#### Scenario: Avg is the share of true rows
- **WHEN** the same query selects `avg(flag)`
- **THEN** it returns 0.5 (two trues among four non-NULL rows) on SQLite, DuckDB and Postgres

#### Scenario: Min and max return booleans
- **WHEN** the same query selects `min(flag)` and `max(flag)`
- **THEN** it returns `false` and `true` on DuckDB and Postgres (`0` and `1` on SQLite, which has
  no boolean type), never a database error

#### Scenario: Post-aggregation filters compare booleans and counts
- **WHEN** a query filters on `max(flag) = true` or on `sum(flag) > 1`
- **THEN** each filter applies after aggregation and executes on Postgres, keeping exactly the
  groups whose value satisfies it

#### Scenario: Statistical aggregations read the integer
- **WHEN** the same query selects `median(flag)`, `median(amount > 15)` (over `amount` values
  `10, 20, 30, NULL`) and `stddev_samp(coalesce(flag, false))`
- **THEN** it returns 0.5, 1 and the sample standard deviation of `1, 1, 0, 0, 0` (≈ 0.5477)

#### Scenario: Literal sources
- **WHEN** a query by `region` selects `sum(True)`, `avg(True)`, `max(True)`, `count(True)` and
  `sum(1 > 2)`
- **THEN** each region returns its row count, 1.0, true, its row count and 0

#### Scenario: Counting keeps the boolean
- **WHEN** a query selects `count(flag)` and `count_distinct(flag)` over the same rows
- **THEN** it returns 4 and 2 — false values counted, NULL not

#### Scenario: Emitted SQL on every dialect
- **WHEN** SQL is generated for `sum(flag)`, `avg(flag)`, `min(flag)` and `max(flag)` on each
  supported dialect
- **THEN** each aggregate takes the integer form of `flag` as its input (T-SQL's `avg` reads it as a
  float, since its integer `AVG` truncates), `min` / `max` convert
  the aggregate back to the dialect's boolean type, and no aggregate receives the raw boolean

#### Scenario: Boolean weight
- **WHEN** a query on `orders` selects `weighted_avg(amount, weight=status = 'paid')`
- **THEN** it returns the plain average of the paid orders' amounts on SQLite, DuckDB and
  Postgres, never a database error

#### Scenario: Boolean other
- **WHEN** a query on `orders` selects `covar_samp(amount, other=status = 'paid')` and
  `corr(amount, other=status = 'paid')`
- **THEN** each equals the same statistic over `amount` and the integer `1` / `0` of the
  comparison, on SQLite, DuckDB and Postgres

#### Scenario: Boolean parameter under FILTER
- **WHEN** a measure is `weighted_avg(amount, weight=status = 'paid') FILTER (WHERE amount > 10)`
- **THEN** it equals the plain average of the paid orders' amounts above 10

#### Scenario: Boolean parameter in every aggregation shape
- **WHEN** `weighted_avg(amount, weight=status = 'paid')` is evaluated with `window=` over a
  month time dimension, with `partition_by=`, cross-model from a query rooted at `customers`
  (`weighted_avg(orders.amount, weight=orders.status = 'paid')`), and by the association producer
  under `to_many_handling: "associate"`
- **THEN** each returns the plain average of the paid amounts in its window, partition, customer
  or entity, never a database error

#### Scenario: Emitted parameter SQL on every dialect
- **WHEN** SQL is generated for `weighted_avg(amount, weight=flag)` and
  `covar_samp(amount, other=flag)` on each supported dialect
- **THEN** each parameter enters the aggregate as the integer form of `flag`, never the raw boolean

#### Scenario: Custom aggregation receives the raw boolean
- **WHEN** a model defines a custom aggregation `true_amount` with formula
  `SUM(CASE WHEN {flag} THEN {value} END)` and a query selects
  `true_amount(amount, flag=status = 'paid')`
- **THEN** `{flag}` reaches the formula as the boolean and the query returns the paid orders'
  amount total on Postgres and DuckDB
