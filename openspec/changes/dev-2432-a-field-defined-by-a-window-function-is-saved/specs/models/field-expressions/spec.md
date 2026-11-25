## ADDED Requirements

### Requirement: A windowed field is a column of its dataset

A field whose runnable definition — the dialect entry run on the datasource — contains a window function (a **windowed field**) SHALL be computed over its dataset's rows, the rows passing the dataset's `filters` and the session policy, before any join, query filter or aggregation reads it; and SHALL then be read as a column of that dataset in every position: dimension, time dimension, aggregation source, inside `FILTER (WHERE …)` and `CASE`, partition key, ranking key, order key of a grouped or raw-row query, query filter, relationship key, and inside other fields' definitions. Its value SHALL NOT depend on the query's filters, on which dataset roots the query, or on the joins that reach it. A field whose definition reads a windowed field SHALL read its computed value; a window over a windowed field SHALL be computed over the computed values; a window reading a to-one joined field SHALL read that field's value for each of its dataset's rows. A windowed field's own `filter` SHALL mask its computed value. A window function appearing only inside a string literal SHALL NOT make a field windowed. Every position SHALL return the values it returns when the field is a stored column holding the computed values.

Scenarios use `orders(id, region, amount, ordered_at, customer_id)` with rows `(1, EU, 10, 2024-01-01, 1)`, `(2, EU, 20, 2024-01-02, 1)`, `(3, EU, 30, 2024-01-03, 2)`, `(4, US, 40, 2024-01-01, 2)`, `(5, US, 50, 2024-01-02, 3)`, `(6, US, 60, 2024-01-03, 3)`, `(7, AS, 70, 2024-01-01, 1)` and `customers(id, tier)` with rows `(1, gold)`, `(2, silver)`, `(3, gold)`, dataset `orders` joining `customers` many-to-one on `customer_id`. Field `orders.rn` is the dialect entry `ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)`, so orders `1` to `7` have `rn` `1, 2, 3, 1, 2, 3, 1`. Every scenario holds on SQLite and on DuckDB.

#### Scenario: As a dimension
- **WHEN** a query has the dimension `orders.rn` and the metric `sum(orders.amount)`
- **THEN** it returns `120` for `rn = 1`, `70` for `rn = 2` and `90` for `rn = 3`

#### Scenario: As an aggregation source
- **WHEN** a query has the metric `max(orders.rn)` and no dimension
- **THEN** it returns `3`

#### Scenario: Inside an aggregate's FILTER
- **WHEN** a query has the metric `sum(orders.amount) FILTER (WHERE orders.rn = 1)` and no dimension
- **THEN** it returns `120`

#### Scenario: As a partition key
- **WHEN** a query has the dimensions `orders.region` and `orders.rn` and the metric `sum(orders.amount, partition_by=orders.rn)`
- **THEN** every row with `rn = 1` carries `120`, every row with `rn = 2` carries `70`, and every row with `rn = 3` carries `90`

#### Scenario: As a ranking key
- **WHEN** a query has the dimension `orders.region` and the metric `last(orders.amount, orders.rn)`
- **THEN** it returns `30` for EU, `60` for US and `70` for AS

#### Scenario: As an order key of a grouped query
- **WHEN** a query has the dimension `orders.region`, the metric `sum(orders.amount)`, and the order `orders.rn` descending then `orders.region` ascending
- **THEN** its rows come in the order EU, US, AS, the order it gives when `rn` is a stored column holding the same values

#### Scenario: As a query filter
- **WHEN** a query has the dimension `orders.region`, the metric `sum(orders.amount)` and the filter `orders.rn = 1`
- **THEN** it returns `10` for EU, `40` for US and `70` for AS

#### Scenario: Raw rows under a query filter keep their values
- **WHEN** a raw-row query lists `orders.id` and `orders.rn` ordered by `orders.rn` with the filter `orders.id > 1`
- **THEN** orders `2` to `7` carry `rn` `2, 3, 1, 2, 3, 1`, the values computed over every order

#### Scenario: As a time dimension
- **WHEN** field `orders.prev_at` is the date dialect entry `LAG(ordered_at) OVER (PARTITION BY region ORDER BY ordered_at)` and a query has the time dimension `orders.prev_at` by day and the metric `count(*)`
- **THEN** it returns `3` for the NULL day, `2` for `2024-01-01` and `2` for `2024-01-02`

#### Scenario: A join target's windowed field is computed over its own rows
- **WHEN** field `customers.crn` is the dialect entry `ROW_NUMBER() OVER (ORDER BY id)` and a query rooted at `orders` has the dimension `orders.customers.crn` and the metric `sum(orders.amount)`, and a raw-row query lists `orders.id` and `orders.customers.crn`
- **THEN** the grouped query returns `100` for `crn = 1`, `70` for `crn = 2` and `110` for `crn = 3`, and orders `1` to `7` carry `crn` `1, 1, 2, 2, 3, 3, 1`

#### Scenario: A derived field over a windowed field
- **WHEN** field `orders.rn10` is `orders.rn * 10` and a query has the dimension `orders.rn10` and the metric `count(*)`
- **THEN** it returns `3` for `10`, `2` for `20` and `2` for `30`

#### Scenario: A window over a windowed field
- **WHEN** field `orders.ww` is the dialect entry `SUM(orders.rn) OVER ()` and field `orders.w3` is `MAX(orders.ww) OVER (PARTITION BY region)`, and a raw-row query lists `orders.id`, `orders.ww` and `orders.w3`, and a grouped query has the dimension `orders.ww` and the metric `count(*)`
- **THEN** every order carries `ww = 13` and `w3 = 13`, and the grouped query returns `7` for `13`

#### Scenario: A window reading a to-one joined field
- **WHEN** field `orders.tier_rn` is the dialect entry `ROW_NUMBER() OVER (PARTITION BY customers.tier ORDER BY id)` and a raw-row query lists `orders.id` and `orders.tier_rn`, and a grouped query has the dimension `orders.tier_rn` and the metric `count(*)`
- **THEN** orders `1` to `7` carry `1, 2, 1, 2, 3, 4, 5`, and the grouped query returns `2, 2, 1, 1, 1` for `tier_rn` `1` to `5`

#### Scenario: A windowed field's filter masks its value
- **WHEN** field `orders.rn_eu` has the dialect entry of `rn` and the filter `region = 'EU'`, and a query has the metric `sum(orders.rn_eu)`, and another has the dimension `orders.rn_eu` and the metric `count(*)`
- **THEN** the first returns `6`, and the second returns `1` for each of `1`, `2` and `3` and `4` for NULL

#### Scenario: The window runs over the dataset's filtered rows
- **WHEN** dataset `big_orders` reads table `orders` with the filter `amount >= 20` and the same field `rn`, and a query has the dimension `big_orders.rn` and the metric `sum(big_orders.amount)`
- **THEN** it returns `130` for `rn = 1`, `80` for `rn = 2` and `60` for `rn = 3`

#### Scenario: A SQL-backed dataset
- **WHEN** a dataset backed by the SQL `SELECT * FROM orders` declares field `rn`
- **THEN** it answers the dimension, query-filter and raw-rows-under-a-filter scenarios with the values `orders` gives

#### Scenario: A windowed relationship key
- **WHEN** dataset `slots` reads `slots(pos, label)` with rows `(1, first)`, `(2, second)`, `(3, third)`, `orders` joins `slots` many-to-one from `rn` to `pos`, and a query has the dimension `orders.slots.label` and the metric `sum(orders.amount)`; and separately the join's target key is a windowed field of the target (`slots` reading `slot_src(label, sort_key)` with rows `(first, 10)`, `(second, 20)`, `(third, 30)` and `pos` the dialect entry `ROW_NUMBER() OVER (ORDER BY sort_key)`), with a source key that is stored or windowed
- **THEN** each query returns `120` for `first`, `70` for `second` and `90` for `third`

#### Scenario: A filter across a fanning hop reads the computed key
- **WHEN** a query rooted at `slots` has the dimension `slots.label`, the metric `count(slots.*)`, and a filter on `orders.amount >= 60` reached across the one-to-many reverse of that join
- **THEN** it returns `1` for `first` and `1` for `third` and no row for `second`, the association matching orders on their computed `rn`

#### Scenario: The session policy restricts the rows before the window
- **WHEN** field `orders.seq` is the dialect entry `ROW_NUMBER() OVER (ORDER BY id)` and a session policy restricts `orders` to `region = 'US'`, and a raw-row query lists `orders.id` and `orders.seq`
- **THEN** it returns orders `4`, `5` and `6` with `seq` `1`, `2` and `3`

#### Scenario: OVER inside a string literal is not a window
- **WHEN** field `orders.note` is the dialect entry `'over (x)' || region` and a query has the dimension `orders.note` and the metric `count(*)`
- **THEN** it returns `3` for `over (x)EU`, `3` for `over (x)US` and `1` for `over (x)AS`, the field read as an ordinary derived field

### Requirement: A portable windowed field is refused at both save doors

A field whose portable `sql` (or `OSSIE_SQL_2026` entry) is the form it runs on the datasource and contains a window function SHALL be refused when saved, through the engine and through the storage backend alike, with the window-function error naming the field, leaving the store unchanged. A query reading such a field stored without validation SHALL fail with the same error. A field whose datasource-dialect entry is windowed SHALL be accepted whatever its portable `sql` holds.

#### Scenario: Both doors refuse a portable window
- **WHEN** a dataset is saved with field `rn` declared as `expression: "ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)"`, once through the engine and once through the storage backend with validation on
- **THEN** both saves fail with the window-function error naming `rn`, and nothing is stored

#### Scenario: A stored portable window fails the query
- **WHEN** such a dataset was stored without validation and a query reads `rn`
- **THEN** the query fails with the window-function error naming `rn`
