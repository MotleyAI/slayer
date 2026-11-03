## MODIFIED Requirements

### Requirement: An aggregation accepts a FILTER clause
`agg(…) FILTER (WHERE c)` SHALL be accepted on every aggregation — built-in, aliased and model-defined custom — in every aggregation position, and SHALL mean the aggregation computed over only the rows where `c` is true. `c` SHALL be a row-level predicate over the aggregation's dataset; an aggregate, a transform or an attached value inside `c` SHALL be rejected with a typed query error naming the clause. Every row the aggregation reads SHALL be restricted by `c` — the source value, every column-valued parameter (`weight=`, `other=`), the ranking key of `first` / `last`, and every row a custom aggregation's formula reads directly (`COUNT(*)`, a bare column); literal parameters are unaffected. The restriction SHALL apply to the aggregation's own rows only, never to the query's result rows, a windowed aggregation's time buckets or any other measure. A grouped cell with rows but none passing `c` SHALL take the aggregation's empty value — NULL for a custom aggregation; with no grain, a custom aggregation SHALL read its formula's value over no rows. The clause SHALL compose with `partition_by=` and `window=`. An unnamed formula containing a filtered aggregation SHALL take its result key from its full text by the existing expression-naming rule — the same rule whether or not the condition holds a `{variable}`, a variable naming itself by its placeholder — and spellings of one formula that differ only in case or whitespace SHALL share that key. A filtered aggregation and the same aggregation unfiltered, or filtered by a different condition, SHALL never share a result key or a value, whether they are named or unnamed, in one formula or in separate measures.

#### Scenario: Filtered count
- **WHEN** a query on `orders` by `region` has measure `count(*) FILTER (WHERE status = 'paid')`
- **THEN** each region's value is its number of paid orders, 0 where it has none

#### Scenario: Filtered sum equals a masked sum
- **WHEN** one measure is `sum(amount) FILTER (WHERE status = 'paid')` and another `sum(iif(status = 'paid', amount, NULL))`
- **THEN** both return the same values

#### Scenario: Filtered weighted average uses only passing rows
- **WHEN** a measure is `weighted_avg(price, weight=quantity) FILTER (WHERE status = 'paid')`
- **THEN** it equals the weighted average computed over the paid rows alone, numerator and denominator both

#### Scenario: Filtered last picks the newest passing row
- **WHEN** a query by `customer_id` has measure `last(balance, updated_at) FILTER (WHERE status = 'active')` and a customer's newest row is inactive
- **THEN** the value is the balance of that customer's newest active row

#### Scenario: Aggregate inside the condition rejected
- **WHEN** a measure is `sum(amount) FILTER (WHERE amount > avg(amount))`
- **THEN** it is rejected with the typed error naming the FILTER clause

#### Scenario: With partition_by
- **WHEN** a query by `region, city` has measure `sum(amount, partition_by=region) FILTER (WHERE status = 'paid')`
- **THEN** each row carries its region's paid total

#### Scenario: Constant condition named from its text
- **WHEN** a query on `orders` has the unnamed measures `count(*) FILTER (WHERE status = 'paid')` and `sum(amount) FILTER (WHERE amount > 10)`
- **THEN** their result keys are `orders.count_filter_where_status_paid` and `orders.amount_sum_filter_where_amount_10`, never a name built from an internal masked form

#### Scenario: Variable and constant conditions follow one rule
- **WHEN** one query has the unnamed measure `sum(amount) FILTER (WHERE amount > {n})` with `n = 10` and another has `sum(amount) FILTER (WHERE amount > 10)`
- **THEN** their result keys are `orders.amount_sum_filter_where_amount_n` and `orders.amount_sum_filter_where_amount_10`, both derived from the formula text as written

#### Scenario: Spellings of one formula share a key
- **WHEN** the unnamed measure is written `count(*) filter (where status = 'paid')` instead
- **THEN** its result key is `orders.count_filter_where_status_paid`

#### Scenario: Filtered first beside the unfiltered first
- **WHEN** a query on `orders` has the unnamed measures `first(amount) FILTER (WHERE status = 'open')` and `first(amount)`, and a second query has the same two measures named `fo` and `f`
- **THEN** both queries execute, each measure under its own result key, with `fo` the amount of the earliest open order and `f` the amount of the earliest order

#### Scenario: Filtered and unfiltered first inside one formula
- **WHEN** a measure is `first(amount) FILTER (WHERE status = 'open') - first(amount)` and the same with `last`
- **THEN** each returns the difference of the two distinct picks, never 0 from one pick read twice

#### Scenario: Conditions whose names coincide stay distinct
- **WHEN** a query has the measures `first(amount) FILTER (WHERE status = 'open')` and `first(amount) FILTER (WHERE status <> 'open')`, named `a` and `b`
- **THEN** `a` is the amount of the earliest open order and `b` the amount of the earliest order that is not open

#### Scenario: Filtered star count over a joined condition beside the plain count
- **WHEN** a query on `orders` has the unnamed measures `count(*) FILTER (WHERE customers.name = 'Alice')` and `count(*)`
- **THEN** both execute under distinct result keys, with Alice's order count and the total order count

#### Scenario: Ordering by the filtered formula
- **WHEN** a query on `orders` by `status` has the measure `count(*) FILTER (WHERE status = 'paid')` and orders by the same formula text descending
- **THEN** the order applies to that measure's column

#### Scenario: Filtered custom row count
- **WHEN** `orders` defines the custom aggregation `rows` with formula `COUNT(*)` and a query on `orders` by `region` has measure `rows(amount) FILTER (WHERE status = 'paid')`
- **THEN** each region's value is its number of paid orders, and NULL for a region whose orders include none that are paid

#### Scenario: Filtered custom formula reading a raw column
- **WHEN** `orders` defines the custom aggregation `per_order` with formula `SUM({value}) / COUNT(DISTINCT id)` and a measure is `per_order(amount) FILTER (WHERE status = 'paid')`
- **THEN** it equals the paid amount divided by the number of distinct paid orders, grouped or ungrouped

#### Scenario: Filtered custom formula reading only placeholders
- **WHEN** `orders` defines `wsum` with formula `SUM({value} * {weight})` (`weight` defaulting to `quantity`) and a measure is `wsum(price) FILTER (WHERE status = 'paid')`
- **THEN** it equals `wsum(price)` over a query restricted to the paid rows

#### Scenario: Ungrouped filtered custom aggregation with no passing rows
- **WHEN** a query on `orders` with no dimensions has measure `rows(amount) FILTER (WHERE status = 'none')`
- **THEN** the value is 0, the formula's value over no rows

#### Scenario: Filtered custom aggregation in a trailing window
- **WHEN** a query on `orders` by month has measure `rows(amount, window='2m') FILTER (WHERE status = 'paid')` and one month has orders but none paid while the month before has a paid order
- **THEN** that month is in the result and its value counts the paid orders of its trailing interval

#### Scenario: Filtered custom aggregation beside unfiltered measures
- **WHEN** a query on `orders` by `region` has the measures `rows(amount) FILTER (WHERE status = 'paid')`, `rows(amount)` and `count(*)`
- **THEN** the result has one row per region and the unfiltered measures equal their values in the query without the filtered one

#### Scenario: Filtered custom aggregation over a joined model
- **WHEN** a query on `customers` by `name` has measure `rows(orders.amount) FILTER (WHERE orders.status = 'paid')`
- **THEN** each customer's value is their number of paid orders, NULL for a customer with no paid order
