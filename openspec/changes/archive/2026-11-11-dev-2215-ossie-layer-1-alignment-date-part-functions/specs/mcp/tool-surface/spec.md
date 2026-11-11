## MODIFIED Requirements

### Requirement: Documented query idioms behave as documented
Every query idiom given as an example in the tool or schema descriptions SHALL execute and return the documented result:
- `change`, `change_pct` and `time_shift(…, -1)` with a `date_range` compare the first in-range bucket against the bucket before the range, and `time_shift(…, 1)` compares the last in-range bucket against the bucket after it;
- `time_shift(…, -1, 'year')` compares each bucket against the same bucket a year earlier;
- an aggregation with `window=` includes rows before the range in the first in-range buckets;
- `cumsum` with a `date_range` accumulates from the range start only;
- `ntile(x, n=4)` puts the lowest values in bucket 1;
- `agg(x) FILTER (WHERE c)` aggregates only the rows where `c` holds: `count(*) FILTER (WHERE status = 'paid')` counts the paid rows;
- a filter `<joined model>.<key> is null` keeps exactly the rows with no joined row;
- a query rooted at a model keeps every row of that model, counting 0 where no joined rows exist;
- ordering by an undisplayed aggregate with a `limit` returns the top rows by that aggregate without projecting it;
- a filter on `rank(..., partition_by=customers.regions.name, direction='desc') <= N` keeps the top N per region;
- `x / x(partition_by=customers.regions.name)` and `x / x(partition_by=[])` give the share of region and of grand total;
- a joined model's aggregate sliced by a non-attributable dimension broadcasts its total by default and splits per group under `associate`;
- a nested aggregation averages the inner per-group values within the outer group;
- a granularity call as a dimension (`date_trunc('month', created_at)`) and the time-dimension string `"date_trunc('month', created_at)"` bucket like the equivalent `{"dimension": ..., "granularity": "month"}` time dimension;
- a computed dimension `iif(sum(amount, partition_by=customers.id) > 150, 'high', 'low')` labels each row by its customer's total;
- an outer stage reads an inner dotted dimension by its flattened `__` name (`customers.regions.name` as `customers__regions__name`);
- a monthly time dimension on `time_spine.timestamp` with a `date_range` returns every month of the range, months without rows included (count 0, other aggregates NULL);
- rooted at customers, `sum(orders.amount)` and `sum(returns.amount)` in one query each equal the sum over that child model's own rows;
- rooted at customers and grouped by a parent dimension, a child row without a parent counts in that dimension's NULL row when the population has one and in no row otherwise, and an ungrouped aggregate includes it;
- rooted at customers, `sum(orders.amount) / count(*)` divides the orders' total by the number of customers;
- rooted at customers, `count(events.id) / count(*)` with `events` joined to nothing divides the events' total by the number of customers and warns `unrelated_dataset`;
- a filter on a transform's value (`rank(...) <= N`, `cumsum(...) > X`) applies after the transform, which is computed over every row the other filters keep;
- a time filter on the bucketed time column restricts `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over buckets and `partition_by` totals to the kept buckets, while `change`, `change_pct`, `time_shift` and `window=` read past it;
- a filter on a dimension restricts `rank`, `dense_rank`, `ntile`, `partition_by` shares and totals and nested aggregates to the kept peers; a filter on a non-time column restricts the rows `window=` reads, before the range as well;
- an inner stage computing a rank over all rows and an outer stage filtering one row returns that row's rank among all rows;
- a time-point filter is a half-open period: `order_date >= '2025-Q1'` keeps rows from the start of the quarter, `order_date <= '2025-Q1'` rows through its end, and `order_date in '2025-Q1'` rows inside it;
- `x ~ '^A[0-9]{3}'` keeps the rows whose value matches the pattern, its braces read as regex text;
- `flag IS NOT TRUE` keeps the rows where `flag` is false or NULL, and `x IS DISTINCT FROM 'a'` keeps the rows where `x` is NULL or another value;
- over a list column, `contains(tags, 'x')` and `tags.value = 'x'` keep the rows whose list holds `x`, `contains_any(tags, [...])` the rows whose list shares an element with the literal, and `contains_all(tags, [...])` the rows whose list holds every element of it;
- a variable inside quotes (`status = '{s}'`) binds its text as one string literal; a list variable as the whole right-hand side of `IN` (`status in ({statuses})`) keeps the rows whose value is in the list, and an empty list there fails with an error naming the variable, while an empty list as a list-function argument (`contains_all(tags, {wanted})`) binds as an empty list.

#### Scenario: Lookback before date_range
- **WHEN** a monthly query with `date_range` `"2025"` measures `change(sum(amount))`
- **THEN** January 2025's change is computed against December 2024

#### Scenario: Year-over-year time shift
- **WHEN** a monthly query with `date_range` `"2025"` measures `time_shift(sum(amount), -1, 'year')`
- **THEN** each month carries the amount of the same month of 2024

#### Scenario: Running total starts at the range
- **WHEN** a monthly query with `date_range` `"2025"` measures `cumsum(sum(amount))`
- **THEN** January 2025's value excludes 2024 amounts

#### Scenario: ntile puts the lowest values first
- **WHEN** a query grouped by customer name measures `ntile(sum(amount), n=4)`
- **THEN** the customers with the lowest totals are in bucket 1 and those with the highest in bucket 4

#### Scenario: A FILTER clause restricts the aggregated rows
- **WHEN** a query on orders measures `count(*) FILTER (WHERE status = 'paid')` and `count(*)`
- **THEN** the first value counts only the paid orders and the second every order

#### Scenario: Anti-join
- **WHEN** a query rooted at customers filters `orders.id is null`
- **THEN** exactly the customers with no orders are returned

#### Scenario: Cross-model aggregate broadcast vs associate
- **WHEN** a query rooted at orders, grouped by order status, measures `sum(customers.credit)`
- **THEN** by default each row carries the total credit of all customers with a broadcast warning, and with `to_many_handling` `associate` each row carries the credit of the distinct customers with orders in that status

#### Scenario: Granularity calls bucket like a time dimension
- **WHEN** one query groups by the dimension `date_trunc('month', created_at)`, another by the time dimension `"date_trunc('month', created_at)"`, and a third by `{"dimension": "created_at", "granularity": "month"}`, each measuring `count(*)`
- **THEN** all three return the same monthly buckets and counts

#### Scenario: A computed dimension over a fixed-grain aggregate
- **WHEN** a query on orders groups by the computed dimension `iif(sum(amount, partition_by=customers.id) > 150, 'high', 'low')` named `spend_band` and measures `count(*)`
- **THEN** each order is counted in the band of its customer's total amount

#### Scenario: Time spine returns empty buckets
- **WHEN** a query buckets `time_spine.timestamp` by month with `date_range` `"2025"` and measures `sum(orders.amount)` and `count(orders.id)`, and one month of 2025 has no orders
- **THEN** twelve rows are returned, the month without orders carrying count 0 and sum NULL

#### Scenario: Several child models in one query
- **WHEN** a query rooted at customers, grouped by customer name, measures `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** each customer's values equal the sums over that customer's own orders and own returns, unmultiplied by the other child model's rows

#### Scenario: Parentless child rows land in the parent dimension's NULL row
- **WHEN** a query rooted at customers, grouped by `regions.name`, measures `sum(orders.amount)`, one customer has no region and some orders have no (or a non-existent) customer
- **THEN** the NULL-region row holds the region-less customer's orders plus the parentless orders, and the same aggregate without dimensions includes the parentless orders

#### Scenario: Parentless child rows without a NULL population row
- **WHEN** the same query runs on data where every customer has a region
- **THEN** no NULL-region row is returned and the parentless orders count in no row

#### Scenario: Cross-model ratio
- **WHEN** a query rooted at customers measures `sum(orders.amount) / count(*)`
- **THEN** the value equals the orders' total divided by the number of customers

#### Scenario: A filter on a transform's value applies after it
- **WHEN** a monthly query over all months measures `cumsum(sum(amount))` and filters on that running total exceeding a threshold
- **THEN** the kept months carry the running total accumulated over every month, not only the kept ones

#### Scenario: A time filter restricts cross-row values except calendar look-backs
- **WHEN** a monthly query filters the bucketed time column to two months and measures one cross-row value
- **THEN** `cumsum`, `lag`, `lead`, `consecutive_periods`, `rank` / `ntile` over months and `sum(amount, partition_by=[])` are computed over the two kept months only, while `change`, `change_pct`, `time_shift` and `window=` equal their values in the unfiltered query

#### Scenario: A dimension filter restricts peer values
- **WHEN** a query grouped by region name and customer name filters one customer and measures a rank, dense rank, ntile, share of region, grand total or nested average
- **THEN** the value is computed over the kept customer only

#### Scenario: Rank among all rows needs a stage
- **WHEN** an inner stage ranks every customer by total amount and an outer stage keeps one customer
- **THEN** that customer carries its rank among all customers

#### Scenario: Unrelated-model ratio
- **WHEN** a query rooted at customers measures `count(events.id) / count(*)` and no join path connects `events` to `customers`
- **THEN** the value equals the number of events divided by the number of customers, and the response carries an `unrelated_dataset` warning

#### Scenario: Time points are half-open periods
- **WHEN** queries on orders measure `count(*)` with the filters `order_date >= '2025-Q1'`, `order_date <= '2025-Q1'` and `order_date in '2025-Q1'` in turn
- **THEN** the first counts the orders from 2025-01-01 on, the second the orders up to and including 2025-03-31, and the third the orders inside the first quarter of 2025

#### Scenario: Regex braces are pattern text
- **WHEN** a query filters `code ~ '^A[0-9]{3}'` with no variables supplied
- **THEN** it keeps exactly the rows whose code starts with `A` and three digits

#### Scenario: Null-aware boolean and distinctness tests
- **WHEN** a query filters `flag IS NOT TRUE`, and another filters `status IS DISTINCT FROM 'paid'`
- **THEN** the first keeps the rows whose `flag` is false or NULL, and the second the rows whose status is NULL or not `paid`

#### Scenario: List-column filters
- **WHEN** queries over a model with a list column `tags` filter `contains(tags, 'x')`, `tags.value = 'x'`, `contains_any(tags, ['x', 'y'])` and `contains_all(tags, ['x', 'y'])` in turn
- **THEN** the first two keep the rows whose list holds `x`, the third the rows whose list holds `x` or `y`, and the fourth the rows whose list holds both

#### Scenario: A quoted variable binds one string literal
- **WHEN** a filter is `status = '{s}'` with `variables={"s": "paid"}`
- **THEN** it keeps the rows whose status is `paid`

#### Scenario: A list variable fills a whole IN list
- **WHEN** a filter is `status in ({statuses})` with `variables={"statuses": ["paid", "shipped"]}`
- **THEN** it keeps the rows whose status is `paid` or `shipped`

#### Scenario: An empty list variable binds only as a list-function argument
- **WHEN** a filter is `status in ({statuses})` with `statuses` set to `[]`, and another is `contains_all(tags, {wanted})` with `wanted` set to `[]`
- **THEN** the first fails with an error naming `statuses` as an empty list, and the second keeps every row whose `tags` is not NULL
