## ADDED Requirements

### Requirement: An ungrained inner aggregate types at the query grain

An aggregate with no `partition_by=` SHALL have the query grain wherever it stands in a measure, filter or order key: directly under a transform, beside coarser or finer aggregates in a transform's operand, inside a second-order aggregation under a transform, inside a transform used as an aggregation source or aggregation parameter, and in the operand of a `first` / `last` over an aggregate — at any depth, and whether it reads the population's dataset, another dataset, or carries `window=` (a windowed one's grain includes the query's active time bucket). Every such query SHALL return the same values as the spelling that writes the query grain on that aggregate with `partition_by=`. A transform's operand grain SHALL be the union of its inner aggregates' grains so read, and the transform SHALL evaluate at that grain in every position.

A grain the aggregate has by default SHALL stay unwritten for the mode axis: an unattributable query dimension SHALL resolve per `to_many_handling` exactly as for the same aggregate standing alone — associated with the `associated` warning when the mode is omitted or `"associate"`, broadcast with its warning under the deprecated `"broadcast"`, refused under `"error"` — and no warning or error SHALL name a `partition_by=` the query did not write. Dimension position SHALL keep its existing rule (`queries/computed-dimensions`): an aggregate with no `partition_by=` there is refused.

Scenarios marked "the city seed" use one dataset `orders(id, customer_id, region, city, status, amount, ordered_at)` with rows North/A 10 (2025-01) and 25 (2025-02), North/B 30 (2025-01) and 40 (2025-02), South/C 50 (2025-01), South/D 20 (2025-01) and 40 (2025-02); `share` abbreviates `sum(orders.amount) / sum(orders.amount, partition_by=orders.region)`. Scenarios marked "the Appendix B seed" use the seed of "Transforms evaluate over their operand's domain". Every scenario holds by executed values on SQLite and DuckDB.

#### Scenario: The rank family over a mixed operand ranks the query's cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `rank(share, direction='desc')`, `dense_rank(share, direction='desc')`, `percent_rank(share)` and `ntile(share, n=2)`
- **THEN** for A, B, C, D the ranks and dense ranks are 4, 1, 3, 2, the percent ranks 0, 1, 1/3, 2/3 and the tiles 1, 2, 1, 2, each equal to the spelling with `sum(orders.amount, partition_by=[orders.region, orders.city])` as the numerator

#### Scenario: The same rank as a filter and as an order key
- **WHEN** the query selects `sum(orders.amount)` and filters `rank(share, direction='desc') = 1`, and another orders by `rank(share, direction='desc')` ascending
- **THEN** the filter keeps exactly North/B, and the order is B, D, C, A

#### Scenario: Other spellings of the mixed operand
- **WHEN** the query selects `rank(sum(orders.amount) - sum(orders.amount, partition_by=orders.region), direction='desc')`, the rank of a dataset metric defined as `share`, and `rank(count(orders.*) / sum(orders.amount, partition_by=orders.region), direction='desc')`
- **THEN** each equals its explicit-grain spelling; the count form reads 1, 1, 4, 3 for A, B, C, D

#### Scenario: A month bucket joins the query grain
- **WHEN** the city-seed query groups by `orders.region`, `orders.city` and month of `orders.ordered_at` and selects `rank(share, direction='desc')`, and again with `partition_by=orders.region` on the rank
- **THEN** in cell order (North A Jan, A Feb, B Jan, B Feb, South C Jan, D Jan, D Feb) the ranks are 7, 5, 4, 2, 1, 6, 3, and within regions 4, 3, 2, 1, 1, 3, 2

#### Scenario: Time-ordered transforms over a mixed operand
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `cumsum(share)`, `lag(share)`, `lead(share)` and `consecutive_periods(share > 0.5)`
- **THEN** for North Jan, North Feb, South Jan, South Feb `cumsum` reads 0.381, 1.0, 0.636, 1.0; `lag` NULL, 0.381, NULL, 0.636; `lead` 0.619, NULL, 0.364, NULL; `consecutive_periods` 0, 1, 1, 0 — no internal planner error

#### Scenario: Nested transforms over a mixed operand
- **WHEN** the same query selects `rank(cumsum(sum(orders.amount)) / sum(orders.amount, partition_by=orders.region), direction='desc')` and `cumsum(rank(share, direction='desc'))`
- **THEN** they read 4, 1, 3, 1 and 3, 5, 1, 5

#### Scenario: Shapes that already work keep their values
- **WHEN** the same query selects `time_shift(share, -1)`, `change(share)`, `change_pct(share)`, `first(share)`, `last(share)`, a rank over `sum(orders.amount) / sum(orders.amount, partition_by=[])`, a rank over a three-way mix of an ungrained, a region-grained and a scalar sum, a mixed operand whose numerator is `sum(orders.amount, window='2m')`, and `time_shift` of an operand mixing a region-grained aggregate with the dimension `orders.region`
- **THEN** each returns the values it returns before this change, equal to its explicit-grain spelling

#### Scenario: A row-level leaf beside a coarser aggregate
- **WHEN** a city-seed query groups by `orders.region` and `orders.city` and selects `rank(sum(orders.amount, partition_by=orders.region) * iif(orders.city = 'A', 1, 2), direction='desc')`
- **THEN** A, B, C, D read 4, 3, 1, 1 — no internal planner error

#### Scenario: A second-order aggregation under a transform keeps its inner at the query grain
- **WHEN** a city-seed query groups by `orders.region` and `orders.city` and selects `rank(max(sum(orders.amount), partition_by=orders.region), direction='desc')`
- **THEN** North reads 1 and South 2 on every row (the largest city total is 70 in North and 60 in South), equal to the spelling with `sum(orders.amount, partition_by=[orders.region, orders.city])`

#### Scenario: A cross-dataset second-order aggregation under a transform
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and `customers.segment` and selects `rank(max(sum(orders.amount), partition_by=customers.region), direction='desc')`
- **THEN** the EAST rows and the WEST row read 1 and NORTH reads NULL, equal to the spelling with `sum(orders.amount, partition_by=[customers.region, customers.segment])`

#### Scenario: A finer operand with a windowed inner as an aggregation source
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `max(rank(sum(orders.amount, window='2m') / sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]), direction='desc'))`
- **THEN** North Jan, North Feb, South Jan, South Feb read 7, 5, 6, 4

#### Scenario: A finer operand with a windowed inner under last
- **WHEN** the same query selects `max(last(sum(orders.amount, window='2m') / sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at])))`
- **THEN** the four cells read 4.2, 4.2, 2.75, 2.75

#### Scenario: A finer operand with a cross-dataset inner
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and selects `max(rank(sum(orders.amount) / sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc'))`, and another adds month of `orders.ordered_at` and selects `max(last(sum(orders.amount) / sum(orders.amount, partition_by=[customers.region, customers.segment, orders.ordered_at])))`
- **THEN** the first reads EAST 2, WEST 3 and NORTH NULL; in the second EAST January reads 3.0

#### Scenario: Windowed inners nested in transforms inside an aggregation
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `max(rank(cumsum(sum(orders.amount, window='2m')) / sum(orders.amount, partition_by=orders.region), direction='desc'))`, and `max(last(sum(orders.amount, window='2m')))`
- **THEN** the first reads 4, 2, 3, 1; the second equals its explicit-grain spelling

#### Scenario: Two time buckets are both in the defaulted grain
- **WHEN** a query over a monthly dataset with totals 15, 35, 30 for January to March 2024 declares time dimensions at month and at year, in either order, and selects `sum(rank(sum(monthly.amount), direction='desc'))`
- **THEN** the months read 3, 1, 2, as before this change: the inner sum's grain holds both buckets

#### Scenario: Cross-dataset shares rank at the query grain
- **WHEN** a query on the Appendix B seed groups by `customers.region` and `customers.segment` and selects `rank(sum(orders.amount) / sum(orders.amount, partition_by=customers.region), direction='desc')`, once without `population` and once with `population: customers`; another, same grouping, selects `rank(sum(orders.amount) - sum(returns.amount, partition_by=customers.region), direction='desc')`; another selects `cumsum` of the cross-dataset share by `customers.region` and month of `orders.ordered_at`
- **THEN** the first reads (EAST, corporate) 3, (EAST, retail) 2, (WEST, retail) 1, with NORTH NULL under the named population; the second (EAST, corporate) 2, (EAST, retail) 1 and WEST NULL; the third EAST 0.429 then 1.0 and WEST 1.0

#### Scenario: A defaulted grain keeps the mode axis
- **WHEN** a query on the Appendix B seed names `population: orders`, groups by `orders.status` and selects `max(rank(sum(returns.amount), direction='desc'))`, under each `to_many_handling`; and likewise `max(rank(count(customers.*), direction='desc'))`, `weighted_avg(orders.amount, weight=rank(sum(returns.amount), direction='desc'))`, `max(rank(count(tickets.*), direction='desc'))` by `customers.region`, and `max(last(sum(returns.amount)))` by status and month
- **THEN** each returns the values and warnings it returns before this change: the `associated` warning when the mode is omitted, the value with the `broadcast` warning under `"broadcast"`, the error-mode refusal naming the metric as written under `"error"`; none raises an error about a `partition_by=`

#### Scenario: A finer operand does not make a sibling's grain written
- **WHEN** the same population and grouping select `max(rank(sum(returns.amount) / sum(returns.amount, partition_by=[orders.status, customers.region]), direction='desc'))` with the mode omitted
- **THEN** every row reads 1 and the `associated` warning is present

#### Scenario: A written and a defaulted spelling of one grain agree
- **WHEN** a city-seed query by `orders.region` and `orders.city` selects `max(rank(sum(orders.amount), direction='desc'))` beside `max(rank(sum(orders.amount, partition_by=[orders.region, orders.city]), direction='desc'))`
- **THEN** both read 4, 1, 3, 2 and the result has two distinct columns

#### Scenario: A sibling measure changes no value
- **WHEN** the mixed-operand rank query adds `sum(orders.amount)` and `rank(sum(orders.amount), direction='desc')`
- **THEN** every value of every measure is unchanged

#### Scenario: A mixed-operand measure beside a computed dimension
- **WHEN** a city-seed query's dimensions are `orders.region` and the computed dimension `sum(orders.amount, partition_by=orders.city)`, and it selects `rank(share, direction='desc')`
- **THEN** the four cells (North 35, North 70, South 50, South 60) read 4, 1, 3, 2, and a dimension `rank(sum(orders.amount), direction='desc')` is still refused with the existing computed-dimension error
