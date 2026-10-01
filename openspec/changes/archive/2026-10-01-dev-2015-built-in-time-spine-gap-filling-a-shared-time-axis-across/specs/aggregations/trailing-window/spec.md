## ADDED Requirements

### Requirement: A windowed aggregate is evaluated at every population cell

A windowed aggregate SHALL have a value at every cell of the population, computed over its home rows in that cell's trailing interval, whether or not its home has rows in the cell's own bucket; this SHALL hold for local and cross-model homes, with `partition_by=`, for `first` / `last`, and with several windowed aggregates from different homes in one query. Adding a windowed measure SHALL NOT change the result's rows or any other measure's values.

#### Scenario: Month without home rows reads its trailing interval

- **WHEN** a query rooted at a `calendar` model (one row per day, joined from `orders.order_date`) groups by month over 2025-01..2025-06 and selects `sum(orders.amount, window='2m')` over orders 2025-01-10 (100), 2025-01-20 (50), 2025-02-05 (70)
- **THEN** the values are Jan 150, Feb 220, Mar 70, Apr NULL, May NULL, Jun NULL on SQLite and DuckDB — March has no orders but its interval holds February's

#### Scenario: Two homes, partitions and ranked picks

- **WHEN** a spine query (`queries/time-spine`) groups by `customers.region` and month over the same range, with `returns` also wired, and selects `sum(orders.amount, window='2m')`, `last(orders.amount, window='2m')` and `count(returns.id, window='3m')`
- **THEN** each cell's values are the aggregates over that region's orders or returns in the cell's trailing interval, 0 for an empty count interval and NULL otherwise, and removing any one of the three measures leaves the rows and the other two measures unchanged
