## ADDED Requirements

### Requirement: Transforms over a constant input

A window transform — `cumsum`, `lag`, `lead`, `change`, `change_pct`, `time_shift`,
`consecutive_periods`, `rank`, `dense_rank`, `percent_rank` and `ntile` — whose input is a
bare literal SHALL execute at the query grain, treating the literal as the same value in
every result row, and SHALL NOT fail with an internal error. `first` and `last` over an
aggregate-free input bind as the aggregation, not the transform, and are outside this
requirement.

#### Scenario: Running and offset transforms over a literal
- **WHEN** a query rooted at `orders` with a month time dimension selects `sum(amount)`
  and, in turn, `cumsum(1)`, `lag(1)`, `lead(1)`, `change(1)`, `change_pct(1)` and
  `time_shift(1, -1)`
- **THEN** each executes on SQLite and DuckDB with hand-computed values: `cumsum(1)` is
  the running count of months within each partition, `lag(1)` is NULL in the first month
  and 1 after, `lead(1)` is 1 except NULL in the last month, `change(1)` is NULL in the
  first month and 0 after, `change_pct(1)` is NULL in the first month and 0 after, and
  `time_shift(1, -1)` is 1 wherever the previous month exists

#### Scenario: consecutive_periods over a literal predicate
- **WHEN** the same query selects `consecutive_periods(1 > 0)`
- **THEN** it executes on SQLite and DuckDB and each month carries its calendar-period
  run length, by hand-computed values

#### Scenario: Rank family over a literal
- **WHEN** a query grouped by a dimension selects `rank(1, direction='desc')`,
  `dense_rank(1, direction='desc')` and `percent_rank(1)`, with and without
  `partition_by=`
- **THEN** every row carries 1, 1 and 0 respectively on SQLite and DuckDB

#### Scenario: ntile over a literal
- **WHEN** a query grouped by a dimension selects `ntile(1, n=2)`
- **THEN** it executes on SQLite and DuckDB and the multiset of bucket numbers is the
  balanced split of the rows into two buckets; which row lands in which bucket is not
  specified, since every input value ties
