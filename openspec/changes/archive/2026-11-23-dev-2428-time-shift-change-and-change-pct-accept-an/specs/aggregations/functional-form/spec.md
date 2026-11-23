## MODIFIED Requirements

### Requirement: Default ranking key
A ranked `first` / `last` aggregation without an explicit ranking key SHALL rank by,
in order: the first entry of the query's `dimensions` (in projection order) that is
row-level and types DATE or TIMESTAMP — a column or a computed dimension, never a
granularity bucket, which ties every row of a cell; else the first time dimension's un-truncated source, column or
expression; else the model's default time dimension; else it SHALL fail with the
existing ranking-key resolution error. The query is the one the aggregation is a
measure of — each stage its own — whatever the aggregation's `partition_by=`,
`window=`, `time_shift` or nesting inside another aggregation. A cross-model
aggregation SHALL read each candidate in its home's coordinates, skipping one its home
does not determine, and fall back to its home model's default time dimension.

#### Scenario: A temporal computed dimension is the default ranking key
- **WHEN** a query groups by the computed dimension `coalesce(shipped_at, created_at)`
  (typed TIMESTAMP) and a month time dimension on `created_at`, and selects
  `last(amount)`
- **THEN** each cell's value is the `amount` of its row latest by the coalesced
  timestamp, by executed values on SQLite and DuckDB

#### Scenario: An expression time dimension's source is the default ranking key
- **WHEN** a query's only temporal dimension is the time dimension
  `coalesce(shipped_at, created_at)` at `month` and it selects `last(amount)`
- **THEN** each month's value is the `amount` of its row latest by the un-truncated
  coalesced timestamp

#### Scenario: A partition coarser than the query reads the query's temporal dimension
- **WHEN** a query on `orders` groups by `region` and the computed dimension
  `e = coalesce(shipped_at, created_at)` with a month time dimension on `created_at`,
  and selects `last(amount, partition_by=region)`
- **THEN** US is 60 and EU is 50 (the rows latest by `e`, the non-temporal `region`
  listed first notwithstanding), never US 40 by the model default `created_at`, by
  executed values on SQLite and DuckDB

#### Scenario: A partition coarser than the query reads the time dimension's source
- **WHEN** a query on `orders` (default time dimension `created_at`) groups by `region`
  with a month time dimension on `shipped_at`, and selects
  `last(amount, partition_by=region)`
- **THEN** the bucket is skipped and its source ranks: US is 60 and EU is 50 by
  `shipped_at`, never US 40 by `created_at`

#### Scenario: No model default with a partition coarser than the query
- **WHEN** `orders` declares no default time dimension and a query groups by `region`
  with a month time dimension on `created_at`, selecting
  `last(amount, partition_by=region)`
- **THEN** it ranks by `created_at`: US is 40 and EU is 50, instead of failing with the
  ranking-key resolution error

#### Scenario: A ranked aggregation nested in a second-order aggregation
- **WHEN** the query of the coarser-partition scenario selects
  `avg(last(amount, partition_by=[region, product]), partition_by=region)`
- **THEN** the inner `last` ranks by `e`: US is 45 and EU is 35, never US 35 by
  `created_at`

#### Scenario: A windowed partitioned ranked aggregation
- **WHEN** the query of the coarser-partition scenario selects
  `last(amount, window='90d', partition_by=region)`
- **THEN** each interval's rows rank by `e`: the US February cells are 60, never 40 by
  the window axis `created_at`

#### Scenario: A shifted partitioned ranked aggregation
- **WHEN** `orders` declares the default time dimension `shipped_at` and a query groups
  by `region` with a year time dimension on `created_at`, selecting
  `time_shift(first(amount, partition_by=[region, created_at]), 1)`
- **THEN** the US 2024 cell is 30 — the US 2025 value ranked by `created_at` — never 40 by
  the model default `shipped_at`

#### Scenario: Each stage reads its own dimensions
- **WHEN** a first stage on `orders` groups by `id`, `region`, `e` and `created_at` and
  selects `last(amount, partition_by=region)`, and a second stage over it groups by its
  `region` and `created_at` and selects `last(id, partition_by=region)`
- **THEN** the first stage's US value is 60 (by `e`) and the second stage's US value is
  4 (by its own `created_at`), never 6 by the first stage's `e`

#### Scenario: A cross-model aggregation reads a query dimension its home determines
- **WHEN** a query on `orders` groups by `customers.region` with a month time dimension
  on `customers.updated_at`, and selects
  `last(customers.spend, partition_by=customers.region)` (`customers` declares no
  default time dimension)
- **THEN** North is 100 and South is 200, ranked by `customers.updated_at`, instead of
  failing with the ranking-key resolution error

#### Scenario: A cross-model aggregation reads a computed query dimension
- **WHEN** a query on `orders` groups by `customers.region` and the computed dimension
  `coalesce(customers.updated_at, customers.signup_ts)`, selecting
  `last(customers.spend, partition_by=customers.region)`
- **THEN** North is 300 and South is 200, ranked by the coalesced timestamp

#### Scenario: A cross-model aggregation skips a query dimension its home does not determine
- **WHEN** `customers` declares the default time dimension `signup_ts`, and a query on
  `orders` groups by `customers.region`, `orders.created_at` and `customers.updated_at`
  in that order, selecting `last(customers.spend, partition_by=customers.region)`
- **THEN** `orders.created_at` is skipped and `customers.updated_at` ranks: North is
  100 and South is 200, never North 300 by the default `signup_ts`

#### Scenario: A cross-model aggregation falls back to its home's default
- **WHEN** `customers` declares the default time dimension `signup_ts`, and a query on
  `orders` groups by `customers.region` with a month time dimension on
  `orders.created_at`, selecting `last(customers.spend, partition_by=customers.region)`
- **THEN** North is 300 and South is NULL, ranked by `signup_ts`

#### Scenario: A defaulted query dimension crossing a fanning hop fails closed
- **WHEN** a query on `accounts` (`accounts → orders` one-to-many) groups by
  `accounts.id` and `orders.created_at`, selecting
  `last(accounts.spend, partition_by=accounts.id)`
- **THEN** it fails at plan time with the ranked-input safety error naming `created_at`
  and the hop to `orders`, as the same query without `partition_by=` does

