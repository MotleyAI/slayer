## ADDED Requirements

### Requirement: A re-aggregation inside a computed dimension is a value of its grain members
A computed dimension containing an aggregation over an aggregate (a re-aggregation, e.g. `CASE WHEN max(sum(rate, partition_by=[team, name]), partition_by=team) > 50 THEN 1 ELSE 0 END` as `band`) SHALL evaluate, wherever the query computes a value grouped, partitioned or filtered by it — the query's own rows and every intermediate result it needs, whatever dataset that result is computed over — as the re-aggregation's value at its grain, read through that result's own path to the grain members. Its home and attributability SHALL be those of its grain members (`queries/semantics`): from a dataset that does not determine them, it associates in associate mode and is refused in error mode, exactly as a plain dimension over the same members. It SHALL compose with every metric, `partition_by=` key, windowed, ranked and time-ordered measure, filter and later stage a plain dimension composes with, its values independent of the query's root, and SHALL never fail with a database error. A field whose name begins with `__regroup__` SHALL be an ordinary field.

#### Scenario: Beside a metric read across a relationship
- **WHEN** a query on `contractors` (list column `skills`; `assignments` joins `contractors` many-to-one) groups by `contractors.team` and `band` and selects `count(assignments.id)`, or `count(contractors.skills.*)`, or a query on `assignments` groups the same way and selects `sum(contractors.rate * assignments.id)`
- **THEN** each query returns, per (team, band), the metric's value for that team with that team's band, correct by executed values

#### Scenario: Independent of the query root
- **WHEN** the same dimensions and the metric `sum(contractors.rate * length(contractors.skills.value))`, `sum(contractors.rate * assignments.id)` or `sum(contractors.rate * length(contractors.skills.value), partition_by=band)` are queried once from `contractors` and once from `assignments`
- **THEN** every (team, band) cell both queries return carries the same value

#### Scenario: As a partition key on one dataset
- **WHEN** a query on a single dataset `invoices (id, account_id, state, fee)` with no relationships groups by `invoices.account_id` and `band` = `CASE WHEN max(sum(invoices.fee, partition_by=[invoices.account_id, invoices.state]), partition_by=invoices.account_id) > 5 THEN 1 ELSE 0 END` and selects `sum(invoices.fee, partition_by=band)`
- **THEN** each row carries its band's total fee, correct by executed values, also when `band` is the only dimension

#### Scenario: Two re-aggregation dimensions in one partition
- **WHEN** a query declares two such dimensions `band` and `band2` and selects `sum(invoices.fee, partition_by=[band, band2])`, or, across a relationship, `sum(contractors.rate * length(contractors.skills.value), partition_by=[band, band2])` from `contractors` and from `assignments`
- **THEN** each row carries its (band, band2) total, correct by executed values

#### Scenario: Beside windowed, ranked and transform measures
- **WHEN** a query groups by a monthly `orders.ordered_at` and `band` and selects `sum(orders.amount, window='60d')`, `last(orders.amount)`, `cumsum(sum(orders.amount))`, `time_shift(sum(orders.amount), -1)` or `rank(sum(orders.amount), partition_by=band, direction='desc')`
- **THEN** each measure equals its value per (month, band) computed without the re-aggregation, from the same rows, correct by executed values

#### Scenario: Unattributable from the population associates
- **WHEN** a query on `customers` (each customer has many `orders`) groups by `customers.tier` and a `band` built over `orders.region` and selects `count(customers.*)` and `sum(customers.spend)`
- **THEN** each customer counts once in every (tier, band) cell one of its orders reaches, never once per order

#### Scenario: Unattributable from the population is refused in error mode
- **WHEN** the same query runs with `to_many_handling` set to `error`
- **THEN** it fails with the unattributable-dimension error naming `band`, never returning values

#### Scenario: Consumed by a later stage
- **WHEN** a first stage groups by `orders.region` and `band` and a second stage groups the first stage's rows by its `band` column
- **THEN** the second stage reads `band` as a plain column, correct by executed values

#### Scenario: A field named with the former reserved prefix
- **WHEN** a dataset declares a field `__regroup__0__max` and a query groups by it beside a re-aggregation dimension
- **THEN** the field reads its own column and the query succeeds
