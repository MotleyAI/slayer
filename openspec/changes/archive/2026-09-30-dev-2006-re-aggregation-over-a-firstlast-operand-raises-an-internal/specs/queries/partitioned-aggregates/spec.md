## ADDED Requirements

### Requirement: Re-aggregation over ranked and windowed operands
A partitioned or bare `first`/`last` aggregation and a windowed aggregation SHALL be legal re-aggregation operands and compute like any other operand, per `queries/semantics` › Second-order aggregation over attached values: each ranked operand picks the value at the earliest/latest ranking timestamp per operand cell — honouring an explicit ranking column, else the model's resolved default — and each windowed operand evaluates its trailing window per operand cell, before the outer aggregation consumes the cells. This SHALL hold for every outer aggregation of the plain scalar family, for operands combining several ranked, windowed and plain constituents (each keeping its own ranking key or window), for local and cross-model operands, with or without a query time dimension, in every consumer position (measure, measure-typed filter, order key, arithmetic, transform input, computed dimension with an explicit outer grain, aggregation parameter, nested re-aggregation), and under every `to_many_handling` mode with the usual attributability rules. A ranked or windowed operand SHALL never fail with an internal error; a shape outside this requirement fails with a typed error.

The scenarios use the snapshot fixture: `account_snapshots(account_id, customer_id, snapshot_date, recorded_at, balance)` with default time dimension `snapshot_date`, many-to-one to `customers(id, name)`, holding account 10 (customer 100) balances 100 / 150 / 160 on 2025-01-05 / 01-25 / 02-10; account 11 (customer 100) 50 on 01-10 (recorded 02-01) and 70 on 01-28 (recorded 01-29); account 12 (customer 200) 30 on 01-15; account 13 (customer 200) 40 on 01-20 and NULL on 02-05; customers 100 `A`, 200 `B`, 300 `C` (no snapshots); every other `recorded_at` equals `snapshot_date`.

#### Scenario: Semi-additive last balance summed per customer
- **WHEN** a query over `account_snapshots` groups by `customer_id` and selects `sum(last(balance, snapshot_date, partition_by=[account_id, customer_id]))`
- **THEN** by executed values on SQLite and DuckDB customer 100 holds 230 and customer 200 holds 30, never an internal error

#### Scenario: Implicit and explicit ranking columns
- **WHEN** the same query omits the ranking column, and again names `recorded_at`
- **THEN** the implicit form returns 230 / 30 and the `recorded_at` form returns 210 / 30

#### Scenario: Outer aggregation family over ranked operands
- **WHEN** the query selects `avg`, `count` and `sum` over `last(balance, partition_by=[account_id, customer_id])`, and `sum` over the matching `first`
- **THEN** `avg` is 115 / 30, `count` is 2 / 1 (a NULL latest value is not counted), and `sum(first(...))` is 150 / 70

#### Scenario: Several ranked constituents in one operand
- **WHEN** the query selects `sum(last(balance, partition_by=[account_id, customer_id]) - first(balance, partition_by=[account_id, customer_id]))`, and separately an operand combining a `last` ranked by `snapshot_date` with a `last` ranked by `recorded_at`
- **THEN** the difference is 80 / 0, and each `last` in the second operand keeps its own ranking column, by executed values

#### Scenario: Ranked and plain constituents mixed
- **WHEN** the query selects `sum(last(balance, partition_by=[account_id, customer_id]) + max(balance, partition_by=[account_id, customer_id]))`
- **THEN** customer 100 holds 460 and customer 200 holds 60

#### Scenario: Ranked operand bucketed by the query time dimension
- **WHEN** the query adds a month time dimension on `snapshot_date` and selects `sum(last(balance, partition_by=[account_id, customer_id, snapshot_date]))`, with the ranking column explicit and implicit
- **THEN** both return (100, Jan) 220, (100, Feb) 160, (200, Jan) 70 and (200, Feb) NULL

#### Scenario: Windowed operands
- **WHEN** a query by `customer_id` and month selects `sum(sum(balance, window='30d'))`, `sum(sum(balance, window='30d', partition_by=[account_id, customer_id]))`, and an operand combining two windowed inners of different durations
- **THEN** each executes with hand-computed values — the first two equal to the single-stage windowed measure `sum(balance, window='30d')` by the same grain — and each windowed inner keeps its own window

#### Scenario: Every consumer position
- **WHEN** `sum(last(balance, partition_by=[account_id, customer_id]))` is consumed as a measure, as the measure-typed filter `> 100`, as a descending order key, inside `/ count(*)`, as a `cumsum` input over month (with the bucketed inner), inside a computed dimension `CASE WHEN sum(last(...), partition_by=[customer_id]) > 100 …`, as the `weight=` of `weighted_avg`, with an explicit outer `partition_by=`, and inside `max(...)` with no dimensions
- **THEN** every position computes by executed values (the filter keeps customer 100; the nested form returns 230) and none raises

#### Scenario: Structural twins share one producer
- **WHEN** the same ranked operand appears twice in one query
- **THEN** it is computed once and both consumers read the same values

#### Scenario: Cross-model ranked operand
- **WHEN** a query rooted at `customers` groups by `name` and selects `sum(last(account_snapshots.balance, partition_by=[account_snapshots.account_id, id]))` and `count` over the same operand
- **THEN** `sum` is A 230, B 30, C NULL and `count` is A 2, B 1, C 0

#### Scenario: Attributability modes over a ranked operand
- **WHEN** a query by `customer_id` selects `sum(last(balance, partition_by=[account_id]))`, whose operand grain does not determine `customer_id`
- **THEN** `broadcast` repeats 260 with its warning, `associate` returns 230 / 30 with its warning, and `error` fails with the typed re-aggregation attributability error

#### Scenario: Associated ranked picks stay a typed refusal
- **WHEN** a cross-model `first`/`last` must itself be resolved by distinct-entity association
- **THEN** the query fails with the existing typed association error, never an internal error
