## MODIFIED Requirements

### Requirement: Time dimensions accept row-level expressions

A time dimension's `field` SHALL be a row-level Mode-B expression — a bare or dotted column reference being the one-leaf case — bucketed at the requested granularity exactly as a column is. The expression SHALL be homed per `queries/semantics` › Home dataset (Axiom 2.2): its leaves MAY be host-model columns, columns reached over provably to-one hops, or columns of the current stage's schema; an expression whose leaves no candidate home determines SHALL fail with the input-safety error of `queries/semantics` › Row-level expressions are homed or refused, naming two such leaves and their hops; an expression homed on a dataset the population does not determine resolves per the `to_many_handling` mode, as a column of that dataset does. The expression SHALL type DATE or TIMESTAMP; any other type, or a type that cannot be determined, SHALL fail before any SQL is generated with the existing temporal-column typed error naming the time dimension and the observed type, and, for an undetermined type, the remedy of wrapping the expression in `CAST(… AS TIMESTAMP)` or `CAST(… AS DATE)`. An aggregate, transform or attached value anywhere in the expression SHALL be rejected with a typed error stating that a time dimension is row-level. The population inference of `queries/population` SHALL read an expression time dimension's leaves as it reads a column's.

#### Scenario: Coalesced timestamp groups by month
- **WHEN** a query on `orders` has `time_dimensions` = `[{"field": "coalesce(shipped_at, created_at)", "granularity": "month"}]` and a count measure
- **THEN** each row is one month of the per-row coalesced timestamp with the count of orders in it, by hand-computed executed values on SQLite and DuckDB

#### Scenario: Cast text to a date
- **WHEN** a query buckets `CAST(created_txt AS DATE)` at `month`, `created_txt` a TEXT column holding ISO dates
- **THEN** it groups by the month of the cast value, by executed values on SQLite and DuckDB

#### Scenario: Cross-model expression over a to-one hop
- **WHEN** a query rooted at `orders` buckets `coalesce(customers.signup_ts, created_at)` at `month`, `orders → customers` provably to-one
- **THEN** it executes over the `orders` rows, each order reading its own customer's value, by executed values

#### Scenario: Expression leaf across a fanning hop fails closed
- **WHEN** a query rooted at `customers` buckets `coalesce(orders.shipped_at, returns.returned_at)` at `month`, `customers → orders` and `customers → returns` one-to-many
- **THEN** planning fails with the input-safety error naming `orders.shipped_at`, `returns.returned_at` and both hops, never a multiplied row set

#### Scenario: A parent and a child leaf are homed on the child
- **WHEN** a query rooted at `customers` buckets `coalesce(orders.shipped_at, signup_ts)` at `month` and counts customers, `customers → orders` one-to-many
- **THEN** it executes homed on `orders`: each customer is counted once in every month of one of its orders' coalesced values, with the `associated` warning, exactly as for a column of `orders` holding those values

#### Scenario: Non-temporal expression rejected
- **WHEN** a query buckets `upper(status)` at `month`
- **THEN** planning fails with the temporal-column typed error naming the time dimension and the type TEXT, and no SQL is generated

#### Scenario: Undetermined type fails closed with the cast remedy
- **WHEN** a query buckets an expression whose type cannot be determined at `month`
- **THEN** planning fails with the temporal-column typed error naming the remedy of an explicit `CAST`

#### Scenario: Aggregate inside a time dimension rejected
- **WHEN** a query buckets `max(created_at, partition_by=status)` at `month`
- **THEN** planning fails with the typed error stating that a time dimension is row-level

#### Scenario: Stage-scope expression
- **WHEN** an outer stage buckets `coalesce(shipped_at, created_at)` over an inner stage that projects both columns as plain dimensions
- **THEN** it groups by the month of the coalesced stage value, by executed values
