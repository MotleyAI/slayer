## MODIFIED Requirements

### Requirement: Every stage column carries a type
Every column a stage emits SHALL carry a type, derived from its value by the same rule
that types the value in the query plan; a later stage, and a consumer of a saved
query-backed dataset, SHALL read the column with exactly that type. An explicitly
declared type (a measure's `type`, a saved metric's `datatype`, an aggregation
definition's `datatype`) SHALL win over the derived one. The derived type SHALL be:
- a field's declared type, through any path, an upstream stage's column included;
- a cast's target type;
- a value-returning conditional's joined branch type (per `queries/mixed-type-coercion`);
- a comparison, `IN` or other predicate: BOOLEAN;
- a literal's type; a value that can only be NULL: INT;
- a scalar function's result type; arithmetic as `queries/mixed-type-coercion` and
  `aggregations/boolean-inputs` type it;
- an aggregation's result type (per `aggregations/boolean-inputs` and, for a custom
  aggregation, `aggregations/formula-templates`), its input typed through any path;
- `lag`, `lead`, `time_shift`: the input's type; `first`, `last` over rows or over an
  aggregate: the picked value's type; `cumsum`: `sum`'s
  result type over the input; `change`: the type of the input minus itself;
  `change_pct` and `percent_rank`: DOUBLE; `rank`, `dense_rank`, `ntile` and
  `consecutive_periods`: INT;
- a value computed from a value the query re-aggregates (a partitioned or second-order
  aggregate): the type of that value.

A conditional in a later stage SHALL therefore see each stage column's real type: a
mixed-type conditional over stage columns SHALL be refused exactly as over model columns,
and a NULL-only stage column SHALL read as INT, not as an absorbed NULL.

#### Scenario: Computed measures keep their type downstream
- **WHEN** a stage over `orders` projects `sum(orders.amount) / count(orders.*)`, `lower(orders.status)`, `cumsum(sum(orders.amount))`, `rank(sum(orders.amount))` and `orders.status IN ('paid')`, and a later stage reads each column
- **THEN** the later stage reads them as DOUBLE, TEXT, the type of `sum(orders.amount)`, INT and BOOLEAN respectively

#### Scenario: A cross-dataset aggregate keeps its type downstream
- **WHEN** a stage over `orders` projects `max(customers.created_at)` and `sum(customers.spend)`, and a later stage reads both
- **THEN** the later stage reads them as `created_at`'s type and `spend`'s aggregated type

#### Scenario: A re-aggregated value keeps its type downstream
- **WHEN** a stage projects `sum(orders.amount, partition_by=orders.region)` and a value computed from it, and a later stage reads both
- **THEN** the later stage reads each with the type the query plan gives it

#### Scenario: Branch typing over stage columns
- **WHEN** a later stage reads a stage's TEXT column `u` and INT column `n` and has the dimensions `iif(n > 1, u, 'x')` and `coalesce(n, 0)`
- **THEN** they are typed TEXT and INT

#### Scenario: A mixed-type conditional over stage columns is refused
- **WHEN** a later stage reads a stage's TEXT column `s` and DATE column `d` and has the dimension `coalesce(s, d)`
- **THEN** the query fails at plan time with the typed conditional-branch error naming TEXT and DATE

#### Scenario: A NULL-only stage column is INT
- **WHEN** a stage projects `null` as `nul` and a later stage reads `sum(s.nul)`, `count(s.nul)` and `coalesce(s.nul, 0)`
- **THEN** the column is INT and the results are NULL, 0 and 0, on SQLite and DuckDB

#### Scenario: An explicit type wins
- **WHEN** a stage measure declares `type` Decimal over a value derived as DOUBLE, and a later stage reads it
- **THEN** the later stage reads it as Decimal

#### Scenario: A saved query-backed dataset reads its columns' real types
- **WHEN** a saved query-backed dataset projects `sum(orders.amount) / count(orders.*)` and a query reads it
- **THEN** the column reads as DOUBLE
