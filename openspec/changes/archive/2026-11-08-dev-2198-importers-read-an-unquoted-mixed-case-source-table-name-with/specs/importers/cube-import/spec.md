## MODIFIED Requirements

### Requirement: Calculated measures with raw SQL aggregates

A Cube calculated measure (`type: number`, `string`, `time` or `boolean` with `sql`) whose SQL contains aggregate calls SHALL import as a SLayer measure computing the same value: each outermost `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `COUNT(*)`, `COUNT(DISTINCT …)` or percentile aggregate (with an optional `FILTER (WHERE …)`) SHALL aggregate its operand over the cube's rows; an operand that is not a bare column SHALL be evaluated as Cube's SQL, verbatim apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`), in the database's dialect; arithmetic over aggregates and over other measures SHALL be kept. A calculated measure using a construct outside that set, or a non-bare operand that reads another cube, SHALL be dropped with a report naming the measure and the construct. Division by zero SHALL yield NULL.

#### Scenario: CASE inside an aggregate, with a colliding dimension
- **WHEN** cube `orders` declares dimension `status` with `sql: {CUBE}.state` and calculated measure `paid_cnt: SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)`
- **THEN** `paid_cnt` is dropped and the report names `status`

#### Scenario: A column the cube does not declare
- **WHEN** cube `orders` declares no member over `status` and calculated measure `paid_amount: SUM(CASE WHEN {CUBE}.status = 'paid' THEN {CUBE}.amount END)`
- **THEN** `paid_amount` imports and returns 30

#### Scenario: Plain aggregate unchanged
- **WHEN** a calculated measure is `SUM({CUBE}.amount)`
- **THEN** it returns 35 and its formula is the sum of the `amount` column

#### Scenario: FILTER clause
- **WHEN** a calculated measure is `SUM({CUBE}.amount) FILTER (WHERE {CUBE}.status = 'paid') / COUNT(*)`
- **THEN** it returns 10

#### Scenario: Ratio of aggregates with a zero divisor
- **WHEN** a calculated measure is `SUM({CUBE}.amount) / SUM(CASE WHEN {CUBE}.status = 'void' THEN 1 ELSE 0 END)`
- **THEN** it returns NULL

#### Scenario: Window function
- **WHEN** a calculated measure is `SUM({CUBE}.amount) OVER ()`
- **THEN** it is dropped and the report names it

#### Scenario: Mixed-case operand on Postgres
- **WHEN** on Postgres a calculated measure is `SUM(CASE WHEN {CUBE}.Status = 'paid' THEN {CUBE}.Amount END)`
- **THEN** its operand reads the physical columns `status` and `amount`, and `'paid'` is kept as written
