## MODIFIED Requirements

### Requirement: Cube reference forms

In every Cube SQL surface of a cube (dimension `sql`, CASE `when` predicates, measure `sql` and `filters`, segments, join `ON`), `{CUBE}.x`, `{<own cube>}.x` and a bare `x` SHALL denote the physical column `x` of the cube's source; `{other}.x` SHALL denote the physical column `x` of the joined cube `other`'s source, read through a physical reader of `other` (importers/meaning-preservation › One hidden reader per physical column), and a construct naming a cube that is not joined SHALL be dropped with a report naming that cube; `{member}`, `{CUBE.member}` and `{<own cube>.member}` SHALL denote that member of the cube, under the name it was imported as; `{other.member}` SHALL denote the member of the joined cube `other`. The cube's own `sql` body SHALL be imported as written, its unquoted identifiers read with the datasource's identifier folding (importers/meaning-preservation).

#### Scenario: Same-cube dotted reference
- **WHEN** cube `orders` declares measure `paid_sum` (`sum`, `sql: {CUBE}.amount`, filter `{CUBE}.status = 'paid'`) and calculated measures `a: {orders.paid_sum}` and `b: {CUBE.paid_sum}`
- **THEN** `a` and `b` import and both return 30, as `paid_sum` does

#### Scenario: Member reference under its imported name
- **WHEN** a cube's measure `rate` is `sum` over `{CUBE}.rate` and a calculated measure is `{rate} * 2`
- **THEN** the calculated measure returns twice the sum of `rate`

#### Scenario: Another cube's physical column
- **WHEN** cube `orders` joins `customers` on `{CUBE}.customer_id = {customers}.id` and declares dimension `cust_name` with `sql: {customers}.name`
- **THEN** grouping by `cust_name` returns `Ann` for 2 orders and `Bo` for 1

#### Scenario: Another cube's physical column, masked
- **WHEN**, in addition, cube `customers` declares dimension `name` with `sql: {CUBE}.region`
- **THEN** `cust_name` imports, reading a hidden field of `customers` over the physical `name`, and grouping by `cust_name` returns `Ann` for 2 orders and `Bo` for 1

### Requirement: Calculated measures with raw SQL aggregates

A Cube calculated measure (`type: number`, `string`, `time` or `boolean` with `sql`) whose SQL contains aggregate calls SHALL import as a SLayer measure computing the same value: each outermost `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `COUNT(*)`, `COUNT(DISTINCT …)` or percentile aggregate (with an optional `FILTER (WHERE …)`) SHALL aggregate its operand over the cube's rows; an operand that is not a bare column SHALL be evaluated as Cube's SQL, verbatim apart from the identifier folding of "Imported SQL reads with the source's identifier folding" (`importers/meaning-preservation`), in the database's dialect, its physical references reading the physical columns whatever members the cube declares; arithmetic over aggregates and over other measures SHALL be kept. A calculated measure using a construct outside that set, or a non-bare operand that reads another cube, SHALL be dropped with a report naming the measure and the construct. Division by zero SHALL yield NULL.

#### Scenario: CASE inside an aggregate, with a colliding dimension
- **WHEN** cube `orders` declares dimension `status` with `sql: {CUBE}.state` and calculated measure `paid_cnt: SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)`
- **THEN** `paid_cnt` imports and returns 2

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

### Requirement: Cube join keys read physical columns

A Cube join whose `ON` is an equality, or a conjunction of equalities, between columns of the two cubes SHALL import with join keys that read exactly the physical columns the `ON` names on each side, whichever members the cubes declare: each side SHALL be keyed on a physical reader of its column, added hidden when no unfiltered member reads the column bare (importers/meaning-preservation › One hidden reader per physical column). Any other `ON` SHALL drop the join with a report.

#### Scenario: Key masked by a renamed dimension
- **WHEN** cube `orders` declares dimension `customer_id` with `sql: {CUBE}.legacy_ref` and joins `customers` on `{CUBE}.customer_id = {customers}.id`
- **THEN** the join imports keyed on a hidden field of `orders` reading the physical `customer_id`, and counting orders by `customers.name` returns `Ann` 2 and `Bo` 1

### Requirement: FILTER_PARAMS bodies read physical columns

The column references inside a FILTER_PARAMS pushdown body SHALL follow the same physical-reference rule as the surface holding it, and the imported optional and required pushdown blocks SHALL behave as before.

#### Scenario: Pushdown body over a masked column
- **WHEN** a sql-mode cube's `sql` has `{FILTER_PARAMS.orders.status.filter('status')}`, and the cube declares dimension `status` with `sql: UPPER({CUBE}.status)`
- **THEN** the self-named `status` dimension imports, grouping by it returns `PAID` for 2 rows and `OPEN` for 1, querying with the pushdown variable set to `paid` keeps ids 1 and 2, and omitting the variable keeps all three rows
