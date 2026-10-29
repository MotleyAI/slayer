## Purpose

Rules every importer (Cube, dbt, OSI) obeys so an imported model reads each source construct with the source's meaning — up to SLayer's documented NULL conventions — and reports anything it cannot carry.

Scenarios below use a table `orders(id, status, state, amount, tags)` with rows `(1, 'paid', 'open', 10, ['a', 'b'])`, `(2, 'paid', 'paid', 20, ['b'])` and `(3, 'open', 'x', 5, NULL)`, `tags` a list of text.

## ADDED Requirements

### Requirement: Physical references read the physical column

Every column reference an importer takes from the source's own SQL (a reference to a column of the source table, as opposed to a reference to a source-defined member) SHALL read that physical column in the imported model, whatever logical columns the model declares. When a logical column shares the physical column's name but reads something else (a derived, filtered or renamed column), the import SHALL read the physical column through a hidden base column instead, and the shared name SHALL keep its own definition. Resolution SHALL NOT depend on the order in which the source declares its members. References inside nested scopes (subqueries, CTEs, `UNION` branches) SHALL be left as written, and the rest of the source's SQL text SHALL be kept verbatim.

#### Scenario: A renamed dimension does not capture a physical reference
- **WHEN** a Cube cube over `orders` declares dimension `status` with `sql: {CUBE}.state` and dimension `status_upper` with `sql: UPPER({CUBE}.status)`
- **THEN** a query grouping by `status_upper` returns groups `PAID` (2 rows) and `OPEN` (1 row)

#### Scenario: The same collision in dbt
- **WHEN** a dbt semantic model over `orders` declares dimension `status` with `expr: state` and measure `paid_cnt` with `agg: sum` and `expr: CASE WHEN status = 'paid' THEN 1 ELSE 0 END`
- **THEN** `paid_cnt` returns 2

#### Scenario: The same collision in OSI
- **WHEN** an OSI dataset over `orders` declares field `status: state` and field `status_upper: UPPER(status)`
- **THEN** a query grouping by `status_upper` returns groups `PAID` (2 rows) and `OPEN` (1 row)

#### Scenario: Declaration order does not matter
- **WHEN** the Cube cube above declares `status_upper` before `status`
- **THEN** the query returns the same groups

#### Scenario: Nested scopes are left alone
- **WHEN** a Cube dimension is `({CUBE}.amount - (SELECT MIN(amount) FROM orders))` and the cube also declares a dimension `amount` with `sql: {CUBE}.amount * 2`
- **THEN** the outer reference reads the physical `amount` and the subquery's `amount` is kept as written

### Requirement: Self-named transforms import

A source member whose definition reads the physical column of its own name (`status: UPPER(status)`) SHALL import, with the member computed over the physical column, in every importer; the model SHALL save without a column cycle.

#### Scenario: Cube self-named dimension
- **WHEN** a Cube cube over `orders` declares dimension `status` with `sql: UPPER({CUBE}.status)`
- **THEN** the cube imports and saves, and grouping by `status` returns `PAID` (2 rows) and `OPEN` (1 row)

#### Scenario: dbt self-named dimension
- **WHEN** a dbt semantic model declares dimension `status` with `expr: UPPER(status)`
- **THEN** the model saves and grouping by `status` returns `PAID` and `OPEN`

#### Scenario: OSI self-named field
- **WHEN** an OSI dataset declares field `status: UPPER(status)`
- **THEN** the model saves and grouping by `status` returns `PAID` and `OPEN`

### Requirement: A filtered helper never masks its source column

A column an importer creates to carry a row filter for one measure SHALL NOT take the name of the physical column it reads, and SHALL NOT be reused by an unfiltered reference. Two measures over the same expression with different filters SHALL get different helpers; the same expression with the same filter SHALL share one.

#### Scenario: Filtered measure next to a plain reference
- **WHEN** a Cube cube over `orders` declares measure `paid_amount` (`sum`, `sql: {CUBE}.amount`, filter `{CUBE}.status = 'paid'`) and dimension `amount_x2` with `sql: {CUBE}.amount * 2`
- **THEN** `paid_amount` returns 30 and `amount_x2` returns 20, 40 and 10

### Requirement: Identifier matching follows the source dialect

A quoted physical identifier SHALL match a column exactly; an unquoted one SHALL match using the datasource dialect's identifier folding. A reference that matches more than one column SHALL drop the construct holding it, with a report naming the reference. Emitted SQL SHALL keep the source's quoting.

#### Scenario: Case-only ambiguity
- **WHEN** a dbt model declares dimensions `s_lower` (`expr: status`) and `s_upper` (`expr: '"Status"'`) over two distinct physical columns, a dimension `s_len` with `expr: LENGTH(Status)`, and the datasource dialect folds unquoted identifiers case-insensitively
- **THEN** `s_len` is dropped and the report names `Status` as ambiguous

### Requirement: Column types come from the database first

When the import's datasource is configured and reachable, each imported column's type SHALL be the type the database reports: a base column's type from the live table (a list column as a list with its element type), an expression column's type from a type probe (a list result with element type `UNKNOWN`). When the database cannot report a type, the importer SHALL use, in order: the source's declared type; the numeric type a `sum` or `avg` implies (`sum_boolean` → integer); `TEXT` for a column only counted. A column with no type from any of these that a `min`, `max` or other order-dependent aggregation reads SHALL drop that measure with a report telling the user to re-import against a reachable datasource or declare the type. A declared type the database contradicts SHALL be reported for information.

#### Scenario: List measure source typed from the database
- **WHEN** a Cube measure `tag_cnt` is `count` over `{CUBE}.tags` and the datasource is reachable
- **THEN** the column `tag_cnt` reads is a list of text, and `validate_models` proposes no deletion for the model

#### Scenario: Offline count source
- **WHEN** the same cube is imported with the datasource not configured
- **THEN** `tag_cnt` imports reading a `TEXT` column and the report holds one informational entry saying types were not checked against the database, naming the reason

#### Scenario: Offline min over an untyped source
- **WHEN** a Cube measure `latest` is `max` over `{CUBE}.created_at`, no dimension reads `created_at`, and the datasource is not configured
- **THEN** `latest` is dropped and the report says to re-import against a reachable datasource or declare the type

#### Scenario: Declared type contradicted by the database
- **WHEN** a dbt column declares `data_type: text` and the live column is an integer
- **THEN** the column is typed `INT` and the report holds an informational entry naming the column

### Requirement: Aggregations SLayer refuses over a type are dropped at import

A source measure whose aggregation SLayer does not accept over its input's type (for example `count_distinct`, `min`, `max`, `sum` or `avg` over a list) SHALL be dropped at import with a report naming the measure and the type, rather than imported to fail at query time. `count` over a list SHALL import.

#### Scenario: count_distinct over a list
- **WHEN** a Cube measure is `count_distinct` over `{CUBE}.tags` and the datasource is reachable
- **THEN** the measure is dropped and the report names it and the list type

### Requirement: Untranslatable measures are dropped with a report

An imported measure whose aggregation reads a name that is neither a column of its model nor a resolvable join path SHALL be dropped at import with a report naming the measure and the reference, for every importer.

#### Scenario: Aggregation over an unknown name
- **WHEN** a Cube calculated measure is `SUM({missing_member})` and the cube declares no member `missing_member`
- **THEN** the measure is dropped and the report names it and `missing_member`

### Requirement: Every importer is checked against the source's numbers

For each importer there SHALL be a fixture holding every known ambiguous form (physical/logical collisions in each surface, self-named transforms, filtered helpers, same-model dotted references, raw SQL aggregates, list columns and list functions) whose import, executed on DuckDB, returns the numbers the source system defines; each refusal SHALL appear in the import report.

#### Scenario: The meaning-preservation fixtures
- **WHEN** the Cube, dbt and OSI fixtures are imported and their measures and dimensions are queried on DuckDB
- **THEN** every value equals the source's expected value and every refused construct has a report entry
