# sql/statement-assembly Specification

## Purpose
Defines how a generated SQL statement is composed and rendered: composition as a syntax
tree with a single render to text, one top-level `WITH` on every dialect, where post-phase
filters apply, and how a query-backed model's backing statement is wrapped.

## Requirements

### Requirement: Statements are composed as a syntax tree and rendered once

Every generated statement, including its CTEs, stages, producers, outer projection and
filters, SHALL be composed as a syntax tree and rendered to SQL text once. SQL text that
has already been emitted for a statement SHALL NOT be parsed back to compose a larger
statement. A dialect-specific clause (a function name, pagination syntax, identifier
quoting) chosen during composition SHALL therefore reach the emitted SQL unchanged.

#### Scenario: A statement is never rendered while it is being composed

- **WHEN** any query in the test suite is compiled on any dialect
- **THEN** no statement-level SQL node is rendered to text before the final render
- **AND** a deliberate render-then-reparse inside composition (verbatim or with the text
  edited in between) fails the harness law

#### Scenario: T-SQL composition needs no re-parse

- **WHEN** a transform-chain query with a limit is compiled for T-SQL
- **THEN** the emitted SQL is produced without parsing any previously emitted SQL
- **AND** a composition failure raises an error instead of falling back to a different
  statement shape

### Requirement: One top-level WITH on every dialect

A generated statement SHALL carry all of its CTEs in a single top-level `WITH`. No `WITH`
SHALL appear inside a derived table or a CTE body, on any dialect. The public projection,
`ORDER BY` and pagination SHALL apply to the outermost `SELECT`; pagination SHALL use the
dialect's own syntax (T-SQL `TOP` / `OFFSET … FETCH NEXT`, with `ORDER BY (SELECT NULL)`
supplied when an offset has no ordering).

#### Scenario: Paginated transform chain on every dialect

- **WHEN** a query with a transform (e.g. `cumsum(sum(amount))`), an `order` and a `limit`
  is compiled on each supported dialect
- **THEN** the statement begins with the chain's `WITH` and no derived table contains a
  `WITH`
- **AND** only the public fields are projected, ordered and limited in the dialect's
  pagination syntax
- **AND** executing it on SQLite and DuckDB returns the same rows as before this change

#### Scenario: T-SQL offset without an ordering

- **WHEN** a transform-chain query with an `offset` and no `order` is compiled for T-SQL
- **THEN** the outer `SELECT` carries `ORDER BY (SELECT NULL)` followed by
  `OFFSET … ROWS FETCH NEXT … ROWS ONLY`

#### Scenario: Ordering by a field that is not projected

- **WHEN** a transform-chain query orders by a field that is not in its public projection
- **THEN** the outer `ORDER BY` resolves that field against the columns the chain carries
- **AND** the field is not added to the result rows

### Requirement: Post-phase filters apply at the chain's final select

A filter evaluated after transforms (a post-phase filter) SHALL be applied as the `WHERE`
of the transform chain's final `SELECT` over the last chain step, combined with `AND`
as syntax-tree nodes. It SHALL NOT be applied in the base aggregation, so it never
removes rows before a transform is computed. Every column it references SHALL be one the
last chain step carries.

#### Scenario: Filter on a transform result keeps the transform's input rows

- **WHEN** a query computes `cumsum(sum(amount))` by month and filters on that cumulative
  value being greater than 50
- **THEN** the filter appears in the final select over the last chain step, not in the
  base CTE
- **AND** the cumulative values of the surviving rows equal those of the unfiltered query

#### Scenario: Filter over a non-projected composite dimension

- **WHEN** a post-phase filter references a computed dimension by its expression
- **THEN** the filter resolves against a column carried by the last chain step
- **AND** executing on DuckDB returns only the matching rows

#### Scenario: Mixed AND / OR filter conjuncts keep their grouping

- **WHEN** a query has several filters in the same phase, one of which is an `OR`
- **THEN** each conjunct keeps its grouping when combined with `AND`
- **AND** executing on DuckDB returns the rows satisfying every filter

### Requirement: Query-backed model wraps compose over the unrewritten statement

The backing SQL of a query-backed model SHALL be built by wrapping the stage statement's
syntax tree in the flat-rename projection before any identifier fitting or dialect alias
mangling, then rendered and fitted once. The persisted backing SQL's column names SHALL be
the fitted flat names that the model's columns reference.

#### Scenario: Over-limit dotted names on a mangling dialect

- **WHEN** a query-backed model whose output names are dotted and exceed the dialect's
  identifier limit is expanded for BigQuery or T-SQL
- **THEN** the backing SQL's output columns equal the model columns' fitted names
- **AND** the same model expanded for DuckDB executes and returns rows keyed by its
  column names

### Requirement: A cast to INT is 64-bit

Wherever generated SQL casts a value to SLayer's INT type, the cast target SHALL be the dialect's 64-bit integer type, so an integer value that fits in 64 bits is returned unchanged. A value already cast to that target SHALL NOT be cast again.

#### Scenario: Large integers survive a derived INT column
- **WHEN** a model's derived INT column computes values above 2³¹ on Postgres and DuckDB and a query selects it
- **THEN** the generated SQL casts it to the 64-bit integer type and the values are returned unchanged

#### Scenario: No double cast
- **WHEN** a derived INT column's expression is itself a cast to the 64-bit integer type
- **THEN** the generated SQL casts it once

### Requirement: A column reference renders the same in every clause

Within one relational scope, every reference that reads the same physical model column — a host
column or a joined model's column across any join path — SHALL render as the same identifier in
every clause that reads it: the projection, `GROUP BY`, `ORDER BY`, `WHERE`, `HAVING`, aggregate
arguments, ranking and partition expressions, and time truncation. That identifier SHALL be quoted
exactly as the dialect quotes any other emitted column identifier, so a mixed-case column reaches the
same physical column on a case-folding dialect. Result aliases and columns read back from a
materialised stage or CTE are not physical model columns and are outside this requirement. A plain
column reference whose join path or leaf does not resolve to a declared column SHALL fail closed with
the typed unknown-reference error in every clause, and no SQL SHALL be emitted.

#### Scenario: A joined mixed-case dimension filtered on itself

- **WHEN** `orders` (`orderNumber`) joins `accounts` (`extRef`) and a query rooted at `orders` has
  dimensions `orderNumber` and `accounts.extRef` and the filter `accounts.extRef = 'a'`, on Postgres
- **THEN** `extRef` is read as the same quoted identifier on the `accounts` join alias in the
  projection, `GROUP BY` and `WHERE`

#### Scenario: A joined mixed-case dimension grouped under a measure

- **WHEN** a query rooted at `orders` has the dimension `accounts.extRef` and the measure
  `{"formula": "count(*)"}`, with no filter, on Postgres or Snowflake
- **THEN** `extRef` is quoted in the projection and `GROUP BY`

#### Scenario: A two-hop mixed-case column

- **WHEN** `accounts` joins `regions` (`regionName`) and a query rooted at `orders` groups by
  `accounts.regions.regionName`
- **THEN** `regionName` is quoted on the two-hop join alias, which equals the alias the `FROM` joins

#### Scenario: A two-hop column under a fitted or colliding join alias

- **WHEN** a two-hop path's join alias is shortened to the dialect's identifier limit or
  disambiguated against a colliding name
- **THEN** the mixed-case column's qualifier equals the emitted `JOIN` alias and the column is quoted

#### Scenario: A joined mixed-case time dimension

- **WHEN** a query rooted at `orders` has the time dimension `month(accounts.signedUpAt)`, directly
  and as the time axis of a duration window and of a collapsed window grain
- **THEN** `signedUpAt` is quoted inside every truncation and window-source expression that reads it

#### Scenario: A ranked aggregate partitioned or ordered by a joined mixed-case column

- **WHEN** a `first` / `last` measure is partitioned by `accounts.extRef`, and separately ranked by
  the joined mixed-case time column `accounts.signedUpAt`
- **THEN** the joined column is quoted inside the ranked subquery

#### Scenario: Ordering by a joined mixed-case column

- **WHEN** a query orders by `accounts.extRef` while projecting it, while grouping without
  projecting it, and in an ungrouped query that neither projects nor materialises it
- **THEN** every source-column read of `extRef` is quoted, and the ungrouped query orders by the
  quoted joined-table reference

#### Scenario: Lowercase columns are unchanged

- **WHEN** the joined columns' names are all lowercase
- **THEN** the emitted SQL is byte-identical to the SQL emitted before this requirement

#### Scenario: Joined mixed-case columns execute on Postgres

- **WHEN** Postgres tables are created with quoted camelCase names and the first two scenarios'
  queries execute against them
- **THEN** both return the hand-computed rows

#### Scenario: An unresolvable plain column fails closed

- **WHEN** a plain column reference reaching SQL emission names a leaf its terminal model does not
  declare, in a projection or in a filter
- **THEN** generation fails with the typed unknown-reference error and no SQL is emitted
