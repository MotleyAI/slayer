## Purpose

Defines how a generated SQL statement is composed and rendered: composition as a syntax
tree with a single render to text, one top-level `WITH` on every dialect, where post-phase
filters apply, and how a query-backed model's backing statement is wrapped.

## ADDED Requirements

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

- **WHEN** a query with a transform (e.g. `cumsum(amount:sum)`), an `order` and a `limit`
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

- **WHEN** a query computes `cumsum(amount:sum)` by month and filters on that cumulative
  value being greater than 50
- **THEN** the filter appears in the final select over the last chain step, not in the
  base CTE
- **AND** the cumulative values of the surviving rows equal those of the unfiltered query

#### Scenario: Filter over a non-projected composite dimension

- **WHEN** a post-phase filter references a composite dimension that is not in the public
  projection
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
