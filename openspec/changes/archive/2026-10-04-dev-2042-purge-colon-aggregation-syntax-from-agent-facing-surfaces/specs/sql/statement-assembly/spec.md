## MODIFIED Requirements

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
