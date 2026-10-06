## MODIFIED Requirements

### Requirement: Compositionality
Each result cell's values SHALL depend only on the evaluated expression, the
population, and the row-level filters — never on which other measures, measure-typed
filters, or order entries the query contains. Adding or removing a projected measure
SHALL NOT change the row set or any other column's values. A measure-typed filter
masks result cells without changing any surviving cell's values; ORDER BY and LIMIT
select and order rows without changing any cell's values. A multi-measure query SHALL
return, cell by cell, the same values as its single-measure splits. Selecting an
expression as a measure SHALL NOT make any other measure, filter or order entry that
contains it fail.

#### Scenario: Adding a measure changes nothing else
- **WHEN** any supported query runs with and without one additional measure
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Measure-typed filter masks without altering values
- **WHEN** a query filters on an aggregate predicate
- **THEN** surviving cells carry exactly the values the unfiltered query gave them

#### Scenario: A query equals its single-measure splits
- **WHEN** a two-measure query and its two single-measure counterparts run
- **THEN** each measure's values match cell by cell across the runs

#### Scenario: A saved ratio measure alongside a transform over it
- **WHEN** a query rooted at `orders` with a month time dimension (with a `date_range`,
  and again with a plain date filter instead) selects the saved measure
  `aov = sum(amount) / count(*)` and `cumsum(aov)`, and again with the inline formulas
  `sum(amount) / count(*)` and `cumsum(sum(amount) / count(*))`
- **THEN** every run returns the monthly ratio and its running sum, by hand-computed
  executed values on SQLite and DuckDB, equal cell by cell to the two single-measure runs

#### Scenario: A selected composite alongside any consumer that contains it
- **WHEN** a query selects a composite measure (a ratio of local aggregates, and a ratio
  with a cross-model aggregate operand) together with one consumer containing it — each of
  `cumsum`, `lag`, `lead`, `change`, `change_pct`, `consecutive_periods` and a rank-family
  transform over it; an arithmetic composite combining it with a transform; a transform
  over a larger expression containing it; a measure-typed filter on a transform of it; an
  ORDER BY on a transform of it
- **THEN** the query executes on SQLite and DuckDB with no internal error, every measure
  equals its single-measure run cell by cell, the filter and the order entry select and
  order exactly the rows they do without the composite selected, and no internal
  placeholder appears in the generated SQL

#### Scenario: Generated SQL for a reused composite is pinned across dialects
- **WHEN** a selected composite is reused by a measure-typed filter, and by an order-only
  composite, and the queries are rendered for PostgreSQL and T-SQL
- **THEN** the generated SQL matches recorded golden baselines
