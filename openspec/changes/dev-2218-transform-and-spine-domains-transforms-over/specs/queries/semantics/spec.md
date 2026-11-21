## MODIFIED Requirements

### Requirement: Compositionality
Each result cell's values SHALL depend only on the evaluated expression, the
cell, and the row-level filters — never on which other measures, measure-typed
filters, or order entries the query contains. In a query naming `population`, adding or
removing a projected measure SHALL NOT change the row set or any other column's values. In a
query omitting it with at least one measure, adding a measure SHALL NOT remove a row or change any value in an existing
row, a row it adds carrying each other measure's value on its own grain there (its empty value
where it has no cell). Transforms obey both guarantees: each evaluates over its operand's own
cells (`queries/transforms` › Transforms evaluate over their operand's domain). A measure-typed
filter masks result cells without changing any surviving cell's values; ORDER BY and LIMIT
select and order rows without changing any cell's values. A multi-measure query SHALL
return, cell by cell, the same values as its single-measure splits in each split's rows, and
in the rows only its siblings supply, its value on its own grain there (its empty value where it
has no cell). Selecting an
expression as a measure SHALL NOT make any other measure, filter or order entry that
contains it fail.

#### Scenario: Adding a measure changes nothing else
- **WHEN** any supported query naming `population` runs with and without one additional measure
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Adding a measure to an inferred query only adds rows
- **WHEN** a query without `population` grouping by `customers.region` and measuring
  `orders.total_revenue` adds `returns.total_returns`
- **THEN** EAST 350 and WEST 300 keep their values, and the added NORTH row carries NULL revenue

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

#### Scenario: A transform keeps its values when a sibling adds rows
- **WHEN** a query without `population` grouping by `customers.region` selects
  `percent_rank(count(orders.*))`, and again with `returns.total_returns` added
- **THEN** EAST 1 and WEST 0 keep their values in both runs, and the added NORTH row carries NULL

### Requirement: Row laws of inferred queries
Before measure-typed filters, `order` and `limit`, every query omitting `population` SHALL obey:
**measure monotonicity** — adding a measure to a query with at least one measure never removes a
row and never changes a value in an existing row, transforms included; **home-row preservation** —
every home row of a full-grain aggregate surviving the filters lands in a row, exactly one when
the home determines the dimensions and the data respects declared cardinality; **filter
non-expansion** — adding a field filter never adds a row and never changes a result key, except
the value of a computed dimension read from an aggregate over the filtered rows; and the
**unit** — a query with measures and no dimensions returns exactly one row. Filter non-expansion
and the unit SHALL hold for a query naming `population` too.

#### Scenario: Monotonicity over generated queries
- **WHEN** the law harness generates root-less queries, transforms among their measures, and adds a
  measure to each
- **THEN** every row of the smaller query is a row of the larger one with the same values

#### Scenario: Home rows are preserved
- **WHEN** the law harness generates root-less queries over data with orphan home rows
- **THEN** the sum of an additive full-grain measure over the result equals its total over the
  home's filtered rows

#### Scenario: Filters never expand
- **WHEN** the law harness adds a field filter to generated queries, with and without `population`
- **THEN** the filtered result's keys are a subset of the unfiltered result's keys

#### Scenario: The unit
- **WHEN** a query with measures and no dimensions runs, with and without `population`, with and
  without a filter no row passes
- **THEN** it returns exactly one row
