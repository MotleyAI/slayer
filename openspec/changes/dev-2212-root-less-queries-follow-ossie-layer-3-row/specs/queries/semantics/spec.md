## MODIFIED Requirements

### Requirement: Grain guarantee
For a query with at least one measure, and for a measure-less query with distinct
dimension values enabled (the default), the result SHALL have exactly one row per
combination of dimension values present among the row-filtered population's rows — for a query
omitting `population`, among its row supply (`queries/population` › Rows of an inferred query
are the row supply), never one row twice. Raw-row mode (`distinct_dimension_values=false`) is the documented exception and
returns one row per population row. A row-level filter that reaches the population root
only across a non-determining path SHALL NOT multiply the population's rows, whatever the
conjunct's boolean shape: raw-row mode returns each population row passing the restriction
exactly once. A value the query evaluates only for a filter, an order key or an attached
aggregate's join SHALL NOT change the result's grain: it never adds a row, splits a
dimension combination, or repeats one.

#### Scenario: One row per dimension combination
- **WHEN** an aggregating query groups by dimensions whose value combinations repeat
  across many population rows
- **THEN** the result contains each present combination exactly once

#### Scenario: Raw rows are never multiplied by a population filter's join
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filter: ["orders.status = 'ok'"]`, and one
  customer has two `ok` orders
- **THEN** by executed values the result has one row per customer with at least one `ok`
  order (five rows on the reference dataset), never one row per matching order

#### Scenario: A filter on a computed dimension's partition key keeps the dimension's grain
- **WHEN** a query rooted at `orders` selects the computed dimension
  `case when sum(amount, partition_by=customer_id) > 50 then 'high' else 'other' end`
  and the measure `sum(amount)`, with a row filter that reads `customer_id`
  (`customer_id is not null`, or `customer_id + 0 > 0`), over customers whose order totals
  are 90, 70 and 50
- **THEN** by executed values the result has exactly one row per band (`high` 160,
  `other` 50), the filter removing rows before every aggregation, never one row per customer

#### Scenario: A filter on a joined partition key keeps the dimension's grain
- **WHEN** the computed dimension partitions by a joined column (`customers.tier` or
  `customers.regions.name`), a row filter reads that column, and one band covers rows of
  two distinct values of it
- **THEN** by executed values the band appears in exactly one row, never once per value of
  the partition key

#### Scenario: An order key beside a ranked measure keeps the query's grain
- **WHEN** a grouped query declares a `first` or `last` measure (e.g.
  `last(amount, ordered_at)`) and orders by a column that is not a dimension, local
  (`city`) or joined (`customers.regions.name`)
- **THEN** by executed values the result has exactly one row per dimension combination
  (one row when the query has no dimensions), never one row per value of the order key

### Requirement: Grain-union broadcasting
When aggregates at different grains combine in one expression, the expression's grain
SHALL be the union of its operands' grains, each operand broadcast from its own grain
to that union; combining at different grains is never, by itself, an error. Broadcast
SHALL only go from coarser to finer; consuming an aggregate at a strictly coarser
grain SHALL fail with a typed error, never collapse implicitly. The population supplies the
rows — for a query omitting `population`, its row supply — and a cell an operand lacks
contributes the operand's empty value.

#### Scenario: Different-grain arithmetic broadcasts to the union
- **WHEN** a measure divides a query-grain aggregate by a
  `partition_by=<coarser subset>` aggregate
- **THEN** by executed values the coarser operand repeats across the dimensions it
  lacks and each cell holds the ratio at the union grain

#### Scenario: Coarser consumption is a typed error
- **WHEN** a query shape would need an aggregate consumed at a strictly coarser grain
  than its own
- **THEN** the query fails with a clear typed error rather than implicitly collapsing
  or duplicating rows

### Requirement: Compositionality
Each result cell's values SHALL depend only on the evaluated expression, the
cell, and the row-level filters — never on which other measures, measure-typed
filters, or order entries the query contains. In a query naming `population`, adding or
removing a projected measure SHALL NOT change the row set or any other column's values. In a
query omitting it, adding a measure SHALL NOT remove a row or change any value in an existing
row, a row it adds carrying the other measures' empty values. A transform over result rows
(the rank family, `lag`, `lead`, cumulative and collapsing transforms) is exempt from the previous
sentence until transforms evaluate over their operand's own cells (DEV-2218). A measure-typed
filter masks result cells without changing any surviving cell's values; ORDER BY and LIMIT
select and order rows without changing any cell's values. A multi-measure query SHALL
return, cell by cell, the same values as its single-measure splits in each split's rows, and
each measure's empty value in the rows only its siblings supply. Selecting an
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

## ADDED Requirements

### Requirement: Filters restrict every home of an inferred query
In a query omitting `population`, a field-typed filter conjunct SHALL restrict the rows of every
aggregation's home, and of the dimension anchor where its cells are supplied, before any tuple is
taken: inlined where the dataset determines every reference of the conjunct, otherwise by
association, exactly as a conjunct restricts a named population (each restricted row counted
once, the restriction reported through the semi-join informational entry naming the aggregate).
Each conjunct's references SHALL be routed from the dataset it restricts (`queries/references`).
A query-declared INNER join SHALL restrict each dataset as the field filter "some row is reached
along the route from that dataset to the join's declaring dataset". A conjunct reading several
homes SHALL restrict each of them by its own disposition. A conjunct on a dataset that supplies no
rows SHALL still restrict every home it reaches. No conjunct SHALL choose the dimension anchor or
make an aggregation supply rows.

#### Scenario: One filter, two homes
- **WHEN** a query without `population` groups by `customers.region`, measures
  `orders.total_revenue` and `returns.total_returns`, and filters `orders.amount > 100`, over the
  seed of `queries/population` › Rows of an inferred query are the row supply
- **THEN** revenue is restricted inline and returns by association: EAST (200, 25), WEST
  (300, NULL)

#### Scenario: A filter on a dataset no metric is homed on
- **WHEN** the same query measures only `orders.total_revenue` and filters `visits.visit_id is
  not null`
- **THEN** the orders are restricted to customers with a visit: the only row is WEST 300

#### Scenario: A query-declared INNER join restricts each home
- **WHEN** a query without `population` declares an `inner` relationship from `customers` to a
  dataset listing C1 and C4, groups by `customers.region` and measures `orders.total_revenue` and
  `returns.total_returns`
- **THEN** the rows are EAST (300, 25) and NORTH (NULL, 10)

### Requirement: Row laws of inferred queries
Before measure-typed filters, `order` and `limit`, every query omitting `population` SHALL obey:
**measure monotonicity** — adding a measure never removes a row and never changes a value in an
existing row (transforms over result rows excepted until DEV-2218); **home-row preservation** —
every home row of a full-grain aggregate surviving the filters lands in a row, exactly one when
the home determines the dimensions and the data respects declared cardinality; **filter
non-expansion** — adding a field filter never adds a row and never changes a result key; and the
**unit** — a query with measures and no dimensions returns exactly one row. Filter non-expansion
and the unit SHALL hold for a query naming `population` too.

#### Scenario: Monotonicity over generated queries
- **WHEN** the law harness generates root-less queries and adds a measure to each
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
