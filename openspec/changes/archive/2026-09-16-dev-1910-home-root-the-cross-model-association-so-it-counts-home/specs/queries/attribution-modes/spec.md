## MODIFIED Requirements

### Requirement: Distinct-entity association semantics
Under `to_many_handling: "associate"`, an aggregate with at least one unattributable
grain dimension SHALL return, for each result cell, the aggregate over the distinct
home entities associated with that cell — each entity counted exactly once per cell,
deduplicated by the home model's unique key. Association is defined by the join path
from the aggregate's home dataset to the dimension (Axiom 3, Association): an entity
belongs to a cell iff its own path reaches the cell's dimension values, so a home entity
with no population row still counts in every cell its path reaches, and the origin of the
population's rows never restricts the association. A dimension the home reaches only back
through the population root associates an entity only when at least one population row
carries it: an entity with no population row is in no such cell, never in a manufactured
NULL cell. A population rooted at the home itself keeps its own row set: an entity with no
related row sits in the NULL cell exactly as its population row does. The result grain,
row count, sibling metrics, and other columns' values MUST be unchanged relative to the
same query without the aggregate. Entity populations of different cells may overlap;
cells are therefore not additive across the unattributable dimensions, and the response
warns accordingly. Attributable grain dimensions retain exact partition values identical
to broadcast mode.

#### Scenario: Cross-model metric attributes per cell by executed values
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `customers.spend:sum` by the orders-level dimension `status`
- **THEN** each status cell equals the summed spend of the distinct customers having
  at least one order with that status, by executed values, with unchanged result grain

#### Scenario: Local metric over a fanning dimension attributes per cell
- **WHEN** a query rooted at `customers` with `to_many_handling: "associate"` selects
  `spend:sum` by `orders.status`, and one customer has two orders with the same status
- **THEN** that customer's spend counts once in that status cell — by executed values,
  never the join-multiplied figure

#### Scenario: Adding an associated measure is cardinality-neutral
- **WHEN** any supported query runs with and without an additional associate-mode
  aggregate
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Multi-hop association attributes per cell
- **WHEN** a query rooted at `orders` under `associate` selects a metric rooted two
  hops away (e.g. `customers.regions.pop:sum`) by an orders-level dimension
- **THEN** each cell aggregates over the distinct entities of the metric's root
  associated with the cell, by executed values

#### Scenario: Home entity absent from the population counts in its home-determined cell
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum`
  and the local `amount:sum` by a regions-level dimension the customer's own path
  determines, and one South customer has no orders
- **THEN** the South cell of `customers.spend:sum` includes that customer's spend (every
  South customer once, on SQLite and DuckDB) while the local `amount:sum` cell is
  unchanged, and the response carries the associated-cells warning

#### Scenario: Dimension through the population root never manufactures a NULL cell
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum` by
  `status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell aggregates exactly the customers owning a NULL-status
  order, and the orderless customer appears in no cell, by executed values

#### Scenario: A population rooted at the home keeps its own NULL cell
- **WHEN** a query rooted at `customers` under `associate` selects `spend:sum` by
  `orders.status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell holds both the orderless customer and the owner of the
  NULL-status order, exactly the customers whose population rows carry a NULL status

#### Scenario: Mixed home-side and population-root dimensions
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum` by
  both a regions-level dimension and `status`, and one South customer has no orders
- **THEN** every customer with orders is counted once in each of its (region, status)
  cells and the orderless customer is in no cell, by executed values
