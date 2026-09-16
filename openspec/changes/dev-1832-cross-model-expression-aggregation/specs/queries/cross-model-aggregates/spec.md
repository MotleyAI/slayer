## MODIFIED Requirements

### Requirement: Target-rooted computation with metric independence
A cross-model aggregate SHALL be computed over the rows of its home dataset (its
root) — the model its source names for a single-column source, the dataset per
`queries/semantics` › Home dataset of a row-level aggregation source for an
expression source — never over the query root's join-multiplied rows. Adding a
cross-model aggregate MUST NOT change the result row count, any other column's
values, or any other metric's values, and its own value MUST NOT depend on which
other metrics are present.

#### Scenario: Joined sum is not multiplied by join fan-out
- WHEN a query rooted at `orders` selects `customers.spend:sum` grouped by a customer-level dimension, and customers have several orders each
- THEN each cell's value counts every customer's spend exactly once, by executed values, regardless of how many orders each customer has

#### Scenario: Expression source homed at the joined model is not multiplied
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - customers.regions.pop)`
  grouped by a customer-level dimension, and customers have several orders each
- **THEN** each cell's value counts every customer exactly once, by executed values,
  identical to `customers.spend:sum - customers.regions.pop:sum` computed at the
  same root

#### Scenario: Adding a cross-model measure is cardinality-neutral
- WHEN any supported query runs with and without an additional cross-model measure
- THEN both runs return the same rows and identical values in all shared columns
