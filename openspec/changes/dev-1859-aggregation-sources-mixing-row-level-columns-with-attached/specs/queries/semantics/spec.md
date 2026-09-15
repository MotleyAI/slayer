# queries/semantics delta

## ADDED Requirements

### Requirement: Row-grain aggregation sources
An aggregation whose source operand combines row-level column references with
attached values — partitioned aggregates, directly or through arithmetic and
scalar functions — SHALL aggregate over the row-filtered population rows of its
home dataset, never over the attached operands' cells. The operand types at row
grain: the union of a row leaf's grain with any attached constituent's grain is
row grain, the finest. Each attached constituent is computed at its own declared
`partition_by=` grain (a constituent with no declared grain is typed at the
query's dimensions) and its value is broadcast onto each population row
null-safely: a row whose constituent lacks a value carries NULL for that
constituent, and the attachment never adds or removes rows. Per-row weighting is
the defined meaning of the shape and the broadcast SHALL NOT warn. The outer
aggregation evaluates at its consumer grain exactly as over any row-level
expression. Adding such a measure MUST NOT change the result row count or any
other column's values.

#### Scenario: Row-weighted value distinguishable from pure re-aggregation
- **WHEN** a query over dimensions `[region]` selects the measure
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** each region row carries the sum, over that region's base rows, of the
  row's `quantity` times its product's average unit price, by executed values,
  distinguishable from the pure re-aggregation
  `sum(avg(unit_price, partition_by=product))` and from
  `sum(quantity * unit_price)`

#### Scenario: Missing constituent value is NULL on a surviving row
- **WHEN** some base row's cell has no value for an attached constituent and the
  operand restores it through a NULL-restoring composite (e.g.
  `coalesce(avg(unit_price, partition_by=product), 0) * quantity`)
- **THEN** that row still contributes to the outer aggregation with the restored
  value — the population row exists and only its constituent value was NULL,
  unlike a fully-attached source, whose carrier excludes the absent cell

#### Scenario: Ungrained inner constituent types at the query's dimensions
- **WHEN** a query selects `sum(quantity * avg(unit_price))` with no
  `partition_by=` on the inner aggregate
- **THEN** the inner value is computed at the query's dimensions, broadcast onto
  each row, and weighted per row, with no degenerate-re-aggregation warning

#### Scenario: Row filters bound both the population and the constituents
- **WHEN** the query carries a row-level filter conjunct
- **THEN** it restricts both the population rows the outer aggregation consumes
  and each attached constituent's producer, and the executed value reflects both
