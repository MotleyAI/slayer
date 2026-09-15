# aggregations/expression-aggregation delta

## MODIFIED Requirements

### Requirement: Unsupported expression shapes fail with clear errors
The system SHALL reject, with errors naming the limitation: expressions
referencing joined-model columns (cross-model) at row level, expressions
referencing columns that carry a column-level filter, and transforms nested
inside the aggregated expression. An aggregation source consisting entirely of
attached values is a re-aggregation and SHALL be accepted (per
`queries/partitioned-aggregates` › Re-aggregation consumes attached operands as
datasets). An aggregation source mixing row-level references with attached
values is a row-grain aggregation and SHALL be accepted (per
`queries/semantics` › Row-grain aggregation sources).

#### Scenario: Cross-model expression rejected
- **WHEN** a measure is written `sum(amount - customers.discount)`
- **THEN** it fails with an error stating cross-model expression aggregation is not supported

#### Scenario: Filtered-column operand rejected
- **WHEN** the expression references a column that has a column-level filter
- **THEN** it fails with an error naming the column and suggesting the colon form on a derived model column

#### Scenario: Nested aggregation rejected
- **WHEN** a measure is written `sum(cumsum(x) - 1)`
- **THEN** it fails with a typed error stating transforms cannot nest inside an
  aggregated expression

#### Scenario: Fully attached source accepted
- **WHEN** a measure is written `avg(sum(amount, partition_by=[city, region]))`
- **THEN** it is accepted and compiles as a re-aggregation, not rejected by the
  expression gate

#### Scenario: Mixed row and attached source accepted
- **WHEN** a measure is written
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** it is accepted and compiles at row grain — the attached value
  broadcast per base row — not rejected by the expression gate
