# aggregations/expression-aggregation — delta

## MODIFIED Requirements

### Requirement: Unsupported expression shapes fail with clear errors
The system SHALL reject, with errors naming the limitation: expressions
referencing joined-model columns (cross-model) at row level, expressions
referencing columns that carry a column-level filter, transforms nested inside
the aggregated expression, aggregation sources mixing row-level column
references with attached (partitioned-aggregate) values, and an attached
(aggregate-valued) parameter on an aggregation whose source is row-level — the
parameter would need the attached value on the aggregation's own input rows.
An aggregation source consisting entirely of attached values is a
re-aggregation and SHALL be accepted (per `queries/partitioned-aggregates` ›
Re-aggregation consumes attached operands as datasets).

#### Scenario: Cross-model expression rejected
- **WHEN** a measure is written `sum(amount - customers.discount)`
- **THEN** it fails with an error stating cross-model expression aggregation is not supported

#### Scenario: Filtered-column operand rejected
- **WHEN** the expression references a column that has a column-level filter
- **THEN** it fails with an error naming the column and suggesting the colon form on a derived model column

#### Scenario: Nested aggregation rejected
- **WHEN** a measure is written `sum(cumsum(x) - 1)` or
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** each fails with a typed error naming its boundary: transforms cannot
  nest inside an aggregated expression, and an aggregation source cannot mix
  row-level references with attached values

#### Scenario: Attached parameter on a row-level source rejected
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  in any `to_many_handling` mode
- **THEN** it fails at plan time with a typed error naming the parameter and the remedy
  (aggregate the source to the parameter's grain, or use a row-level parameter),
  containing no issue reference; the same aggregation with a row-level parameter is
  unaffected

#### Scenario: Fully attached source accepted
- **WHEN** a measure is written `avg(sum(amount, partition_by=[city, region]))`
- **THEN** it is accepted and compiles as a re-aggregation, not rejected by the
  expression gate
