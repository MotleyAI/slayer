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
`queries/semantics` › Row-grain aggregation sources). An attached
(aggregate-valued) parameter on an aggregation whose source is row-level SHALL
be accepted when the aggregation's operating grain determines it (per
`queries/partitioned-aggregates` › Attached parameters on row-level sources).
Under `broadcast`/`error` a cross-model aggregation's attached input SHALL read
only columns attributable from the aggregation's root — a typed error names the
root, the leaf and the `associate` remedy otherwise (per
`queries/partitioned-aggregates` › Default-mode twin of the associate shape).
Whether an aggregation runs over rows or over an operand dataset's cells is
decided by its source alone; every attached input, in the source or in a
parameter, is then attached by one mechanism — into the input relation for a
row-level source, as a constituent of the operand dataset for an attached one.

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

#### Scenario: Attached parameter on a row-level source accepted
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under `to_many_handling: "associate"`
- **THEN** it is accepted and compiles with the parameter's value attached into
  the aggregation's input relation — never the attached-parameter rejection —
  and the same aggregation with a row-level parameter is unaffected

#### Scenario: Attached parameter on a row-level source rejected
- **WHEN** a measure is written
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  rooted at `orders` under the default `broadcast` `to_many_handling`
- **THEN** it fails at plan time with a typed error naming the producer's root
  `customers`, the unreachable leaf `amount` and the `associate` remedy,
  containing no issue reference; the same aggregation with a parameter reading
  only `customers`-side columns executes
