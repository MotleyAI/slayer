## MODIFIED Requirements

### Requirement: Ambiguous first and last names dispatch by argument shape
For `first` and `last` (both aggregation and transform names), the call SHALL
parse to one node and dispatch by the type of its bound first argument, which
binds exactly like any transform input: an argument that resolves to a row-level
value (a column, a literal, or an expression with a row-level leaf) SHALL be the
aggregation; an argument that resolves to an aggregate-valued operand — an
aggregation, a saved measure, or a grained transform, alone or composed through
arithmetic and scalar calls — SHALL be the transform over that series. The
dispatch SHALL NOT depend on the spelling of the argument, and SHALL NOT pre-empt
transform-input validation: an ill-formed transform inside the argument fails
with that transform's own error.

#### Scenario: Aggregation reading
- **WHEN** a measure is written `last(balance)` or `last(balance, updated_at)`
- **THEN** it is the `last` aggregation, identical to `balance:last` / `balance:last(updated_at)`

#### Scenario: Transform reading
- **WHEN** a measure is written `last(revenue:sum)` or `last(sum(revenue))`
- **THEN** it is the `last` transform over the aggregated series

#### Scenario: Saved-measure operand reads as the transform
- **WHEN** the model declares a measure `rev` with formula `revenue:sum` and a
  query measure is written `first(rev)` or `first(rev * 2)`
- **THEN** it is the `first` transform over the saved measure's series — the
  same plan and executed values as `first(revenue:sum)` / `first(revenue:sum * 2)`
  on SQLite and DuckDB — never an unknown-reference error

#### Scenario: Row-grain composite keeps the aggregation reading
- **WHEN** a measure is written `first(quantity * avg(unit_price, partition_by=product))`
  or `first(1)`
- **THEN** it is the `first` aggregation, which rejects the expression source with
  the existing "not supported over an expression" error

#### Scenario: Dispatch does not pre-empt transform-input validation
- **WHEN** a measure is written `first(cumsum(weight))` with `weight` an unprojected
  row-level column
- **THEN** it fails at plan time with the grain-refining row-level-leaf error naming
  `cumsum` — never the aggregation's "not supported over an expression" error
