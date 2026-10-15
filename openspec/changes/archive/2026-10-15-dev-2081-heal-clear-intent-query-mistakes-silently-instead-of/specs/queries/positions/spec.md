## ADDED Requirements

### Requirement: An order column object may carry an expression
An order entry whose `column` is an object with `name` as its only key, where `name` is not a valid
column name but is an aggregation, transform or other expression the string form accepts, SHALL be
an exact equivalent of the string form `{"column": "<name>"}` — same sort, columns, rows and SQL — in
query `order` and in a refinement's `order`. An object that also carries `model` or `label` SHALL keep
the invalid-name error.

#### Scenario: Expression in the object form
- **WHEN** a query on `orders` grouped by `region` orders by `{"column": {"name": "sum(amount)"}, "direction": "desc"}`
- **THEN** its columns, rows and SQL equal those of the same query ordering by `{"column": "sum(amount)", "direction": "desc"}`

#### Scenario: Expression in a refinement's order
- **WHEN** a saved query runs with refinement `{"order": [{"column": {"name": "sum(amount)"}}]}`
- **THEN** it equals the refinement ordering by `{"column": "sum(amount)"}`

#### Scenario: Object with a model keeps the error
- **WHEN** an order entry is `{"column": {"name": "sum(amount)", "model": "orders"}}`
- **THEN** the query is rejected with the invalid-name error
