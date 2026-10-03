## MODIFIED Requirements

### Requirement: MCP create_model accepts model-defined aggregations

The MCP `create_model` tool SHALL accept an optional `aggregations` list of aggregation
definitions (the same shape `edit_model` upserts), persisting them on the created model
subject to the same save-time checks. Combining `aggregations` with `query` SHALL be
rejected like the other table-model parameters.

#### Scenario: Custom aggregation declared at creation

- **WHEN** `create_model` is called with a table model and an aggregation
  `{"name": "sum_sq", "formula": "SUM({value} * {value})"}`
- **THEN** the model is persisted with that aggregation and a query using
  `sum_sq(amount)` succeeds
