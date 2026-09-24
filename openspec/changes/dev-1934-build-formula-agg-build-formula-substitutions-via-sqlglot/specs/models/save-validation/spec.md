## ADDED Requirements

### Requirement: Aggregation formulas are parse-checked at save time

When a model is saved through any engine create or edit door, every model-defined
aggregation `formula` SHALL be parsed with its placeholders replaced, in the model's
datasource dialect (sqlglot's generic dialect when the datasource cannot be resolved —
the check is never skipped). A formula that does not parse, or that places a placeholder
in a non-expression position, SHALL fail the save with an error naming the model and the
aggregation, before anything is persisted or trial-executed. Placeholder names SHALL NOT
be checked at save time, since query-time arguments may legitimately supply them; but a
declared parameter the rendered formula never references (for a formula-less built-in, one
that is not among its own parameters), or a formula on the ranked `first`/`last`, SHALL
fail the save with a typed error naming the model, the aggregation and the parameter.
Ingestion / YAML-load persistence SHALL remain out of scope.

#### Scenario: Unparseable formula blocks the save

- **WHEN** a model declaring an aggregation with formula `SUM({value}` is saved through the
  MCP `create_model` tool
- **THEN** the tool returns an error naming the model and the aggregation
- **AND** the model is not persisted

#### Scenario: Query-time-only placeholder is accepted

- **WHEN** a model declaring an aggregation with formula `SUM({value} * {scale})` and no
  declared `scale` parameter is saved
- **THEN** the save succeeds

#### Scenario: A declared parameter the formula never references blocks the save

- **WHEN** a model declaring an aggregation with formula `SUM({value}) * {k}` and parameters
  `k` and `unused` is saved
- **THEN** the save fails with a typed error naming the model, the aggregation and `unused`
- **AND** the model is not persisted

#### Scenario: Edit introducing a broken formula leaves the original intact

- **WHEN** the MCP `edit_model` tool changes an aggregation's formula to `SUM({value}`
- **THEN** the tool returns an error and the previously-persisted model is unchanged

### Requirement: MCP create_model accepts model-defined aggregations

The MCP `create_model` tool SHALL accept an optional `aggregations` list of aggregation
definitions (the same shape `edit_model` upserts), persisting them on the created model
subject to the same save-time checks. Combining `aggregations` with `query` SHALL be
rejected like the other table-model parameters.

#### Scenario: Custom aggregation declared at creation

- **WHEN** `create_model` is called with a table model and an aggregation
  `{"name": "sum_sq", "formula": "SUM({value} * {value})"}`
- **THEN** the model is persisted with that aggregation and a query using
  `amount:sum_sq` succeeds
