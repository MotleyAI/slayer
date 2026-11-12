## ADDED Requirements

### Requirement: A bare sql_table resolves in the connection's default schema
Schema-drift checks — explicit validation (`validate_models`, ingest validation,
`validate-models --force-clean`) and query-time drift attribution — SHALL resolve a model
whose `sql_table` is unqualified against the connection's default schema, however many other
schemas the database holds. The default schema SHALL be the one the connection itself uses
for unqualified names; on BigQuery that is the dataset in the connection URL
(`bigquery://<project>/<dataset>`). An unqualified `sql_table` SHALL NOT resolve to an object
in any other schema.

#### Scenario: A bare model in the URL dataset survives a second dataset
- **WHEN** a BigQuery datasource's connection URL names dataset `a`, the project also holds dataset `b`, and a model's `sql_table` is the bare name of a table in `a`
- **THEN** `validate_models` reports no `WholeModelDelete` for that model

#### Scenario: Column drift on a bare model is still reported
- **WHEN** the default schema is known from the connection, the database holds a second schema, and a column of the live table behind a bare-named model was dropped
- **THEN** `validate_models` reports an edit removing that column, not a `WholeModelDelete`

#### Scenario: A bare name never resolves to another schema
- **WHEN** a model's bare `sql_table` names an object that exists only in a non-default schema
- **THEN** `validate_models` reports a `WholeModelDelete` for that model

#### Scenario: A same-named object elsewhere does not shadow the default
- **WHEN** an object named like a model's bare `sql_table` exists in both the default schema and another schema, and only the other schema's object has the model's columns
- **THEN** drift checks compare the model against the default schema's object, and a model whose `sql_table` is qualified with the other schema compares against that schema's object

#### Scenario: Query-time attribution resolves bare names the same way
- **WHEN** the default schema is known from the connection, the database holds a second schema, and a query reading a bare-named model whose live table is unchanged fails for an unrelated reason
- **THEN** the original database error propagates, not `SchemaDriftError`

#### Scenario: A schema list that cannot be read falls back to the default
- **WHEN** the database's schema list cannot be read during query-time attribution and the default schema is known only from the connection
- **THEN** a bare-named model whose live table is unchanged is not reported as deleted
