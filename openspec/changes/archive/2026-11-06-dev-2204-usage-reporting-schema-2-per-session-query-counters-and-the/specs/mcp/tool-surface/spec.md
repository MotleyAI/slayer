## ADDED Requirements

### Requirement: Database failures are tool errors
When the database fails during the `query`, `create_model`, `validate_models` or `ingest_datasource_models` tool, the tool call SHALL fail as an MCP tool error (`isError: true`) whose text carries the friendly database-error message ("Database error: …"), never return that message as a successful result.

#### Scenario: Query hits a database failure
- **WHEN** a `query` call fails because the database rejects the generated SQL
- **THEN** the result has `isError: true` and its text holds the "Database error:" message

#### Scenario: Model creation hits a database failure
- **WHEN** `create_model`, `validate_models` or `ingest_datasource_models` fails because the database is unreachable
- **THEN** the result has `isError: true` and its text holds the "Database error:" message
