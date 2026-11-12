## MODIFIED Requirements

### Requirement: Single polymorphic query argument

The MCP server SHALL expose exactly one query-execution tool, named `query`, whose input schema consists of a required `query` argument accepting a string, a single query object, or a list of query objects, plus only the execution-wrapper arguments `variables`, `refine`, `show_sql`, `dry_run`, `explain`, and `format`. Per-field query arguments (`source_model`, `measures`, `dimensions`, `filters`, `time_dimensions`, `order`, `limit`, `offset`, `whole_periods_only`, `to_many_handling`, `distinct_dimension_values`) SHALL NOT appear in the tool schema, and no `query_nested` tool SHALL be registered.

#### Scenario: Tool schema exposes only the unified arguments

- **WHEN** an MCP client lists the server's tools
- **THEN** the `query` tool's input schema contains exactly `query` (required), `variables`, `refine`, `show_sql`, `dry_run`, `explain`, and `format`, with `query` accepting string, object, and array forms
- **AND** no tool named `query_nested` is present

### Requirement: Query-object execution

The `query` tool SHALL accept a single query object with the documented query fields (`source_model` in its three forms — stored-model name, inline model extension, inline model — plus measures, dimensions, filters, time dimensions, `main_time_dimension`, order, limit, offset, `whole_periods_only`, per-query `variables`, and the in-query control fields `to_many_handling` and `distinct_dimension_values`) and SHALL execute it with the same semantics as the engine's single-query execution.

#### Scenario: Single query object runs

- **WHEN** `query` is called with `query={"source_model": "orders", "measures": [{"formula": "count(*)"}], "dimensions": ["status"]}`
- **THEN** the aggregated result rows are returned in the requested output format

#### Scenario: In-query control fields are honored

- **WHEN** `query` is called with a query object containing `"to_many_handling": "error"` (or `"distinct_dimension_values": false`)
- **THEN** execution applies that setting exactly as the engine does for a query carrying that field
