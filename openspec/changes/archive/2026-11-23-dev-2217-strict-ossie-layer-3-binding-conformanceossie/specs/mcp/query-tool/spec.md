## MODIFIED Requirements

### Requirement: Single polymorphic query argument

The MCP server SHALL expose exactly one query-execution tool, named `query`, whose input schema consists of a required `query` argument accepting a string, a single query object, or a list of query objects, plus only the execution-wrapper arguments `variables`, `refine`, `show_sql`, `dry_run`, `explain`, `format`, and `conformance` (`slayer`, the default, or `ossie-l3`, selecting the strict Ossie Layer 3 binding of `queries/ossie-l3-binding`). Per-field query arguments (`population`, `metrics`, `dimensions`, `filter`, `time_dimensions`, `order`, `limit`, `offset`, `whole_periods_only`, `to_many_handling`, `distinct_dimension_values`) SHALL NOT appear in the tool schema, and no `query_nested` tool SHALL be registered.

#### Scenario: Tool schema exposes only the unified arguments

- **WHEN** an MCP client lists the server's tools
- **THEN** the `query` tool's input schema contains exactly `query` (required), `variables`, `refine`, `show_sql`, `dry_run`, `explain`, `format`, and `conformance`, with `query` accepting string, object, and array forms
- **AND** no tool named `query_nested` is present


#### Scenario: The conformance argument selects the strict binding
- **WHEN** `query` is called with `conformance="ossie-l3"` and `query={"metrics": ["orders.total_revenue"], "limit": 10}`
- **THEN** the call fails with an error whose text starts with `OssieQueryError: E_INVALID_QUERY`, and the same call without `conformance` answers
