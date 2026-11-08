## MODIFIED Requirements

### Requirement: Single polymorphic query argument

The MCP server SHALL expose exactly one query-execution tool, named `query`, whose input schema consists of a required `query` argument accepting a string, a single query object, or a list of query objects, plus only the execution-wrapper arguments `variables`, `refine`, `show_sql`, `dry_run`, `explain`, and `format`. Per-field query arguments (`source_model`, `measures`, `dimensions`, `filters`, `time_dimensions`, `order`, `limit`, `offset`, `whole_periods_only`, `strict`, `distinct_dimension_values`) SHALL NOT appear in the tool schema, and no `query_nested` tool SHALL be registered.

#### Scenario: Tool schema exposes only the unified arguments

- **WHEN** an MCP client lists the server's tools
- **THEN** the `query` tool's input schema contains exactly `query` (required), `variables`, `refine`, `show_sql`, `dry_run`, `explain`, and `format`, with `query` accepting string, object, and array forms
- **AND** no tool named `query_nested` is present

### Requirement: Run-by-name string execution

The `query` tool SHALL treat a bare string as run-by-name execution of a query-backed model, with exactly the engine's string semantics: a query-backed model's backing query runs (honoring `variables`, and merging an optional `refine` into its final stage as the engine does); a stored model that is not query-backed SHALL raise the engine's error directing the caller to pass a query object with `source_model` instead. A `refine` passed with a query object or a list SHALL raise the engine's error that a refinement applies only to a saved query run by name.

#### Scenario: Query-backed model runs by name

- **WHEN** `query` is called with `query="monthly_revenue"` and `monthly_revenue` is a query-backed model
- **THEN** its backing query executes and the final-stage rows are returned

#### Scenario: Non-query-backed model name errors

- **WHEN** `query` is called with `query="orders"` and `orders` is a plain table-backed model
- **THEN** an error states the model is not query-backed and directs the caller to pass a query with `source_model="orders"`

#### Scenario: Query-backed model runs refined

- **WHEN** `query` is called with `query="monthly_revenue"` and `refine={"dimensions": ["region"]}`
- **THEN** the saved query runs with `region` merged into its final stage

#### Scenario: Refine with a query object errors

- **WHEN** `query` is called with a query object or a list and a `refine`
- **THEN** an error states that a refinement applies only to a saved query run by name
