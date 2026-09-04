# MCP query tool

## Purpose

Defines the MCP `query` tool's input contract — one polymorphic `query` argument mirroring the engine's accepted union (model name, single query json, or multi-stage list) — plus its execution-wrapper arguments and output shaping.

## ADDED Requirements

### Requirement: Single polymorphic query argument

The MCP server SHALL expose exactly one query-execution tool, named `query`, whose input schema consists of a required `query` argument accepting a string, a single query object, or a list of query objects, plus only the execution-wrapper arguments `variables`, `show_sql`, `dry_run`, `explain`, and `format`. Per-field query arguments (`source_model`, `measures`, `dimensions`, `filters`, `time_dimensions`, `order`, `limit`, `offset`, `whole_periods_only`, `strict`, `distinct_dimension_values`) SHALL NOT appear in the tool schema, and no `query_nested` tool SHALL be registered.

#### Scenario: Tool schema exposes only the unified arguments

- **WHEN** an MCP client lists the server's tools
- **THEN** the `query` tool's input schema contains exactly `query` (required), `variables`, `show_sql`, `dry_run`, `explain`, and `format`, with `query` accepting string, object, and array forms
- **AND** no tool named `query_nested` is present

### Requirement: Query-object execution

The `query` tool SHALL accept a single query object with the documented query fields (`source_model` in its three forms — stored-model name, inline model extension, inline model — plus measures, dimensions, filters, time dimensions, order, limit, offset, and the in-query control fields `strict` and `distinct_dimension_values`) and SHALL execute it with the same semantics as the engine's single-query execution.

#### Scenario: Single query object runs

- **WHEN** `query` is called with `query={"source_model": "orders", "measures": [{"formula": "*:count"}], "dimensions": ["status"]}`
- **THEN** the aggregated result rows are returned in the requested output format

#### Scenario: In-query control fields are honored

- **WHEN** `query` is called with a query object containing `"strict": true` (or `"distinct_dimension_values": false`)
- **THEN** execution applies that setting exactly as the engine does for a query carrying that field

### Requirement: Multi-stage list execution

The `query` tool SHALL accept a non-empty list of query objects forming a multi-stage DAG with the engine's list semantics: every non-final entry is named, stages reference siblings by name, the engine reorders stages so references resolve, and the last entry is the root whose rows are returned. An empty list SHALL be rejected with a clear error.

#### Scenario: Two-stage query returns the root stage's rows

- **WHEN** `query` is called with `query=[{"name": "monthly", "source_model": "orders", "measures": [{"formula": "revenue:sum"}], "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]}, {"source_model": "monthly", "measures": [{"formula": "*:count"}]}]`
- **THEN** the result of the final (root) stage is returned

#### Scenario: Empty list is rejected

- **WHEN** `query` is called with `query=[]`
- **THEN** an error states that the list must be non-empty

### Requirement: Run-by-name string execution

The `query` tool SHALL treat a bare string as run-by-name execution of a query-backed model, with exactly the engine's string semantics: a query-backed model's backing query runs (honoring `variables`); a stored model that is not query-backed SHALL raise the engine's error directing the caller to pass a query object with `source_model` instead.

#### Scenario: Query-backed model runs by name

- **WHEN** `query` is called with `query="monthly_revenue"` and `monthly_revenue` is a query-backed model
- **THEN** its backing query executes and the final-stage rows are returned

#### Scenario: Non-query-backed model name errors

- **WHEN** `query` is called with `query="orders"` and `orders` is a plain table-backed model
- **THEN** an error states the model is not query-backed and directs the caller to pass a query with `source_model="orders"`

### Requirement: Execution wrappers and output shaping

The `query` tool SHALL support, uniformly across all three input forms: `variables` (merged with precedence runtime > named-stage > outer-query > model query variables), `dry_run` (return generated SQL without executing), `explain` (return SQL plus the query plan), `show_sql` (prefix results with the SQL), and `format` in {`markdown`, `json`, `csv`} case-insensitively — any other value SHALL be rejected with an error naming the valid options. When a result carries dimension/measure attribute metadata, the attributes block SHALL be appended regardless of input form.

#### Scenario: Dry run returns SQL only

- **WHEN** `query` is called with any valid `query` value and `dry_run=true`
- **THEN** the response contains the generated SQL and no result rows

#### Scenario: Invalid format is rejected

- **WHEN** `query` is called with `format="xml"`
- **THEN** an error lists the valid formats json, csv, and markdown

#### Scenario: Attributes appended on run-by-name results

- **WHEN** `query` is called with a query-backed model name whose result carries attribute metadata
- **THEN** the formatted output ends with the attributes block
