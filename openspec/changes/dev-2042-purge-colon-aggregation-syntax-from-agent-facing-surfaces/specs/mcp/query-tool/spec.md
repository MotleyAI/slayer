## MODIFIED Requirements

### Requirement: Query-object execution

The `query` tool SHALL accept a single query object with the documented query fields (`source_model` in its three forms — stored-model name, inline model extension, inline model — plus measures, dimensions, filters, time dimensions, `main_time_dimension`, order, limit, offset, `whole_periods_only`, per-query `variables`, and the in-query control fields `strict` and `distinct_dimension_values`) and SHALL execute it with the same semantics as the engine's single-query execution.

#### Scenario: Single query object runs

- **WHEN** `query` is called with `query={"source_model": "orders", "measures": [{"formula": "count(*)"}], "dimensions": ["status"]}`
- **THEN** the aggregated result rows are returned in the requested output format

#### Scenario: In-query control fields are honored

- **WHEN** `query` is called with a query object containing `"strict": true` (or `"distinct_dimension_values": false`)
- **THEN** execution applies that setting exactly as the engine does for a query carrying that field

### Requirement: Multi-stage list execution

The `query` tool SHALL accept a non-empty list of query objects forming a multi-stage DAG with the engine's list semantics: every non-final entry is named, stages reference siblings by name (as a `source_model` string or a `source_model.joins[].target_model` — there is no top-level `joins` field), the engine reorders stages so references resolve, and the last entry is the root whose rows are returned. An empty list SHALL be rejected with a clear error.

#### Scenario: Two-stage query returns the root stage's rows

- **WHEN** `query` is called with `query=[{"name": "monthly", "source_model": "orders", "measures": [{"formula": "sum(revenue)"}], "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]}, {"source_model": "monthly", "measures": [{"formula": "count(*)"}]}]`
- **THEN** the result of the final (root) stage is returned

#### Scenario: Empty list is rejected

- **WHEN** `query` is called with `query=[]`
- **THEN** an error states that the list must be non-empty
