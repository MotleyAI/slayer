# queries/saved-query-refinement Specification

## Purpose
Defines refining a saved (query-backed) model at run-by-name: extra clauses merge into the saved
query's final stage, which then runs as one query, plus how agents discover a model's saved queries.

## Requirements

### Requirement: A refined run equals the hand-written merged final stage
Run-by-name SHALL accept a refinement carrying any of `dimensions`, `time_dimensions`, `measures`,
`filters`, `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`,
`distinct_dimension_values` and `to_many_handling`, with the same input forms and validation as the
same fields of a query. The refinement SHALL merge into the saved query's final stage only — earlier
stages are never changed — and the result SHALL be exactly the values, result keys, warnings and
errors of that merged stage written by hand in place of the saved final stage. A refinement carrying
`source_model`, `name`, `version`, `variables` or any unknown field SHALL be rejected. An empty
refinement SHALL behave exactly like no refinement.

Scenario data for this capability (executed on SQLite and DuckDB): table `orders(id, customer_id, region,
status, amount, ordered_at)` with rows (1, c1, US, paid, 100, 2025-01-05), (2, c1, US, refunded, 40,
2025-01-20), (3, c2, EU, paid, 70, 2025-02-03), (4, c3, EU, paid, 35, 2025-02-17), (5, c2, US, paid,
50, 2025-03-09); model `orders` over it with default time dimension `ordered_at`. Saved query
`monthly_revenue` = `{"source_model": "orders", "time_dimensions": [{"dimension": "ordered_at",
"granularity": "month"}], "measures": [{"formula": "sum(amount)", "name": "revenue"}], "filters":
["status = 'paid'"]}`, which runs by name to 2025-01 → 100, 2025-02 → 105, 2025-03 → 50 under keys
`orders.ordered_at`, `orders.revenue`.

#### Scenario: Extra dimension
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["region"]}`
- **THEN** the rows are (US, 2025-01, 100), (EU, 2025-02, 105), (US, 2025-03, 50)

#### Scenario: Extra measure
- **WHEN** `monthly_revenue` runs with refinement `{"measures": ["count(*)"]}`
- **THEN** the result adds `orders._count` = 1, 2, 1 for 2025-01, 2025-02, 2025-03

#### Scenario: Extra filter ANDs with the saved filters
- **WHEN** `monthly_revenue` runs with refinement `{"filters": ["amount >= 50"]}`
- **THEN** the rows are 2025-01 → 100, 2025-02 → 70, 2025-03 → 50

#### Scenario: Window added to the saved time dimension
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": [{"dimension": "ordered_at", "granularity": "month", "date_range": ["2025-02-01", "2025-03-31"]}]}`
- **THEN** the rows are 2025-02 → 105, 2025-03 → 50

#### Scenario: Order and limit
- **WHEN** `monthly_revenue` runs with refinement `{"order": [{"column": "revenue", "direction": "desc"}], "limit": 1}`
- **THEN** the single row is 2025-02 → 105

#### Scenario: Second granularity renames the saved key
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": ["year(ordered_at)"]}`
- **THEN** the result keys are `orders.ordered_at.month`, `orders.ordered_at.year` and `orders.revenue`, as in the hand-written query with both granularities

#### Scenario: Functional granularity in dimensions
- **WHEN** a refinement's `dimensions` contains `year(ordered_at)`
- **THEN** it merges as that time dimension, exactly as a query's `dimensions` entry does

#### Scenario: Empty refinement
- **WHEN** `monthly_revenue` runs with refinement `{}`
- **THEN** the result equals the plain run by name, with the same SQL

#### Scenario: Forbidden refinement fields
- **WHEN** a refinement carries `source_model`, `name`, `version`, `variables`, or an unknown key
- **THEN** it is rejected as invalid input before anything executes

### Requirement: Merge rules per field
Dimensions and measures SHALL be the saved entries followed by the refinement entries not already
present. A dimension column reference SHALL be identified by its reference with the source-model
prefix stripped (`region` ≡ `orders.region`), a computed dimension by its name, a measure by its name
and formula (an unnamed measure by its formula). Two entries with the same identity SHALL collapse
into one when they are equal in every field. Time dimensions SHALL be keyed by (column,
granularity): a new key is appended; for an existing key each of `date_range` and `label` takes the
side that sets it, keeps equal values, and conflicts when both sides set different values,
`date_range` being compared after input normalization. Filters SHALL be the saved filters followed
by the refinement's, exact duplicate strings dropped. `order`, `limit`, `offset`,
`main_time_dimension`, `whole_periods_only`, `distinct_dimension_values` and `to_many_handling` SHALL
replace the saved value when the refinement supplies them, an explicit empty `order` or `null`
clearing the saved value, and SHALL leave it unchanged otherwise.

#### Scenario: Identical duplicates collapse
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["orders.region"], "measures": [{"formula": "sum(amount)", "name": "revenue"}]}`
- **THEN** the result equals the extra-dimension scenario's, with no name-collision error

#### Scenario: Same date range on the saved window
- **WHEN** a saved query whose month time dimension carries `date_range` `["2025-01-01", "2025-02-28"]` runs with a refinement giving that time dimension the same range, spelled as a list or with equal normalized values
- **THEN** it runs unchanged, with no error

#### Scenario: Filter narrowing a saved window
- **WHEN** that saved query (which also orders by `revenue` desc with `limit` 2) runs with refinement `{"filters": ["ordered_at >= '2025-02-01'"]}`
- **THEN** the single row is 2025-02 → 105

#### Scenario: Clearing a saved limit
- **WHEN** that saved query runs with refinement `{"limit": null}`
- **THEN** both window months return, in order 2025-02 → 105, 2025-01 → 100

#### Scenario: Scalar settings replace only when supplied
- **WHEN** a refinement supplies `to_many_handling` (or another replaceable setting)
- **THEN** the merged stage uses the refinement's value
- **WHEN** a refinement omits it
- **THEN** the saved value stands

#### Scenario: Duplicate filters dropped
- **WHEN** a refinement repeats a saved filter string exactly
- **THEN** the merged stage carries that filter once

### Requirement: Conflicting refinements fail closed
A refinement entry with the same identity as a saved entry but differing in any other field, and a
time-dimension `date_range` or `label` set differently on both sides, SHALL raise a refinement
conflict error naming the field, the key (measure or dimension name, or `column@granularity`), the
saved value and the refinement value, with a suggestion; for a `date_range` conflict the suggestion
SHALL be to add a filter on the time column or to save the window as a filter with variables.
Nothing SHALL execute.

#### Scenario: Same measure name, different formula
- **WHEN** `monthly_revenue` runs with refinement `{"measures": [{"formula": "count(*)", "name": "revenue"}]}`
- **THEN** a refinement conflict error names `measures`, `revenue`, `sum(amount)` and `count(*)`

#### Scenario: Same measure, different metadata
- **WHEN** a refinement repeats a saved measure's name and formula with a different `label` (or `type`, `description`, `meta`), for a named and for an unnamed measure
- **THEN** a refinement conflict error is raised

#### Scenario: Same computed dimension name, different expression
- **WHEN** a refinement's computed dimension shares a saved computed dimension's name with a different expression
- **THEN** a refinement conflict error names `dimensions` and that name

#### Scenario: Conflicting date range
- **WHEN** the saved window query runs with a refinement giving its month time dimension `date_range` `["2025-02-01", "2025-03-31"]`
- **THEN** a refinement conflict error names `time_dimensions` and `ordered_at@month` and suggests a filter on the time column

### Requirement: Multi-stage saved queries refine their final stage
For a multi-stage saved query the refinement SHALL merge into the final stage, reading the earlier
stages exactly as the saved final stage does; a reference the final stage cannot resolve SHALL fail
with the ordinary resolution error.

#### Scenario: Refining the final stage
- **WHEN** the saved query `[{"name": "per_customer", "source_model": "orders", "dimensions": ["customer_id", "region"], "measures": [{"formula": "sum(amount)", "name": "revenue"}], "filters": ["status = 'paid'"]}, {"source_model": "per_customer", "measures": [{"formula": "avg(revenue)", "name": "avg_revenue"}]}]` runs by name
- **THEN** `per_customer.avg_revenue` is 63.75
- **WHEN** it runs with refinement `{"dimensions": ["region"]}`
- **THEN** the rows are (US, 75.0), (EU, 52.5)
- **WHEN** it runs with refinement `{"filters": ["region = 'EU'"]}`
- **THEN** the value is 52.5

#### Scenario: A column aggregated away
- **WHEN** it runs with refinement `{"dimensions": ["status"]}`
- **THEN** the ordinary unknown-reference error for `status` is raised

### Requirement: Variables apply to the refined run
Runtime variables SHALL override the saved defaults exactly as for an unrefined run, and a refinement
filter containing `{placeholders}` SHALL be substituted with the same precedence as the saved final
stage's own filters.

#### Scenario: Refinement with runtime variables
- **WHEN** the saved query `{"source_model": "orders", "dimensions": ["region"], "measures": [{"formula": "sum(amount)", "name": "revenue"}], "filters": ["status = '{status}'"]}` saved with variables `{"status": "paid"}` runs by name
- **THEN** the rows are US → 150, EU → 105
- **WHEN** it runs with variables `{"status": "refunded"}`
- **THEN** the single row is US → 40
- **WHEN** it runs with refinement `{"measures": ["count(*)"]}` and variables `{"status": "refunded"}`
- **THEN** the single row is US → 40 with `_count` 1

#### Scenario: Placeholder in a refinement filter
- **WHEN** a refinement filter contains `{status}` and the run supplies or saves a `status` variable
- **THEN** it is substituted with the same value the saved stage's filters receive

### Requirement: Refinement is accepted only with a saved-query name
The refinement SHALL be accepted on every run-by-name surface — the Python engine and client, the
REST query endpoint next to `name`, the MCP query tool next to a model-name string, and the CLI next
to a model-name argument — and SHALL be rejected with an error stating it applies only to a saved
query run by name when combined with a query object or a stage list. On REST, a refinement without
`name` and any query field supplied next to `name` outside the refinement (including an explicit
`null`) SHALL be rejected with status 400 naming `refine` as the place for query clauses; a
refinement conflict SHALL respond 400; a malformed refinement body SHALL respond 422 like any
malformed request body.

#### Scenario: Every surface runs a refinement
- **WHEN** `monthly_revenue` is run by name with refinement `{"dimensions": ["region"]}` through the Python engine, the Python client (in process and over HTTP), REST, MCP and the CLI (inline JSON and `@file`)
- **THEN** each returns the extra-dimension scenario's rows

#### Scenario: Refinement with a query object
- **WHEN** a refinement is passed with a query object or a stage list on the engine, the client, MCP or the CLI
- **THEN** it fails with the error that a refinement applies only to a saved query run by name

#### Scenario: REST misuse
- **WHEN** a REST body carries `refine` without `name`, or `name` with a flat query field such as `"limit": null`
- **THEN** the response is 400 and the message points to `refine`

#### Scenario: REST conflict and malformed refinement
- **WHEN** a REST refinement conflicts with the saved query
- **THEN** the response is 400
- **WHEN** a REST refinement carries an unknown key
- **THEN** the response is 422

#### Scenario: MCP row cap unchanged
- **WHEN** the MCP tool runs a saved query with a refinement and more rows than the cap
- **THEN** the response is capped response-side and no limit is pushed into the saved query

### Requirement: Refined results cache per refinement
A refined run SHALL cache under its own entry, eviction by name and the same refinement SHALL remove
exactly that entry, and refreshing a stale refined entry SHALL re-run the refinement.

#### Scenario: Separate cache entries
- **WHEN** a saved query runs cached with and without a refinement
- **THEN** the two results are cached separately and `evict` with the name and the refinement removes only the refined entry

#### Scenario: Refresh keeps the refinement
- **WHEN** a cached refined run goes stale and is refreshed
- **THEN** the refreshed rows equal a fresh refined run, not the unrefined run

### Requirement: Saved queries are discoverable from their models
Inspecting a model SHALL list the non-hidden saved queries of its datasource any of whose stages
name that model as `source_model` (directly or as the base of a model extension), each once, with
name and description (descriptions truncated like other inspect descriptions), in the markdown and
JSON outputs of the compact and full model views and of views listing model skeletons; the listing
SHALL be omitted when empty and SHALL be a selectable inspect section. A model SHALL never list
itself. Search SHALL find a saved query by its description.

#### Scenario: Listed on the source model
- **WHEN** model `orders` is inspected and `monthly_revenue` and a saved query whose stage extends `orders` exist
- **THEN** both appear under saved queries in markdown and JSON, compact and full

#### Scenario: Hidden, unrelated and empty
- **WHEN** a saved query is hidden, or reads a different model, or no saved query reads the model
- **THEN** it is not listed, and an empty listing is omitted

#### Scenario: Section selection
- **WHEN** the full view is requested with sections including or excluding saved queries
- **THEN** the listing follows the selection like every other section

#### Scenario: Search finds a saved query
- **WHEN** a search question matches a saved query's description
- **THEN** the saved query is among the results
