# mcp/response-row-cap Specification

## Purpose
Protects the calling agent's context window: MCP query responses are capped at a small default row count unless the caller sets an explicit limit, and a truncated response says so and tells the caller how to get more rows.

## Requirements

### Requirement: Default row cap on MCP query responses

When the MCP `query` tool is called without a `limit`, the response SHALL contain at most 20 data rows. When the underlying result has more rows than the cap, the response SHALL be truncated to exactly 20 rows and carry a truncation notice.

#### Scenario: Uncapped query over a large result

- WHEN `query` runs without `limit` against a model whose result has more than 20 rows
- THEN the response contains exactly 20 data rows and a truncation notice

#### Scenario: Result exactly at the cap

- WHEN `query` runs without `limit` and the result has exactly 20 rows
- THEN all 20 rows are returned and no truncation notice appears

#### Scenario: Result one past the cap

- WHEN `query` runs without `limit` and the result has exactly 21 rows
- THEN the response contains exactly 20 rows and a truncation notice

### Requirement: Explicit limit is trusted verbatim

When the caller passes an explicit `limit`, the MCP layer SHALL return the rows as executed, with no response-side truncation and no truncation notice — regardless of how many rows come back, including `explain` plan rows.

#### Scenario: Explicit limit honored

- WHEN `query` runs with `limit=25` against a result with 30 available rows
- THEN 25 rows are returned and no truncation notice appears

#### Scenario: Explicit small limit

- WHEN `query` runs with `limit=5`
- THEN 5 rows are returned and no truncation notice appears

#### Scenario: Rows exceeding an explicit limit are not sliced by the MCP layer

- WHEN the engine returns more rows than an explicit `limit` (e.g. a mocked execution)
- THEN the MCP layer neither slices the rows nor adds a truncation notice

#### Scenario: Explain with explicit limit untouched

- WHEN `query` runs with `explain=True` and an explicit `limit`, and the plan has more rows than the limit
- THEN all plan rows are returned and no truncation notice appears

### Requirement: Cap push-down into the generated query

When no explicit `limit` is given on the structured query path, the generated SQL SHALL carry `LIMIT 21` (cap + 1) so truncation is detectable without fetching the full result. Run-by-name execution of a stored query SHALL leave the stored query's SQL untouched.

#### Scenario: Pushed-down limit visible in SQL

- WHEN `query` runs without `limit` on the structured path with `show_sql=True` or `dry_run=True`
- THEN the generated SQL contains `LIMIT 21`, not `LIMIT 20`

#### Scenario: Stored query SQL untouched

- WHEN a stored query runs by bare name (run-by-name path) without `limit`
- THEN the SQL executed is the stored query's own, with no injected LIMIT

### Requirement: Run-by-name responses are capped response-side

A stored query executed by bare name without a `limit` SHALL have its response sliced to 20 rows with a truncation notice when it returns more, and returned whole with no notice when it returns 20 or fewer.

#### Scenario: Stored query above the cap

- WHEN a run-by-name stored query returns more than 20 rows
- THEN the response contains exactly 20 rows and a truncation notice

#### Scenario: Stored query at the cap

- WHEN a run-by-name stored query returns exactly 20 rows
- THEN all 20 rows are returned and no truncation notice appears

### Requirement: query_nested capped by the root stage's limit only

The `query_nested` tool SHALL apply the same rule keyed on the ROOT stage (last entry of `queries`): an explicit root `limit` is trusted verbatim; without one, the final response is capped at 20 with a truncation notice whose hint points at the root query's `limit`. Non-root stages' limits SHALL NOT affect the cap. The tool SHALL NOT mutate the caller's `queries` dicts.

#### Scenario: Root without limit is capped

- WHEN `query_nested` runs with a root stage that has no `limit` and the final result has more than 20 rows
- THEN the response contains exactly 20 rows and a truncation notice telling the caller to set a higher `limit` on the root query

#### Scenario: Non-root limit does not lift the cap

- WHEN a non-root stage has an explicit `limit` but the root stage has none
- THEN the default cap of 20 still applies to the final response

#### Scenario: Root limit trusted

- WHEN the root stage has an explicit `limit`
- THEN no response-side truncation occurs and no notice appears

#### Scenario: Caller dicts unchanged

- WHEN `query_nested` pushes the cap into the root stage
- THEN the caller's submitted `queries` dicts are structurally unchanged afterwards (no `limit` key added)

### Requirement: Truncation notice content and rendering

The truncation notice SHALL state the returned row count, say that more rows exist, and tell the caller how to get more rows. It SHALL appear in every output format through the warnings channel: the markdown `Warnings:` block, a leading `#` comment line in csv, and a warning entry with kind `"truncated"` in the json `{"data", "warnings"}` payload. It SHALL coexist with other warnings, appended last.

#### Scenario: Markdown notice

- WHEN a truncated result is formatted as markdown
- THEN the output ends with a `Warnings:` block containing "showing first 20 rows — more rows exist" and a hint to pass a higher `limit`

#### Scenario: CSV notice

- WHEN a truncated result is formatted as csv
- THEN a leading `#` comment line carries the notice and the data rows below keep a uniform column count

#### Scenario: JSON notice

- WHEN a truncated result is formatted as json
- THEN the payload has the `{"data", "warnings"}` shape and `warnings` contains an entry with `kind == "truncated"` and the returned row count

#### Scenario: Coexists with other warnings

- WHEN a truncated result already carries an engine warning
- THEN both warnings render in every format and the truncation notice comes last

#### Scenario: Warning round-trips through the union

- WHEN a response carrying the truncation warning is serialized and re-validated
- THEN the warning deserializes back to the truncation kind with its fields intact

### Requirement: Explain plan rows capped without a limit

When `query` runs with `explain=True` and no `limit`, the returned plan rows SHALL be subject to the same 20-row cap and notice. `dry_run` output (SQL only) SHALL never carry a truncation notice.

#### Scenario: Large explain plan capped

- WHEN `explain=True` without `limit` yields a plan of more than 20 rows
- THEN 20 plan rows are returned with a truncation notice

#### Scenario: Dry run unaffected

- WHEN `dry_run=True`
- THEN the response contains only SQL and never a truncation notice
