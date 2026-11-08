# Delta: mcp/response-row-cap — fold the query_nested row cap into the unified query tool

## REMOVED Requirements

### Requirement: query_nested capped by the root stage's limit only
**Reason**: The `query_nested` tool is deleted by this change; its multi-stage list form now lives in the single polymorphic `query` tool, so the row cap for the list form is re-specified against `query`.
**Migration**: The identical root-stage cap behavior is re-specified by "Multi-stage list form capped by the root stage's limit only" (added below), keyed on the `query` tool's list input instead of `query_nested`.

## ADDED Requirements

### Requirement: Multi-stage list form capped by the root stage's limit only

When the `query` tool is called with a list of query objects (the multi-stage form), the cap SHALL key on the ROOT stage (the last entry): an explicit root `limit` is trusted verbatim; without one, the final response is capped at 20 with a truncation notice whose hint points at the root query's `limit`. Non-root stages' limits SHALL NOT affect the cap. The tool SHALL NOT mutate the caller's submitted query dicts.

#### Scenario: Root without limit is capped

- WHEN `query` runs with a list whose root stage has no `limit` and the final result has more than 20 rows
- THEN the response contains exactly 20 rows and a truncation notice telling the caller to set a higher `limit` on the root query

#### Scenario: Non-root limit does not lift the cap

- WHEN a non-root stage has an explicit `limit` but the root stage has none
- THEN the default cap of 20 still applies to the final response

#### Scenario: Root limit trusted

- WHEN the root stage has an explicit `limit`
- THEN no response-side truncation occurs and no notice appears

#### Scenario: Caller dicts unchanged

- WHEN `query` pushes the cap into the root stage of a list
- THEN the caller's submitted query dicts are structurally unchanged afterwards (no `limit` key added)
