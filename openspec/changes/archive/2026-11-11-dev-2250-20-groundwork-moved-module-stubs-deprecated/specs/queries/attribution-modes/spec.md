## REMOVED Requirements

### Requirement: The strict flag is retired
**Reason**: Fresh `strict` input is no longer refused; it converts with a deprecation until 3.0 (see "The strict flag is a deprecated alias").
**Migration**: Use `to_many_handling: "error"` in place of `strict: true`; drop `strict: false`.

## ADDED Requirements

### Requirement: The strict flag is a deprecated alias

`SlayerQuery` SHALL NOT have a `strict` field. Fresh query input carrying `strict` — no `version`, a version of 4 or later, or a version that is not an integer — SHALL be converted with a deprecation scheduled for removal in 3.0, once per query object: `strict: true` or a legacy true token becomes `to_many_handling: "error"`, and `strict: false` or a legacy false token is dropped; an explicit `to_many_handling` on the same object wins. An unrecognized `strict` token SHALL fail with a typed error naming `to_many_handling`. Stored queries saved before version 4 SHALL migrate silently by the same rules. The conversion SHALL apply to Python query objects, the REST query request, the MCP `query` tool, refinements of a saved query, and the queries of model and memory saves; a run-by-name REST request with a top-level `strict` SHALL stay rejected. Every surface SHALL expose `to_many_handling`, and none SHALL document `strict` outside the migration guide.

#### Scenario: Strict input converts with a deprecation
- **WHEN** a fresh query is submitted with `strict: true` through Python, REST and the MCP `query` tool
- **THEN** each runs under `to_many_handling: "error"` and reports one `deprecated_input` warning naming `to_many_handling`

#### Scenario: Strict false is dropped with a deprecation
- **WHEN** a fresh query carries `strict: false`, or the legacy token `"off"`
- **THEN** it runs under the default mode and reports one `deprecated_input` warning

#### Scenario: An explicit mode wins over strict
- **WHEN** a fresh query carries `strict: true` and `to_many_handling: "associate"`
- **THEN** it runs under `associate` and reports one `deprecated_input` warning

#### Scenario: An unrecognized strict token fails
- **WHEN** a fresh query, or a stored pre-version-4 query, carries `strict: "maybe"`
- **THEN** it fails with a typed error naming `to_many_handling`

#### Scenario: A malformed version is fresh input
- **WHEN** a query carrying `strict: true` has `version: 3.9`
- **THEN** it converts with a deprecation, exactly as a version-less query does

#### Scenario: Stored strict queries migrate
- **WHEN** a stored query saved with `strict: true` (and another with `strict: false`) before version 4 is loaded
- **THEN** the first loads with `to_many_handling: "error"`, the second with the default, both execute with the semantics those modes define, and neither emits a deprecation

#### Scenario: Run-by-name top-level strict stays rejected
- **WHEN** a REST run-by-name request carries a top-level `strict`
- **THEN** it is rejected like every other top-level query clause there

#### Scenario: Surfaces accept the mode parameter
- **WHEN** a query sets `to_many_handling` through the REST API or the MCP query tool
- **THEN** the mode reaches the engine and governs resolution exactly as a direct `SlayerQuery` field does
