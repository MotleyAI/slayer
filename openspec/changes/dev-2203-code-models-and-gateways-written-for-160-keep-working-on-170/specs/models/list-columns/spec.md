## MODIFIED Requirements

### Requirement: Re-ingest re-types TEXT list columns

When a stored column is `TEXT` and its live column is a native list, re-ingest SHALL change it to `type: ARRAY` with the live `element_type` and `db_type`, keeping every other stored attribute (description, label, meta) and keeping `allowed_aggregations` only as far as it names `count`. A stored `ARRAY` column's `element_type` SHALL NOT be overwritten, except a Snowflake `UNKNOWN` being re-inferred.

Before saving such a re-type, re-ingest SHALL check every stored definition of the datasource that depends on the re-typed column — measures, derived columns and model filters of any model reaching it, transitively through derived columns, and saved queries (query-backed models) reading it — by planning it without execution against the models as stored and as re-typed. Each definition that plans before the re-type and fails after it SHALL be reported in the re-ingest result's `broken_dependents`, naming its model, its kind (`measure`, `column`, `filter` or `saved_query`), its name, the re-typed column and the error's type and message, and SHALL be emitted as a structured warning; a definition whose check could not run SHALL be reported as unchecked with the reason. The CLI and MCP ingest output SHALL list each broken dependant. Definitions that failed before the re-type SHALL NOT be reported. The re-type SHALL be saved whether or not dependants break, and the check SHALL leave the ingest's SQL client open and usable.

#### Scenario: Re-typing keeps metadata
- **WHEN** a stored model has `skills: TEXT` with a description, the live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY`, `element_type: TEXT`, and keeps its description

#### Scenario: Re-typing keeps only the count aggregation
- **WHEN** a stored `skills: TEXT` has `allowed_aggregations: [count, count_distinct]`, its live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY` with `allowed_aggregations: [count]`

#### Scenario: A user's element type survives
- **WHEN** a stored `ARRAY` column has `element_type: TEXT` set by the user over a live `INTEGER[]` column, and the datasource is re-ingested
- **THEN** its `element_type` stays `TEXT`

#### Scenario: Dependants broken by the re-type are reported
- **WHEN** a stored model with `tags: TEXT` over a live `VARCHAR[]` column has the measure `nt` = `count_distinct(tags)`, the derived column `first_tag` = `upper(tags)`, and a saved query groups by `tags`, and the datasource is re-ingested
- **THEN** `tags` is re-typed, and `broken_dependents` names the measure `nt`, the column `first_tag` and the saved query, each with the list-type error, each also a warning, and listed by the CLI and MCP ingest output

#### Scenario: A dependant across a join
- **WHEN** a model joined to-one to the re-typed model has a measure `count_distinct(contractors.tags)`
- **THEN** that measure is reported on its own model

#### Scenario: A dependant that still works is not reported
- **WHEN** the re-typed model's measure `n_tagged` is `count(tags)`
- **THEN** `n_tagged` is not reported

#### Scenario: A definition already broken is not reported
- **WHEN** a measure referencing `tags` failed to plan before the re-type for an unrelated reason
- **THEN** it is not reported

#### Scenario: A name mention that is no reference
- **WHEN** another model's derived column is `'tags'` (a string literal) or its own column is named `tags` without reaching the re-typed model
- **THEN** it is not checked or reported

#### Scenario: The ingest's client stays usable
- **WHEN** a re-ingest through a caller's client reports broken dependants
- **THEN** that client executes a statement afterwards
