## ADDED Requirements

### Requirement: Stored query-backed models drop their cached backing SQL

A model SHALL NOT store the rendered SQL of its backing query. On load, a stored model below schema version 15 SHALL drop its `backing_query_sql` field. Inspect and REST SHALL NOT return backing SQL; a query-backed model's SQL SHALL be obtained by a `dry_run` query against it.

#### Scenario: A v14 query-backed model loads without backing SQL

- **WHEN** a stored v14 query-backed model holding `backing_query_sql` is loaded
- **THEN** it loads without the field, and is written back at version 15 without it

#### Scenario: Inspect shows no backing SQL

- **WHEN** a query-backed model is inspected with `show_sql=True`
- **THEN** the output has no backing-SQL section, and a `dry_run` query on the model returns its SQL
