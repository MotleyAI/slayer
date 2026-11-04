## ADDED Requirements

### Requirement: A non-list column over a live list is a re-type finding

When a stored base column of a table-backed model is not `ARRAY` and its live column is a native list, explicit validation and query-time attribution SHALL report a re-type finding naming the model, the column, the live list type, and re-ingesting as the remedy. They SHALL NOT report it as a type mismatch to delete. Re-ingesting SHALL re-type such a column to the live list in place, keeping its description, label and meta, keeping only `count` among its allowed aggregations, and dropping its `format` and time granularity. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding. A stored `ARRAY` column whose live column is a native list SHALL NOT be reported as drift; this includes `sql`-mode models, whose live types are read from the query result and compared as lists only, never by element type.

#### Scenario: Stale TEXT list column
- **WHEN** `validate_models` runs on a datasource whose stored `contractors.skills` is `TEXT` and whose live column is `VARCHAR[]`
- **THEN** it reports a re-type finding for `contractors.skills` with the re-ingest remedy, and no deletion of `skills`

#### Scenario: A refused GROUP BY explained
- **WHEN** a BigQuery query groups by a stored `TEXT` column whose live column is `REPEATED STRING`, and the database refuses it
- **THEN** the query raises `SchemaDriftError` carrying the re-type finding

#### Scenario: An imported DOUBLE measure column over a list
- **WHEN** `validate_models` runs on a model whose stored `scores` is `DOUBLE` and whose live column is `INTEGER[]`
- **THEN** it reports a re-type finding for `scores`, no deletion, and re-ingesting re-types `scores` to the list with only `count` allowed

#### Scenario: Re-typing drops format and granularity
- **WHEN** a stored `TIMESTAMP` column `seen` with a `format` and a time granularity reads a live `TIMESTAMP[]` column, and the datasource is re-ingested twice
- **THEN** after the first re-ingest `seen` is `type: ARRAY`, `element_type: TIMESTAMP`, with no `format` or granularity and its description kept, and the second re-ingest changes nothing

#### Scenario: A declared list in an sql-mode model
- **WHEN** an `sql`-mode model declares `skills` as `ARRAY` with `element_type: TEXT` and its query returns a text list
- **THEN** validation reports no drift for `skills`

#### Scenario: A declared element type differs from the live one in an sql-mode model
- **WHEN** an `sql`-mode model declares `scores` as `ARRAY` with `element_type: TEXT` and its query returns an integer list
- **THEN** validation reports no drift for `scores`
