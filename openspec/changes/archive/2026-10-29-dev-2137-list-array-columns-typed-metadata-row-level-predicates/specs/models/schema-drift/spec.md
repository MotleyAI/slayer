## ADDED Requirements

### Requirement: A TEXT column over a live list is a re-type finding

When a stored column is `TEXT` and its live column is a native list, explicit validation and query-time attribution SHALL report a re-type finding naming the model, the column, the live list type, and re-ingesting as the remedy. They SHALL NOT report it as a type mismatch to delete. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding. A stored `ARRAY` column whose live column is a native list SHALL NOT be reported as drift; this includes `sql`-mode models, whose live types are read from the query result.

#### Scenario: Stale TEXT list column
- **WHEN** `validate_models` runs on a datasource whose stored `contractors.skills` is `TEXT` and whose live column is `VARCHAR[]`
- **THEN** it reports a re-type finding for `contractors.skills` with the re-ingest remedy, and no deletion of `skills`

#### Scenario: A refused GROUP BY explained
- **WHEN** a BigQuery query groups by a stored `TEXT` column whose live column is `REPEATED STRING`, and the database refuses it
- **THEN** the query raises `SchemaDriftError` carrying the re-type finding

#### Scenario: A declared list in an sql-mode model
- **WHEN** an `sql`-mode model declares `skills` as `ARRAY` with `element_type: TEXT` and its query returns a text list
- **THEN** validation reports no drift for `skills`
