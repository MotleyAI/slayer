## MODIFIED Requirements

### Requirement: A non-list column over a live list is a re-type finding

When a stored column's type is not `ARRAY` and its live column is a native list, explicit validation and query-time attribution SHALL report a re-type finding naming the model, the column, the live list type, and re-ingesting as the remedy. They SHALL NOT report it as a type mismatch to delete. Re-ingesting SHALL re-type such a column to the live list in place, keeping its description, label and meta, keeping among its allowed aggregations only those a list column allows, and dropping its `format` and time granularity; a second re-ingest SHALL change nothing. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding. A stored `ARRAY` column whose live column is a native list SHALL NOT be reported as drift; this includes `sql`-mode models, whose live types are read from the query result. An `sql`-mode model SHALL get the same finding and the same in-place re-type on re-ingest, each result column typed from the live schema of the tables its query reads, else from the type the database reports for the result column; a result column neither yields as a list is not detected. An `sql`-mode model whose query takes variables is exempt, since that query runs only with its variables bound.

#### Scenario: Stale TEXT list column
- **WHEN** `validate_models` runs on a datasource whose stored `contractors.skills` is `TEXT` and whose live column is `VARCHAR[]`
- **THEN** it reports a re-type finding for `contractors.skills` with the re-ingest remedy, and no deletion of `skills`

#### Scenario: A refused GROUP BY explained
- **WHEN** a BigQuery query groups by a stored `TEXT` column whose live column is `REPEATED STRING`, and the database refuses it
- **THEN** the query raises `SchemaDriftError` carrying the re-type finding

#### Scenario: A declared list in an sql-mode model
- **WHEN** an `sql`-mode model declares `skills` as `ARRAY` with `element_type: TEXT` and its query returns a text list
- **THEN** validation reports no drift for `skills`

#### Scenario: An imported DOUBLE measure column over a list
- **WHEN** `validate_models` runs on a model whose stored `scores` is `DOUBLE` and whose live column is `INTEGER[]`
- **THEN** it reports a re-type finding for `scores`, no deletion, and re-ingesting re-types `scores` to the list with only `count` allowed

#### Scenario: Re-typing drops format and granularity
- **WHEN** a stored `TIMESTAMP` column `seen` with a description, label, meta, a `format` and a time granularity reads a live `TIMESTAMP[]` column, and the datasource is re-ingested twice
- **THEN** after the first re-ingest `seen` is `type: ARRAY`, `element_type: TIMESTAMP`, with no `format` or granularity and its description, label and meta kept, and the second re-ingest changes nothing

#### Scenario: A stale column in an sql-mode model
- **WHEN** an `sql`-mode model declares `scores` as `DOUBLE` and its query selects the live `INTEGER[]` column `scores`
- **THEN** `validate_models` reports a re-type finding for `scores` with the re-ingest remedy and no deletion, a query failing on `scores` raises `SchemaDriftError` carrying it, and re-ingesting re-types `scores` to a list of `INT` with only `count` allowed
