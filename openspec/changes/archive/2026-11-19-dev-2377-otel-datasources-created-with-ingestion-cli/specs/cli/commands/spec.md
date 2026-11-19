## ADDED Requirements

### Requirement: datasources create --ingest runs the idempotent ingest
`slayer datasources create <url> --ingest` SHALL ingest the new datasource exactly as `slayer ingest` would with the same scope flags (`--schema`, `--all-schemas`, `--include`, `--exclude`, `--no-views`, `--surface-internals`): it SHALL create missing models and additively merge existing ones, never overwriting a stored model's user metadata, and SHALL NOT ask before touching stored models. It SHALL print the result as `slayer ingest` does (`Created:` / `Updated:` lines, then the description, drift, skipped, skipped-schema, hidden, broken-dependent and error sections). Once the datasource is saved, it SHALL exit 0 whatever the ingest reports — skipped objects, per-model errors, or nothing ingested. An exception raised by the ingest SHALL print one `Ingestion failed: <message>` error and exit 1, the datasource staying created.

#### Scenario: Fresh create
- **WHEN** `slayer datasources create sqlite:///app.db --ingest` runs against a database with tables `a` and `b`
- **THEN** models `a` and `b` are stored, the output has `Created: a` and `Created: b` lines, and the exit status is 0

#### Scenario: Re-create keeps model edits
- **WHEN** model `a` of datasource `app` is stored with a user-written description, and `slayer datasources create sqlite:///app.db --name app --ingest --yes` runs again
- **THEN** model `a` keeps its user-written description, no prompt mentions overwriting models, and the exit status is 0

#### Scenario: Skipped object
- **WHEN** `slayer datasources create <url> --ingest` runs and one object cannot be modelled
- **THEN** the output lists it under `Skipped`, and the exit status is 0

#### Scenario: Ingest raises
- **WHEN** the ingest raises after the datasource is saved
- **THEN** stderr has one `Ingestion failed:` error line, the exit status is 1, and the datasource is stored

### Requirement: The empty-ingest message needs an empty result
`slayer ingest` and `slayer datasources create --ingest` SHALL print the "No tables or views found" message only when the ingest reports nothing at all: no live objects, no model changes, no drift, no skipped objects or schemas, no hidden internals, no broken dependents, no errors and no imported description. Otherwise they SHALL print the result's sections. `slayer ingest` SHALL keep exiting 1 on an empty result or on any skip or error.

#### Scenario: Every requested schema skipped
- **WHEN** `slayer ingest --datasource ds --schema missing` runs and the requested schema is skipped with a reason
- **THEN** the output lists `missing` under `Skipped schemas` with its reason, and does not say "No tables or views found"

#### Scenario: Truly empty
- **WHEN** `slayer ingest` runs against a schema with no tables or views
- **THEN** the output says "No tables or views found" and the exit status is 1
