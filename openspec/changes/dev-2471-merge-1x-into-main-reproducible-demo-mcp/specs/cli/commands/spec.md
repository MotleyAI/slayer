## MODIFIED Requirements

### Requirement: CLI errors are one line on stderr
When any `slayer` command fails with an exception, the CLI SHALL print exactly one line to stderr
and exit with status 1, printing no traceback. A SLayer error SHALL print as `Error: <message>`. A
validation error SHALL print as `Error: <count> validation error(s) for <model>: <location>: <reason>`,
listing every failing field as `<location>: <reason>` separated by `; ` — the location's parts joined by
dots, list positions included — up to ten fields followed by `…and <n> more` when there are more; it SHALL
NOT echo the rejected input values. Any other exception SHALL print as `Error: <exception class>:
<message> (rerun with --debug for the traceback)`. Every error message a command prints SHALL go to
stderr, never stdout. An interrupt or an explicit exit SHALL keep its own behaviour.

#### Scenario: A failing query prints one line
- **WHEN** `slayer query` runs a query whose dimension no join path reaches
- **THEN** stderr holds one line starting `Error: ` with the SLayer error's message, stdout is empty
  of error text, and the exit status is 1

#### Scenario: An unexpected error names its class and the debug flag
- **WHEN** a command fails with a database driver error
- **THEN** stderr holds one line naming the exception class, its message and `--debug`, and the exit
  status is 1

#### Scenario: A validation error names its fields
- **WHEN** `slayer query '{"populaton": "orders", "metricz": ["count(*)"]}'` runs
- **THEN** stderr holds one line naming `populaton` and `metricz`, each with the reason it failed, and
  the exit status is 1

#### Scenario: A nested validation error names its path
- **WHEN** a query fails validation inside its second metric
- **THEN** the error line names the failing field by its dotted path including the list position

#### Scenario: Rejected values are not echoed
- **WHEN** a query fails validation on a field whose value is a secret-looking string
- **THEN** the error line names the field and the reason but not the value

### Requirement: datasources create --ingest runs the idempotent ingest
`slayer datasources create <url> --ingest` SHALL ingest the new datasource exactly as `slayer ingest` would with the same scope flags (`--schema`, `--all-schemas`, `--include`, `--exclude`, `--no-views`, `--surface-internals`): it SHALL create missing datasets and additively merge existing ones, never overwriting a stored dataset's user metadata, and SHALL NOT ask before touching stored datasets. It SHALL print the result as `slayer ingest` does (`Created:` / `Updated:` lines, then the description, drift, skipped, skipped-schema, key-read-failure, hidden, broken-dependent and error sections). Once the datasource is saved, it SHALL exit 0 whatever the ingest reports — skipped objects, per-dataset errors, or nothing ingested. An exception raised by the ingest SHALL print one `Ingestion failed: <message>` error and exit 1, the datasource staying created. `slayer datasources create demo --ingest` SHALL print its ingest result the same way and follow the same exit rules.

#### Scenario: Fresh create
- **WHEN** `slayer datasources create sqlite:///app.db --ingest` runs against a database with tables `a` and `b`
- **THEN** datasets `a` and `b` are stored, the output has `Created: a` and `Created: b` lines, and the exit status is 0

#### Scenario: Re-create keeps dataset edits
- **WHEN** dataset `a` of datasource `app` is stored with a user-written description, and `slayer datasources create sqlite:///app.db --name app --ingest --yes` runs again
- **THEN** dataset `a` keeps its user-written description, no prompt mentions overwriting datasets, and the exit status is 0

#### Scenario: Skipped object
- **WHEN** `slayer datasources create <url> --ingest` runs and one object cannot be modelled
- **THEN** the output lists it under `Skipped`, and the exit status is 0

#### Scenario: Ingest raises
- **WHEN** the ingest raises after the datasource is saved
- **THEN** stderr has one `Ingestion failed:` error line, the exit status is 1, and the datasource is stored

#### Scenario: Demo create prints like any ingest
- **WHEN** `slayer datasources create demo --ingest` runs on empty storage
- **THEN** the output has a `Created: <dataset>` line for each demo dataset, no `Ingested:` line, and the exit status is 0

#### Scenario: Demo ingest output matches slayer ingest
- **WHEN** the same ingest result is printed by `slayer datasources create demo --ingest`, `slayer datasources create <url> --ingest`, `slayer ingest` and the MCP startup ingest
- **THEN** all four print the same `Created:` / `Updated:` lines and report sections
