# cli/commands Specification

## Purpose
Defines the `slayer` command line's user-facing contract: how errors are reported, the version and
debug flags, the search and sample-refresh command shapes, and how query results print as a table.

## Requirements

### Requirement: CLI errors are one line on stderr
When any `slayer` command fails with an exception, the CLI SHALL print exactly one line to stderr
and exit with status 1, printing no traceback. A SLayer error SHALL print as `Error: <message>`; any
other exception SHALL print as `Error: <exception class>: <message> (rerun with --debug for the
traceback)`. Every error message a command prints SHALL go to stderr, never stdout. An interrupt or an
explicit exit SHALL keep its own behaviour.

#### Scenario: A failing query prints one line
- **WHEN** `slayer query` runs a query whose dimension no join path reaches
- **THEN** stderr holds one line starting `Error: ` with the SLayer error's message, stdout is empty
  of error text, and the exit status is 1

#### Scenario: An unexpected error names its class and the debug flag
- **WHEN** a command fails with a database driver error
- **THEN** stderr holds one line naming the exception class, its message and `--debug`, and the exit
  status is 1

### Requirement: --debug shows the traceback
Every `slayer` command SHALL accept `--debug`, written before or after the command and its
subcommand; with it, a failing command SHALL print the full traceback.

#### Scenario: --debug in any position
- **WHEN** a failing command runs as `slayer --debug query …`, as `slayer query … --debug`, or as
  `slayer datasources refresh-samples --debug`
- **THEN** each prints the full traceback

### Requirement: slayer --version
`slayer --version` SHALL print `slayer <installed package version>` and exit with status 0.

#### Scenario: Version is printed
- **WHEN** `slayer --version` runs
- **THEN** stdout is `slayer <version>`, where `<version>` is the installed `motley-slayer` version

### Requirement: search takes its question positionally
`slayer search` SHALL accept the free-text question as an optional positional argument and SHALL keep
accepting it as `--question`; giving both SHALL fail as a CLI error. A positional question equal to
`refresh-samples` SHALL fail with an error telling the user to run `slayer datasources
refresh-samples`.

#### Scenario: Positional question
- **WHEN** `slayer search revenue` runs
- **THEN** it returns the same results as `slayer search --question revenue`

#### Scenario: The moved command's old spelling fails loudly
- **WHEN** `slayer search refresh-samples` runs
- **THEN** it exits with status 1 and its error names `slayer datasources refresh-samples`

### Requirement: Sample refresh is a datasources command
`slayer datasources refresh-samples` SHALL re-profile and persist sample values for table-backed
models, optionally limited to one datasource (`--data-source`) and to named models (`--dataset`,
repeatable), printing text or, with `--format json`, a JSON result envelope. A named model that does
not resolve in scope SHALL make the command exit with status 1. It SHALL NOT write anything other
than the refreshed sample values.

#### Scenario: Refresh one model
- **WHEN** `slayer datasources refresh-samples --data-source jaffle_shop --dataset orders` runs
- **THEN** `orders`' sample values are refreshed and persisted and the command exits 0

#### Scenario: Unknown model
- **WHEN** `slayer datasources refresh-samples --data-source jaffle_shop --dataset nope --format json`
  runs
- **THEN** the JSON envelope lists `jaffle_shop.nope` as unresolved and the exit status is 1

### Requirement: Query table output applies number formats
`slayer query` table output SHALL render each numeric cell through its column's or measure's number
format when the result carries one — the same rendering as the MCP markdown output — and SHALL print
other cells as their plain value. `--format json` output SHALL carry the raw values.

#### Scenario: Currency measure in table and JSON output
- **WHEN** a query returns a measure whose format is currency with precision 2 and value `193647.33`
- **THEN** the table cell shows `$193.65K` and the JSON output carries the unformatted value

### Requirement: export-osi writes an Ossie document
`slayer export-osi --datasource X --out <file>` SHALL write the datasource's semantic model as one Ossie document to `<file>` (`importers/osi-export`), as JSON when the file name ends in `.json` and as YAML otherwise, and SHALL print the export report. `--datasets` SHALL limit the export to the named datasets, and `--no-extensions` SHALL omit the `SLAYER` extensions. An unknown datasource or an unknown dataset name SHALL fail as a CLI error and write no file.

#### Scenario: Export to YAML
- **WHEN** `slayer export-osi --datasource jaffle_shop --out shop.osi.yaml` runs
- **THEN** `shop.osi.yaml` holds a YAML Ossie document of the datasource's semantic model, the report is printed, and the exit status is 0

#### Scenario: Unknown model
- **WHEN** `slayer export-osi --datasource jaffle_shop --datasets nope --out shop.osi.yaml` runs
- **THEN** stderr holds one `Error: ` line naming `nope`, no file is written, and the exit status is 1

### Requirement: datasources create --ingest runs the idempotent ingest
`slayer datasources create <url> --ingest` SHALL ingest the new datasource exactly as `slayer ingest` would with the same scope flags (`--schema`, `--all-schemas`, `--include`, `--exclude`, `--no-views`, `--surface-internals`): it SHALL create missing datasets and additively merge existing ones, never overwriting a stored dataset's user metadata, and SHALL NOT ask before touching stored datasets. It SHALL print the result as `slayer ingest` does (`Created:` / `Updated:` lines, then the description, drift, skipped, skipped-schema, key-read-failure, hidden, broken-dependent and error sections). Once the datasource is saved, it SHALL exit 0 whatever the ingest reports — skipped objects, per-dataset errors, or nothing ingested. An exception raised by the ingest SHALL print one `Ingestion failed: <message>` error and exit 1, the datasource staying created.

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

### Requirement: The empty-ingest message needs an empty result
`slayer ingest` and `slayer datasources create --ingest` SHALL print the "No tables or views found" message only when the ingest reports nothing at all: no live objects, no dataset changes, no drift, no skipped objects or schemas, no failed key reads, no hidden internals, no broken dependents, no errors and no imported description. Otherwise they SHALL print the result's sections. `slayer ingest` SHALL keep exiting 1 on an empty result or on any skip or error.

#### Scenario: Every requested schema skipped
- **WHEN** `slayer ingest --datasource ds --schema missing` runs and the requested schema is skipped with a reason
- **THEN** the output lists `missing` under `Skipped schemas` with its reason, and does not say "No tables or views found"

#### Scenario: Truly empty
- **WHEN** `slayer ingest` runs against a schema with no tables or views
- **THEN** the output says "No tables or views found" and the exit status is 1

### Requirement: semantic-models lists, shows and deletes documents
`slayer semantic-models list [--datasource X]` SHALL print each stored document's datasource, name and datasets; `slayer semantic-models show <name> --datasource X` SHALL print the document's stored text; `slayer semantic-models delete <name> --datasource X` SHALL delete the document and its datasets after a confirmation prompt that `--yes` skips. An unknown document SHALL fail as a CLI error.

#### Scenario: Show prints the stored text
- **WHEN** `slayer semantic-models show sales --datasource shop` runs
- **THEN** stdout holds the stored text of document `sales` and the exit status is 0

#### Scenario: Unknown document
- **WHEN** `slayer semantic-models delete nope --datasource shop --yes` runs
- **THEN** stderr holds one `Error: ` line naming `nope` and the exit status is 1

### Requirement: ossie-roundtrip imports and exports one document
`slayer ossie-roundtrip <in> <out>` SHALL import the Ossie document `<in>` natively into scratch storage under a scratch datasource, export that datasource to `<out>` (JSON when `<out>` ends in `.json`, YAML otherwise), leave the user's storage untouched, and exit non-zero with the import's reasons when `<in>` is refused.

#### Scenario: A valid document round-trips unchanged
- **WHEN** `slayer ossie-roundtrip m4_cross_dataset.ossie.yaml out.yaml` runs
- **THEN** `out.yaml` equals the input text, the configured storage is unchanged, and the exit status is 0

### Requirement: slayer query takes a conformance
`slayer query` SHALL accept `--conformance` with the values `slayer` (the default) and `ossie-l3`, and SHALL run the query under that conformance exactly as the engine does (`queries/ossie-l3-binding`). A strict refusal SHALL print as the command's one error line, starting with `OssieQueryError: <code>`; any other value SHALL be refused by the argument parser.

#### Scenario: A strict refusal through the CLI
- **WHEN** `slayer query '{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue", "tickets.ticket_count"]}' --conformance ossie-l3` runs on the Appendix B retail model
- **THEN** it exits non-zero with one stderr line starting `OssieQueryError: E_PARTIAL_DIMENSION_REACHABILITY`, naming `customers.region` and `tickets.ticket_count`

#### Scenario: Output names through the CLI
- **WHEN** `slayer query '{"dimensions": ["customers.region"], "metrics": ["orders.total_revenue"]}' --conformance ossie-l3` runs on the same model
- **THEN** the output's columns are `region` and `total_revenue`

### Requirement: The CLI prints each deprecation once
Within one `slayer` command, each deprecation SHALL reach the operator as exactly one line. A deprecation shown as its `Deprecated: …` line SHALL NOT be printed again from the command's response warnings, and identical deprecation lines from the response warnings SHALL print once. A deprecation carried only on the response — because it emits no Python warning, or because the Python warning was filtered out or already shown earlier in the process — SHALL still print once. A deprecation's Python warning and its structured response warning SHALL carry the same text. This SHALL hold for the lines `slayer query`, `slayer memory save` and `slayer search` (text output) print; warnings that are not deprecations SHALL print as before, and structured JSON output and `slayer inspect`'s rendered document SHALL keep every warning they carry.

#### Scenario: A query with several deprecated forms prints each once
- **WHEN** `slayer query` runs a query using `source_model`, `measures`, `filters`, `to_many_handling: "broadcast"` and the `==` comparison
- **THEN** stderr carries exactly one line per deprecation

#### Scenario: A deprecation carried only on the response prints once
- **WHEN** `slayer query` runs by name a saved query stored with `to_many_handling: "broadcast"` on two stages
- **THEN** stderr carries the broadcast deprecation exactly once

#### Scenario: memory save prints each deprecation once
- **WHEN** `slayer memory save --query` is given a query using `source_model` and `measures`
- **THEN** the output carries each deprecation exactly once

#### Scenario: search prints a renamed label once
- **WHEN** `slayer search` runs with a `--cypher-filter` using the pre-2.0 label `:Model`
- **THEN** the text output carries the label's deprecation exactly once

#### Scenario: search JSON keeps its warnings
- **WHEN** `slayer search --format json` runs with a `--cypher-filter` using `:Model`
- **THEN** the JSON response's `warnings` carries the label's deprecation

#### Scenario: Filtered Python warnings still leave one line
- **WHEN** a deprecated query runs with Python's warnings filtered out, or runs a second time in the same process
- **THEN** each deprecation still prints exactly once

#### Scenario: A warning that is not a deprecation still prints
- **WHEN** a query's response carries a warning that is not a deprecation
- **THEN** the CLI prints it

#### Scenario: A spelling deprecation has one wording
- **WHEN** a query uses a deprecated Mode-B spelling
- **THEN** its Python warning's text equals its structured response warning's text
