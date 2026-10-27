## Purpose

Defines the `slayer` command line's user-facing contract: how errors are reported, the version and
debug flags, the search and sample-refresh command shapes, and how query results print as a table.

## ADDED Requirements

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
models, optionally limited to one datasource (`--data-source`) and to named models (`--model`,
repeatable), printing text or, with `--format json`, a JSON result envelope. A named model that does
not resolve in scope SHALL make the command exit with status 1. It SHALL NOT write anything other
than the refreshed sample values.

#### Scenario: Refresh one model
- **WHEN** `slayer datasources refresh-samples --data-source jaffle_shop --model orders` runs
- **THEN** `orders`' sample values are refreshed and persisted and the command exits 0

#### Scenario: Unknown model
- **WHEN** `slayer datasources refresh-samples --data-source jaffle_shop --model nope --format json`
  runs
- **THEN** the JSON envelope lists `jaffle_shop.nope` as unresolved and the exit status is 1

### Requirement: Query table output applies number formats
`slayer query` table output SHALL render each numeric cell through its column's or measure's number
format when the result carries one — the same rendering as the MCP markdown output — and SHALL print
other cells as their plain value. `--format json` output SHALL carry the raw values.

#### Scenario: Currency measure in table and JSON output
- **WHEN** a query returns a measure whose format is currency with precision 2 and value `193647.33`
- **THEN** the table cell shows `$193.65K` and the JSON output carries the unformatted value
