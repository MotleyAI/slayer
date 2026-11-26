## MODIFIED Requirements

### Requirement: The gate runs every claimed suite on every Tier-1 database
The gate SHALL consist of these suites, read from the corpus root:
- the Layer 1 construct table (every REQUIRED and RECOMMENDED construct of `core-spec/expression_language.md`), executed on every Tier-1 database;
- every wide-table case script `compliance/wide_table/<case>.ost`, executed on every Tier-1 database on the data and model its script loads, under `conformance="ossie-l3"` and, for a row case, under the default conformance; a case upstream marks `skip` SHALL run like any other, and a script the gate cannot read SHALL fail the gate rather than be skipped;
- a mixed-case source column read through a relationship as a dimension, a filter and a metric input, executed on every Tier-1 database;
- every corpus document's metrics, compiled in every Tier-1 dialect;
- the expression fixtures `compliance/expressions/valid.txt` and `invalid.txt`, the portability ladder m0–m8, and the corpus load and round trip, once each.

The Tier-1 databases SHALL be SQLite, DuckDB, Postgres, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery. Every executed suite SHALL seed each database from one canonical form of its fixture: upstream's wide-table fixture SQL and the Layer 1 seed SHALL load into every Tier-1 database with the same rows, and a database that cannot be seeded SHALL fail the gate rather than skip. Ossie Layer 2 (`compliance/query`) and EXPERIMENTAL constructs SHALL NOT be part of the gate.

#### Scenario: A wide-table case runs on a server database
- **WHEN** the gate runs on MySQL
- **THEN** case `a08_model_metric_components` runs on the `retail` data and `retail_model` seeded into MySQL and returns its expected rows

#### Scenario: A case upstream skips still runs
- **WHEN** a case script holds `skip(...)` before its `executeLower`, as `a21_single_path_on_ambiguous_model` does
- **THEN** the gate runs the case and compares its result with the script's expected output

#### Scenario: An unreadable case script fails the gate
- **WHEN** a case script holds a command, table or error line the gate does not recognise
- **THEN** the gate fails naming the script and line

#### Scenario: Every database seeds every fixture
- **WHEN** the `retail`, `stores` and Layer 1 fixtures are seeded into each Tier-1 database
- **THEN** each table reads back with the same rows on every database

#### Scenario: The corpus compiles in every dialect
- **WHEN** every corpus document's metrics are compiled with a dry run on a datasource of each Tier-1 type
- **THEN** each metric yields SQL, or the typed error the corpus requirement names for it, identically in every dialect

### Requirement: The gate runs on pull requests, nightly and weekly
Pull requests SHALL run the gate's in-process suites on the vendored snapshot: the Layer 1 construct table, the wide-table cases and the mixed-case column on SQLite and DuckDB, the corpus and wide-table cases compiled in every Tier-1 dialect, the completeness check and the compliance-page check. A nightly workflow SHALL run the gate's database suites (the Layer 1 construct table, the wide-table cases and the mixed-case column) on every server Tier-1 database on the vendored snapshot. A weekly workflow SHALL fetch apache/ossie `main` and run every suite on every Tier-1 database with the fetched copy as corpus root. Each scheduled workflow SHALL fail unless every database's run passes; a run that skips a database, including for missing credentials, SHALL fail the workflow. The compliance-page check, which pins the vendored revision, SHALL NOT run against the fetched copy. Every job that runs against the shared BigQuery project, the gate's included, SHALL queue behind the project's other such jobs, never run beside one, and never be cancelled by one.

#### Scenario: A skipped database fails the weekly run
- **WHEN** the weekly run's Snowflake job skips for lack of credentials
- **THEN** the workflow fails

#### Scenario: A pull request runs no server database
- **WHEN** a pull request's CI runs the gate
- **THEN** it runs on SQLite and DuckDB and compiles in every Tier-1 dialect, and starts no database server

#### Scenario: The gate's BigQuery job queues
- **WHEN** a CI BigQuery run and another queued BigQuery run are pending while the nightly gate's BigQuery job waits
- **THEN** the gate's BigQuery job runs after the job ahead of it, and none of the three is cancelled
