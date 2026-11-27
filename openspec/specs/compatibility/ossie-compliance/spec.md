# compatibility/ossie-compliance Specification

## Purpose
The Ossie compliance gate: which upstream compliance suites SLayer claims, on which databases they run, how every result
cell is declared and judged, how the gate follows upstream `main`, and what the compliance page may claim from it.

## Requirements

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

### Requirement: Every cell has one declared state
Each (Layer 1 row, database) and (wide-table case, database) cell SHALL be declared in exactly one state: **pass** (Ossie's value, or Ossie's rows or error code); **refused** (a typed SLayer error, asserted by class and listed on the compliance page); or **gap** (a strict expected failure naming the issue that will add the construct, asserted by error class). A refused state SHALL be allowed for a RECOMMENDED row, and for a REQUIRED row only where the database has no translation for the construct. A value other than Ossie's SHALL fail the gate in every state. The declared cells SHALL equal the discovered cells: a discovered row or case with no declaration, and a declaration naming no discovered row or case, SHALL each fail the gate. A REQUIRED construct whose cells are not all pass SHALL NOT be declared pass on one database and refused on another unless that database has no translation.

#### Scenario: A gap flips when its construct lands
- **WHEN** a construct declared gap starts returning Ossie's value on a database
- **THEN** its strict expected failure fails the gate until the declaration says pass

#### Scenario: A wrong value is never a refusal
- **WHEN** a cell declared refused returns a value instead of raising its typed error
- **THEN** the gate fails

#### Scenario: A stale declaration fails
- **WHEN** a declaration names a wide-table case that the corpus no longer holds
- **THEN** the gate fails naming the declaration

### Requirement: The Layer 1 table covers the spec in both directions
Every construct the spec as read lists under a heading marked REQUIRED or RECOMMENDED, at any heading depth — every table row, every listed syntax form, every supported date part, cast target and frame clause — SHALL have a row, and every row SHALL name a construct the spec lists. Each row's argument counts SHALL match the spec's syntax. A line inside a REQUIRED or RECOMMENDED section that the check cannot classify SHALL fail the check rather than be skipped. Sections marked EXPERIMENTAL SHALL have no rows.

#### Scenario: A new upstream construct fails the check
- **WHEN** the spec as read gains a row in a REQUIRED table that no Layer 1 row names
- **THEN** the completeness check fails naming the construct

#### Scenario: A RECOMMENDED construct has a row
- **WHEN** the completeness check reads section "Trigonometric (RECOMMENDED)"
- **THEN** `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `RADIANS`, `DEGREES` and `PI` each have a row

#### Scenario: An unrecognised line fails closed
- **WHEN** a REQUIRED section of the spec as read holds a line that is neither a table row, a syntax listing, a list item nor known prose
- **THEN** the completeness check fails naming the line

### Requirement: Results compare by logical type
A cell's value SHALL be compared to Ossie's by its logical type: an exact number exactly, with no float conversion; a boolean as a boolean, never equal to 1 or 0; a date, timestamp or time of day at the precision the row states; NULL only to NULL; a floating-point value within a stated tolerance only where the construct is approximate. A wide-table result SHALL compare its rows as a multiset, its header as the expected header, and its row order only where Ossie defines one.

#### Scenario: A boolean is not an integer
- **WHEN** a row whose Ossie value is `TRUE` returns `1` on a database
- **THEN** the cell's value is normalised from that database's boolean representation, and a `1` from an integer-typed result fails

#### Scenario: A decimal keeps its digits
- **WHEN** a row's Ossie value is `12.35` and the database returns `12.350000000000001`
- **THEN** the cell fails

### Requirement: Current date and time rows read the database's clock
A row reading `CURRENT_DATE`, `CURRENT_TIMESTAMP` or `CURRENT_TIME` SHALL hold its value to lie within the instants captured immediately before and after the query, widened by one day to cover the database session's time zone, on every Tier-1 database.

#### Scenario: CURRENT_DATE is today
- **WHEN** the `CURRENT_DATE` row runs on Trino
- **THEN** its value is a date within one day of the dates captured around the query

### Requirement: One corpus root
Every suite of the gate SHALL read the corpus from `$SLAYER_OSSIE_CORPUS` when it is set and from the vendored snapshot otherwise, including the spec text the Layer 1 completeness check reads and every collection of documents, cases and rungs.

#### Scenario: An alternate corpus is read by every suite
- **WHEN** `$SLAYER_OSSIE_CORPUS` names a copy of the corpus holding an added construct, wide-table case and document
- **THEN** the completeness check, the wide-table suite and the corpus suites each collect the added item

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

### Requirement: The compliance page states what the gate proves
The compliance page SHALL claim full Ossie Layer 3 under `conformance="ossie-l3"` on every Tier-1 database, stating that expressions inside a query are subject to the listed Layer 1 gaps. For Layer 1 it SHALL report, per database, the counts of pass, refused and gap cells, and list every refused and gap construct. It SHALL claim full Layer 1 if and only if no REQUIRED cell is a gap or a refusal. The page SHALL name the corpus revision it was measured against. A check SHALL assert that the page's lists, counts and claims equal the gate's declarations.

#### Scenario: The full Layer 1 claim follows the gaps
- **WHEN** the last REQUIRED gap is declared pass
- **THEN** the page check fails until the page claims full Layer 1

#### Scenario: The Layer 3 claim names every database
- **WHEN** the page check reads the Layer 3 row
- **THEN** it claims conformance on all nine Tier-1 databases
