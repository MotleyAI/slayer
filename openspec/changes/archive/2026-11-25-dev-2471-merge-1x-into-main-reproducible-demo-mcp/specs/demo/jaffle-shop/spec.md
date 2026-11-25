## ADDED Requirements

### Requirement: Demo data is deterministic
Demo generation SHALL be seeded so that two builds with the same jafgen and Faker versions, the same SLayer
seed recipe and the same number of years produce identical demo tables, apart from the shift that moves the
latest order date to today. Row counts and every value except customer names SHALL depend only on the
jafgen version, the seed recipe and the number of years. The demo SHALL never run an unseeded generator: when the bundled generator cannot be run
seeded, setup SHALL fail with an error naming jafgen. The row count that the CLI guide documents for the
demo SHALL be the count the CLI prints on a freshly built default demo.

#### Scenario: Two builds produce the same data
- **WHEN** the demo is built twice into empty storage with the default years
- **THEN** every demo table holds the same rows in both builds

#### Scenario: Generation does not depend on the working directory
- **WHEN** the demo generator runs twice from different working directories
- **THEN** every generated data file is byte-identical across the two runs

#### Scenario: The documented count matches
- **WHEN** `slayer query '{"population": "orders", "metrics": ["count(orders.*)"]}'` runs on a freshly built
  default demo
- **THEN** its output equals the expected-output block that `docs/getting-started/cli.md` shows for it

#### Scenario: An unseedable generator fails setup
- **WHEN** jafgen cannot be imported by the interpreter running slayer
- **THEN** demo setup fails with an error naming jafgen, and no demo data is generated

## MODIFIED Requirements

### Requirement: A stale demo database is rebuilt
Demo setup SHALL regenerate the demo database when its tables or columns, or their types, differ from
the bundled schema, when it carries no generator stamp, or when its stamp names a different jafgen version,
Faker version or seed recipe than the current setup would use, and SHALL NOT regenerate an up-to-date one.
The stamp SHALL also record the number of years the database was built with; setup SHALL regenerate on a
different number of years only when the years were given explicitly, and SHALL reuse an otherwise
up-to-date database of any years when they were not. The stamp SHALL be invisible to ingestion: it SHALL
never become a dataset, whatever schemas are ingested. Regeneration SHALL never leave the demo without a
database file: the old file is replaced only by a complete new one. If another process holds the file,
setup SHALL fail with a one-line error naming the file.

#### Scenario: A demo built with floating-point money is rebuilt
- **WHEN** demo setup finds an existing demo database whose `orders.order_total` is a floating-point
  column
- **THEN** the database is regenerated with the bundled schema

#### Scenario: An up-to-date demo is reused
- **WHEN** demo setup runs twice in a row
- **THEN** the second run regenerates nothing

#### Scenario: A demo without a stamp is rebuilt
- **WHEN** demo setup finds a demo database with the bundled schema but no generator stamp
- **THEN** the database is regenerated and carries the current stamp

#### Scenario: A demo stamped by another generator version is rebuilt
- **WHEN** demo setup finds a demo database whose stamp names a different jafgen or Faker version or
  seed recipe
- **THEN** the database is regenerated

#### Scenario: Explicit years rebuild a demo of other years
- **WHEN** a 2-year demo database exists and `slayer datasources create demo --years 4` runs
- **THEN** the database is regenerated with 4 years of data

#### Scenario: Implicit years reuse a demo of any years
- **WHEN** a 4-year demo database exists and `slayer mcp --demo` or `slayer datasources create demo`
  without `--years` runs
- **THEN** the 4-year database is reused

#### Scenario: The stamp never becomes a dataset
- **WHEN** `slayer ingest --datasource jaffle_shop --all-schemas` runs on a stamped demo database
- **THEN** no dataset, skipped entry or hidden entry is reported for the stamp
