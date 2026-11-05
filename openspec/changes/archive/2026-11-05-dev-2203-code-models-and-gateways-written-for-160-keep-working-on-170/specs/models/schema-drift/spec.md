## ADDED Requirements

### Requirement: A type mismatch on an existing column is a re-type finding

When a stored column's live column exists but its type conflicts with the live type (different type buckets, an operable stored type over an opaque live type, or an `INT` column SQLite's storage probe finds holding non-integer values), explicit validation and query-time attribution SHALL report a re-type finding and SHALL NOT delete the column or cascade from it, for table-backed and `sql`-mode models alike. Lists and scalars SHALL share one re-type finding kind (`finding: "retype"`), naming the model, data source, column, stored type, live SLayer type, the live database type name where the database reports one, and the remedy: re-ingesting for a live list or a SQLite column the storage probe widened, otherwise setting the column's type to the live type. `validate_models` SHALL return it as a report-only entry, `--force-clean` SHALL NOT apply it, the CLI SHALL print it as a `RE-TYPE:` line, and REST and MCP SHALL return it in the same serialized shape. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding, under the existing read-set attribution rules. A column absent from the live table or result, a missing table, and a missing join key SHALL still be deletions and cascade as before.

#### Scenario: A column typed by 1.6.0 reflection
- **WHEN** a table-backed model ingested by 1.6.0 stores `duration: TIMESTAMP` over a live Postgres `interval` column and `kind: TEXT` over a live MySQL `YEAR` column, and `validate_models` runs
- **THEN** it reports a re-type finding for each, naming the live types, and no deletion

#### Scenario: A column typed by the 1.6.0 probe
- **WHEN** an `sql`-mode model stores `payload: TEXT` over a live Postgres `json` result column and `validate_models` runs
- **THEN** it reports a re-type finding naming `UNKNOWN` and `json`, and no deletion

#### Scenario: Force-clean keeps the column
- **WHEN** `slayer validate-models --force-clean` runs on that datasource
- **THEN** every such column and the measures over it are still stored, and the output carries a `RE-TYPE:` line per column

#### Scenario: A genuinely retyped column
- **WHEN** a stored `code: TEXT` column's live column became `INTEGER`
- **THEN** validation reports a re-type finding with the set-the-type remedy and no deletion

#### Scenario: A SQLite column the storage probe widens
- **WHEN** a stored SQLite `INT` column holds doubles or text, with a measure and a derived column over it
- **THEN** validation reports a re-type finding with the probe's verdict (`DOUBLE` or `TEXT`) and the re-ingest remedy, and the column, measure and derived column are not deleted

#### Scenario: A list keeps the re-ingest remedy
- **WHEN** a stored `skills: TEXT` column's live column is `VARCHAR[]`
- **THEN** the re-type finding has kind `retype`, the live list type and the re-ingest remedy

#### Scenario: A missing column is still deleted
- **WHEN** a stored column's live column no longer exists
- **THEN** validation reports its deletion and the cascade, as before

#### Scenario: A query failure explained by a re-type
- **WHEN** a query reading a column with a re-type finding fails on the database
- **THEN** it raises `SchemaDriftError` carrying that finding

#### Scenario: One serialized shape
- **WHEN** the same datasource is validated through REST, MCP and the Python API
- **THEN** each returns the re-type finding with the same fields
