## MODIFIED Requirements

### Requirement: A type mismatch on an existing column is a re-type finding

When a stored column's live column exists but its type conflicts with the live type (different type buckets, an operable stored type over an opaque live type, or an `INT` column SQLite's storage probe finds holding non-integer values), explicit validation and query-time attribution SHALL report a re-type finding and SHALL NOT delete the column or cascade from it, for table-backed and `sql`-mode models alike. Lists and scalars SHALL share one re-type finding kind (`finding: "retype"`), naming the model, data source, column, stored type, live SLayer type, the live database type name where the database reports one, the typed remedy kind, and the remedy. The remedy kind SHALL be `reingest` for a live list on a table-backed model or a SQLite column the storage probe widened, and `set_type` otherwise, including every conflict on an `sql`-mode model. The remedy SHALL agree with the remedy kind: re-ingesting the datasource for `reingest`, setting the column's type to the live type for `set_type`. A serialized re-type finding without a remedy kind SHALL still load, taking its kind from its remedy text; a remedy text that is neither form SHALL be refused. `validate_models` SHALL return it as a report-only entry, `--force-clean` SHALL NOT apply it, the CLI SHALL print it as a `RE-TYPE:` line, and REST and MCP SHALL return it in the same serialized shape. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding, under the existing read-set attribution rules. A column absent from the live table or result, a missing table, and a missing join key SHALL still be deletions and cascade as before.

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
- **THEN** validation reports a re-type finding with remedy kind `set_type`, the set-the-type remedy and no deletion

#### Scenario: A SQLite column the storage probe widens
- **WHEN** a stored SQLite `INT` column holds doubles or text, with a measure and a derived column over it
- **THEN** validation reports a re-type finding with the probe's verdict (`DOUBLE` or `TEXT`), remedy kind `reingest` and the re-ingest remedy, and the column, measure and derived column are not deleted

#### Scenario: A list keeps the re-ingest remedy
- **WHEN** a stored `skills: TEXT` column of a table-backed model reads a live `VARCHAR[]` column
- **THEN** the re-type finding has kind `retype`, the live list type, remedy kind `reingest` and the re-ingest remedy

#### Scenario: A list in an sql-mode model is a type change
- **WHEN** an `sql`-mode model stores `skills: TEXT` and its query returns a text list
- **THEN** the re-type finding has remedy kind `set_type` and the set-the-type remedy

#### Scenario: Any sql-mode conflict is a type change
- **WHEN** an `sql`-mode model stores `span: TIMESTAMP` and its query returns an `INTERVAL`
- **THEN** the re-type finding has remedy kind `set_type` and the set-the-type remedy

#### Scenario: A finding serialized without a remedy kind loads
- **WHEN** a re-type finding serialized before the remedy kind existed is loaded, with either remedy text
- **THEN** it loads with remedy kind `reingest` for the re-ingest text and `set_type` for the set-the-type text, and its remedy and summary are unchanged

#### Scenario: An unrecognized remedy text is refused
- **WHEN** a re-type finding without a remedy kind is loaded and its remedy text is neither form
- **THEN** loading fails with a validation error

#### Scenario: A missing column is still deleted
- **WHEN** a stored column's live column no longer exists
- **THEN** validation reports its deletion and the cascade, as before

#### Scenario: A query failure explained by a re-type
- **WHEN** a query reading a column with a re-type finding fails on the database
- **THEN** it raises `SchemaDriftError` carrying that finding

#### Scenario: One serialized shape
- **WHEN** the same datasource is validated through REST, MCP and the Python API
- **THEN** each returns the re-type finding with the same fields, the remedy kind included
