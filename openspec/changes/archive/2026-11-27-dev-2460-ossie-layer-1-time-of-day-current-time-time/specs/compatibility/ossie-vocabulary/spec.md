## MODIFIED Requirements

### Requirement: Datatype values are Ossie's names

A `datatype` or list element type SHALL be one of `String`, `Integer`, `Decimal`, `Float`, `Boolean`, `Date`, `Time`, `DateTime`, `DateTimeTz`, `Opaque` and `Array` (a list), matched by exact case, and SHALL be stored, dumped, exported and displayed as written. `Decimal` and `Float` SHALL behave in queries as the double type, `DateTime` and `DateTimeTz` as the timestamp type (without a time zone, `queries/time-zones`), `Time` as the time of day (`queries/time-of-day`) and `Opaque` as the opaque type. A pre-2.0 value SHALL convert: `TEXT`, `INT`, `BOOLEAN`, `DATE`, `UNKNOWN` and `ARRAY` (and their lowercase legacy spellings) to `String`, `Integer`, `Boolean`, `Date`, `Opaque` and `Array`; `DOUBLE` and `number` to `Decimal` when the field's database type is exact numeric, else `Float`; `TIMESTAMP` and lowercase `time` to `DateTimeTz` when the field's database type carries a time zone, else `DateTime`. Ingestion and drift SHALL assign the finer value from the database type by the same rule, SHALL also assign `DateTimeTz` to BigQuery `TIMESTAMP`, ClickHouse `DateTime(tz)` / `DateTime64(p, tz)` and MySQL `TIMESTAMP` columns, which hold instants, and SHALL assign `Time` to a time-of-day column without a time zone. A re-ingest SHALL re-type a stored `DateTime` field whose live column holds instants to `DateTimeTz`, SHALL re-type a stored field whose live column is a time of day to `Time`, and SHALL leave every other stored type as it is. The Ossie importer SHALL keep an Ossie datatype as written.

#### Scenario: Ossie's types are kept as written
- **WHEN** a dataset with fields typed `Decimal`, `DateTimeTz` and `Time` is saved, loaded, dumped and exported to Ossie
- **THEN** each field still carries its type as written

#### Scenario: Behaviour follows the old kind
- **WHEN** a query aggregates a `Decimal` field and filters a `DateTimeTz` field by a date range
- **THEN** the results equal those of the same dataset typed `DOUBLE` and `TIMESTAMP` before this change

#### Scenario: A pre-2.0 type converts by database type
- **WHEN** a fresh dataset has a field typed `DOUBLE` with database type `NUMERIC(18, 2)`, a field typed `TIMESTAMP` with database type `TIMESTAMPTZ`, a field typed `DOUBLE` with no database type and a metric typed `TIMESTAMP`
- **THEN** they are stored as `Decimal`, `DateTimeTz`, `Float` and `DateTime`, with one `deprecated_input` warning for `DOUBLE` and one for `TIMESTAMP`

#### Scenario: Case matters
- **WHEN** fresh datasets type a field `Time` and another `time`
- **THEN** the first is stored as `Time`, and the second as `DateTime` with a `deprecated_input` warning naming `time`

#### Scenario: A list type shows its element
- **WHEN** a list field of strings is inspected
- **THEN** its type is shown as `Array<String>`

#### Scenario: Instant columns ingest as DateTimeTz
- **WHEN** a BigQuery `TIMESTAMP`, a ClickHouse `DateTime('UTC')`, a ClickHouse `DateTime64(3, 'Asia/Tokyo')` and a MySQL `TIMESTAMP` column are ingested, beside a BigQuery `DATETIME`, a ClickHouse `DateTime` and a MySQL `DATETIME`
- **THEN** the first four are `DateTimeTz` and the last three `DateTime`

#### Scenario: Re-ingest re-types an instant column
- **WHEN** a stored MySQL dataset has a `DateTime` field over a `TIMESTAMP` column and a `DateTime` field over a `DATETIME` column, and the datasource is re-ingested
- **THEN** the first becomes `DateTimeTz` and loses any `time_zone`, the second stays `DateTime`, and every other attribute of both is unchanged

#### Scenario: A Time field is a time of day
- **WHEN** a query groups by a `Time` field and takes its `min`
- **THEN** it runs and returns times of day
