## ADDED Requirements

### Requirement: A 1.6.0 client's category probe keeps working and warns

A client whose only type probe is the 1.6.0 `get_column_types` — one without `get_result_column_types`, or a `DelegatingSqlClient` subclass overriding `get_column_types` but not `get_result_column_types` — SHALL be probed through its `get_column_types` wherever SLayer takes a client, reading `number` as DOUBLE, `string` as TEXT, `time` as TIMESTAMP, `boolean` as BOOLEAN and any other category as TEXT, with no type name, and each such probe SHALL emit a deprecation warning naming `get_result_column_types` and SLayer 2.0. A client serving `get_result_column_types` SHALL be used unchanged. `DelegatingSqlClient.get_column_types` SHALL keep returning the categories of the wrapped client's typed probe, with a deprecation warning naming `get_result_column_types`.

#### Scenario: A 1.6.0 client built from scratch
- **WHEN** an engine's client factory returns a client implementing the 1.6.0 contract and a SQL-backed model's column types are probed
- **THEN** the probe runs through the client's `get_column_types`, a numeric column is DOUBLE, and a deprecation warning names `get_result_column_types`

#### Scenario: A 1.6.0 gateway overriding the probe
- **WHEN** the factory returns a `DelegatingSqlClient` subclass overriding only `get_column_types` and a column type is probed
- **THEN** the probe runs through that override, not the wrapped client, and warns

#### Scenario: OSI import through a 1.6.0 client
- **WHEN** OSI `collect_metadata` is given a 1.6.0 client and a derived field `amount * 2` needs a type
- **THEN** the field is DOUBLE, probed through the client's `get_column_types`, with a deprecation warning

#### Scenario: Drift validation through a 1.6.0 client
- **WHEN** `validate_datasource` is given a 1.6.0 client and validates a SQL-backed model
- **THEN** the model's SQL is probed through the client's `get_column_types`, with a deprecation warning

#### Scenario: A 1.6.0 gateway deferring to the base probe
- **WHEN** a `DelegatingSqlClient` subclass's `get_column_types` returns `super().get_column_types(sql)`
- **THEN** it receives `number` for an integer column and `string` for a text column, with a deprecation warning

#### Scenario: A current client
- **WHEN** the client is the default client, a plain `DelegatingSqlClient`, or a subclass overriding `get_result_column_types`
- **THEN** SLayer uses it unchanged

#### Scenario: The conformance suite checks the current contract
- **WHEN** `run_conformance` is given a client serving only the 1.6.0 `get_column_types`
- **THEN** its `get_result_column_types` check fails

## MODIFIED Requirements

### Requirement: The 1.5.0 category probes keep working and warn

The deprecated category probes — the synchronous entry point over an engine, the default client's probe, the query engine's per-model probe and `DelegatingSqlClient.get_column_types` — SHALL keep returning one of `number`, `string`, `time` or `boolean` per column, and SHALL emit a deprecation warning naming the typed replacement. Each column's category SHALL be the one SLayer 1.6.0 returned for the same database type on the same database: each dialect declares its 1.6.0 classification of database type names, and a column whose type the dialect does not classify falls back to its SLayer type (INT and DOUBLE as `number`; TEXT, UNKNOWN and ARRAY as `string`; DATE and TIMESTAMP as `time`; BOOLEAN as `boolean`). A column whose type the database does not report (no type code: a value-typed result, SQLite's storage census) SHALL take its SLayer type's category.

#### Scenario: A 1.5.0 caller
- **WHEN** code written for 1.5.0 probes a statement with an integer and a text column through the synchronous entry point
- **THEN** it receives `number` and `string` and a deprecation warning naming the typed probe

#### Scenario: Types whose classification was refined since 1.6.0
- **WHEN** a deprecated category probe reads a Postgres or DuckDB `interval` column, a DuckDB `time with time zone` column, or a MySQL `YEAR` or `BIT` column
- **THEN** it returns `time` for the interval, time-with-time-zone and `YEAR` columns and `number` for the `BIT` column, as 1.6.0 did

#### Scenario: Parity with every 1.6.0 classification
- **WHEN** each cursor description 1.6.0 classified — every Postgres OID, MySQL field type, ODBC SQL type and Snowflake field type in its tables, DuckDB type names and an unknown code — is probed through a deprecated category probe
- **THEN** each returns the category 1.6.0 returned for it

#### Scenario: Untyped columns take their SLayer type's category
- **WHEN** a deprecated category probe reads a result column the database reports no type for (a SQLite column, an absent type code)
- **THEN** it returns the category of that column's typed-probe SLayer type
