## ADDED Requirements

### Requirement: Stored BigQuery instant fields load as DateTimeTz

On load, a stored dataset below schema version 31 whose datasource is BigQuery SHALL type each `DateTime` field whose `db_type` is `TIMESTAMP` as `DateTimeTz`; a load SHALL not write it back. Queries without a time zone SHALL return the same values and emit the same SQL before and after the upgrade.

#### Scenario: A stored BigQuery TIMESTAMP field
- **WHEN** a raw v30 BigQuery dataset with a `DateTime` field of `db_type` `TIMESTAMP` and a `DateTime` field with no `db_type` is loaded
- **THEN** the first is `DateTimeTz`, the second `DateTime`, and the stored document is unchanged

#### Scenario: No change without a zone
- **WHEN** a query without a time zone filters and buckets the upgraded field
- **THEN** its SQL and result equal those before the upgrade

#### Scenario: Other datasources untouched
- **WHEN** a raw v30 Postgres dataset with a `DateTime` field of `db_type` `TIMESTAMP` is loaded
- **THEN** the field stays `DateTime`
