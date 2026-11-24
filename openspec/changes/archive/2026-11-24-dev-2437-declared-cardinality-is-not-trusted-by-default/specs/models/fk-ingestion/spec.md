## ADDED Requirements

### Requirement: Ingestion records enforced keys
Ingestion and re-ingestion SHALL record each reflected primary key and unconditional unique
key set of a table as an enforced key of its dataset iff the datasource's dialect enforces
unique keys. Postgres, MySQL, SQLite, SQL Server and DuckDB enforce them; Snowflake,
BigQuery, Redshift, Databricks, ClickHouse and Trino do not, and every supported dialect
SHALL state which. Partial and expression unique indexes are never recorded. Re-ingestion
SHALL add a missing enforced key and never remove one. Importers (Ossie documents, Cube,
dbt) SHALL NOT record enforced keys or set `trusted`.

#### Scenario: A DuckDB primary key is enforced
- **WHEN** a DuckDB table `customers` with `PRIMARY KEY (customer_id)` is ingested
- **THEN** the `customers` dataset records `[customer_id]` as an enforced key and a join onto
  it from `orders` is trusted

#### Scenario: A Snowflake primary key is not enforced
- **WHEN** a Snowflake table with a declared primary key is ingested
- **THEN** the dataset records the primary key and no enforced key

#### Scenario: A partial unique index is not enforced
- **WHEN** a Postgres table carries `UNIQUE (customer_id) WHERE active` and no other key
- **THEN** no enforced key is recorded

#### Scenario: Re-ingestion fills an enforced key
- **WHEN** a dataset stored without enforced keys is re-ingested from a DuckDB table with a
  primary key
- **THEN** the primary key is recorded as enforced and the change is reported as a metadata fill
