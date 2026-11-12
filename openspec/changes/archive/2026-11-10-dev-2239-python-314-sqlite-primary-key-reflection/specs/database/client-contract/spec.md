## MODIFIED Requirements

### Requirement: Reflected metadata uses SLayer types

Column metadata returned by a client SHALL carry SLayer data types, a float flag, the database's type name where it is opaque or exact-numeric, and the column comment; it SHALL NOT carry driver-library type objects. Ingestion and drift SHALL produce the same model types and drift verdicts through any conforming client as through the default client over the same database. On SQLite the default client SHALL read columns from SQLite's own catalog, scoped to the table's schema, including generated columns and excluding hidden ones, with the types SLayer gave them before this change; no SQLite column read SHALL run a statement that fails.

#### Scenario: Default reflection keeps today's types
- **WHEN** a table with integer, exact-numeric, float, text, timestamp, boolean, UUID and opaque columns is ingested through the default client
- **THEN** the model's column types, formats and recorded database types are those ingestion produced before this change

#### Scenario: SQLite columns keep their types without a failing statement
- **WHEN** a SQLite table whose columns cover the declared-type spellings SQLite's affinity rules distinguish, including an empty declared type and a generated column, is reflected through the default client
- **THEN** every column, the generated one included, has the type SLayer gave it before this change, and no statement executed while reading the columns fails

#### Scenario: SQLite missing and virtual tables
- **WHEN** the default client reflects the columns of a SQLite table that does not exist, and of a virtual table with hidden columns
- **THEN** the missing table is a not-found failure, and the virtual table's columns exclude its hidden ones

## ADDED Requirements

### Requirement: A key read lists the reads that failed

A client's key read SHALL list each of its reads — primary key, foreign keys, unique constraints, unique indexes — that failed, with the error, alongside the keys it did read. A client that reports no failed reads SHALL be treated as having none, so a client built before this change stays conforming.

#### Scenario: The default client lists a failed read
- **WHEN** the default client's primary-key read of a table fails and its other key reads succeed
- **THEN** the key read returns the foreign keys and unique keys, no primary key, and lists the primary-key read as failed with its error

#### Scenario: A client that lists nothing
- **WHEN** a gateway client's key read returns keys without listing any failed read, and a datasource is ingested through it
- **THEN** ingestion succeeds and reports no failed key read
