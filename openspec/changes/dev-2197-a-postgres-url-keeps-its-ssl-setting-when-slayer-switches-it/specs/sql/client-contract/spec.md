## MODIFIED Requirements

### Requirement: Column-type probes report SLayer types and database type names

A client's column-type probe SHALL return, for every result column of a statement, a SLayer data type and the database's own name for the column's type, without fetching the statement's data beyond what type certification needs. The type SHALL be the one reflection gives the same database type: integer types INT; exact-numeric types INT at scale 0 and DOUBLE at a positive or unknown scale; floating types DOUBLE; character types TEXT; boolean types BOOLEAN; date types DATE; timestamp types TIMESTAMP; container types and unrecognised types as reflection maps them; and only the types reflection treats as opaque UNKNOWN. A probe SHALL NOT report INT for a column it has not proven integer: on a database whose cursor reports no types, a number is INT only when every value in a sample of at most 100 000 rows is an integer and the sample is not saturated, and DOUBLE otherwise. The database type name SHALL be present whenever the driver names the type or reports a code SLayer can name, carrying precision and scale for an exact-numeric type when the database reports them, and SHALL be absent only when the driver supplies no type name. The probe's answer for a datasource SHALL NOT depend on whether it was requested from synchronous or asynchronous code.

#### Scenario: Integer results are INT
- **WHEN** a statement selecting an integer column, the length of a text column, and a count is probed on SQLite, DuckDB and Postgres
- **THEN** every column is INT, and on DuckDB and Postgres each carries the database's integer type name

#### Scenario: Exact numerics are typed by scale
- **WHEN** a statement selecting a `NUMERIC(10,0)` and a `NUMERIC(10,2)` column is probed on DuckDB and on Postgres through its default driver
- **THEN** the first is INT and the second DOUBLE, and each type name carries its precision and scale

#### Scenario: Floats stay DOUBLE
- **WHEN** a statement selecting a double-precision column and an average is probed
- **THEN** both columns are DOUBLE

#### Scenario: Postgres answers the same from either path
- **WHEN** the same Postgres statement with integer, `NUMERIC(12,0)` and `NUMERIC(12,3)` columns is probed from asynchronous code and through the synchronous entry point
- **THEN** both answers are identical, including the type names

#### Scenario: SQLite certifies integers from the data
- **WHEN** SQLite statements are probed whose numeric column holds only integers, mixes integers and reals, holds more than 100 000 integer rows, holds integer-looking text, or returns no rows
- **THEN** the column is INT, DOUBLE, DOUBLE, TEXT and TEXT respectively, and no column carries a type name

#### Scenario: Containers are not integers
- **WHEN** a DuckDB statement selecting an `INTEGER[]` list and a struct with an integer field is probed
- **THEN** neither column is INT, and each is typed as reflection types the same database type

#### Scenario: Opaque and unrecognised types
- **WHEN** a Postgres statement selecting a `json`, a `jsonb`, a `uuid` and a user-defined enum column is probed
- **THEN** the `json` column is UNKNOWN, the other three are TEXT, and only the enum column has no type name

#### Scenario: SQL Server reports declared type names
- **WHEN** a SQL Server statement selecting `int`, `decimal(10,2)`, `datetime2` and `nvarchar(50)` columns is probed
- **THEN** the columns are INT, DOUBLE, TIMESTAMP and TEXT, each carrying the declared type name

#### Scenario: SQL Server falls back when it cannot describe a statement
- **WHEN** a SQL Server statement whose result SQL Server cannot describe without running it (a temporary table) is probed
- **THEN** the columns are still typed from the driver's values, carry no type name, and any other statement error reaches the caller unchanged

#### Scenario: MySQL boolean and tiny integers
- **WHEN** a MySQL statement selecting a `BOOL`, a `TINYINT`, a `BIT(1)` and a `BIT(8)` column is probed
- **THEN** the columns are BOOLEAN, INT, BOOLEAN and TEXT

#### Scenario: asyncpg reports no exact-numeric scale
- **WHEN** a Postgres statement selecting a `NUMERIC(12,0)` and a `NUMERIC(12,3)` column is probed on a datasource whose URL names `asyncpg`
- **THEN** both columns are DOUBLE and carry the type name `numeric`
