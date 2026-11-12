## ADDED Requirements

### Requirement: The default client sends only reads
The default client's `execute` SHALL refuse a text, with a typed SQL-client error, unless it parses
in its datasource's SQL dialect into statements that each:
- are a query, `VALUES`, `SHOW` or `DESCRIBE`;
- hold no statement that changes data or schema, and no `INTO`.

The error SHALL name the refused statement's leading keyword where the text's statements can be
told apart, and otherwise say the text cannot be classified. This SHALL hold in any letter case,
whether the statement is the first of the text or a later one, and whether or not statements are
separated by `;`.
A refused text SHALL send nothing to the database. `EXPLAIN` SHALL be refused by `execute`; a plan
is obtained through the client's explain operation. A keyword inside a string literal, a quoted
identifier or a comment SHALL NOT count.

The client's explain operation SHALL refuse, sending nothing, a plan statement that is not one of:
- the dialect's EXPLAIN keywords before a statement `execute` would admit;
- for a dialect whose plan mode is session state, a statement `execute` would admit.

On a datasource whose connection is an in-memory SQLite or DuckDB database, statements that change
data SHALL be admitted. Session and transaction statements SHALL still be refused there. Side
effects of a function or procedure called inside an admitted statement are not detected.

#### Scenario: Writes are refused unsent
- **WHEN** the default client of a file-backed DuckDB, a file-backed SQLite and a Postgres datasource
  executes `CREATE TABLE t (x INT)`, `INSERT INTO t VALUES (1)`, `UPDATE t SET x = 2`,
  `DELETE FROM t`, `DROP TABLE t`, `TRUNCATE t`, `CALL p()`, `PRAGMA query_only = OFF`,
  `SELECT 1 AS x INTO t2` and `WITH d AS (DELETE FROM t RETURNING x) SELECT x FROM d`
- **THEN** each is refused with the typed SQL-client error, and nothing reaches the database

#### Scenario: EXPLAIN through execute is refused
- **WHEN** a default client executes `EXPLAIN SELECT 1` and `EXPLAIN ANALYZE CREATE TABLE t AS SELECT 1`
- **THEN** each is refused with the typed SQL-client error and nothing is sent

#### Scenario: A trailing write is refused
- **WHEN** a default client executes `SELECT 1; DELETE FROM t`
- **THEN** the text is refused with the typed SQL-client error and nothing is sent

#### Scenario: Statements without separators are refused
- **WHEN** a SQL Server datasource's default client executes `SELECT 1 AS x TRUNCATE TABLE t`,
  `SELECT 1 AS x DROP TABLE t` and `SELECT 1 AS x DBCC SHRINKDATABASE(0)`
- **THEN** each is refused with the typed SQL-client error and nothing is sent

#### Scenario: Write keywords in literals and comments do not count
- **WHEN** a default client executes `SELECT 'DELETE' AS "INSERT"`, and `-- DROP TABLE t` followed
  by `SELECT 1`
- **THEN** each runs and returns its rows

#### Scenario: Explain refuses a plan of a write
- **WHEN** a Postgres datasource's default client is asked to explain the plan statement
  `EXPLAIN ANALYZE INSERT INTO t VALUES (1)`, and a SQL Server datasource's to explain the
  session-mode plan statement `DELETE FROM t`
- **THEN** each is refused with the typed SQL-client error and nothing is sent, not even the
  switch-on statement

#### Scenario: An in-memory database accepts writes
- **WHEN** the default client of an in-memory SQLite and of an in-memory DuckDB datasource executes
  `CREATE TABLE t (x INT)`, `INSERT INTO t VALUES (1)` and then `SELECT x FROM t`
- **THEN** the select returns the inserted row, and `SET x = 1` is still refused there

## MODIFIED Requirements

### Requirement: The default client refuses session and transaction statements
The default client's `execute` SHALL refuse, with a typed SQL-client error naming the statement's
leading keywords, any text in which a statement leads with a keyword that changes the connection's
session state or controls transactions: `USE`, `SET`, `RESET`, `ALTER SESSION`, `DISCARD`, `BEGIN`, `START TRANSACTION`,
`COMMIT`, `ROLLBACK`, `END`, `ABORT`, `SAVEPOINT`, `RELEASE`, `SAVE` and `XA`, in any letter case,
as the first statement or as any later statement of a multi-statement text. A refused text SHALL send nothing to the database. Queries SHALL
be unaffected, including when a refused keyword appears inside a string literal, a quoted identifier,
a comment, or later in a statement that leads with another keyword.
Session state changed through a function or procedure call (for example Postgres `set_config`) is
not detected.

#### Scenario: A Snowflake USE is refused
- **WHEN** a Snowflake datasource's default client executes `USE WAREHOUSE other_wh`
- **THEN** a typed SQL-client error is raised, nothing is sent, and the next query runs on the
  configured warehouse

#### Scenario: A session setting is refused
- **WHEN** a Postgres datasource's default client executes `SET statement_timeout = 0`,
  `reset statement_timeout` or `SET ROLE analyst`
- **THEN** each is refused with a typed SQL-client error and nothing is sent

#### Scenario: Transaction control is refused
- **WHEN** a default client executes `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `END`,
  `ABORT`, `SAVEPOINT a`, `RELEASE SAVEPOINT a`, `SAVE TRANSACTION a` or `XA START 'x'`
- **THEN** each is refused with a typed SQL-client error and nothing is sent

#### Scenario: A trailing session statement is refused
- **WHEN** a Postgres datasource's default client executes `SELECT 1; SET statement_timeout = 0`
- **THEN** the text is refused with a typed SQL-client error and nothing is sent

#### Scenario: Ordinary statements are unaffected
- **WHEN** a default client executes `SELECT 'SET x' AS "USE"`, `-- BEGIN` followed by `SELECT 1`,
  and `WITH a AS (SELECT 1 AS x) SELECT x FROM a`
- **THEN** each runs and returns its rows

### Requirement: Client failures are typed

A client SHALL report an unreachable database as an unavailable failure, rejected credentials as an authentication failure, and a metadata read of a schema or table that does not exist as a not-found failure. Any other statement failure SHALL reach the caller as the database's own error with its message unchanged. Schema drift SHALL treat an object as deleted only when it is absent from a successful listing of its schema; a failure reading a listed object, including a not-found failure, SHALL yield no verdict for it. A row-level-security column check that cannot be confirmed SHALL refuse the query.

#### Scenario: An unreachable client refuses validation
- **WHEN** models are validated through a client that reports its database unavailable
- **THEN** validation fails with an unavailable error and reports no model as deleted

#### Scenario: A table missing from the listing is drift
- **WHEN** a model's table is absent from a successful listing of its schema
- **THEN** validation reports the model's table as deleted

#### Scenario: A listed but unreadable table yields no verdict
- **WHEN** a model's table appears in the listing but reading its columns fails with a not-found failure, a permission error, or an unsupported-reflection error
- **THEN** validation reports no drift verdict for that model

#### Scenario: An unconfirmable policy column refuses the query
- **WHEN** a query runs under a column-scoped session policy and the client reports its database unavailable while checking the column
- **THEN** the query is refused

#### Scenario: A database error reaches the caller unchanged
- **WHEN** a statement the client admits fails in the database, such as a read of a table that does not exist
- **THEN** the caller receives that error with the database's message unchanged

#### Scenario: Rejected credentials are reported as such
- **WHEN** the default client's database rejects its credentials
- **THEN** the client raises an authentication failure and discards its cached connections
