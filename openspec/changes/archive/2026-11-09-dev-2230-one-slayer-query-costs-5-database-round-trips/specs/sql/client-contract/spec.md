## ADDED Requirements

### Requirement: The default client refuses session and transaction statements
The default client's `execute` SHALL refuse, with a typed SQL-client error naming the statement's
leading keywords, any text in which a statement leads with a keyword that changes the connection's
session state or controls transactions: `USE`, `SET`, `RESET`, `ALTER SESSION`, `DISCARD`, `BEGIN`, `START TRANSACTION`,
`COMMIT`, `ROLLBACK`, `END`, `ABORT`, `SAVEPOINT`, `RELEASE`, `SAVE` and `XA`, in any letter case,
as the first statement or as any later statement of a multi-statement text. A refused text SHALL send nothing to the database. Queries, DDL and DML SHALL
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
- **WHEN** a default client executes `CREATE TABLE t (x INT)`, `INSERT INTO t VALUES (1)`,
  `UPDATE t SET x = 2`, `SELECT 'SET x' AS "USE"` and `-- BEGIN` followed by `SELECT 1`
- **THEN** each runs and returns its rows, or no rows for a statement that returns none
