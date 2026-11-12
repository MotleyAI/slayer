## ADDED Requirements

### Requirement: Column.sql is one expression

A column's `sql` SHALL parse as exactly one SQL expression. Text that is a statement — `SELECT …`, `WITH …`, a data-modifying or DDL statement, a command — or more than one statement SHALL be rejected when the column is constructed, with a typed error naming the column. A parenthesised scalar subquery inside an expression SHALL remain accepted.

#### Scenario: Statements are rejected at construction
- **WHEN** a column is constructed with `sql` `SELECT pw FROM secrets`, `WITH x AS (SELECT 1) SELECT * FROM x`, `amount; DROP TABLE t` or `DELETE FROM t`
- **THEN** construction fails with the typed error naming the column

#### Scenario: Expressions are accepted
- **WHEN** a column is constructed with `sql` `amount * 2`, `CASE WHEN status = 'SELECT' THEN 1 END`, `amount -- SELECT` or `(SELECT max(x) FROM t)`
- **THEN** it is constructed
