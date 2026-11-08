## ADDED Requirements

### Requirement: Value-returning conditionals share one type

A value-returning conditional SHALL be checked at plan time. The conditionals are `CASE` (searched or simple), `iif`, `coalesce`, `ifnull`, `greatest` and `least`; `nullif` is not one, since it can return only its first argument. The check SHALL cover every query position: dimensions, time dimensions, measures (including inside an aggregation, a transform or a FILTER condition), filters and ordering. It SHALL apply in a query over a model and in a query over a stage, on every dialect.

The types of every argument the conditional may return SHALL be joined:
- a NULL literal is absorbed;
- identical types pass through;
- INT and DOUBLE widen to DOUBLE;
- DATE and TIMESTAMP widen to TIMESTAMP;
- a BOOLEAN beside a number is its integer;
- any other known mix SHALL be rejected with a typed query error that names the conditional and both types and suggests a cast.

The check SHALL see each argument after the plan-time coercions, so TEXT that the rules above already read as a number counts as a number. An argument whose type cannot be determined (an untyped or `UNKNOWN` column, an unrecognised expression) SHALL be absorbed, never reported as a mismatch.

The joined type SHALL be the conditional's result type. Every measure or dimension type that is inferred rather than declared SHALL be derived from the expression after the plan-time coercions, and SHALL be reported the same in the response metadata and in a later stage's schema.

#### Scenario: A concatenation branch is TEXT
- **WHEN** a dimension on `orders` is `CASE WHEN amount > 1 THEN 'a' || 'b' ELSE amount END`, or the same with `concat('a', 'b')` or `status || 'b'` as the THEN value
- **THEN** the query fails at plan time with the typed error naming TEXT and INT, on SQLite, DuckDB and Postgres

#### Scenario: A joined-column branch
- **WHEN** a dimension on `orders` is `CASE WHEN amount > 1 THEN customers.name ELSE amount END`
- **THEN** the query fails at plan time with the typed error

#### Scenario: An arithmetic branch
- **WHEN** a dimension on `orders` is `CASE WHEN amount > 1 THEN status ELSE amount + 1 END`
- **THEN** the query fails at plan time with the typed error

#### Scenario: A conditional inside an aggregation
- **WHEN** a measure on `orders` is `max(CASE WHEN amount > 1 THEN 'a' || 'b' ELSE amount END)`
- **THEN** the query fails at plan time with the typed error

#### Scenario: Null-handling and scalar min/max mixes
- **WHEN** a dimension on `orders` is `coalesce(status, amount)`, `ifnull(status, amount)` or `greatest(order_date, amount)`
- **THEN** each query fails at plan time with the typed error naming the call

#### Scenario: Compatible mixes still pass
- **WHEN** dimensions on `orders` are `CASE WHEN amount > 1 THEN amount ELSE 0.5 END`, `coalesce(order_date, TIMESTAMP '2025-01-01 00:00:00')`, `iif(amount > 15, status = 'paid', 2)` and `CASE WHEN amount > 1 THEN status END`
- **THEN** each executes, typed DOUBLE, TIMESTAMP, INT and TEXT respectively

#### Scenario: Coerced TEXT is a number
- **WHEN** a dimension on `orders` is `greatest(status, amount)`
- **THEN** it executes with the TEXT read as a number, typed DOUBLE in the response metadata

#### Scenario: An untyped argument is not a mismatch
- **WHEN** a model column has no declared type (or type `UNKNOWN`) and a dimension is `CASE WHEN amount > 1 THEN that_column ELSE amount END`
- **THEN** the plan-time check passes it to the database unchanged

#### Scenario: Inside a stage
- **WHEN** a second stage reads a first stage's TEXT column `s` and INT column `n` and has the dimension `coalesce(s, n)`
- **THEN** the query fails at plan time with the typed error

#### Scenario: Same rejection on SQLite
- **WHEN** `coalesce(status, amount)` is queried on SQLite, which would return mixed values
- **THEN** the query fails at plan time exactly as on Postgres and DuckDB
