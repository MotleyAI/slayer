## MODIFIED Requirements

### Requirement: Date operands must be temporal
Every date operand (`ts`, `start`, `end`) SHALL have a determinable DATE or TIMESTAMP type. A
temporal operand SHALL be one of: a column (local, joined or stage) declared DATE or TIMESTAMP;
`min`, `max`, `first` or `last` of a temporal value; `date_add`; a bucket `date_trunc(unit, x)` of a
temporal value (typed per › A bucket has one logical type on every dialect); `current_date()` (DATE);
`now()` (TIMESTAMP); an ISO date literal; or `coalesce`, `ifnull`, `nullif`, `greatest`, `least` or an
`iif` / `CASE` whose value arguments are all temporal or NULL (DATE mixed with TIMESTAMP is
TIMESTAMP). Any other operand — a column of another or undeclared type, arithmetic, a number, a
non-date string — SHALL be rejected with a typed query error naming the operand and suggesting a
DATE / TIMESTAMP column type. One exception: `date_part` with the part `hour`, `minute` or `second`,
in every spelling (`EXTRACT`, `DATE_PART`, `HOUR()`, `MINUTE()`, `SECOND()`), SHALL also take a
time-of-day operand (`queries/time-of-day`) and SHALL return the integer the same part of a
timestamp at that clock time returns, on every Tier-1 dialect.

#### Scenario: Text column rejected
- **WHEN** a filter uses `date_part('month', status) = 1`
- **THEN** the query is rejected with a type error naming `status`

#### Scenario: Numeric column rejected
- **WHEN** a measure uses `max(date_part('year', amount))`
- **THEN** the query is rejected with a type error naming `amount`

#### Scenario: Aggregate of a timestamp accepted
- **WHEN** a measure uses `date_diff('day', min(created_at), max(created_at))`
- **THEN** it binds and returns the day span of the orders

#### Scenario: Coalesce with the clock accepted
- **WHEN** a measure uses `avg(date_diff('day', created_at, coalesce(shipped_at, now())))`
- **THEN** it binds, treating unshipped orders as shipped now

#### Scenario: Bucket accepted
- **WHEN** a dimension uses `date_add(date_trunc('month', order_date), 1, 'month')`
- **THEN** it binds and returns the start of the following month, a DATE

#### Scenario: Sub-day parts of a time of day
- **WHEN** dimensions on DuckDB and on SQLite are `date_part('hour', shifts.starts_at)`, `EXTRACT(MINUTE FROM shifts.starts_at)` and `SECOND(TIME '10:30:45')` over a `Time` field holding 09:30:00
- **THEN** they yield 9, 30 and 45, each planned as INT
