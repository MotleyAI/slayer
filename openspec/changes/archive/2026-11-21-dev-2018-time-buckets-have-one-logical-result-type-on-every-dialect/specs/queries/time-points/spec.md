## MODIFIED Requirements

### Requirement: Temporal operands and time-literal typing

A temporal operand SHALL be an operand that `queries/date-functions` ("Date operands must be temporal") types DATE or TIMESTAMP — including `min`, `max`, `first` or `last` with `partition_by=` or a window, a bucket `date_trunc(unit, x)` in any position (typed per `queries/date-functions` › A bucket has one logical type on every dialect), and with an ISO string literal that is a value argument of a conditional in the operand typed as a date value when the whole operand then types temporal. Time-point semantics SHALL apply only to comparisons with a temporal operand. A relative token, or a single-string `in` / `not in`, against an operand that is not temporal SHALL fail with the date-operand type error date functions raise, naming the operand and the remedy (declare the column's type), except against an extraction, which fails per `queries/date-functions` › Extraction compared with a date-shaped literal is refused. A plain date or date-time string compared with a non-temporal operand SHALL keep its existing plain-literal meaning. A sub-day point (an instant with a non-midnight time part, or a relative token at `hour`, `minute` or `second`) against a DATE operand SHALL fail with a typed error.

#### Scenario: Temporal aggregate in a measure filter

- **WHEN** a query grouped by customer has the filter `max(ordered_at) >= 'last month'`
- **THEN** only customers whose latest order falls on or after the start of last month are returned, by executed values on SQLite and DuckDB

#### Scenario: Joined, derived and stage columns are temporal

- **WHEN** filters compare `customers.created_at` (joined), a derived TIMESTAMP column, and a stage column recorded as TIMESTAMP with `'2025-Q1'` using `=`
- **THEN** each restricts to `[2025-01-01, 2025-04-01)` identically to the same filter on a base column

#### Scenario: Relative token against an untyped column

- **WHEN** a filter compares a column with no recorded type with `'last month'`
- **THEN** the query fails with the typed error naming the column and the remedy

#### Scenario: Date string against a text column keeps its meaning

- **WHEN** a filter is `code = '2025'` on a TEXT column
- **THEN** it matches rows whose value is the string `2025`, as before

#### Scenario: Sub-day point against a DATE column

- **WHEN** a filter compares a DATE column with `'last 6 hours'` or `'2025-01-01 10:00:00'`
- **THEN** the query fails with the typed error stating that the column has day resolution

#### Scenario: Date-function, interval and conditional operands

- **WHEN** the clock reads `2026-09-29 12:00:00` and filters are `date_add(ordered_at, 1, 'day') >= 'last month'`, `ordered_at - interval(1, 'day') < '2025-Q1'` and `coalesce(shipped_at, ordered_at) in '2025-Q1'`
- **THEN** they match exactly the rows with `ordered_at >= 2026-07-31`, `ordered_at < 2025-01-02`, and `coalesce(shipped_at, ordered_at)` in `[2025-01-01, 2025-04-01)` respectively, by executed values on SQLite and DuckDB

#### Scenario: ISO literal inside a conditional operand

- **WHEN** a filter is `coalesce(shipped_at, '2099-12-31') >= 'last month'`
- **THEN** unshipped orders match

#### Scenario: Bad date-function operand is named

- **WHEN** a filter is `date_add(status, 1, 'day') >= 'last month'` on a TEXT `status`
- **THEN** the query fails with the date-operand type error naming `status`

#### Scenario: Bucket nested in an aggregate against a time point

- **WHEN** the clock reads `2026-09-29 12:00:00` and a query grouped by customer has the filter `max(date_trunc('month', ordered_at)) >= 'last month'`
- **THEN** only customers with an order on or after 2026-08-01 are returned, by executed values on SQLite and DuckDB

#### Scenario: Sub-day point against a day bucket of a DATE

- **WHEN** a filter compares `date_add(date_trunc('month', order_date), 1, 'day')` over a DATE column with `'2025-01-01 10:00:00'`
- **THEN** the query fails with the typed error stating that the operand has day resolution
