## MODIFIED Requirements

### Requirement: The interval spelling is date_add
`x + interval(n, unit)`, `interval(n, unit) + x` and `x - interval(n, unit)` SHALL mean exactly
`date_add(x, n, unit)` (for subtraction, `date_add(x, -n, unit)`) and SHALL resolve to the same
value as that call, sharing its result key and SQL. The SQL interval literals `INTERVAL '<n> <unit>'`,
`INTERVAL '<n>' <unit>` and `INTERVAL <n> <unit>` (one unit; keyword and unit case-insensitive; a
unit's plural form accepted) SHALL mean `interval(n, unit)` in all of these positions. Chains SHALL
apply left to right. `interval` SHALL follow the same count and unit rules as `date_add`. An interval
in any other position — standalone, as the left side of `-`, negated, multiplied, compared, nested in
another interval, added to another interval, or as a function argument — SHALL be rejected before
binding with an error pointing to `date_add`, and an interval literal naming more than one unit
(`INTERVAL '1 day 2 hours'`) SHALL be rejected with an error suggesting a chain.

#### Scenario: Operator spelling equals date_add
- **WHEN** one measure uses `max(shipped_at - interval(2, 'day'))` and another `max(date_add(shipped_at, -2, 'day'))`
- **THEN** both resolve to the same value and render identical SQL

#### Scenario: Chained intervals
- **WHEN** a filter uses `created_at + interval(1, 'month') + interval(2, 'hour') < now()`
- **THEN** it binds as `date_add(date_add(created_at, 1, 'month'), 2, 'hour') < now()`

#### Scenario: Commuted addition
- **WHEN** a filter uses `interval(7, 'day') + order_date > current_date()`
- **THEN** it binds as `date_add(order_date, 7, 'day') > current_date()`

#### Scenario: Stray interval rejected
- **WHEN** a filter uses `interval(7, 'day') - order_date > 0`, `-interval(7, 'day') + order_date > order_date`, or `order_date + interval(1, 'day') * 2 > order_date`
- **THEN** each is rejected with an error pointing to `date_add`

#### Scenario: SQL interval literals equal date_add
- **WHEN** filters use `order_date + INTERVAL '7 days' > CURRENT_DATE`, `order_date + INTERVAL '7' DAY > CURRENT_DATE` and `order_date + INTERVAL 7 DAY > CURRENT_DATE`
- **THEN** each binds as `date_add(order_date, 7, 'day') > current_date()`

#### Scenario: Multi-unit interval literal rejected
- **WHEN** a filter uses `order_date + INTERVAL '1 day 2 hours' > now()`
- **THEN** it is rejected with an error suggesting `+ INTERVAL '1 day' + INTERVAL '2 hours'`

## ADDED Requirements

### Requirement: SQL date literals and clock keywords
`DATE '<literal>'` SHALL be a DATE value and `TIMESTAMP '<literal>'` a TIMESTAMP value, accepting
exactly the shapes and calendar checks of ISO literals in date positions, in every position; a
malformed or impossible value SHALL be rejected with the same typed query error. `CURRENT_DATE` SHALL
mean `current_date()` and `CURRENT_TIMESTAMP` SHALL mean `now()`, sharing their result keys, SQL and
result-cache bypass.

#### Scenario: Typed literals
- **WHEN** a filter uses `created_at >= TIMESTAMP '2024-01-01 00:00'` and a dimension uses `date_diff('day', DATE '2024-01-01', created_at)`
- **THEN** both bind with typed temporal literals and execute

#### Scenario: Impossible typed literal
- **WHEN** a filter uses `created_at >= DATE '2024-02-30'`
- **THEN** the query is rejected with the type error naming the literal

#### Scenario: Clock keywords
- **WHEN** one filter uses `created_at >= CURRENT_DATE` and another `created_at >= current_date()`
- **THEN** both render identical SQL and neither query is served from or stored in the result cache
