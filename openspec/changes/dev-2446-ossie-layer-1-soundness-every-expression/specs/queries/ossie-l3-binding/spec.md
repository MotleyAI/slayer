## MODIFIED Requirements

### Requirement: The filter is an Ossie expression
Under `ossie-l3` the filter SHALL be an OSSIE_SQL_2026 expression (Ossie Layer 3 §1.4). A construct SHALL be admitted only when Ossie Layer 1 defines it and SLayer gives it Ossie's meaning; every other construct SHALL raise `E_INVALID_QUERY`: SLayer's list functions and list children, the regex operator `~`, the JSON operator `->>`, a single-string `IN '…'`, deprecated spellings, transforms, a `{variable}` placeholder left unbound, and a string literal compared with a temporal field that is not an ISO-8601 date or timestamp (period and relative tokens such as `'2025-Q1'` and `'last month'`). A scalar function SHALL be admitted only at an argument count Ossie Layer 1 defines for it (`LOG(base, x)` but not `LOG(x)`, `ROUND(x, d)` but not `ROUND(x)`), and a call at an argument count SLayer does not accept SHALL raise `E_INVALID_QUERY`. `CAST` SHALL be admitted to every target type Ossie Layer 1 names, `TIMESTAMP_NTZ` included. An aggregate SHALL count as an Ossie construct only when Ossie Layer 1 defines its spelling and shape and SLayer evaluates it. An Ossie construct SLayer does not support yet SHALL raise `E_INVALID_QUERY` as a property the engine does not support (§3.5). Aggregates and metric references SHALL raise `E_AGGREGATE_IN_WHERE` and window functions `E_WINDOW_IN_WHERE`, at their rows. A filter with no field references SHALL have no dataset and SHALL apply in every branch.

#### Scenario: SLayer-only constructs are refused
- **WHEN** a strict query's filter is `orders.status ~ '^c'`, `contains(orders.tags, 'x')`, `orders.order_date >= '2025-Q1'` or `orders.order_date >= 'last month'`
- **THEN** each fails with `E_INVALID_QUERY`, and each answers under `slayer`

#### Scenario: Ossie constructs are admitted
- **WHEN** a strict query's filter uses comparison, `BETWEEN`, `IN (…)`, `LIKE`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `CASE WHEN`, arithmetic, `CAST`, an ISO date literal compared with a date field, or an Ossie scalar function SLayer implements
- **THEN** it answers with the rows Ossie defines

#### Scenario: A filter without fields reaches unconnected metrics
- **WHEN** `{"metrics": ["orders.total_revenue", "tickets.ticket_count"], "filter": "FALSE"}` runs under `ossie-l3` on the retail model
- **THEN** it returns one row with `total_revenue` NULL and `ticket_count` 0

#### Scenario: Ossie's argument counts decide admission
- **WHEN** a strict query's filter is `LOG(10, orders.amount) > 2`, and others are `LOG(orders.amount) > 2`, `ROUND(orders.amount / 120.0) = 1`, `TRUNC(orders.amount / 120.0) = 0`, `SUBSTRING(orders.status, 1) = 'complete'` and `REGEXP_LIKE(orders.status, '^C', 'i')`
- **THEN** the first answers with the rows Ossie defines, and each other fails with `E_INVALID_QUERY` and answers under `slayer`

#### Scenario: A TIMESTAMP_NTZ cast is admitted
- **WHEN** a strict query's filter is `orders.order_date > CAST('2025-01-01 00:00:00' AS TIMESTAMP_NTZ)`
- **THEN** it answers with the rows Ossie defines, as under `slayer`

#### Scenario: Aggregates SLayer cannot evaluate are refused
- **WHEN** a strict query's filter is `APPROX_PERCENTILE(orders.amount, 0.5) > 1` or `percentile_disc(orders.amount, 0.5) > 1`, and another's is `MEDIAN(orders.amount) > 1`
- **THEN** the first two fail with `E_INVALID_QUERY`, and the third with `E_AGGREGATE_IN_WHERE`

#### Scenario: A wrong argument count is refused
- **WHEN** a strict query's filter is `ABS(orders.amount, 1) > 0` or `UPPER(orders.status, 1) = 'X'`
- **THEN** each fails with `E_INVALID_QUERY`, never with an uncoded error
