## MODIFIED Requirements

### Requirement: The filter is an Ossie expression
Under `ossie-l3` the filter SHALL be an OSSIE_SQL_2026 expression (Ossie Layer 3 §1.4). A construct SHALL be admitted only when Ossie Layer 1 defines it and SLayer gives it Ossie's meaning; every other construct SHALL raise `E_INVALID_QUERY`: SLayer's list functions and list children, the regex operator `~`, the JSON operator `->>`, a single-string `IN '…'`, deprecated spellings, transforms, a `{variable}` placeholder left unbound, and a string literal compared with a temporal field that is not an ISO-8601 date or timestamp (period and relative tokens such as `'2025-Q1'` and `'last month'`). An aggregate whose arguments are not Ossie's (one value, `COUNT(*)`, two for `APPROX_PERCENTILE`, `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`) SHALL raise `E_INVALID_QUERY`, windowed or not. An Ossie construct SLayer does not support yet SHALL raise `E_INVALID_QUERY` as a property the engine does not support (§3.5). Aggregates and metric references SHALL raise `E_AGGREGATE_IN_WHERE` and window functions `E_WINDOW_IN_WHERE`, at their rows. A window function SHALL be read as Ossie Layer 1 writes it, `function(args) OVER ([PARTITION BY …] [ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]] [ROWS|RANGE frame])`, so the rest of the filter and everything inside the window are judged at their own rows before row 7; a malformed window, a function that is not one of Ossie's ranking, offset or aggregate functions (or takes other arguments than Ossie's), a window inside a window and a frame whose bounds are out of ANSI order SHALL raise `E_INVALID_QUERY`. A windowed aggregate SHALL raise `E_WINDOW_IN_WHERE`, not `E_AGGREGATE_IN_WHERE`. A filter with no field references SHALL have no dataset and SHALL apply in every branch. The native conformance SHALL keep refusing a window in a filter exactly as before.

#### Scenario: SLayer-only constructs are refused
- **WHEN** a strict query's filter is `orders.status ~ '^c'`, `contains(orders.tags, 'x')`, `orders.order_date >= '2025-Q1'` or `orders.order_date >= 'last month'`
- **THEN** each fails with `E_INVALID_QUERY`, and each answers under `slayer`

#### Scenario: Ossie constructs are admitted
- **WHEN** a strict query's filter uses comparison, `BETWEEN`, `IN (…)`, `LIKE`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `CASE WHEN`, arithmetic, `CAST`, an ISO date literal compared with a date field, or an Ossie scalar function SLayer implements
- **THEN** it answers with the rows Ossie defines

#### Scenario: A filter without fields reaches unconnected metrics
- **WHEN** `{"metrics": ["orders.total_revenue", "tickets.ticket_count"], "filter": "FALSE"}` runs under `ossie-l3` on the retail model
- **THEN** it returns one row with `total_revenue` NULL and `ticket_count` 0

#### Scenario: A malformed window is malformed
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `ROW_NUMBER() OVER (`, `FOO() OVER ()`, `ROW_NUMBER(orders.amount) OVER ()`, `SUM(ROW_NUMBER() OVER ()) OVER ()`, `ROW_NUMBER() OVER (ORDER BY orders.amount ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) = 1` or `ROW_NUMBER() OVER (ORDER BY orders.amount GROUPS UNBOUNDED PRECEDING) = 1`
- **THEN** each fails with `E_INVALID_QUERY`

#### Scenario: Names inside a window resolve before the window
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `ROW_NUMBER() OVER (ORDER BY orders.no_such_field) = 1`
- **THEN** it fails with `E_NAME_NOT_FOUND`; with `SUM(orders.amount) OVER (PARTITION BY orders.total_revenue) > 10` it fails with `E_AGGREGATE_IN_WHERE`

#### Scenario: The rest of a windowed filter is judged
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `orders.status ~ '^c' AND ROW_NUMBER() OVER (ORDER BY orders.amount) = 1`
- **THEN** it fails with `E_INVALID_QUERY`

#### Scenario: A complete window is a window in the filter
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `SUM(orders.amount) OVER (PARTITION BY orders.status) > 10`, `AVG(orders.amount) OVER (ORDER BY orders.amount DESC NULLS LAST ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) > 1`, or `ROW_NUMBER() OVER (ORDER BY orders.amount) = {n}` with the variable `n` bound to 1
- **THEN** each fails with `E_WINDOW_IN_WHERE`

#### Scenario: An aggregate takes Ossie's arguments
- **WHEN** a strict query with the dimension `orders.order_id` has the filter `SUM(orders.amount, orders.amount) > 1` or `SUM(orders.amount, orders.amount) OVER () > 1`
- **THEN** each fails with `E_INVALID_QUERY`; `COUNT(*) > 1` fails with `E_AGGREGATE_IN_WHERE`

#### Scenario: The native binding still refuses a window in the filter
- **WHEN** a query with the filter `ROW_NUMBER() OVER (ORDER BY orders.amount) = 1` runs under `slayer`, before or after the same filter is judged under `ossie-l3`
- **THEN** it fails with the same window-in-filter error as before
