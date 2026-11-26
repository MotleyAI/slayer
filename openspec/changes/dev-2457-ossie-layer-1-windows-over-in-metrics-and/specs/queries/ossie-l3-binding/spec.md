## MODIFIED Requirements

### Requirement: The filter is an Ossie expression
Under `ossie-l3` the filter SHALL be an OSSIE_SQL_2026 expression (Ossie Layer 3 §1.4). A construct SHALL be admitted only when Ossie Layer 1 defines it and SLayer gives it Ossie's meaning; every other construct SHALL raise `E_INVALID_QUERY`: SLayer's list functions and list children, the regex operator `~`, the JSON operator `->>`, a single-string `IN '…'`, deprecated spellings, transforms, a `{variable}` placeholder left unbound, and a string literal compared with a temporal field that is not an ISO-8601 date or timestamp (period and relative tokens such as `'2025-Q1'` and `'last month'`). A scalar function SHALL be admitted only at an argument count Ossie Layer 1 defines for it (`LOG(base, x)` but not `LOG(x)`, `ROUND(x, d)` but not `ROUND(x)`), and a call at an argument count SLayer does not accept SHALL raise `E_INVALID_QUERY`. `CAST` SHALL be admitted to every target type Ossie Layer 1 names, `TIMESTAMP_NTZ` included. An aggregate SHALL count as an Ossie construct only when Ossie Layer 1 defines its spelling and shape and SLayer evaluates it. An aggregate whose arguments are not Ossie's (one value, `COUNT(*)`, two for `APPROX_PERCENTILE`, `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)`) SHALL raise `E_INVALID_QUERY`, windowed or not. An Ossie construct SLayer does not support yet SHALL raise `E_INVALID_QUERY` as a property the engine does not support (§3.5). Aggregates and metric references SHALL raise `E_AGGREGATE_IN_WHERE` and window functions `E_WINDOW_IN_WHERE`, at their rows. A window function SHALL be read as Ossie Layer 1 writes it, `function(args) OVER ([PARTITION BY …] [ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]] [ROWS|RANGE frame])`, so the rest of the filter and everything inside the window are judged at their own rows before row 7; a malformed window, a function that is not one of Ossie's ranking, offset or aggregate functions (or takes other arguments than Ossie's), a window inside a window and a frame whose bounds are out of ANSI order SHALL raise `E_INVALID_QUERY`. A windowed aggregate SHALL raise `E_WINDOW_IN_WHERE`, not `E_AGGREGATE_IN_WHERE`. A filter with no field references SHALL have no dataset and SHALL apply in every branch. The native conformance SHALL evaluate a window in a filter (`queries/window-functions`); judging a filter under `ossie-l3` SHALL NOT change what the same filter does under `slayer`.

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
- **WHEN** a query with the dimension `orders.order_id` and the filter `ROW_NUMBER() OVER (ORDER BY orders.amount) = 1` runs under `slayer`, before or after the same filter is judged under `ossie-l3`
- **THEN** under `slayer` it returns the one order with the smallest amount both times, and under `ossie-l3` it fails with `E_WINDOW_IN_WHERE`

### Requirement: Model-scoped metrics are judged by their components
Each aggregate call of a model-scoped metric SHALL be a component homed on the one dataset its arguments read, an `AGG(D.m)` call a component homed on `D`, and the metric's datasets its components' homes; this analysis SHALL cover every model-scoped metric of the model, those of `SLAYER` entries included. Under `ossie-l3` a query naming a model-scoped metric SHALL raise `E_UNAGGREGATED_FINER_GRAIN_REFERENCE` when its expression reads a field outside every aggregate, `E_DEFERRED_WINDOWED_METRIC` when it contains a window function, and `E_AMBIGUOUS_MEASURE_GRAIN` when an aggregate's arguments read several datasets or none, or its `FILTER` reads another dataset than its arguments — never the native error a query reading the metric would otherwise raise. A windowed field and a dataset-scoped windowed metric SHALL be evaluated under `ossie-l3` as under `slayer`.

#### Scenario: The model-scoped restrictions
- **WHEN** `amount_less_order`, `running_revenue`, `order_less_return` and `row_count` of the retail model are each queried under `ossie-l3`
- **THEN** they raise `E_UNAGGREGATED_FINER_GRAIN_REFERENCE`, `E_DEFERRED_WINDOWED_METRIC`, `E_AMBIGUOUS_MEASURE_GRAIN` and `E_AMBIGUOUS_MEASURE_GRAIN`

#### Scenario: Components aggregate their own datasets
- **WHEN** `{"metrics": ["revenue_per_customer"]}` runs under `ossie-l3` on the retail model
- **THEN** it returns 162.5 (650 / 4), the customer count including the customer without orders

#### Scenario: A dataset-scoped windowed metric answers under strict
- **WHEN** dataset `orders` of the retail model gains the metric `revenue_rank` with expression `RANK() OVER (ORDER BY SUM(amount) DESC)`, and `{"dimensions": ["orders.status"], "metrics": ["orders.revenue_rank"]}` runs under `ossie-l3` and under `slayer`
- **THEN** both return the same rows, and `{"metrics": ["running_revenue"]}` still raises `E_DEFERRED_WINDOWED_METRIC` under `ossie-l3` while answering under `slayer`
