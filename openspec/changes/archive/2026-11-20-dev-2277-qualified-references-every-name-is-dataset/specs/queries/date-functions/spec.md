## MODIFIED Requirements

### Requirement: Date functions are Mode-B scalars
The Mode-B scalar allowlist SHALL include `date_part(part, ts)`, `date_diff(unit, start, end)`,
`date_add(ts, n, unit)`, `current_date()` and `now()`, matched case-insensitively and usable in
every Mode-B position (measure formulas, filters, expression dimensions, order keys). Each SHALL
take exactly its listed number of arguments (`current_date` / `now` take none); a call with any
other argument count, or with keyword arguments, SHALL be rejected naming the function. The
one-argument calls `year(x)`, `quarter(x)`, `month(x)`, `week(x)`, `day(x)`, `hour(x)`, `minute(x)`
and `second(x)` SHALL be extraction (› Date-part aliases mean their date_part); a time bucket SHALL
be written `date_trunc(unit, x)` (› date_trunc is the granularity call).

#### Scenario: Extraction as an expression dimension
- **WHEN** a query `{"population": "orders", "dimensions": [{"field": "date_part('day_of_week', created_at)", "alias": "dow"}], "metrics": [{"metric": "count(*)"}]}` executes
- **THEN** the result has one row per weekday with key `dow` holding an integer 1–7

#### Scenario: Date functions in a filter
- **WHEN** a query on `orders` has filter `"date_diff('day', created_at, shipped_at) > 3"`
- **THEN** only orders shipped on a calendar day more than three days after creation are counted

#### Scenario: Case-insensitive names
- **WHEN** a filter spells `DATE_DIFF('day', created_at, shipped_at) > 3`
- **THEN** it binds to the same value as the lowercase spelling

#### Scenario: Wrong argument count
- **WHEN** a filter uses `date_add(created_at, 3)`
- **THEN** the query is rejected with an error naming `date_add` and its argument count

#### Scenario: Zero-argument clock calls
- **WHEN** a filter uses `created_at <= now()` or `order_date <= current_date()`
- **THEN** both bind and execute; `now(1)` is rejected for its argument count

#### Scenario: Bucket form unchanged
- **WHEN** a query lists `"date_trunc('month', created_at)"` in `dimensions`
- **THEN** it is still the month time bucket, not a month number

#### Scenario: Month call is month of year
- **WHEN** a query lists `"month(created_at)"` in `dimensions` with measure `count(*)`
- **THEN** it groups by the month number 1–12 (rows of every year's March share one group), not by month bucket
