## ADDED Requirements

### Requirement: Quoted text is never a reference
Text inside a string literal or a double-quoted identifier SHALL never be read as a reference by any
part of query processing, on every Mode-B surface (measures, filters, dimensions, time dimensions,
order, `main_time_dimension`): a string literal's value SHALL reach the database exactly as written, a
quoted identifier SHALL name exactly the identifier written, and neither SHALL change which models a
query reads, whether it is a spine query, or which query-backed models it splices. Reference-shaped
text written outside quotes keeps its meaning.

#### Scenario: A literal naming the root keeps its text
- **WHEN** a query rooted at `orders` filters on `note = 'see orders.x'`
- **THEN** it returns exactly the rows whose `note` is `see orders.x`

#### Scenario: A quoted identifier naming the root keeps its text
- **WHEN** a query rooted at `orders` filters on `"orders.x" = 1`
- **THEN** it resolves the single identifier `orders.x`, failing as an unknown reference when no column
  of that name exists, never reading a column `x`

#### Scenario: A literal naming the spine is not a spine query
- **WHEN** a query rooted at `orders` with a month time dimension filters on `note = 'time_spine.x'`
- **THEN** it is not a spine query: its population is `orders`, it needs no spine lower bound, and it
  returns only the months that have matching orders

#### Scenario: A literal naming a query-backed model does not splice it
- **WHEN** a query grouping by `time_spine.timestamp` by month counts `orders.id` and filters on
  `orders.note <> 'qb.x'`, where `qb` is a query-backed model the query does not otherwise read
- **THEN** `qb` is not spliced into the statement

#### Scenario: Unchanged parsing of time dimensions and order with quoted text
- **WHEN** a time dimension is `month(iif(note = 'a)b', created_at, created_at))`, or an order entry is
  `status || 'a:b'` or `iif(note = 'x:y', 1, 0)`
- **THEN** the time dimension buckets that whole expression by month, and each order entry sorts by its
  whole expression, the quoted text intact
