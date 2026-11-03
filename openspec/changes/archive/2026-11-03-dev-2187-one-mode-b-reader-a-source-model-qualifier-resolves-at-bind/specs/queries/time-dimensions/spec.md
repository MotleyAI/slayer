## MODIFIED Requirements

### Requirement: Result keys disambiguate same-column time dimensions

When two or more projected unnamed time dimensions share the same bound source — a column or an expression — at different granularities, each of their result keys SHALL be the usual key with the granularity appended (`orders.created_at.month`, `orders.created_at.year`). An unnamed time dimension whose source no other projected time dimension shares SHALL keep its granularity-free key, and a named one SHALL never carry the suffix (› Time dimension names). Exact-duplicate time dimensions (the same dimension, granularity, date range, name and label) SHALL be deduplicated at construction; time dimensions sharing dimension and granularity but differing in date range, name or label SHALL be rejected at construction. At construction two dimensions are the same when their parsed expressions are equal once a leading root qualifier is dropped (`created_at` ≡ `orders.created_at` from root `orders`), never on their text, so text inside a string literal or quoted identifier is compared exactly and never read as a qualifier. Time dimensions whose dimensions are spelled differently but resolve to the same bucket (the same bound time-truncation identity), disagreeing in date range, name or label, SHALL be rejected at binding — backstopping the construction-time check for spellings that only prove equivalent once resolved. Such spellings that share their name (an unnamed one's derived from its template) and agree in date range and label SHALL be deduplicated at binding into one projected time dimension, as exact duplicates are at construction.

#### Scenario: two granularities of one column

- WHEN a query projects `month(created_at)` and `year(created_at)`
- THEN the result has two columns keyed `<model>.created_at.month` and `<model>.created_at.year` with correctly bucketed values

#### Scenario: two granularities of one expression

- WHEN a query projects `month(coalesce(shipped_at, created_at))` and `year(coalesce(shipped_at, created_at))`
- THEN the result has two columns keyed `<model>.coalesce_shipped_at_created_at.month` and `<model>.coalesce_shipped_at_created_at.year`

#### Scenario: single time dimension keeps its key

- WHEN a query projects a single time dimension on `created_at`
- THEN its result key stays `<model>.created_at` with no granularity suffix

#### Scenario: exact duplicates dedupe

- WHEN a query lists `month(created_at)` twice (in either form)
- THEN the query behaves as if it were listed once

#### Scenario: a root-qualified duplicate dedupes

- WHEN a query rooted at `orders` lists `month(created_at)` and `month(orders.created_at)`
- THEN the query behaves as if `month(created_at)` were listed once

#### Scenario: literals are never conflated

- WHEN a query lists the month time dimensions `iif(note = 'orders.a', created_at, shipped_at)` and `iif(note = 'a', created_at, shipped_at)` with different date ranges
- THEN construction accepts both as distinct time dimensions

#### Scenario: same column and granularity with differing metadata rejected

- WHEN a query lists two time dimensions on the same dimension and granularity that differ in date range or label
- THEN construction fails with an error naming the collision

#### Scenario: equivalent spellings with differing metadata rejected at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) that resolve to the same bucket but differ in date range or label
- THEN the query fails with an error naming the conflict

#### Scenario: equivalent spellings with agreeing metadata dedupe at binding

- WHEN two projected time dimensions name one source via different spellings (e.g. `coalesce(shipped_at, created_at)` and `COALESCE(shipped_at,created_at)`) at one granularity, agreeing in date range, name and label
- THEN the query behaves as if it were listed once, by executed values on SQLite and DuckDB
