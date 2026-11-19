## MODIFIED Requirements

### Requirement: Time dimension names

A time dimension SHALL accept an optional `name`. An unnamed column time dimension SHALL keep its existing result key. An unnamed expression time dimension SHALL be named by the same sanitising rule as an unnamed computed dimension, and its result key SHALL be that name prefixed by its references' common anchor (`queries/references`), extended with the granularity exactly when another projected time dimension shares its bound source. A named time dimension's result key SHALL be its bare name, never extended with the granularity. Names SHALL be unique across a query's dimensions and time dimensions; a collision SHALL fail at construction naming it. A time dimension MAY be referred to by its name from `main_time_dimension` and from `partition_by=`, exactly as a computed dimension is by its name.

#### Scenario: Unnamed expression time dimension's key
- **WHEN** a query on `orders` buckets `coalesce(shipped_at, created_at)` at `month`
- **THEN** its result key is `orders.coalesce_shipped_at_created_at`

#### Scenario: Named time dimension
- **WHEN** the same time dimension carries `"name": "effective_at"`
- **THEN** its result key is `effective_at`, and `main_time_dimension` `"effective_at"` selects it as the axis of a `cumsum` measure in a query that also buckets `created_at`

#### Scenario: Named column time dimension
- **WHEN** a time dimension on `created_at` at `month` carries `"name": "m"`
- **THEN** its result key is `m` and its values equal the unnamed form's

#### Scenario: Partition by a time dimension's name
- **WHEN** a query buckets `coalesce(shipped_at, created_at)` at `month` named `effective_at`, groups by `region`, and selects `sum(amount, partition_by=effective_at)`
- **THEN** each row carries its month's total across regions, by executed values

#### Scenario: Name collision rejected
- **WHEN** a query declares a computed dimension and a time dimension both named `m`
- **THEN** construction fails with an error naming `m`

### Requirement: Result keys disambiguate same-column time dimensions

When two or more projected unnamed time dimensions share the same bound source — a column or an expression — at different granularities, each of their result keys SHALL be the usual key with the granularity appended (`orders.created_at.month`, `orders.created_at.year`). An unnamed time dimension whose source no other projected time dimension shares SHALL keep its granularity-free key, and a named one SHALL never carry the suffix (› Time dimension names). Exact-duplicate time dimensions (the same dimension, granularity, date range, name and label) SHALL be deduplicated at construction; time dimensions sharing dimension and granularity but differing in date range, name or label SHALL be rejected at construction. At construction two dimensions are the same when their parsed expressions are equal once read in the qualified syntax (`created_at` converts to `orders.created_at` in a query naming population `orders`, `queries/references`), never on their text, so text inside a string literal or quoted identifier is compared exactly and never read as a qualifier. Time dimensions whose dimensions are spelled differently but resolve to the same bucket (the same bound time-truncation identity), disagreeing in date range, name or label, SHALL be rejected at binding — backstopping the construction-time check for spellings that only prove equivalent once resolved. Such spellings that share their name (an unnamed one's derived from its template) and agree in date range and label SHALL be deduplicated at binding into one projected time dimension, as exact duplicates are at construction.

#### Scenario: two granularities of one column

- WHEN a query projects `date_trunc('month', created_at)` and `date_trunc('year', created_at)`
- THEN the result has two columns keyed `<dataset>.created_at.month` and `<dataset>.created_at.year` with correctly bucketed values

#### Scenario: two granularities of one expression

- WHEN a query projects `date_trunc('month', coalesce(shipped_at, created_at))` and `date_trunc('year', coalesce(shipped_at, created_at))`
- THEN the result has two columns keyed `<dataset>.coalesce_shipped_at_created_at.month` and `<dataset>.coalesce_shipped_at_created_at.year`

#### Scenario: single time dimension keeps its key

- WHEN a query projects a single time dimension on `created_at`
- THEN its result key stays `<dataset>.created_at` with no granularity suffix

#### Scenario: exact duplicates dedupe

- WHEN a query lists `date_trunc('month', created_at)` twice (in either form)
- THEN the query behaves as if it were listed once

#### Scenario: a root-qualified duplicate dedupes

- WHEN a query rooted at `orders` lists `date_trunc('month', created_at)` and `date_trunc('month', orders.created_at)`
- THEN the query behaves as if `date_trunc('month', created_at)` were listed once

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
