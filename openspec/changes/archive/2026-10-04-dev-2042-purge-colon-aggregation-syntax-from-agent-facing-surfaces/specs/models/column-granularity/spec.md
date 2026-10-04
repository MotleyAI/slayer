## MODIFIED Requirements

### Requirement: Query-backed model columns record their bucket granularity

For a query-backed model the engine SHALL stamp each cached column produced by a time dimension of the final stage with that time dimension's granularity, and SHALL leave the granularity unset on every other cached column (a raw temporal column projected as a plain dimension, an aggregate output, a computed dimension). The stamped column SHALL keep its temporal type. The stamp SHALL be present on the persisted column snapshot written at save time and on the model as resolved for querying, so a nested query-backed model built over a bucketed one records the outer bucket.

#### Scenario: Bucketed final-stage column is stamped and persisted

- **WHEN** a query-backed model is created from a query with a `month` time dimension on `created_at` and a revenue sum
- **THEN** the returned model's cached `created_at` column has granularity `month` and a temporal type, the cached `rev` column has no granularity, and the model reloaded from storage shows the same

#### Scenario: Unbucketed cached columns carry no granularity

- **WHEN** a query-backed model's final stage projects `created_at` as a plain dimension, or selects `max(created_at)`
- **THEN** the corresponding cached column has no granularity and any time-dimension granularity over it binds

#### Scenario: Nested query-backed models compose

- **WHEN** a query-backed model `yearly` is created over the query-backed model `monthly` with a `year` time dimension on `created_at`
- **THEN** `yearly`'s cached `created_at` has granularity `year`, and a `day` time dimension over `yearly` is the typed re-bucketing error
