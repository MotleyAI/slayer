## MODIFIED Requirements

### Requirement: Clock functions read the database clock
Without a time zone (`queries/time-zones`), `current_date()` SHALL return the database's current
date as a DATE and `now()` its current timestamp as a TIMESTAMP, evaluated by the database at
execution time (on SQLite, in UTC; elsewhere in the database session's time zone). Under a stage's
effective time zone, `now()` SHALL be the engine's one clock reading for the execution taken in the
zone's wall clock and `current_date()` its date, each rendered as a literal.

#### Scenario: Recent orders
- **WHEN** a filter `created_at >= date_add(current_date(), -30, 'day')` runs
- **THEN** it keeps orders created within the last 30 days by the database's date

#### Scenario: Clock functions in the query zone
- **WHEN** the clock reads the instant `2025-01-01 03:30:00Z` and a query with `time_zone: America/New_York` has the dimensions `current_date()` and `now()`
- **THEN** they are the DATE `2024-12-31` and the TIMESTAMP `2024-12-31 22:30:00`, rendered as literals

### Requirement: Clock-dependent queries bypass the result cache
A query whose computation includes `now()` in any stage, or `current_date()` in a stage without a
time zone (`queries/time-zones`), SHALL neither be served from nor stored in the engine's result
cache, including on refresh; other queries, including those with `current_date()` only under a time
zone, SHALL cache as before.

#### Scenario: Clock query not cached
- **WHEN** a query with filter `created_at >= date_add(current_date(), -30, 'day')` is executed twice with caching enabled
- **THEN** both executions run against the database

#### Scenario: Plain query still cached
- **WHEN** a query with filter `date_diff('day', created_at, shipped_at) > 3` is executed twice with caching enabled
- **THEN** the second execution is served from the cache

#### Scenario: Zoned current_date caches
- **WHEN** a query with `time_zone: UTC` and filter `created_at >= date_add(current_date(), -30, 'day')` is executed twice with caching enabled and a clock reading the same day
- **THEN** the second execution is served from the cache

#### Scenario: Zoned now still bypasses
- **WHEN** a query with `time_zone: UTC` and the dimension `now()` is executed twice with caching enabled
- **THEN** both executions run against the database
