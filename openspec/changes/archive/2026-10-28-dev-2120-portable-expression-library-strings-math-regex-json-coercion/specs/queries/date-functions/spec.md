## ADDED Requirements

### Requirement: date_trunc is the granularity call

`date_trunc(unit, x)` SHALL mean the granularity call `<unit>(x)` — the same bucket, type and behaviour in every position: a row value in filters, aggregation sources, computed dimensions and arithmetic, and a time dimension in a `dimensions` / `time_dimensions` entry or an `order` key. `unit` SHALL be a string literal naming a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`), matched case-insensitively; anything else SHALL be rejected with the granularity call's error. `date_trunc` SHALL be reserved like the other date function names.

#### Scenario: Row value

- **WHEN** a filter is `date_trunc('month', created_at) = '2025-03'`
- **THEN** it keeps the same rows as `month(created_at) = '2025-03'`

#### Scenario: Time dimension

- **WHEN** a query's `dimensions` entry is `"date_trunc('month', created_at)"`
- **THEN** it is the time dimension `month(created_at)`, with the same result key and SQL

#### Scenario: Unknown unit

- **WHEN** a dimension is `date_trunc('fortnight', created_at)`
- **THEN** the query fails with the granularity call's unknown-granularity error
