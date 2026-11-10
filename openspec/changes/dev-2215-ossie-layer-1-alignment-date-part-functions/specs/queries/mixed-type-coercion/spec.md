## MODIFIED Requirements

### Requirement: A naive timestamp meets a zoned column in the column's type

SLayer's TIMESTAMP SHALL be a naive wall-clock value on every dialect (BigQuery `DATETIME`) — the result of `now()`, timestamp literals, `TRY_CAST(… AS TIMESTAMP)`, `json_value(… RETURNING TIMESTAMP)` and the date functions. A timestamp literal against such a column SHALL compare as ISO text in the column's type, read as UTC. Where a column's `db_type` is the dialect's zoned instant type (BigQuery `TIMESTAMP`), a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) between that column and a non-literal TIMESTAMP value SHALL convert the value to the column's type, reading it as UTC, and SHALL leave the column unconverted. Everywhere else — buckets, date functions, windows, the time spine, scalar functions and returned values — such a column SHALL read as its UTC wall clock, a naive TIMESTAMP.

#### Scenario: now() against a BigQuery TIMESTAMP column

- **WHEN** a filter is `created_at < now()` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`
- **THEN** it keeps the rows before the current UTC instant, comparing the column itself

#### Scenario: now() against a BigQuery DATETIME column

- **WHEN** a filter is `created_at < now()` over a BigQuery `DATETIME` column
- **THEN** it keeps the rows before the current UTC wall clock

#### Scenario: Timestamp literal against a BigQuery TIMESTAMP column

- **WHEN** a filter is `created_at < TIMESTAMP '2025-01-01 00:00:00'` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`
- **THEN** it keeps the rows before the UTC instant 2025-01-01 00:00:00, comparing the column itself

#### Scenario: Monthly bucket of a BigQuery TIMESTAMP column

- **WHEN** a query has the time dimension `date_trunc('month', created_at)` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`, on the time spine
- **THEN** each bucket is the naive month start of the UTC wall clock and joins the spine
