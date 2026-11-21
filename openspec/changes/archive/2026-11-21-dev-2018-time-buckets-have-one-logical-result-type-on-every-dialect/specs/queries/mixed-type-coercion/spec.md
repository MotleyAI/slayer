## ADDED Requirements

### Requirement: A DATE meeting a TIMESTAMP reads as its midnight instant

Wherever a DATE value meets a TIMESTAMP value, the DATE SHALL read as the TIMESTAMP at its midnight, on every Tier-1 dialect: in a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`), in `in` / `not in`, in the value arguments of a value-returning conditional typed TIMESTAMP (`CASE`, `iif`, `coalesce`, `ifnull`, `greatest`, `least`), in the comparison `nullif` makes (its result keeping its first argument's type), and in the join that attaches one grain to another, the time spine's join to a dataset's axis included. A time point or date literal compared with a column SHALL be typed to the column, never convert the column, and a time-spine bound applied to a DATE axis SHALL restrict the axis as its midnight instant. On SQLite a comparison between two non-literal temporal values SHALL hold for every storage spelling of a TIMESTAMP (space- or `T`-separated, date-only, fractional seconds); a value that is not a valid date SHALL compare as NULL.

#### Scenario: DATE column against TIMESTAMP column

- **WHEN** a filter is `order_date = created_at`, `order_date` DATE and `created_at` TIMESTAMP
- **THEN** it keeps exactly the rows whose `created_at` is midnight of `order_date`, by executed values on SQLite, DuckDB and Postgres

#### Scenario: Day bucket of a timestamp against a DATE

- **WHEN** a filter is `date_trunc('day', created_at) = order_date`
- **THEN** it keeps exactly the rows whose `created_at` falls on `order_date`

#### Scenario: DATE arm of a timestamp conditional

- **WHEN** a filter is `coalesce(shipped_at, order_date) = created_at`, `shipped_at` and `created_at` TIMESTAMP and `order_date` DATE
- **THEN** an unshipped row matches exactly when `created_at` is midnight of `order_date`

#### Scenario: nullif keeps its first argument's type

- **WHEN** a dimension is `nullif(order_date, created_at)`
- **THEN** it is a DATE, NULL exactly where `created_at` is midnight of `order_date`

#### Scenario: Time spine over a DATE axis

- **WHEN** a model's axis is a DATE column and a query on the time spine buckets by `month` with `date_range` `["2024-05-01", "2024-08-01"]`
- **THEN** every month in range is returned and each fact row counts in its own month, by executed values on SQLite and DuckDB, and the emitted SQL is valid on every Tier-1 dialect

#### Scenario: Storage spelling does not matter on SQLite

- **WHEN** a SQLite TIMESTAMP column holds `2025-03-01T00:00:00`, `2025-03-01 00:00:00` and `2025-03-01`, and a filter is `date_trunc('day', created_at) = created_at`
- **THEN** all three rows match
