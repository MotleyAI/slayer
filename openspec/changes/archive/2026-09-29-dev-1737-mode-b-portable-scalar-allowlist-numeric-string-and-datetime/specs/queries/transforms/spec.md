## ADDED Requirements

### Requirement: time_shift offsets are exact on every dialect
`time_shift(x, periods, granularity)` SHALL look up the bucket whose start is the current bucket's
start moved by `periods` units with the same calendar rules as `date_add`: sub-day units keep the
time of day, and a `month` / `quarter` / `year` move landing past the target month's last day
clamps to that day. The looked-up bucket SHALL be identical on every Tier-1 dialect, SQLite
included.

#### Scenario: Hourly shift on SQLite
- **WHEN** a SQLite model `ev` has rows at 2024-01-01 10:15 (`v` 1), 11:20 (`v` 2) and 12:30 (`v` 4) and a query over an `hour` time dimension on `ts` selects `sum(v)` and `time_shift(sum(v), -1, 'hour')`
- **THEN** the shifted values are NULL at 10:00, 1 at 11:00 and 2 at 12:00

#### Scenario: Month shift of a month-end daily bucket
- **WHEN** a query over a `day` time dimension has rows dated 2024-02-29 (`v` 10), 2024-03-02 (`v` 5) and 2024-03-31 (`v` 1) and selects `time_shift(sum(v), -1, 'month')`
- **THEN** on every dialect the shifted value at 2024-03-31 is 10 (the 2024-02-29 bucket), never 5
