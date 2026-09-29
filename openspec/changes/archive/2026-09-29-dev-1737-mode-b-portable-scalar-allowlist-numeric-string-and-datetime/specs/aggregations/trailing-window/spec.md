## ADDED Requirements

### Requirement: Calendar-unit frame bounds clamp at month-end on every dialect
The interval start `bucket_end − window` SHALL be computed with the same calendar rules as
`date_add` with a negative count, applied part by part in the order written: a `m` or `y` part
landing past the target month's last day clamps to that day, and sub-day parts keep the time of
day. The interval SHALL be identical on every Tier-1 dialect, SQLite included.

#### Scenario: One-month window ending at month-end
- **WHEN** a query over a `day` time dimension has rows dated 2024-02-28 (`amount` 1), 2024-02-29 (10), 2024-03-01 (100) and 2024-03-30 (1000) and selects `amount:sum(window='1m')`
- **THEN** on every dialect the value at 2024-03-30 is 1110 — its interval starts 2024-02-29 (2024-03-31 minus one month, clamped), so the 2024-02-28 row is excluded and the 2024-02-29 row included
