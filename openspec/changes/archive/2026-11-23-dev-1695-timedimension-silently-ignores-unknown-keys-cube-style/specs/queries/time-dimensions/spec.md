## ADDED Requirements

### Requirement: Cube's time-dimension keys

A time dimension carrying Cube's `dateRange` SHALL behave exactly as the same time dimension carrying `date_range` with that value. A time dimension carrying Cube's `compareDateRange` SHALL be refused with a typed error naming the key and stating that period comparison is written with the `change`, `change_pct` or `time_shift` metrics.

#### Scenario: dateRange filters like date_range
- **WHEN** the demo query of `orders` with metrics `total_revenue` and `order_count` and time dimension `ordered_at` at month carries `dateRange: ["2026-05-01", "2026-06-30"]`
- **THEN** it returns the same two rows (May and June 2026) and the same SQL as with `date_range`, not every month of history

#### Scenario: compareDateRange is refused with the SLayer equivalent
- **WHEN** a time dimension carries `compareDateRange`
- **THEN** it is refused with a typed error naming `compareDateRange` and pointing to `change`, `change_pct` and `time_shift`

#### Scenario: String shorthand time dimensions are unaffected
- **WHEN** a time dimension is given in its string shorthand
- **THEN** it is accepted and runs as before
