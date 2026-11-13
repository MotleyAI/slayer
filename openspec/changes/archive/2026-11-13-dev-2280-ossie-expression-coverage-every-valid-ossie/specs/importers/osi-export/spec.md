## ADDED Requirements

### Requirement: Instant comparisons export as standard SQL

A comparison of a temporal value with an instant — a date-time literal or a date-only literal, which is the instant at its midnight — SHALL export inside a metric as a standard-SQL comparison with the same operator and literal, so the metric is not dropped as SLayer-only on that account. A comparison with a period literal other than a date-only one, a single-string `in` / `not in`, or a relative token SHALL remain SLayer-only. Querying the re-imported metric SHALL return the value of the original measure.

#### Scenario: Date-only bound in a filtered measure

- **WHEN** `ev` has the measure `early: count(*) FILTER (WHERE ts <= '2024-04-10')` over rows at `2024-03-05 08:00` and `2024-04-10 09:30`
- **THEN** dataset `ev` has the metric `early` whose `sql` compares `ts <= '2024-04-10'`, and the re-imported measure returns 1, as the original does

#### Scenario: Date-time bound

- **WHEN** a measure is `count(*) FILTER (WHERE ts > '2024-04-10 09:00:00')`
- **THEN** it exports as a metric with that comparison

#### Scenario: Period and relative comparisons stay SLayer-only

- **WHEN** measures are `count(*) FILTER (WHERE ts in '2024-04-10')`, `count(*) FILTER (WHERE ts = '2025-Q1')` and `count(*) FILTER (WHERE ts >= 'last month')`, exported without extensions
- **THEN** none is exported as a metric, and each is reported as SLayer-only
