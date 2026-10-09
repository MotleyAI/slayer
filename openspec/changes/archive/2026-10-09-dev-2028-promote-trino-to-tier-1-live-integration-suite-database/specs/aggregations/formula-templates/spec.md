## ADDED Requirements

### Requirement: Median and percentile on Trino are approximate
On a `trino` datasource, `median` and `percentile(p=...)` SHALL return Trino's approximate
percentile of the aggregated values (`APPROX_PERCENTILE`), which need not equal the interpolated
`PERCENTILE_CONT` value the other Tier-1 databases return.

#### Scenario: Result equals Trino's own approximate percentile
- **WHEN** `percentile(amount, p=0.9)` and `median(amount)` run on Trino
- **THEN** the results equal `APPROX_PERCENTILE(amount, 0.9)` and `APPROX_PERCENTILE(amount, 0.5)`
  computed directly by Trino over the same rows

#### Scenario: Approximation differs from interpolation
- **WHEN** `percentile(amount, p=0.9)` runs on Trino over the amounts 100, 200, 50, 150, 300, 25
  and 10.01
- **THEN** the result is 300, where the interpolated value would be 240
