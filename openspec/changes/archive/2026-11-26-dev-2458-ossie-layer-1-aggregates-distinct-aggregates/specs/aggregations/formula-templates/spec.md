## REMOVED Requirements

### Requirement: Median and percentile on Trino are approximate
**Reason**: Trino emits the exact interpolated percentile, and every Tier-1 database now returns the exact value
(`aggregations/distinct-and-percentiles` › Percentiles are exact on every Tier-1 database).
**Migration**: None for exact results; an author who wants Trino's approximation writes `percentile_approx(x, p)` or
`APPROX_PERCENTILE(x, p)`.
