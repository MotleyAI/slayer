## ADDED Requirements

### Requirement: dbt percentile flags import with their meaning
A dbt `percentile` measure, and a metric whose aggregation is a percentile, SHALL import with its `agg_params`: no flag
as `percentile(x, p=…)`, `use_discrete_percentile` as `percentile_disc(x, p=…)`, `use_approximate_percentile` as
`percentile_approx(x, p=…)`, and both flags as `percentile_disc(x, p=…)` — the exact discrete value being within any
approximation of it. A percentile with no `percentile` value SHALL still be dropped with a report.

#### Scenario: Discrete percentile measure
- **WHEN** a dbt measure is `agg: percentile` with `agg_params: {percentile: 0.9, use_discrete_percentile: true}` over `amount`
- **THEN** it imports as `percentile_disc(amount, p=0.9)`

#### Scenario: Approximate percentile measure
- **WHEN** a dbt measure is `agg: percentile` with `agg_params: {percentile: 0.9, use_approximate_percentile: true}` over `amount`
- **THEN** it imports as `percentile_approx(amount, p=0.9)`

#### Scenario: Both flags
- **WHEN** a dbt measure, and a metric over a filtered percentile, set both `use_discrete_percentile` and `use_approximate_percentile` with `percentile: 0.5`
- **THEN** each imports as `percentile_disc` with `p=0.5`, with no drop reported

#### Scenario: Missing percentile value
- **WHEN** a dbt measure is `agg: percentile` with `agg_params: {use_discrete_percentile: true}`
- **THEN** it is dropped with a report naming the measure
