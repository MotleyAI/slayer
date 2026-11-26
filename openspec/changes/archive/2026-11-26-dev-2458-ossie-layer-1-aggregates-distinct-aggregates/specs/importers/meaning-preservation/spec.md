## ADDED Requirements

### Requirement: DISTINCT and percentile aggregates import with their meaning
Every importer reading aggregate SQL (Cube measures, dbt measure expressions, Ossie dialect entries) SHALL import
`SUM(DISTINCT x)` as `sum_distinct(x)`, `AVG(DISTINCT x)` as `avg_distinct(x)`, `MIN(DISTINCT x)` and
`MAX(DISTINCT x)` as `min(x)` and `max(x)`, `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)` as
`percentile_disc(x, p=p)`, and `APPROX_PERCENTILE(x, p)` as `percentile_approx(x, p=p)`, each with an optional
`FILTER (WHERE …)`. An approximate percentile whose arguments are not one value and one fraction literal in [0, 1] — an
accuracy or weight argument, or a bucket count such as BigQuery's `APPROX_QUANTILES(x, n)` — SHALL be dropped with a
report, as SHALL a DISTINCT over several operands, a DISTINCT in any other aggregate, and a percentile fraction that is
not a numeric literal in [0, 1].

#### Scenario: Cube DISTINCT sum
- **WHEN** a Cube calculated measure is `SUM(DISTINCT {CUBE}.qty)`
- **THEN** it imports as `sum_distinct(qty)` and returns the same value as the source

#### Scenario: Discrete percentile
- **WHEN** a Cube calculated measure is `PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY {CUBE}.amount)`
- **THEN** it imports as `percentile_disc(amount, p=0.9)`

#### Scenario: Approximate percentile on Trino and Snowflake
- **WHEN** a measure's SQL on a Trino and on a Snowflake datasource is `APPROX_PERCENTILE(amount, 0.5)`
- **THEN** each imports as `percentile_approx(amount, p=0.5)`

#### Scenario: Bucketed approximate quantiles are dropped
- **WHEN** a measure's SQL on a BigQuery datasource is `APPROX_QUANTILES(amount, 100)[OFFSET(50)]`
- **THEN** the measure is dropped with a report naming it

#### Scenario: Unsupported DISTINCT stays dropped
- **WHEN** a measure's SQL is `STDDEV(DISTINCT amount)` or `COUNT(DISTINCT a, b)`
- **THEN** the measure is dropped with a report naming it and the construct
