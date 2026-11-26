## MODIFIED Requirements

### Requirement: Metrics

A measure SHALL export as an Ossie metric when every aggregate in it reads exactly one model and, for a measure reading several models, each of them is reachable from the measure's model by exactly one join route. Each aggregate SHALL be evaluated over the rows of the one model it reads, which is Ossie's component semantics. A measure whose aggregates all read one model SHALL export as a dataset-scoped metric on that model's dataset, with unqualified field names, and its references to other measures SHALL be expanded; any other exportable measure SHALL export as a model-scoped metric with `dataset.field` references, and its references to measures SHALL be written as `AGG(dataset.metric)` of the referenced measure's exported dataset metric. A stored model-scoped metric SHALL export as a top-level metric unchanged; one that Ossie does not define (it refers to another model-scoped metric) SHALL travel only in the `SLAYER` extension. Metric expressions SHALL be standard portable SQL (`OSSIE_SQL_2026` in `sql`); `sum_distinct` SHALL be written `SUM(DISTINCT …)` and `percentile_disc` `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY …)`; an approximate distinct count and an approximate percentile, which have no portable value, and a DISTINCT average, which Ossie does not define, SHALL be written only as a datasource-dialect entry, and so SHALL a measure containing a bucket of a datasource granularity, under "Time buckets in metrics export as standard SQL". A datasource-dialect entry SHALL be written only when it binds as a SLayer expression and calls no function SLayer registers on its own database connections; otherwise the measure SHALL be SLayer-only and reported as such. Generated metric names SHALL be unique in their scope. An identifier that would be ambiguous under case-insensitive matching SHALL be double-quoted.

#### Scenario: Dataset-scoped metric
- **WHEN** `orders` has the measure `revenue: sum(amount)`
- **THEN** dataset `orders` has the metric `revenue` with `sql: SUM(amount)`

#### Scenario: Measure over another model
- **WHEN** `orders` has the measure `customer_discount: sum(customers.discount)` and `orders` joins `customers` many-to-one
- **THEN** dataset `customers` has a metric with `sql: SUM(discount)`, and querying the re-imported measure on `orders` returns the sum over all customers, including customers with no orders

#### Scenario: Cross-model ratio
- **WHEN** `orders` has the measure `aov_per_customer: sum(amount) / count_distinct(customers.id)`
- **THEN** the document has a model-scoped metric `SUM(orders.amount) / COUNT(DISTINCT customers.id)`

#### Scenario: Measure reference expanded
- **WHEN** `orders` has measures `revenue: sum(amount)`, `n: count(*)` and `aov: revenue / n`
- **THEN** metric `aov` has `sql: SUM(amount) / COUNT(*)`

#### Scenario: Model-scoped reference to a dataset metric uses AGG
- **WHEN** `orders` has measures `revenue: sum(amount)` and `revenue_per_customer: revenue / count_distinct(customers.id)`
- **THEN** the document has a model-scoped metric `AGG(orders.revenue) / COUNT(DISTINCT customers.id)`

#### Scenario: Percentile on SQLite
- **WHEN** a SQLite datasource's model has the measure `p90: percentile(amount, 0.9)`
- **THEN** the metric's `sql` is `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)`

#### Scenario: DISTINCT sum and discrete percentile are portable
- **WHEN** a model has the measures `q: sum_distinct(qty)` and `p25: percentile_disc(amount, 0.25)`
- **THEN** metric `q` has `sql: SUM(DISTINCT qty)` and metric `p25` has `sql: PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY amount)`, and both re-import with their original values on DuckDB and SQLite

#### Scenario: DISTINCT average and approximate percentile are dialect entries
- **WHEN** a DuckDB datasource's model has the measures `a: avg_distinct(qty)` and `m: percentile_approx(amount, 0.5)`
- **THEN** neither metric has portable `sql`, each has a datasource-dialect entry, and both re-import

#### Scenario: Boolean sum
- **WHEN** a measure is `sum(is_paid)` over a BOOLEAN column
- **THEN** the metric counts the true values on every supported database

