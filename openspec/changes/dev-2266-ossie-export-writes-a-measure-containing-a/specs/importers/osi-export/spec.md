## MODIFIED Requirements

### Requirement: Metrics

A measure SHALL export as an Ossie metric when every aggregate in it reads exactly one model and, for a measure reading several models, each of them is reachable from the measure's model by exactly one join route. Each aggregate SHALL be evaluated over the rows of the one model it reads, which is Ossie's component semantics. A measure whose aggregates all read one model SHALL export as a dataset-scoped metric on that model's dataset, with unqualified field names, and its references to other measures SHALL be expanded; any other exportable measure SHALL export as a model-scoped metric with `dataset.field` references, and its references to measures SHALL be written as `AGG(dataset.metric)` of the referenced measure's exported dataset metric. A stored model-scoped metric SHALL export as a top-level metric unchanged; one that Ossie does not define (it refers to another model-scoped metric) SHALL travel only in the `SLAYER` extension. Metric expressions SHALL be standard portable SQL (`OSSIE_SQL_2026` in `sql`); an approximate distinct count, which has no portable form, SHALL be written only as a datasource-dialect entry, and so SHALL a measure containing a bucket of a datasource granularity, under "Time buckets in metrics export as standard SQL". Generated metric names SHALL be unique in their scope. An identifier that would be ambiguous under case-insensitive matching SHALL be double-quoted.

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

#### Scenario: Boolean sum
- **WHEN** a measure is `sum(is_paid)` over a BOOLEAN column
- **THEN** the metric counts the true values on every supported database

## ADDED Requirements

### Requirement: Time buckets in metrics export as standard SQL

A time bucket inside a measure SHALL export in the metric's `sql` as `DATE_TRUNC('<UNIT>', CAST(<expr> AS TIMESTAMP))` for every built-in unit other than `week_sunday`, wrapped in `CAST(… AS DATE)` when the bucket is a DATE (a day-or-coarser bucket of a DATE expression), so that the exported value has the bucket's type on every engine. A `week_sunday` bucket SHALL export as the Monday week of the following day, one day back, written with `DATE_TRUNC('WEEK', …)` and `DATEADD(DAY, …)`. A `date_add` inside a measure SHALL export as `DATEADD(<PART>, <count>, <expr>)`. No exported metric `sql` SHALL contain an `INTERVAL` literal. A measure containing a bucket of a datasource granularity SHALL export only as a datasource-dialect entry holding the datasource's own rendering of the measure; when that rendering calls a function SLayer registers on its own database connections, the measure SHALL instead be SLayer-only and reported as such. Querying the re-imported measure, with or without extensions, SHALL return the original measure's value, grouped and ungrouped.

#### Scenario: Month bucket of a timestamp
- **WHEN** `orders` has the measure `months: count_distinct(date_trunc('month', ordered_at))` over a TIMESTAMP `ordered_at`
- **THEN** metric `months` has `sql: COUNT(DISTINCT DATE_TRUNC('MONTH', CAST(ordered_at AS TIMESTAMP)))`, and the measure re-imported without extensions returns the original value on DuckDB and SQLite

#### Scenario: Day-or-coarser bucket of a date
- **WHEN** `orders` has the measure `last_month: max(date_trunc('month', ordered_on))` over a DATE `ordered_on`
- **THEN** metric `last_month` has `sql: MAX(CAST(DATE_TRUNC('MONTH', CAST(ordered_on AS TIMESTAMP)) AS DATE))`

#### Scenario: Every built-in unit
- **WHEN** a measure is `max(date_trunc('<unit>', x))` for each built-in unit other than `week_sunday`, with `x` a TIMESTAMP and with `x` a DATE
- **THEN** each metric's `sql` is the `DATE_TRUNC` form of that unit, cast to DATE exactly when `x` is a DATE and the unit is `day` or coarser

#### Scenario: Bucket under FILTER and inside a scalar call
- **WHEN** `orders` has the measures `january: count(*) FILTER (WHERE date_trunc('month', ordered_at) = '2026-01-01')` and `last_day: max(coalesce(date_trunc('day', ordered_at), ordered_at))`
- **THEN** both export as metrics whose `sql` holds the `DATE_TRUNC` form, and both measures re-imported without extensions return the original values on DuckDB and SQLite

#### Scenario: Bucket in a model-scoped metric
- **WHEN** `orders` joins `customers` many-to-one and has the measure `months_per_customer: count_distinct(date_trunc('month', ordered_at)) / count_distinct(customers.id)`
- **THEN** the document has the model-scoped metric `COUNT(DISTINCT DATE_TRUNC('MONTH', CAST(orders.ordered_at AS TIMESTAMP))) / COUNT(DISTINCT customers.id)`, and the re-imported measure returns the original value

#### Scenario: Exported buckets keep their type
- **WHEN** the exported bucket expressions of a DATE column at `day`, `month` and `hour` are read with the importer's portable reader and evaluated on DuckDB and on Postgres, the Postgres session running with a non-UTC `TimeZone`
- **THEN** the `day` and `month` buckets evaluate to DATE values and the `hour` bucket to a TIMESTAMP value at the date's midnight

#### Scenario: Sunday week
- **WHEN** `orders` has the measure `weeks: count_distinct(date_trunc('week_sunday', ordered_at))`
- **THEN** metric `weeks` has portable `sql` built from `DATE_TRUNC('WEEK', …)` and `DATEADD(DAY, …)` with no `INTERVAL`, and the measure re-imported without extensions returns the original value on DuckDB and SQLite

#### Scenario: Date addition
- **WHEN** `orders` has the measure `next_day: max(date_add(ordered_at, 1, 'day'))`
- **THEN** metric `next_day` has `sql` calling `DATEADD(DAY, 1, …)` with no `INTERVAL`, and the measure re-imported without extensions returns the original value on DuckDB and SQLite

#### Scenario: Extraction keeps exporting as DATE_PART
- **WHEN** `orders` has the measure `month_numbers: count_distinct(month(ordered_at))`
- **THEN** metric `month_numbers` has `sql: COUNT(DISTINCT CAST(DATE_PART(MONTH, ordered_at) AS INT))`, and the measure re-imported without extensions returns the original value on DuckDB and SQLite

#### Scenario: Custom granularity on DuckDB
- **WHEN** a DuckDB datasource defines the granularity `fiscal_year` (`base: month`, `multiple: 12`, `origin: 2024-04-01`) and `orders` has the measure `fiscal_years: count_distinct(date_trunc('fiscal_year', ordered_on))`
- **THEN** metric `fiscal_years` has no `sql` and one `DUCKDB` dialect entry, and the re-imported measure returns the original value with and without extensions

#### Scenario: Custom granularity on SQLite
- **WHEN** a SQLite datasource defines the granularity `fiscal_year` and `orders` has the measure `fiscal_years: count_distinct(date_trunc('fiscal_year', ordered_on))`
- **THEN** no metric `fiscal_years` is exported, `orders`' extension carries the measure, and the report lists it as SLayer-only
