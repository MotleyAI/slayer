# queries/time-dimensions Delta

## ADDED Requirements

### Requirement: Time dimensions bind on stage datasets

A `TimeDimension` in a downstream stage of a multi-stage query SHALL bind against any column of its upstream stage's schema whose type is DATE or TIMESTAMP, exactly as it binds against a model column: the stage column is bucketed at the requested granularity, the result key is the stage-prefixed column name, and the bucket takes part in the stage's grain alongside its plain dimensions. A column the upstream stage already bucketed SHALL re-bucket at the same granularity (idempotent) or at a coarser granularity the upstream one nests into. A column the upstream stage did not bucket — a raw temporal column projected as a plain dimension, or an aggregate output such as the `max` of a timestamp, including the auto-named `partition_by` form — SHALL accept every granularity. The binding SHALL work at any depth of the stage chain and for multi-hop flat names.

#### Scenario: Same granularity re-binds a multi-hop flat name

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` and the outer stage declares a time dimension on the flat name `customers__regions__last_activity_at` at `month`
- **THEN** the outer stage groups by that column's month bucket, its result key is the stage-prefixed flat name, and the executed values equal the upstream buckets

#### Scenario: Coarser granularity re-buckets

- **WHEN** an inner stage buckets `created_at` at `month` with a revenue sum, and the outer stage declares a time dimension on `created_at` at `year` with the sum of the inner sums
- **THEN** the outer stage returns one row per year whose value is the sum of that year's monthly sums, by executed values on SQLite and DuckDB

#### Scenario: Raw temporal column projected by the inner stage

- **WHEN** an inner stage projects `created_at` as a plain dimension and the outer stage declares a time dimension on `created_at` at `month`
- **THEN** the outer stage groups by month with correct executed values

#### Scenario: Aggregate-output timestamp with the partition_by auto-name and a same-column filter

- **WHEN** an inner stage grouped by `customer_id` selects `ordered_at:max(partition_by=customer_id)` (auto-named `ordered_at_max_partition_by_customer_id`), and the outer stage declares a time dimension on that column at `month`, a filter `ordered_at_max_partition_by_customer_id >= '<date>'` and a count
- **THEN** the query executes on SQLite and DuckDB, returns one row per cohort month with the count of customers whose last order falls in that month and after the filter date, and the same query binds at every granularity, `day` included, with no re-bucketing error

#### Scenario: Three-stage chain

- **WHEN** stage one buckets `created_at` at `month`, stage two declares a time dimension on `created_at` at `month` over stage one, and stage three declares one at `year` over stage two
- **THEN** stage three returns one row per year with correct executed values

#### Scenario: Stage time dimension joins the stage grain

- **WHEN** an outer stage declares a plain stage dimension and a stage time dimension together
- **THEN** the result has one row per (dimension value, bucket) with correct executed values

### Requirement: Re-bucketing a stage column is typed

Planning SHALL fail with a typed error (a `ValueError` subclass naming the time dimension), raised by the checker before any SQL is generated, when a stage time dimension's column is not DATE / TIMESTAMP, when the column has no recorded type, or when the requested granularity does not equal the upstream bucket's granularity and the upstream granularity does not nest into it. Nesting is: `second → minute → hour → day`, `day → week`, `day → week_sunday`, `day → month → quarter → year`, closed under transitivity; every other pair (finer, or non-nesting such as `week` against `month` in either direction, or `week` against `week_sunday`) is rejected. The re-bucketing error MUST name both granularities and the remedy (request the same or a nesting-coarser granularity, or bucket the raw column in the upstream stage). An unknown stage column SHALL raise the existing unknown-reference error listing the stage's columns, and a dotted name against a stage SHALL keep the existing illegal-scope error.

#### Scenario: Finer granularity rejected

- **WHEN** an inner stage buckets `created_at` at `month` and the outer stage declares a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed error naming `month`, `day` and the remedy, and no SQL is generated

#### Scenario: Non-nesting granularities rejected in both directions

- **WHEN** an inner stage buckets `created_at` at `month` and the outer stage requests `week`, or the inner stage buckets at `week` and the outer requests `month`
- **THEN** planning fails with the same typed error naming both granularities

#### Scenario: Non-temporal stage column rejected

- **WHEN** the outer stage declares a time dimension on a TEXT column of the inner stage
- **THEN** planning fails with the typed error stating the column must be temporal (DATE / TIMESTAMP) and naming the observed type

#### Scenario: Untyped stage column fails closed

- **WHEN** the outer stage declares a time dimension on an inner-stage column whose type is not recorded
- **THEN** planning fails with the temporal-column typed error rather than binding

#### Scenario: Unknown and dotted names keep their binder errors

- **WHEN** the outer stage declares a time dimension on a name the inner stage does not project, or on a dotted name
- **THEN** planning fails with the unknown-reference error listing the stage's columns, or the illegal-scope error, respectively

### Requirement: Functional surfaces apply to stage time dimensions

A `gran(col)` order entry over a stage time dimension SHALL sort by that bucket, and two stage time dimensions on one stage column at different granularities SHALL carry granularity-suffixed result keys, identically to the model-scope behaviour.

#### Scenario: Order by the stage bucket

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` and orders by `"month(created_at)"` descending
- **THEN** rows sort by the month bucket descending, identically to ordering by `created_at`

#### Scenario: Two granularities of one stage column

- **WHEN** an outer stage declares time dimensions on `created_at` at `month` and at `year`
- **THEN** the result has keys `<stage>.created_at.month` and `<stage>.created_at.year` with correctly bucketed values
