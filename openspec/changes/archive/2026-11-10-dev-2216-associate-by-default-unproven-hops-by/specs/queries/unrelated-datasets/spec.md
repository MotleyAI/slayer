## MODIFIED Requirements

### Requirement: Aggregates across a product hop resolve like unattributable aggregates

An aggregation homed on an unrelated dataset `D` SHALL be counted over `D`'s own rows (restricted by the query's one-sided `D` filters) and SHALL be attributable to no population dimension except a time-spine dimension, which it SHALL attribute through `D`'s own axis. The remaining dimensions SHALL resolve per `to_many_handling`: under `broadcast` the value at the attributable grain repeats across them with the existing `broadcast` warning; when the mode is omitted or `associate`, every cell associates all of `D`'s rows, so it carries the same value with the existing `associated` warning; under `error` the query fails with the existing unattributable-dimension error. An explicit `partition_by` naming a population dimension SHALL follow the existing rule for a dimension unattributable from the aggregate's home. Transforms, nested aggregation, measure-typed filters, order entries and explicitly grained computed dimensions over such aggregates SHALL behave as over any aggregate.

#### Scenario: Grouped query broadcasts the unrelated total

- **WHEN** a `customers`-rooted query with `to_many_handling: broadcast` groups by `city` and measures `count(*)` and `count(events.id)`
- **THEN** the rows are A (2, 5) and B (1, 5), with a `broadcast` warning naming `count(events.id)` and `city`

#### Scenario: Associate mode gives the same total

- **WHEN** the grouped query omits the mode, and again sets `to_many_handling: associate`
- **THEN** in both runs the rows are A (2, 5) and B (1, 5), with an `associated` warning and no `broadcast` warning

#### Scenario: Error mode refuses a population dimension

- **WHEN** the grouped query sets `to_many_handling: error`
- **THEN** it fails with the existing unattributable-dimension error naming `count(events.id)` and `city`

#### Scenario: Error mode accepts a dimensionless query

- **WHEN** the grand-total ratio query sets `to_many_handling: error`
- **THEN** it returns 5 / 3

#### Scenario: A spine query attributes through the unrelated dataset's own axis

- **WHEN** a query with `source_model: customers` groups by `city` and by `time_spine.timestamp` at month with `date_range: ["2024-01-01", "2024-03-31"]` and measures `count(events.id)`
- **THEN** the rows are A and B for each of January–March, each January row reading 2, each February row 2 and each March row 1

#### Scenario: Partitioning by a population dimension

- **WHEN** the grouped query measures `count(events.id, partition_by=[city])`
- **THEN** under `broadcast` and `error` it fails with the existing error for a `partition_by` key unattributable from the aggregate's home, and omitting the mode or under `associate` each city row carries 5

#### Scenario: Composed aggregates

- **WHEN** the grouped query measures `count(events.id) * 2` and `count(*) / count(events.id)`, filters `count(events.id) > 4`, and orders by `count(events.id)`
- **THEN** both rows survive the measure filter, reading A (10, 0.4) and B (10, 0.2)

### Requirement: Reading an unrelated dataset is announced

Every query stage in which an aggregation crosses a product hop SHALL carry one warning of kind `unrelated_dataset` per unrelated dataset, naming the dataset, the population, the measures homed on it and the stage location, surfaced both as a Python warning and on the response's warnings, whether or not the stage has dimensions. The `broadcast` and `associated` warnings SHALL fire independently of it, exactly as for any unattributable aggregate.

#### Scenario: Dimensionless query

- **WHEN** the grand-total ratio query runs
- **THEN** its warnings hold exactly one `unrelated_dataset` entry, naming `events`, `customers` and the measure `events_per_customer`, and no `broadcast` entry

#### Scenario: Grouped query

- **WHEN** the grouped query runs omitting the mode
- **THEN** its warnings hold one `unrelated_dataset` entry and one `associated` entry, and under `to_many_handling: broadcast` one `unrelated_dataset` entry and one `broadcast` entry

#### Scenario: One entry per dataset and stage

- **WHEN** a stage measures `count(events.id)` and `sum(events.amount)`, and a second stage of the same list measures `count(events.id)`
- **THEN** each stage carries exactly one `unrelated_dataset` entry for `events`, listing its own measures
