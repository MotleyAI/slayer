## MODIFIED Requirements

### Requirement: Aggregates across a product hop resolve like unattributable aggregates

An aggregation homed on an unrelated dataset `D` SHALL be counted over `D`'s own rows (restricted by the query's one-sided `D` filters) and SHALL be attributable to no population dimension except a time-spine dimension, which it SHALL attribute through `D`'s own axis. The remaining dimensions SHALL resolve per `to_many_handling`: under `broadcast` the value at the attributable grain repeats across them with the existing `broadcast` warning; when the mode is omitted or `associate`, every cell associates all of `D`'s rows, so it carries the same value with the existing `associated` warning; under `error` the query fails with the existing unattributable-dimension error. An explicit `partition_by` naming a population dimension SHALL follow the existing rule for a dimension unattributable from the aggregate's home. Transforms, nested aggregation, measure-typed filters, order entries and explicitly grained computed dimensions over such aggregates SHALL behave as over any aggregate. In a query omitting `population`, a dataset SHALL be unrelated when no join path connects it to the dataset of any dimension; such an aggregation SHALL never be full-grain (association needs a join path), so its one cell extends to every cell of the dimension anchor (`queries/population` › Rows of an inferred query are the row supply).

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

#### Scenario: An unrelated metric in an inferred query supplies the anchor's cells

- **WHEN** a query without `population` groups by `customers.city` and measures `count(events.id)` alone
- **THEN** the rows are A (5) and B (5): every city of `customers`, the dimension anchor

#### Scenario: A spine query attributes through the unrelated dataset's own axis

- **WHEN** a query with `population: customers` groups by `city` and by `time_spine.timestamp` at month with `date_range: ["2024-01-01", "2024-03-31"]` and measures `count(events.id)`
- **THEN** the rows are A and B for each of January–March, each January row reading 2, each February row 2 and each March row 1

#### Scenario: Partitioning by a population dimension

- **WHEN** the grouped query measures `count(events.id, partition_by=[city])`
- **THEN** under `broadcast` and `error` it fails with the existing error for a `partition_by` key unattributable from the aggregate's home, and omitting the mode or under `associate` each city row carries 5

#### Scenario: Composed aggregates

- **WHEN** the grouped query measures `count(events.id) * 2` and `count(*) / count(events.id)`, filters `count(events.id) > 4`, and orders by `count(events.id)`
- **THEN** both rows survive the measure filter, reading A (10, 0.4) and B (10, 0.2)

### Requirement: Filters across a product hop are one-sided associations

A field-typed filter conjunct whose dependency closure reads only an unrelated dataset `D` SHALL restrict `D`'s rows for every aggregation homed on `D` and SHALL restrict the population by association — in a query omitting `population`, the rows of every aggregation homed elsewhere and of the dimension anchor — so each keeps every row when at least one row of `D` passes and keeps none otherwise. A conjunct reading only the population side (in a query omitting `population`, only datasets with a join path to a dimension) SHALL restrict each unrelated aggregation's rows by association, which keeps all of `D`'s rows when at least one population row passes and none otherwise. A conjunct whose dependency closure reads both an unrelated dataset and anything outside it SHALL fail with a typed error naming the conjunct and the remedy of splitting it into one-sided conjuncts. An unrelated dataset with no rows and no filter on it SHALL leave the population unchanged, its aggregates taking their empty value.

#### Scenario: A filter on the unrelated dataset

- **WHEN** the grouped query adds the filter `events.amount > 25`
- **THEN** the rows are A (2, 3) and B (1, 3)

#### Scenario: A filter on the population

- **WHEN** the grouped query adds the filter `city = 'A'`
- **THEN** the only row is A (2, 5)

#### Scenario: A filter on the unrelated dataset in an inferred query

- **WHEN** a query without `population` groups by `customers.city`, measures `count(customers.*)` and `count(events.id)`, and filters `events.amount > 1000`
- **THEN** it returns no rows

#### Scenario: Mixed conjuncts are refused

- **WHEN** a `customers`-rooted query measuring `count(events.id)` filters `events.amount > customers.id`, or `city = 'A' or events.amount > 100`
- **THEN** it fails with the typed error naming the conjunct and the split remedy

#### Scenario: A filter no unrelated row passes

- **WHEN** the grouped query filters `events.amount > 1000`, and separately the grand-total ratio query does
- **THEN** the grouped query returns no rows, and the grand-total query returns one row with `count(*)` 0, `count(events.id)` 0 and the ratio NULL

#### Scenario: An empty unrelated dataset

- **WHEN** `events` has no rows and the grouped query has no filter
- **THEN** the rows are A (2, 0) and B (1, 0)

### Requirement: Reading an unrelated dataset is announced

Every query stage in which an aggregation crosses a product hop SHALL carry one warning of kind `unrelated_dataset` per unrelated dataset, naming the dataset, the population, the measures homed on it and the stage location, surfaced both as a Python warning and on the response's warnings, whether or not the stage has dimensions. A stage omitting `population` with no dimensions SHALL instead carry one `unrelated_dataset` warning per metric home when its metric homes fall into two or more groups with no join path between them, naming each home and its metrics with the population `null`, and none otherwise. The `broadcast` and `associated` warnings SHALL fire independently of it, exactly as for any unattributable aggregate.

#### Scenario: Dimensionless query

- **WHEN** the grand-total ratio query runs
- **THEN** its warnings hold exactly one `unrelated_dataset` entry, naming `events`, `customers` and the measure `events_per_customer`, and no `broadcast` entry

#### Scenario: Grouped query

- **WHEN** the grouped query runs omitting the mode
- **THEN** its warnings hold one `unrelated_dataset` entry and one `associated` entry, and under `to_many_handling: broadcast` one `unrelated_dataset` entry and one `broadcast` entry

#### Scenario: Dimensionless inferred query over unrelated homes

- **WHEN** a query without `population` and without dimensions measures `count(customers.*)` and `count(events.id)`, and another measures `count(customers.*)` and `count_distinct(customers.city)`
- **THEN** the first carries one `unrelated_dataset` entry for `customers` and one for `events`, each with population `null`; the second carries none

#### Scenario: One entry per dataset and stage

- **WHEN** a stage measures `count(events.id)` and `sum(events.amount)`, and a second stage of the same list measures `count(events.id)`
- **THEN** each stage carries exactly one `unrelated_dataset` entry for `events`, listing its own measures
