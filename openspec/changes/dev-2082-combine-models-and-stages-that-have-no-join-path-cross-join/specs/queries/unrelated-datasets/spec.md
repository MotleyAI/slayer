## Purpose

How one query combines datasets that no join path connects: an unrelated dataset meets the population as the product `P × D`, read only through aggregates and one-sided filters, so result rows are always the population's and no evaluation is quadratic.

Scenario seed data, unless a scenario says otherwise: `customers` (id 1–3, city A, A, B; no time column) and `events` (id 1–5, amount 10, 20, 30, 40, 50, created_at 2024-01-01, 2024-01-02, 2024-02-01, 2024-02-02, 2024-03-01, kind x, y, x, x, y) in one datasource, with no join between them.

## ADDED Requirements

### Requirement: An unrelated dataset is reached by naming it

In a query frame — the main query and every stage of a query list, including the stored stages of a query-backed model — a reference path whose first hop resolves to no edge and no route from the population SHALL resolve through a product hop to the dataset its token names, when that token names a model visible to the caller or a query-local stage (the stage spelling taking precedence) in the population's datasource. The product hop SHALL pair every population row with every row of that dataset (`P × D`) and determine nothing across it. Every existing edge and route resolution SHALL take precedence, and no route search — short-form routing, route counting, population inference, root recommendation's determination probe — SHALL take a product hop. A path SHALL continue past the product hop over ordinary edges. A token naming no visible dataset of the population's datasource, the time spine, or the population itself SHALL keep its existing error. Stored definitions (`Column.sql`, model filters, saved measures, joins) SHALL keep their existing error for a reference with no join path.

#### Scenario: A grand-total ratio across unrelated models

- **WHEN** a query with `source_model: customers` measures `{"formula": "count(events.id) / count(*)", "name": "events_per_customer"}`
- **THEN** one row is returned with `events_per_customer` = 5 / 3, equal to dividing the two separate single-model answers

#### Scenario: A path continues past the product hop

- **WHEN** `events` joins `users` many-to-one and a query rooted at `customers` measures `count(events.users.id)`
- **THEN** the value equals that of an `events`-rooted query measuring `count(users.id)`

#### Scenario: A query-local stage is an unrelated dataset

- **WHEN** a query list has stages `c` (`source_model: customers`, measure `count(*)` named `nc`) and `e` (`source_model: events`, measure `count(*)` named `ne`) and a final stage `{"source_model": "c", "dimensions": ["nc"], "measures": [{"formula": "max(e.ne)", "name": "ne"}]}`
- **THEN** exactly one row (nc 3, ne 5) is returned

#### Scenario: References in every position are found

- **WHEN** the only reference to `events` in a `customers`-rooted query sits in a filter, an order entry, or an aggregation's `partition_by=[]` measure
- **THEN** it resolves through the product hop exactly as it would in a measure

#### Scenario: Existing paths and routing are unchanged

- **WHEN** a query references a model that a join path or a unique short-form route reaches
- **THEN** it resolves exactly as before this change, and no product hop is taken

#### Scenario: Names that are not reachable datasets keep their errors

- **WHEN** a `customers`-rooted query references `evnts.id` (no such model), a model of another datasource, a model hidden from the caller by its access context, or `customers.id` spelled through the population's own name
- **THEN** each fails or resolves exactly as before this change; a hidden model is reported as unknown

#### Scenario: Row-level security scopes the unrelated dataset

- **WHEN** an engine-level tenant policy restricts both models to one tenant, owning customers 1–2 and events 1–3, and the grand-total ratio query runs
- **THEN** the ratio is 3 / 2

#### Scenario: Stored definitions keep refusing

- **WHEN** a saved measure on `customers` reads `count(events.id)`, or a `customers` column's `sql` reads `events.amount`
- **THEN** saving or using it fails with the existing no-join-path error

### Requirement: Aggregates across a product hop resolve like unattributable aggregates

An aggregation homed on an unrelated dataset `D` SHALL be counted over `D`'s own rows (restricted by the query's one-sided `D` filters) and SHALL be attributable to no population dimension except a time-spine dimension, which it SHALL attribute through `D`'s own axis. The remaining dimensions SHALL resolve per `to_many_handling`: under `broadcast` the value at the attributable grain repeats across them with the existing `broadcast` warning; under `associate` every cell associates all of `D`'s rows, so it carries the same value with the existing `associated` warning; under `error` the query fails with the existing unattributable-dimension error. An explicit `partition_by` naming a population dimension SHALL follow the existing rule for a dimension unattributable from the aggregate's home. Transforms, nested aggregation, measure-typed filters, order entries and explicitly grained computed dimensions over such aggregates SHALL behave as over any aggregate.

#### Scenario: Grouped query broadcasts the unrelated total

- **WHEN** a `customers`-rooted query groups by `city` and measures `count(*)` and `count(events.id)`
- **THEN** the rows are A (2, 5) and B (1, 5), with a `broadcast` warning naming `count(events.id)` and `city`

#### Scenario: Associate mode gives the same total

- **WHEN** the grouped query sets `to_many_handling: associate`
- **THEN** the rows are A (2, 5) and B (1, 5), with an `associated` warning and no `broadcast` warning

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
- **THEN** it fails with the existing error for a `partition_by` key unattributable from the aggregate's home, outside `associate` mode

#### Scenario: Composed aggregates

- **WHEN** the grouped query measures `count(events.id) * 2` and `count(*) / count(events.id)`, filters `count(events.id) > 4`, and orders by `count(events.id)`
- **THEN** both rows survive the measure filter, reading A (10, 0.4) and B (10, 0.2)

### Requirement: Other row-level reads across a product hop fail closed

A row-level read of an unrelated dataset that is neither beneath an aggregation whose input lies wholly on that dataset's side nor a one-sided field filter SHALL fail with a typed query type error naming the dataset, the reference, the population, and the remedy of aggregating it (for a one-row stage, `max(<stage>.<column>)`). This SHALL cover dimensions, time dimensions, row-level operands of computed dimensions, row-level order keys, and raw-row (`distinct_dimension_values: false`) queries, and SHALL be judged on dependency closures, so a derived column or saved definition reached through the product hop is classified by everything its definition reads. A row-level combination of population and unrelated columns inside one aggregation SHALL fail with the existing input-safety error. A join declared with empty `join_pairs` SHALL remain invalid, with a message naming the same aggregate remedy.

#### Scenario: Unrelated dimension

- **WHEN** a `customers`-rooted query groups by `city` and `events.kind`
- **THEN** it fails with the typed error naming `events`, `events.kind`, `customers` and the aggregate remedy

#### Scenario: Other row-level positions

- **WHEN** a `customers`-rooted query has the time dimension `events.created_at` at month, the computed dimension `{"expression": "events.amount * 2", "name": "a2"}`, the order key `events.amount`, or `distinct_dimension_values: false` with dimension `events.kind`
- **THEN** each fails with the same typed error

#### Scenario: Mixed operands inside one aggregation

- **WHEN** a `customers`-rooted query measures `sum(events.amount * customers.id)`
- **THEN** it fails with the existing input-safety error

#### Scenario: Unrelated derived column in an aggregation

- **WHEN** `events` declares `amount_x2` with `sql: amount * 2` and a `customers`-rooted query measures `sum(events.amount_x2)`
- **THEN** the value is 300

#### Scenario: Empty join pairs

- **WHEN** a stage's `source_model` extension declares a join with `join_pairs: []`
- **THEN** validation fails with a message naming the aggregate remedy

### Requirement: Filters across a product hop are one-sided associations

A field-typed filter conjunct whose dependency closure reads only an unrelated dataset `D` SHALL restrict `D`'s rows for every aggregation homed on `D` and SHALL restrict the population by association, so the population keeps every row when at least one row of `D` passes and keeps none otherwise. A conjunct reading only the population side SHALL restrict each unrelated aggregation's rows by association, which keeps all of `D`'s rows when at least one population row passes and none otherwise. A conjunct whose dependency closure reads both an unrelated dataset and anything outside it SHALL fail with a typed error naming the conjunct and the remedy of splitting it into one-sided conjuncts. An unrelated dataset with no rows and no filter on it SHALL leave the population unchanged, its aggregates taking their empty value.

#### Scenario: A filter on the unrelated dataset

- **WHEN** the grouped query adds the filter `events.amount > 25`
- **THEN** the rows are A (2, 3) and B (1, 3)

#### Scenario: A filter on the population

- **WHEN** the grouped query adds the filter `city = 'A'`
- **THEN** the only row is A (2, 5)

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

Every query stage in which an aggregation crosses a product hop SHALL carry one warning of kind `unrelated_dataset` per unrelated dataset, naming the dataset, the population, the measures homed on it and the stage location, surfaced both as a Python warning and on the response's warnings, whether or not the stage has dimensions. The `broadcast` and `associated` warnings SHALL fire independently of it, exactly as for any unattributable aggregate.

#### Scenario: Dimensionless query

- **WHEN** the grand-total ratio query runs
- **THEN** its warnings hold exactly one `unrelated_dataset` entry, naming `events`, `customers` and the measure `events_per_customer`, and no `broadcast` entry

#### Scenario: Grouped query

- **WHEN** the grouped query runs
- **THEN** its warnings hold one `unrelated_dataset` entry and one `broadcast` entry

#### Scenario: One entry per dataset and stage

- **WHEN** a stage measures `count(events.id)` and `sum(events.amount)`, and a second stage of the same list measures `count(events.id)`
- **THEN** each stage carries exactly one `unrelated_dataset` entry for `events`, listing its own measures

### Requirement: Product hops never multiply rows or work

A query reading unrelated datasets SHALL return exactly the rows its population defines, and each of its values SHALL equal the value of the same measure computed alone, regardless of how many unrelated datasets or aggregations it reads and in every `to_many_handling` mode. No emitted relation SHALL join an unrelated dataset's rows with the population's rows or with another unrelated dataset's rows; an unrelated dataset SHALL appear only in its own aggregate producers and in uncorrelated existence tests, and no reference to it SHALL be correlated with a column outside it.

#### Scenario: Three unrelated datasets

- **WHEN** in a separate datasource `customers` has 200 rows over 7 cities, three unrelated models hold 1,000, 3 and 0 rows, and a `customers`-rooted query grouped by `city` measures two aggregates over each of them plus `count(*)`, under `broadcast` and under `associate`
- **THEN** each returns exactly 7 rows, and every value equals the same measure queried alone

#### Scenario: Emitted SQL never pairs unrelated rows

- **WHEN** the three-dataset query, an associate-mode query, a spine query, and a query with one-sided filters on both sides are compiled
- **THEN** each unrelated table occurs only inside its own producers and uncorrelated `EXISTS` subqueries, never in the population's relation, another dataset's producer, or a correlated subquery
