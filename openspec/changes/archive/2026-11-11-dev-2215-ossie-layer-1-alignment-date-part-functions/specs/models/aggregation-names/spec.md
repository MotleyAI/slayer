## MODIFIED Requirements

### Requirement: Granularity names are reserved

A custom model aggregation SHALL be rejected at model validation when its name case-insensitively matches a `TimeGranularity` value, with an error naming the reserved granularity set — so the extraction calls `year(col)` … `second(col)` and the `date_trunc` units can never be shadowed by a like-named aggregation.

#### Scenario: aggregation named month rejected

- WHEN a model defines a custom aggregation named `month` (in any case)
- THEN model validation fails with an error naming the reserved time-granularity names

## ADDED Requirements

### Requirement: agg is reserved for measure evaluation
The function name `agg` SHALL be reserved in every case, as Ossie reserves `AGG` for measure evaluation. A Mode-B call `agg(...)` in any spelling and position SHALL fail with a typed error stating that `AGG` is reserved for measure evaluation. A custom model aggregation named `agg` in any case SHALL be rejected when saved; one already stored SHALL be upgraded per `models/stored-upgrade`. A column or saved measure named `agg` SHALL remain legal and be referenced by name as before.

#### Scenario: A call is refused
- **WHEN** a query has the measure `AGG(amount)`
- **THEN** it fails with the typed error stating that `AGG` is reserved for measure evaluation

#### Scenario: A new aggregation is refused
- **WHEN** a model is saved with a custom aggregation named `Agg`
- **THEN** the save fails naming the reserved name `agg`

#### Scenario: A measure or column named agg is legal
- **WHEN** one model has a column `agg`, and another model has the saved measure `agg` with formula `sum(amount)`
- **THEN** both models save, and queries read the column `agg` and the measure `agg` by name
