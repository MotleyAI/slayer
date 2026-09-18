# queries/time-dimensions Delta

## MODIFIED Requirements

### Requirement: Re-bucketing a stage column is typed

Planning SHALL fail with a typed error (a `ValueError` subclass naming the time dimension), raised by the checker before any SQL is generated, when a stage time dimension's column is not DATE / TIMESTAMP, when the column has no recorded type, or when the requested granularity does not equal the upstream bucket's granularity and the upstream granularity does not nest into it. Nesting is: `second → minute → hour → day`, `day → week`, `day → week_sunday`, `day → month → quarter → year`, closed under transitivity; every other pair (finer, or non-nesting such as `week` against `month` in either direction, or `week` against `week_sunday`) is rejected. The re-bucketing error MUST name both granularities and the remedy (request the same or a nesting-coarser granularity, or bucket the raw column instead), and MUST be the same message whether the bucketed column belongs to a stage or to a model — it does not refer to an upstream stage. An unknown stage column SHALL raise the existing unknown-reference error listing the stage's columns, and a dotted name against a stage SHALL keep the existing illegal-scope error.

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

#### Scenario: One message for stage and model columns

- **WHEN** a finer time dimension is requested over a stage column bucketed at `month` and over a model column carrying granularity `month`
- **THEN** both fail with the same typed error text, which names both granularities and the remedy and does not mention an upstream stage

## ADDED Requirements

### Requirement: Re-bucketing a bucketed model column is typed

A time dimension over a model column that carries a granularity — a query-backed model's cached bucketed column, or a table-backed column declaring one — SHALL follow the same rule as a stage column: the same granularity re-binds, a granularity the recorded one nests into re-buckets, and a finer or non-nesting granularity is the typed re-bucketing error raised before any SQL is generated. The rule SHALL apply however the column is reached — bare, dotted through a join path, or through a nested query-backed model.

#### Scenario: Finer granularity over a query-backed model column rejected

- **WHEN** a query-backed model `monthly` is saved from a query bucketing `created_at` at `month` with a revenue sum, and a query on `monthly` requests a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed re-bucketing error naming `month`, `day` and the remedy, and no SQL is generated, on SQLite and DuckDB

#### Scenario: Same granularity over a query-backed model column executes

- **WHEN** a query on `monthly` requests a time dimension on `created_at` at `month` with the sum of `rev`
- **THEN** it executes on SQLite and DuckDB and returns the same buckets and values as `monthly` itself

#### Scenario: Coarser granularity over a query-backed model column executes

- **WHEN** a query on `monthly` requests a time dimension on `created_at` at `year` with the sum of `rev`
- **THEN** it executes on SQLite and DuckDB and returns one row per year whose value is the sum of that year's monthly sums

### Requirement: Sibling-stage stand-ins carry the bucket

When a stage of a multi-stage query binds against a sibling stage through a model-shaped scope — a `ModelExtension` over a named sibling, or a join or dotted reference to a sibling — the sibling's bucketed columns SHALL carry their granularity into that scope, so the re-bucketing rule applies exactly as it does for the flat stage-schema binding.

#### Scenario: ModelExtension over a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage whose source is a `ModelExtension` over that sibling declares a time dimension on `created_at` at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `year` executes with correct values

#### Scenario: Stage join to a bucketed sibling rejects finer

- **WHEN** a named stage buckets `created_at` at `month`, and a later stage joins it and declares a time dimension on the dotted sibling column at `day`
- **THEN** planning fails with the typed re-bucketing error, and the same shape at `month` binds
