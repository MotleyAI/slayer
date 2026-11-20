## MODIFIED Requirements

### Requirement: Re-bucketing a stage column is typed

Planning SHALL fail with a typed error (a `ValueError` subclass naming the time dimension), raised by the checker before any SQL is generated, when a stage time dimension's column is not DATE / TIMESTAMP, or when the requested granularity does not equal the upstream bucket's granularity and the upstream granularity does not nest into it. Every stage column carries a type (per `queries/multi-stage` › Every stage column carries a type). Nesting is: `second → minute → hour → day`, `day → week`, `day → week_sunday`, `day → month → quarter → year`, closed under transitivity; every other pair (finer, or non-nesting such as `week` against `month` in either direction, or `week` against `week_sunday`) is rejected. The re-bucketing error MUST name both granularities and the remedy (request the same or a nesting-coarser granularity, or bucket the raw column instead), and MUST be the same message whether the bucketed column belongs to a stage or to a model — it does not refer to an upstream stage. An unknown stage column SHALL raise the existing unknown-reference error listing the stage's columns, and a dotted name against a stage SHALL keep the existing illegal-scope error.

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

- **WHEN** the outer stage declares a time dimension on an inner-stage column that projects only `null` (a stage column always carries a type; this one is INT)
- **THEN** planning fails with the temporal-column typed error naming the observed type INT

#### Scenario: Unknown and dotted names keep their binder errors

- **WHEN** the outer stage declares a time dimension on a name the inner stage does not project, or on a dotted name
- **THEN** planning fails with the unknown-reference error listing the stage's columns, or the illegal-scope error, respectively

#### Scenario: One message for stage and model columns

- **WHEN** a finer time dimension is requested over a stage column bucketed at `month` and over a model column carrying granularity `month`
- **THEN** both fail with the same typed error text, which names both granularities and the remedy and does not mention an upstream stage
