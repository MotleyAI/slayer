## Purpose

Defines when a derived column's definition — its `Column.sql` expression or its
`Column.filter` value mask — is well-formed with respect to the join hops it
crosses from its declaring model, and how a malformed definition is reported at
model save time.

## ADDED Requirements

### Requirement: A derived column may reference only provably to-one targets

A derived `Column.sql` or `Column.filter` reference SHALL be a function of its
declaring model's row. When such a reference's join path from the declaring
model provably crosses a fanning hop — a hop that is not provably many-to-one
and whose declared cardinality in its traversal orientation is `one_to_many` or
`many_to_many` — the model save SHALL be rejected with an error naming the
column, the hop, and the remedy (aggregate the target column as `<hop>.<column>:<aggregation>`,
or filter by it; declare a to-one cardinality or a covering unique key if the
hop is really to-one). A hop that is provably many-to-one — its declared
cardinality is `many_to_one`/`one_to_one`, or its target-side join columns cover
a unique key of the target — SHALL be accepted with no error and no warning,
even if its declared cardinality is contradictorily `one_to_many` (the proof
takes precedence over the declaration).

#### Scenario: sql reference across a declared one-to-many hop is rejected
- **WHEN** a model with a join declared `one_to_many` to a loaded target is saved with a derived column whose `sql` references a column on that target
- **THEN** the save is rejected with an error naming the column, the hop, and the aggregate-or-filter remedy

#### Scenario: filter reference across a declared one-to-many hop is rejected
- **WHEN** a model with a join declared `one_to_many` to a loaded target is saved with a column whose `filter` references a column on that target
- **THEN** the save is rejected with the same error

#### Scenario: to-one reference is accepted silently
- **WHEN** a model is saved with a derived column referencing a target across a hop that is provably many-to-one (declared `many_to_one`/`one_to_one`, or the target-side join columns cover a unique key)
- **THEN** the save succeeds with no error and no warning

#### Scenario: proof beats a contradictory to-many declaration
- **WHEN** a hop is declared `one_to_many` but its target-side join columns cover a unique key of the target
- **THEN** the hop is treated as provably to-one and a derived column crossing it is accepted with no error

### Requirement: Unproven references are accepted with a save-time warning and a query-time backstop

When a derived `Column.sql` / `Column.filter` reference's path crosses a hop
that is neither provably many-to-one nor provably fanning — an undeclared hop,
or a hop whose only proof is that its reverse orientation covers the source's
unique key — the model save SHALL succeed and SHALL emit a warning naming the
column and the hop. Such a column is refused at query time by the input-safety
gate when it is aggregated as a column of its declaring model (the permanent
backstop). A reference whose target model is not loaded, does not resolve, or is
ambiguous SHALL be skipped at save time (no error, no warning), leaving the
query-time gate as the sole check.

#### Scenario: reverse-PK-covered undeclared hop warns and saves
- **WHEN** a model with an undeclared join whose reverse orientation covers the source's primary key is saved with a derived column referencing the target
- **THEN** the save succeeds and a warning names the column and the hop

#### Scenario: fully undeclared hop warns and saves
- **WHEN** a model with an undeclared join, provable in neither orientation, is saved with a derived column referencing the target
- **THEN** the save succeeds and a warning names the column and the hop

#### Scenario: aggregating an unproven-hop column is still refused at query time
- **WHEN** a saved derived column across an unproven hop is aggregated as a column of its declaring model
- **THEN** the query fails closed with the input-safety error naming the hop and the cross-model remedy

#### Scenario: an unloaded target is skipped at save time
- **WHEN** a derived column references a target model that is not loaded at save time
- **THEN** the save succeeds with no error and no warning, and the query-time gate remains the only check
