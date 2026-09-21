## MODIFIED Requirements

### Requirement: Unproven references are accepted with a save-time warning and a query-time backstop

When a derived `Column.sql` / `Column.filter` reference's path crosses a hop
that is neither provably many-to-one nor provably fanning — an undeclared hop,
or a hop whose only proof is that its reverse orientation covers the source's
unique key — the model save SHALL succeed and SHALL emit a warning naming the
column and the hop. Such a column is refused at query time by the input-safety
gate when it is aggregated as a column of its declaring model, and equally when
it ranks a `first`/`last` as the model's `default_time_dimension` (the permanent
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

#### Scenario: a default_time_dimension across an unproven hop is refused when it ranks a first/last
- **WHEN** a model's `default_time_dimension` names a saved derived column across an unproven hop and a query selects `amount:last` on that model with no explicit ranking column, temporal dimension or time dimension
- **THEN** the query fails closed with the input-safety error naming the column and the hop, exactly as the explicit `amount:last(<column>)` spelling does
