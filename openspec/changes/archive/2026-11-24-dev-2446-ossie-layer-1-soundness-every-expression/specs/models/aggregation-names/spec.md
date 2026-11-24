## ADDED Requirements

### Requirement: AGG is refused in a field expression
A dataset field's Mode-B `expression` that calls `AGG` in any case SHALL be refused when saved, with a typed error naming the field and stating that `AGG` is allowed only in metrics, as `AGG(dataset.metric)` in a model-scoped metric and `AGG(source.measure)` in a dataset-scoped metric — the rule Ossie documents already follow (`models/semantic-model-documents`). A field stored without that check SHALL fail every query that reads it with the same typed error; its `AGG` argument SHALL never be evaluated as a plain reference. Mode-A text — a field's `filter`, a dataset's `filters` and a definition's entry for a dialect — SHALL be unaffected, as Ossie's reservation covers only `OSSIE_SQL_2026` expressions.

#### Scenario: A field calling AGG is refused when saved
- **WHEN** a dataset is saved with the field `a` whose expression is `AGG(amount)`, and another with the field `b` whose expression is `agg(amount) + 1`
- **THEN** each save fails with the typed error naming the field and stating that `AGG` is allowed only in metrics

#### Scenario: A field saved without the check fails every query
- **WHEN** a dataset holding the field `a` with expression `AGG(amount)` is saved without validation and a query reads `orders.a`
- **THEN** the query fails with the same typed error, and no SQL reading `amount` for `a` is executed

#### Scenario: Mode-A text is not judged
- **WHEN** a dataset is saved with a field whose `filter` is `AGG(amount) > 1`
- **THEN** the save succeeds and the filter reaches the database as written
