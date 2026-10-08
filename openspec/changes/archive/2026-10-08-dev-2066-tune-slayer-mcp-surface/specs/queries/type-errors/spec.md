## ADDED Requirements

### Requirement: A measure-name collision suggests a free name
When a query measure's declared name equals a source column of the query's source model, the `MeasureNameCollidesWithColumnError` SHALL carry a `suggestion:` naming a free alternative: the formula's derived (canonical) name when that name is neither a source column nor already used in the query, otherwise the first `<name>_<n>` (n = 2, 3, …) that is neither.

#### Scenario: The derived name is free
- **WHEN** a query rooted at a model with a column `credit` measures `{"formula": "sum(credit)", "name": "credit"}`
- **THEN** the query fails with `MeasureNameCollidesWithColumnError` whose `suggestion` names `credit_sum`

#### Scenario: The derived name is taken
- **WHEN** the same query also declares another measure named `credit_sum`
- **THEN** the suggestion names `credit_2`, which is neither a source column nor used in the query
