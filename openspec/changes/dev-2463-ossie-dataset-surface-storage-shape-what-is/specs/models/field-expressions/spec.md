## MODIFIED Requirements

### Requirement: A portable windowed field is refused at both save doors

A field whose portable `sql` (or `OSSIE_SQL_2026` entry) is the form it runs on the datasource and contains a window function SHALL be refused when saved, through the engine and through the storage backend alike, with the window-function error naming the field, leaving the store unchanged. A query reading such a field stored without validation SHALL fail as unevaluable with a typed error naming the field. A field whose datasource-dialect entry is windowed SHALL be accepted whatever its portable `sql` holds. A field of a dataset belonging to a semantic-model document that is unchanged from its stored definition is its document's to carry and is not refused at save; a query reading it is refused as unevaluable. A save adding or changing such a field is refused.

#### Scenario: Both doors refuse a portable window
- **WHEN** a dataset is saved with field `rn` declared as `expression: "ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)"`, once through the engine and once through the storage backend with validation on
- **THEN** both saves fail with the window-function error naming `rn`, and nothing is stored

#### Scenario: A stored portable window fails the query
- **WHEN** such a dataset was stored without validation and a query reads `rn`
- **THEN** the query fails with the unevaluable-entity error naming `rn`
