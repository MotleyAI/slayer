## ADDED Requirements

### Requirement: Exported portable SQL is valid OSSIE_SQL_2026
Every portable `sql` the exporter emits, for fields, dataset-scoped metrics and model-scoped metrics, SHALL pass the native validity check: `INTERVAL` arithmetic SHALL be exported as `DATEADD(<unit>, <n>, <expr>)`, a subtraction as a negative `<n>`, a chain as nested calls. The exported value SHALL equal the original's. A stored document SHALL still export with its original text.

#### Scenario: An INTERVAL field exports as DATEADD
- **WHEN** a standalone DuckDB dataset stored before this change has the field `due` with expression `order_date + INTERVAL '7 days'` and is exported
- **THEN** the exported field has `sql: DATEADD(day, 7, order_date)`, the exported document passes upstream's validator, and re-importing it gives `due` the same values
