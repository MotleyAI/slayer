## ADDED Requirements

### Requirement: Stage errors name the stage
An error about a query stage's output schema SHALL name the stage by its user-facing name (its name in the query list, or the query-backed model's name), never by the stage's source model.

#### Scenario: Schema mismatch names the stage
- **WHEN** a stage named `cr` over source model `orders` renders output columns that do not match its expected schema
- **THEN** the error message names `stage 'cr'` and does not name `stage 'orders'`
