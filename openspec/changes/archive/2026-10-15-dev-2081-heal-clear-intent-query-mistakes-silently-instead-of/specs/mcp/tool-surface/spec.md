## ADDED Requirements

### Requirement: The order column's string form is advertised
The query input schema SHALL advertise an order entry's `column` as either a string (a column name or
an expression) or a column-reference object, and SHALL NOT publish the internal `raw_formula` field of
an order entry. Input carrying `raw_formula` — such as a stored query — SHALL still be accepted and
persisted unchanged.

#### Scenario: Schema shows both column forms
- **WHEN** a client lists the server's tools and reads the `query` tool's order entry schema
- **THEN** `column` accepts a string or a column-reference object, and no `raw_formula` property appears

#### Scenario: Stored order expressions still load
- **WHEN** a saved query whose order entry carries `raw_formula` is loaded and executed
- **THEN** it sorts by that expression exactly as before
