## MODIFIED Requirements

### Requirement: Invalid query argument reports only its own form's errors
The `query` argument's form SHALL be selected by its JSON type — a string is a saved query-backed model's name, an object is a query object, an array is a list of stages — and a validation failure SHALL report only the errors of the selected form, never the mismatch errors of the other forms.

#### Scenario: Invalid query object
- **WHEN** the `query` tool is called with a query object whose measure is named `bad name`
- **THEN** the call fails with exactly one validation error, naming `bad name` as an invalid name, and the error text does not say the input should be a valid string or a valid list

#### Scenario: Invalid stage in a list
- **WHEN** the `query` tool is called with a list of stages whose second stage names a measure `bad name`
- **THEN** the call fails with exactly one validation error, located at that stage and naming `bad name` as an invalid name, and the error text does not say the input should be a valid string or a valid dictionary

#### Scenario: A name still runs
- **WHEN** the `query` tool is called with a string
- **THEN** it is treated as a saved query-backed model's name, as before

#### Scenario: Unsupported JSON type
- **WHEN** the `query` tool is called with a number
- **THEN** the call fails with exactly one validation error
