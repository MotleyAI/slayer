## ADDED Requirements

### Requirement: Documented examples are written qualified
Every query, reference and expression example in the tool descriptions, their input schemas, the server instructions and the help topics SHALL be written in the qualified syntax of `queries/references`: every field and dataset metric written as `<dataset>.<name>` (or with an explicit route), every row count as `count(<dataset>.*)`, every later-stage reference as `<stage>.<column>`, and no bare name other than an alias declared by the same example.

#### Scenario: No example uses a pre-2.0 spelling
- **WHEN** every example query in the `query` tool description, its input schema, the server instructions and every help topic body is executed against a datasource seeded with the datasets it names
- **THEN** none emits a `deprecated_input` warning

#### Scenario: Unqualified row counts are not documented
- **WHEN** the same texts are collected
- **THEN** no example contains `count(*)`
