## MODIFIED Requirements

### Requirement: Upstream's wide-table cases are the query oracle
Every case of upstream `compliance/wide_table/cases` SHALL run against its named fixture model, stored as a native document over its SQL data loaded into in-process DuckDB, with its `query.json` submitted verbatim under `conformance="ossie-l3"` (`queries/ossie-l3-binding`). A row case (`a*`) SHALL return exactly the expected rows, with the result columns named and ordered as the expected header; an error case (`e*`) SHALL raise its expected Ossie code. Every row case SHALL also return exactly the expected rows under the default conformance. A case whose metric binds through a host with two routes to its home SHALL be a strict expected failure naming DEV-2388, under both conformances.

#### Scenario: The model-scoped metric components case
- **WHEN** case `a08_model_metric_components` runs on the `retail` fixture
- **THEN** the result equals its expected rows

#### Scenario: Error cases raise their codes under strict
- **WHEN** case `e16_partial_dimension_reachability` runs on the `retail` fixture under `conformance="ossie-l3"`
- **THEN** it raises `E_PARTIAL_DIMENSION_REACHABILITY`, while the same query under the default conformance answers

#### Scenario: Output names label the strict result
- **WHEN** case `a09_aliases` runs under `conformance="ossie-l3"`
- **THEN** the result columns are exactly `area`, `revenue`, in that order
