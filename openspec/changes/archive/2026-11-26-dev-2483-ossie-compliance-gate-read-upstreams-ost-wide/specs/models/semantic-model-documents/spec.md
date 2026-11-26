## MODIFIED Requirements

### Requirement: Upstream's wide-table cases are the query oracle
Every upstream case script `compliance/wide_table/<case>.ost` SHALL run against the model its script loads, stored as a native document over the SQL data its script connects to, seeded into each Tier-1 database (`compatibility/ossie-compliance`), with the query of its `executeLower` submitted verbatim under `conformance="ossie-l3"` (`queries/ossie-l3-binding`). A row case (`a*`) SHALL return exactly the script's expected rows, compared as a multiset by logical type, with the result columns named and ordered as the expected header; an error case (`e*`) SHALL raise the Ossie code its expected error starts with. The script's lowering SQL is not part of the oracle. Every row case SHALL also return exactly the expected rows under the default conformance.

#### Scenario: The model-scoped metric components case
- **WHEN** case `a08_model_metric_components` runs on the `retail` data and `retail_model`
- **THEN** the result equals its expected rows on every Tier-1 database

#### Scenario: Error cases raise their codes under strict
- **WHEN** case `e16_partial_dimension_reachability` runs on the `retail` data and `retail_model` under `conformance="ossie-l3"`
- **THEN** it raises `E_PARTIAL_DIMENSION_REACHABILITY`, while the same query under the default conformance answers

#### Scenario: Output names label the strict result
- **WHEN** case `a09_aliases` runs under `conformance="ossie-l3"`
- **THEN** the result columns are exactly `area`, `revenue`, in that order

#### Scenario: A NULL group matches on every database
- **WHEN** case `a17_unmatched_home_null_group` runs on SQL Server, ClickHouse and BigQuery
- **THEN** its NULL-keyed row equals the expected row on each

#### Scenario: A quoted cell keeps its text
- **WHEN** an expected table cell is written quoted, such as `"a|b"` or `""`
- **THEN** it is compared as the string it encodes, `a|b` or the empty string, not split or read as NULL
