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

### Requirement: SLAYER entries are read on every load
An entity's `SLAYER` entry SHALL be applied on every load. Its content that duplicates Ossie core content SHALL apply only when the payload's fingerprint matches the entity's core content as stored; otherwise the core content SHALL apply alone. A payload that is malformed, of an unknown version, attached to an entity it does not describe, or one of several `SLAYER` entries on one entity SHALL be ignored; so SHALL a payload whose content would make its dataset invalid or names an entity the document does not hold. Each ignored payload SHALL produce a structured load warning naming the entity and the reason, visible on `inspect`, and SHALL affect no other entity. A relationship's `SLAYER` entry declaring a cardinality or join type SHALL override the many-to-one, LEFT default. A relationship's `SLAYER` entry SHALL restore exactly one relationship; an entry declaring several SHALL be ignored.

#### Scenario: A hand edit makes its payload stale
- **WHEN** a document dataset metric's core `sql` is edited by hand after its `SLAYER` entry recorded the formula `revenue / n`
- **THEN** the metric evaluates its edited core `sql`, and `inspect` shows a load warning naming the metric

#### Scenario: A relationship keeps its declared cardinality
- **WHEN** `edit_dataset` adds a `one_to_many` relationship from document dataset `customers` to document dataset `orders`
- **THEN** the relationship reads back as `one_to_many`

#### Scenario: An entry declaring several relationships is ignored
- **WHEN** a document relationship `orders_to_customers`'s `SLAYER` entry declares both `buyer` on `orders` and `placed` on `customers`
- **THEN** it loads as one many-to-one LEFT relationship `orders_to_customers` on `orders`, and `inspect` shows a load warning naming it
