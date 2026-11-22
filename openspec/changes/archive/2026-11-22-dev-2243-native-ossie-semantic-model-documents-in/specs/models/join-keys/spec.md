## MODIFIED Requirements

### Requirement: Join keys name model columns

Each entry of a join's `from_columns` and `to_columns` SHALL name a column of the model on its side by that column's `name` — never by its physical spelling — and SHALL obey the column-name rules (no `.` or `:`, no reserved prefix). Each named column MUST be a declared field without a `filter` whose expression reads only its own dataset's source columns: a physical reader (`models/field-expressions`), or a computed field, as Ossie allows relationship keys to name any declared field. A model whose join names an undeclared or filtered source-side column, or a source-side column whose expression reads another dataset, SHALL be rejected when the model is constructed, with an error naming the model, the join target, the column and the remedy; the same check SHALL apply to joins added by a query-level model extension. A join whose target-side key is not such a field of the target model SHALL be rejected at save time with a typed error, and model validation SHALL report such a join on an already-stored model with the remedy.

#### Scenario: A renamed source key is written by its column name

- **WHEN** a model declares `DatasetField(name="region_id", expression="region_fk")` and a join with `from_columns: ["region_id"]` and `to_columns: ["id"]`
- **THEN** the model saves, and queries crossing the join execute against the physical column `region_fk`

#### Scenario: An undeclared source key is rejected at construction

- **WHEN** a model's join names a source-side column the model does not declare
- **THEN** construction fails with an error naming the model, the join target, the column and the remedy to declare it

#### Scenario: An expression source key is rejected at construction

- **WHEN** a model's join names a source-side column whose `expression` reads a field of another dataset (`customers.region_id`)
- **THEN** construction fails with an error naming the column and the remedy to key on a field of the model's own source

#### Scenario: A computed source key joins on its expression

- **WHEN** a model declares `DatasetField(name="customer_key", expression="CAST(cust_fk AS INT)")` and a join with `from_columns: ["customer_key"]` to the target's `id`
- **THEN** the model saves, and a query crossing the join executes with the ON clause comparing `CAST(cust_fk AS INT)` to the target key, returning the hand-computed values

#### Scenario: A filtered source key is rejected at construction

- **WHEN** a model's join names a source-side column that carries a `filter`
- **THEN** construction fails with an error naming the column

#### Scenario: A dotted key entry is rejected

- **WHEN** a join is constructed with a `from_columns` or `to_columns` entry containing `.`
- **THEN** construction fails with the column-name rule error

#### Scenario: An expression target key is rejected at save time

- **WHEN** a model is saved whose join names a target-side column whose `expression` reads another dataset on the target model
- **THEN** the save fails with a typed join-key error naming the target model, the column and the remedy

#### Scenario: Model validation reports an undeclared target key

- **WHEN** model validation runs over a stored model whose join names a target-side column the target does not declare
- **THEN** the report carries a finding for that join naming the column and the remedy

#### Scenario: An extension join with an undeclared source key is rejected

- **WHEN** a query extension adds a join whose source-side key is not a column of the extended model
- **THEN** the query fails with the same construction error, before any SQL is generated

### Requirement: The physical spelling is applied once, at emission

Join-graph reasoning — provable arity, grain determination, the entity and foreign-key seeds, the association kernel's entity keys — SHALL compare key fields, grain members and unique-key sets by field `name`. The physical spelling (the source column a physical reader reads, unquoted, else the field's `name`; `models/field-expressions` › A physical reader is a bare source column without a filter) SHALL be applied only where a key becomes SQL: the join ON clause, the semi-join correlation spine, and cardinality profiling; a computed key SHALL be emitted at those same places as its expression over its own dataset's source. A mixed-case physical identifier SHALL be quoted exactly as any other emitted column identifier of the dialect. Emission SHALL fail closed with a typed error rather than emit SQL for a key it cannot resolve to a declared field of its own dataset.

#### Scenario: A renamed source foreign key executes

- **WHEN** `orders` joins `customers` on the field `customer_id` with `expression: cust_fk` → `id` and a query rooted at `orders` groups a measure by `customers.name`
- **THEN** the query executes on SQLite and DuckDB with the hand-computed per-customer values, joining on `cust_fk`

#### Scenario: A renamed target primary key proves the hop and executes

- **WHEN** `customers` declares the field `id` with `expression: customer_pk` and `primary_key: true`, and `orders` joins it on `customer_id` → `id` with no cardinality declaration
- **THEN** the hop is provably many-to-one, the dimension keeps exact attributed values with no broadcast warning, and the query executes with the hand-computed values

#### Scenario: Renames on both sides across two hops execute

- **WHEN** `orders → customers → regions` are joined with a renamed key on each side of each hop
- **THEN** a query rooted at `orders` grouping by `customers.regions.name` executes with the hand-computed values

#### Scenario: Association over a renamed primary-key root executes

- **WHEN** the aggregation's home dataset declares its primary key as the field `id` with `expression: customer_pk` and `primary_key: true`, and the query runs in associate mode over an unattributable dimension
- **THEN** the query executes with the distinct-entity values, the entity key referenced by its field name

#### Scenario: Population pushdown over renamed keys executes

- **WHEN** a row filter restricts the population across a hop whose keys are renamed on both sides, so the restriction is pushed as a correlated semi-join
- **THEN** the query executes with the correctly restricted population, the spine correlating on the physical columns

#### Scenario: Cardinality detection profiles renamed keys

- **WHEN** cardinality detection runs over a join whose keys are renamed physical readers on both sides
- **THEN** it profiles the physical columns and detects `many_to_one` for a unique target key

#### Scenario: A mixed-case physical key is quoted at every emitter

- **WHEN** a join key's physical spelling is mixed-case (`expression: '"CustPK"'`)
- **THEN** the ON clause, the semi-join spine and the cardinality-profiling SQL all quote it, per the dialect's quoting of any other emitted column

#### Scenario: A computed key correlates a pushed-down restriction

- **WHEN** a row filter restricts the population across a hop keyed on a computed field, so the restriction is pushed as a correlated semi-join
- **THEN** the spine correlates on the computed key's expression and the query returns the correctly restricted population

#### Scenario: An unresolvable target key fails closed at emission

- **WHEN** a stored dataset's relationship names a target-side field that is not a declared field of the target and a query crosses that relationship
- **THEN** planning fails with the typed join-key error, and no SQL is generated
