## MODIFIED Requirements

### Requirement: The physical spelling is applied once, at emission

Join-graph reasoning — provable arity, grain determination, the entity and foreign-key seeds, the association kernel's entity keys — SHALL compare key fields, grain members and unique-key sets by field `name`. The physical spelling (the source column a physical reader reads, unquoted, else the field's `name`; `models/field-expressions` › A physical reader is a bare source column without a filter) SHALL be applied only where a key becomes SQL: the join ON clause, the semi-join correlation spine, and cardinality profiling. A mixed-case physical identifier SHALL be quoted exactly as any other emitted column identifier of the dialect. Emission SHALL fail closed with a typed error rather than emit an identifier for a key it cannot resolve to a declared physical reader.

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

#### Scenario: An unresolvable target key fails closed at emission

- **WHEN** a stored dataset's relationship names a target-side field that is not a declared physical reader and a query crosses that relationship
- **THEN** planning fails with the typed join-key error, and no SQL is generated

### Requirement: Drift and refinement resolve base columns by their physical spelling

Schema drift and type refinement SHALL treat exactly the physical readers (`models/field-expressions` › A physical reader is a bare source column without a filter) as base fields, a double-quoted bare identifier included, and SHALL compare each against the live schema by the unquoted source column it reads. A field whose definition reads another field by path SHALL NOT be compared against the live schema as a base field.

#### Scenario: A quoted self-name column present in the live table is not drift

- **WHEN** a dataset declares the field `legalEntityType` with `expression: '"legalEntityType"'` and the live table has the column `legalEntityType`
- **THEN** schema drift reports no missing column and keeps a join keyed on it

#### Scenario: A quoted base column is type-refined

- **WHEN** a stored `DOUBLE` field whose expression is a double-quoted bare identifier names a live integer column
- **THEN** type refinement on load narrows it exactly as it would an unquoted base field

### Requirement: Importers emit logical key names

Every importer SHALL emit `from_columns` / `to_columns` in field-`name` spelling naming declared physical readers on both sides (`models/field-expressions` › A physical reader is a bare source column without a filter). An importer SHALL key a join on a physical column through a physical reader of that column, added as a hidden field when none exists (`importers/meaning-preservation` › One hidden reader per physical column), on the dataset and on any view facade carrying the join, whichever members or dimensions hold the column's name. The Cube importer SHALL keep join keys as member names where the member is a physical reader, and SHALL drop a join, with a conversion-report warning, when an ON operand is a member reference whose definition is not a physical reader. The dbt importer SHALL skip, with a warning, a join whose entity `expr` is not a single identifier. The OSI importer SHALL check both sides of a relationship against the contract before adding it and SHALL skip a violating relationship with its existing warning. Auto-ingestion SHALL apply its field renaming to join keys so a generated join always names the generated field.

#### Scenario: Cube join keys are member names

- **WHEN** a Cube join's ON is `{CUBE}.customer_id = {customers.id}` and the `id` member's `sql` is `{CUBE}.cust_pk`
- **THEN** the imported relationship carries `from_columns: ["customer_id"]` and `to_columns: ["id"]`, and the imported `id` field carries `expression: cust_pk`

#### Scenario: A Cube ON operand naming no member synthesises a hidden key column

- **WHEN** a Cube join's ON references a target column that is not a declared member of the target cube
- **THEN** the joined dataset carries a hidden physical reader of that column and the relationship keys name it

#### Scenario: dbt synthesises a hidden foreign-key column

- **WHEN** a dbt semantic model declares a foreign entity whose `expr` no physical reader covers
- **THEN** the converted dataset carries a hidden physical reader of that column and the relationship keys name it

#### Scenario: Ingestion's column rename applies to join keys

- **WHEN** an ingested table has a foreign-key column named `_count`
- **THEN** the generated field and the generated relationship key are both spelled `count_col`
