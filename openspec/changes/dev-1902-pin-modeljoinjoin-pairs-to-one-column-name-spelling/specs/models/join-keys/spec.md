# models/join-keys Delta

## Purpose

Defines the spelling contract of a join's key columns: how `join_pairs` name columns, which columns may serve as keys, where the physical spelling is applied, how stored spellings are canonicalised on load, and what the importers emit.

## ADDED Requirements

### Requirement: Join keys name model columns

Each entry of a join's `join_pairs` SHALL name a column of the model on its side by that column's `name` — never by its physical `sql` spelling — and SHALL obey the column-name rules (no `.` or `:`, no reserved prefix). Each named column MUST be a base column: its `sql` is unset or a single identifier (optionally double-quoted), and it carries no `filter`. A model whose join names an undeclared, expression-valued, or filtered source-side column SHALL be rejected when the model is constructed, with an error naming the model, the join target, the column and the remedy; the same check SHALL apply to joins added by a query-level model extension. A join whose target-side key is not a declared base column of the target model SHALL be rejected at save time with a typed error, and model validation SHALL report such a join on an already-stored model with the remedy.

#### Scenario: A renamed source key is written by its column name

- **WHEN** a model declares `Column(name="region_id", sql="region_fk")` and a join with `join_pairs: [["region_id", "id"]]`
- **THEN** the model saves, and queries crossing the join execute against the physical column `region_fk`

#### Scenario: An undeclared source key is rejected at construction

- **WHEN** a model's join names a source-side column the model does not declare
- **THEN** construction fails with an error naming the model, the join target, the column and the remedy to declare it

#### Scenario: An expression source key is rejected at construction

- **WHEN** a model's join names a source-side column whose `sql` is an expression (`CAST(id AS INT)`)
- **THEN** construction fails with an error naming the column and the remedy to key on a base column

#### Scenario: A filtered source key is rejected at construction

- **WHEN** a model's join names a source-side column that carries a `filter`
- **THEN** construction fails with an error naming the column

#### Scenario: A dotted key entry is rejected

- **WHEN** a join is constructed with a `join_pairs` entry containing `.`
- **THEN** construction fails with the column-name rule error

#### Scenario: An expression target key is rejected at save time

- **WHEN** a model is saved whose join names a target-side column whose `sql` is an expression on the target model
- **THEN** the save fails with a typed join-key error naming the target model, the column and the remedy

#### Scenario: Model validation reports an undeclared target key

- **WHEN** model validation runs over a stored model whose join names a target-side column the target does not declare
- **THEN** the report carries a finding for that join naming the column and the remedy

#### Scenario: An extension join with an undeclared source key is rejected

- **WHEN** a query's `source_model` extension adds a join whose source-side key is not a column of the extended model or of the extension
- **THEN** the query fails with the same construction error, before any SQL is generated

### Requirement: The physical spelling is applied once, at emission

Join-graph reasoning — provable arity, grain determination, the entity and foreign-key seeds, the association kernel's entity keys — SHALL compare key columns, grain members and unique-key sets by column `name`. The physical spelling (the column's bare `sql` identifier, unquoted, else its `name`) SHALL be applied only where a key becomes SQL: the join ON clause, the semi-join correlation spine, and cardinality profiling. A mixed-case physical identifier SHALL be quoted exactly as any other emitted column identifier of the dialect. Emission SHALL fail closed with a typed error rather than emit an identifier for a key it cannot resolve to a declared base column.

#### Scenario: A renamed source foreign key executes

- **WHEN** `orders` joins `customers` on `Column(name="customer_id", sql="cust_fk")` → `id` and a query rooted at `orders` groups a measure by `customers.name`
- **THEN** the query executes on SQLite and DuckDB with the hand-computed per-customer values, joining on `cust_fk`

#### Scenario: A renamed target primary key proves the hop and executes

- **WHEN** `customers` declares `Column(name="id", sql="customer_pk", primary_key=True)` and `orders` joins it with `join_pairs: [["customer_id", "id"]]` and no cardinality declaration
- **THEN** the hop is provably many-to-one, the dimension keeps exact attributed values with no broadcast warning, and the query executes with the hand-computed values

#### Scenario: Renames on both sides across two hops execute

- **WHEN** `orders → customers → regions` are joined with a renamed key on each side of each hop
- **THEN** a query rooted at `orders` grouping by `customers.regions.name` executes with the hand-computed values

#### Scenario: Association over a renamed primary-key root executes

- **WHEN** the aggregation's home model declares its primary key as `Column(name="id", sql="customer_pk", primary_key=True)` and the query runs in associate mode over an unattributable dimension
- **THEN** the query executes with the distinct-entity values, the entity key referenced by its column name

#### Scenario: Population pushdown over renamed keys executes

- **WHEN** a row filter restricts the population across a hop whose keys are renamed on both sides, so the restriction is pushed as a correlated semi-join
- **THEN** the query executes with the correctly restricted population, the spine correlating on the physical columns

#### Scenario: Cardinality detection profiles renamed keys

- **WHEN** cardinality detection runs over a join whose keys are renamed base columns on both sides
- **THEN** it profiles the physical columns and detects `many_to_one` for a unique target key

#### Scenario: A mixed-case physical key is quoted at every emitter

- **WHEN** a join key's physical spelling is mixed-case (`sql="CustPK"`)
- **THEN** the ON clause, the semi-join spine and the cardinality-profiling SQL all quote it, per the dialect's quoting of any other emitted column

#### Scenario: An unresolvable target key fails closed at emission

- **WHEN** a stored model's join names a target-side column that is not a declared base column and a query crosses that join
- **THEN** planning fails with the typed join-key error, and no SQL is generated

### Requirement: Stored physical spellings are canonicalised on load

WHEN a stored model below the current schema version carries a `join_pairs` entry that names no declared column of its side but equals exactly one declared base column's unquoted `sql` rename, THEN the entry SHALL be rewritten to that column's `name` on load — for the source side against this model's columns and for the target side against the target model's stored columns — and the model SHALL be written back at the current version. An entry naming a declared column SHALL be left untouched; an ambiguous or unmatched entry SHALL be left verbatim for validation to judge. The canonicalisation SHALL precede exact-inverse join deduplication, and the counterpart comparison SHALL use canonicalised spellings on both sides.

#### Scenario: A source-side physical spelling is rewritten on load

- **WHEN** a v10 stored model declares `Column(name="customer_id", sql="cust_fk")` and its join carries `join_pairs: [["cust_fk", "id"]]`
- **THEN** after loading, the join carries `[["customer_id", "id"]]` and the stored document is at the current version with the rewritten entry, on both the YAML and the SQLite backends

#### Scenario: A target-side physical spelling is rewritten on load

- **WHEN** a v10 stored `orders` carries `join_pairs: [["customer_id", "customer_pk"]]` and the stored `customers` declares `Column(name="id", sql="customer_pk")`
- **THEN** after loading `orders`, the join carries `[["customer_id", "id"]]`

#### Scenario: A mirror pair spelled two ways collapses to one edge

- **WHEN** a v10 store holds `orders → customers` on `[["customer_id", "customer_pk"]]` and `customers → orders` on `[["id", "customer_id"]]`, where `customers.id` has `sql="customer_pk"`
- **THEN** after loading, exactly one edge connects the pair

#### Scenario: An unmatched entry is left for validation

- **WHEN** a v10 stored model's join names a source-side column that neither is declared nor equals any column's rename
- **THEN** loading fails with the construction error naming the column and the remedy, and sibling models still load

### Requirement: Drift and refinement resolve base columns by their physical spelling

Schema drift and type refinement SHALL treat a column whose `sql` is a double-quoted bare identifier as a base column and SHALL compare it against the live schema by its unquoted identifier.

#### Scenario: A quoted self-name column present in the live table is not drift

- **WHEN** a model declares `Column(name="legalEntityType", sql="\"legalEntityType\"")` and the live table has the column `legalEntityType`
- **THEN** schema drift reports no missing column and keeps a join keyed on it

#### Scenario: A quoted base column is type-refined

- **WHEN** a stored `DOUBLE` column with a double-quoted `sql` names a live integer column
- **THEN** type refinement on load narrows it exactly as it would an unquoted base column

### Requirement: Importers emit logical key names

Every importer SHALL emit `join_pairs` in column-`name` spelling naming declared base columns on both sides. The Cube importer SHALL keep join keys as member names and SHALL drop a join, with a conversion-report warning, when an ON operand names a member whose `sql` is not a base column or names no member of its cube. The dbt importer SHALL synthesise a hidden base column for a foreign entity that no dimension of the model covers, and SHALL skip, with a warning, a join whose entity `expr` is not a single identifier. The OSI importer SHALL check both sides of a relationship against the contract before adding it and SHALL skip a violating relationship with its existing warning. Auto-ingestion SHALL apply its column renaming to join keys so a generated join always names the generated column.

#### Scenario: Cube join keys are member names

- **WHEN** a Cube join's ON is `{CUBE}.customer_id = {customers.id}` and the `id` member's `sql` is `{CUBE}.cust_pk`
- **THEN** the imported join carries `[["customer_id", "id"]]` and the imported `id` column carries `sql: cust_pk`

#### Scenario: A Cube ON operand naming no member drops the join

- **WHEN** a Cube join's ON references a target column that is not a declared member of the target cube
- **THEN** the join is dropped and the conversion report warns naming the operand

#### Scenario: dbt synthesises a hidden foreign-key column

- **WHEN** a dbt semantic model declares a foreign entity whose `expr` no dimension covers
- **THEN** the converted model carries a hidden base column named by that `expr` and the join keys name it

#### Scenario: Ingestion's column rename applies to join keys

- **WHEN** an ingested table has a foreign-key column named `_count`
- **THEN** the generated column and the generated join key are both spelled `count_col`
