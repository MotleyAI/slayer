## MODIFIED Requirements

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

- **WHEN** a query extension adds a join whose source-side key is not a column of the extended model
- **THEN** the query fails with the same construction error, before any SQL is generated
