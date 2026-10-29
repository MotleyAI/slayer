## ADDED Requirements

### Requirement: Attribution blames only drift the failing statement reads

Within the read models, a failure SHALL be attributed to schema drift only on a drift entry or finding that touches what the failed statement reads: a whole-model drop of a read model; a removal, from a read model, of a column the statement reads (including through a derived column's definition and as a join key), of a join it crosses, of a measure or aggregation it uses, or of a model filter; or a re-type finding on a column it reads. Any other drift in a read model SHALL NOT turn the failure into a `SchemaDriftError`.

#### Scenario: Unrelated drifted column leaves the database error

- **WHEN** on SQLite a model over `t(id, x, s, ts)` declares `ts` as `INT` while the live column is `TEXT`, and a query for `sum(ln(x))` fails because `x` holds 0
- **THEN** the original database error propagates, not `SchemaDriftError`

#### Scenario: A read drifted column wraps

- **WHEN** the live column `x` of the same table was dropped and a query for `sum(x)` fails
- **THEN** the query raises `SchemaDriftError` naming the removal of `x`

#### Scenario: A column read through a derived column

- **WHEN** a derived column `y` is defined over `x`, the live `x` was dropped, and a query for `sum(y)` fails
- **THEN** the query raises `SchemaDriftError`

#### Scenario: A join key the statement joins on

- **WHEN** a query joins `orders` to `customers` on `customer_id`, the live `customer_id` column was dropped, and the query fails
- **THEN** the query raises `SchemaDriftError`

### Requirement: SQLite temporal columns stored as TEXT are not drift

On SQLite, a stored `DATE` or `TIMESTAMP` column whose live column is `TEXT` SHALL NOT be reported as drift by explicit validation or query-time attribution.

#### Scenario: Hand-typed SQLite date column

- **WHEN** `validate_models` runs on a SQLite model declaring `signup_at` as `TIMESTAMP` over a live `TEXT` column
- **THEN** it reports nothing for `signup_at`

## RENAMED Requirements

- FROM: `### Requirement: A TEXT column over a live list is a re-type finding`
- TO: `### Requirement: A non-list column over a live list is a re-type finding`

## MODIFIED Requirements

### Requirement: Attribution is scoped to the read set
When a data or EXPLAIN query fails, the system SHALL attribute the failure to schema drift only on evidence about the models whose relations the executed statement contains (the read set), across every stage, producer and semi-join it renders and excluding stages pruned from the final statement. A spliced query-backed model SHALL count as read when its stages are in the final statement. Drift on a model outside the read set SHALL NOT turn the failure into a `SchemaDriftError`. A drift entry restating the query failure itself (`invalid_sql`) SHALL NOT count as evidence, including as a whole-model drop of a read model.

#### Scenario: Drift in a read model wraps the failure
- **WHEN** a query reading `orders` and joining `customers` fails and the live `customers` table was dropped
- **THEN** the query raises `SchemaDriftError` whose `to_delete` names `customers`, with the database error as its cause

#### Scenario: Drift only in an unread model of the same join component does not wrap
- **WHEN** `orders` joins `customers` and `products`, a query reading only `orders` and `customers` fails for a reason unrelated to drift, and a column of the live `products` table was dropped
- **THEN** the original database error propagates, not `SchemaDriftError`

#### Scenario: Single-stage and multi-stage statements attribute alike
- **WHEN** the dropped table is read by a single-stage query, by a stage of a multi-stage query, by a cross-model producer, or by a filter semi-join, and the query fails
- **THEN** each query raises `SchemaDriftError` naming the model over the dropped table

#### Scenario: A pruned stage is not read
- **WHEN** a multi-stage query declares a stage whose relation the final statement does not use, that stage's model has drift, and the query fails for an unrelated reason
- **THEN** the original database error propagates, not `SchemaDriftError`

#### Scenario: A query-backed source is read through its stages
- **WHEN** a query over a query-backed model fails and a base model its stages read has a dropped column the stages use
- **THEN** the query raises `SchemaDriftError` naming that base model

#### Scenario: An invalid sql model is no evidence
- **WHEN** a query reading an `sql` model fails and the model's only drift entry is its own `invalid_sql` whole-model drop
- **THEN** the original database error propagates, not `SchemaDriftError`

### Requirement: A non-list column over a live list is a re-type finding

When a stored column's type is not `ARRAY` and its live column is a native list, explicit validation and query-time attribution SHALL report a re-type finding naming the model, the column, the live list type, and re-ingesting as the remedy. They SHALL NOT report it as a type mismatch to delete. Re-ingesting SHALL re-type such a column to the live list in place, keeping only `count` among its allowed aggregations. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding. A stored `ARRAY` column whose live column is a native list SHALL NOT be reported as drift; this includes `sql`-mode models, whose live types are read from the query result.

#### Scenario: Stale TEXT list column
- **WHEN** `validate_models` runs on a datasource whose stored `contractors.skills` is `TEXT` and whose live column is `VARCHAR[]`
- **THEN** it reports a re-type finding for `contractors.skills` with the re-ingest remedy, and no deletion of `skills`

#### Scenario: A refused GROUP BY explained
- **WHEN** a BigQuery query groups by a stored `TEXT` column whose live column is `REPEATED STRING`, and the database refuses it
- **THEN** the query raises `SchemaDriftError` carrying the re-type finding

#### Scenario: A declared list in an sql-mode model
- **WHEN** an `sql`-mode model declares `skills` as `ARRAY` with `element_type: TEXT` and its query returns a text list
- **THEN** validation reports no drift for `skills`

#### Scenario: An imported DOUBLE measure column over a list
- **WHEN** `validate_models` runs on a model whose stored `scores` is `DOUBLE` and whose live column is `INTEGER[]`
- **THEN** it reports a re-type finding for `scores`, no deletion, and re-ingesting re-types `scores` to the list with only `count` allowed
