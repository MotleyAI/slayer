## Purpose

Query-time schema-drift attribution: when a query fails, decide whether drift in the models the failed statement read explains it, cheaply and without blaming models the statement never read.

## ADDED Requirements

### Requirement: Attribution is scoped to the read set
When a data or EXPLAIN query fails, the system SHALL attribute the failure to schema drift only on evidence about the models whose relations the executed statement contains (the read set), across every stage, producer and semi-join it renders and excluding stages pruned from the final statement. A spliced query-backed model SHALL count as read when its stages are in the final statement. Drift on a model outside the read set SHALL NOT turn the failure into a `SchemaDriftError`. A drift entry restating the query failure itself (`invalid_sql`) SHALL NOT count as evidence.

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

### Requirement: The drift payload names the blamed models
`SchemaDriftError.models` (the REST 422 `schema_drift` body's `models`) SHALL list, sorted and without duplicates, exactly the models named by its `to_delete` entries.

#### Scenario: Only drifted read models are listed
- **WHEN** a query reads `orders` and `customers`, only the live `customers` table has drift that does not cascade to `orders`, and the query fails
- **THEN** `SchemaDriftError.models` is `["customers"]`

### Requirement: Attribution inspects only read models
Attribution SHALL introspect only the live objects of read `sql_table` models, trial-execute only read `sql` models, and run the SQLite affinity probe only on read models, resolving each model's table exactly as full validation does. Explicit validation (`validate_models`, ingest validation, `validate-models --force-clean`) SHALL remain whole-datasource.

#### Scenario: Unread tables are not introspected
- **WHEN** a datasource holds ten tables and a failing query reads one of them
- **THEN** attribution introspects the columns, keys and foreign keys of that one table only

#### Scenario: Unread sql models are not trial-executed
- **WHEN** a datasource holds several `sql` models and a failing query reads none of them
- **THEN** attribution trial-executes no `sql` model

#### Scenario: Explicit validation stays whole-datasource
- **WHEN** `validate_models` is called for a datasource
- **THEN** it reports drift on every model of that datasource, as before

#### Scenario: Explicit validation of an unreachable datasource reports no sql drift
- **WHEN** `validate_models` is called for a datasource that cannot be connected to
- **THEN** it raises the connection error instead of reporting its `sql` models as dropped

### Requirement: Attribution reuses live-schema facts for a bounded time
Attribution SHALL reuse the live-schema facts it gathered for a datasource (schema listings, table metadata, `sql`-model trial results, and an "introspection unavailable" outcome) for at most 60 seconds from when they were first gathered, sharing them between concurrent failures. Verdicts SHALL be recomputed from the current persisted models on every failure. A cached listing's absence of a table SHALL be trusted only for tables that listing was taken to look for; otherwise the listing SHALL be re-taken. Explicit validation SHALL NOT use reused facts.

#### Scenario: Repeated failures reuse the snapshot
- **WHEN** the same failing query runs five times within 60 seconds
- **THEN** the datasource's schemas are listed once and each read table is introspected once

#### Scenario: Concurrent failures share one snapshot
- **WHEN** several queries reading the same table fail concurrently on one datasource
- **THEN** that table is introspected once

#### Scenario: Facts expire
- **WHEN** a query fails, more than 60 seconds pass, and it fails again
- **THEN** the second failure gathers the live facts afresh

#### Scenario: Unavailable introspection is reused as no verdict
- **WHEN** the datasource's schemas cannot be listed and a query fails twice within 60 seconds
- **THEN** both failures propagate the original database error and schema listing is attempted once

#### Scenario: An unreachable datasource is reused as no verdict
- **WHEN** the datasource cannot be connected to and queries reading its `sql_table` or `sql` models fail twice within 60 seconds
- **THEN** both failures propagate the original database error and the connection is attempted once

#### Scenario: A model edit within the window is honoured
- **WHEN** a query fails, then within 60 seconds the model is edited to drop the column the live table lost, and a different query on the model fails
- **THEN** attribution judges the edited model and does not blame the removed column

#### Scenario: A newly created table is not reported dropped
- **WHEN** a query fails, then within 60 seconds a new table is created and a model is re-pointed at it, and a query reading that model fails
- **THEN** attribution re-lists the schema and does not report the model's table as dropped

#### Scenario: A dropped table's absence is reused
- **WHEN** a read table is dropped and queries reading it fail three times within 60 seconds
- **THEN** each failure raises `SchemaDriftError` and the schemas are listed once

#### Scenario: Explicit validation reads live
- **WHEN** a failed query cached live facts and `validate_models` is then called within 60 seconds
- **THEN** `validate_models` introspects the live schema afresh

### Requirement: Attribution uses the query's resolved datasource
Attribution SHALL validate only against the datasource the failed statement executed on, including when the source model declares no `data_source`.

#### Scenario: Only the executing datasource is validated
- **WHEN** a query fails and two other datasources are configured, one of them named by the source model
- **THEN** attribution introspects only the datasource the query ran on

### Requirement: An unreadable live table is no drift evidence
Validation, explicit and query-time, SHALL NOT report a model as drifted when its table is present in the schema listing but its metadata could not be read; such a model SHALL get no verdict.

#### Scenario: A listed table that fails to introspect is not reported dropped
- **WHEN** a datasource's tables `a` and `b` are listed, reading `b`'s metadata fails while `a` succeeds, and `validate_models` runs
- **THEN** no entry names `b`'s model
