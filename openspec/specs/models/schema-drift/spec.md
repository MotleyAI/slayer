# models/schema-drift Specification

## Purpose
Query-time schema-drift attribution: when a query fails, decide whether drift in the models the failed statement read explains it, cheaply and without blaming models the statement never read.

## Requirements

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

### Requirement: A join drop addresses exactly one edge
Explicit validation SHALL report a dropped join by its edge reference (the target model's name when it
is the only join from the model to that target, else the edge name) and SHALL additionally carry an
exact reference (`target_model`, `name`, `join_pairs`) for every dropped join. Applying the drift SHALL
remove exactly the joins whose live keys or target are gone, never another join to the same target.
The exact references SHALL be accepted unchanged by the model-editing surface.

#### Scenario: Dropping one FK column keeps the parallel edge
- **WHEN** `orders` joins `addresses` on `billing_address_id` and on `shipping_address_id`, the live
  `billing_address_id` column is dropped, and the drift is applied
- **THEN** only the billing join is removed and the shipping join remains

#### Scenario: Non-parallel join drops report the target as before
- **WHEN** the only join from `orders` to `customers` loses its live key column
- **THEN** the drift entry's join list reports `customers`

#### Scenario: Exact references replay through model editing
- **WHEN** a drift entry's removal, serialised as JSON, is passed unchanged to the model-editing
  surface
- **THEN** exactly the reported joins are removed

#### Scenario: A query-backed stage cascades only on the edges it walks
- **WHEN** a query-backed stage over `orders` reads `shipping_address.city` (or `orders.shipping_address.city`)
  and the billing join is dropped
- **THEN** that model is kept, while a stage reading `billing_address.city` is removed

#### Scenario: Every hop of a stage reference's route counts
- **WHEN** a stage over `Invoice` reads `Consumer.email` along `Invoice → Subscription → Customer → Consumer`,
  spelled in full or short form, and any one of those joins is dropped
- **THEN** the query-backed model is removed; dropping a join off that route does not remove it

### Requirement: A non-list column over a live list is a re-type finding

When a stored column's type is not `ARRAY` and its live column is a native list, explicit validation and query-time attribution SHALL report a re-type finding naming the model, the column, the live list type, and re-ingesting as the remedy. They SHALL NOT report it as a type mismatch to delete. Re-ingesting SHALL re-type such a column to the live list in place, keeping only `count` among its allowed aggregations. A query failure explained by such a column SHALL raise `SchemaDriftError` carrying the finding. A stored `ARRAY` column whose live column is a native list SHALL NOT be reported as drift; this includes `sql`-mode models, whose live types are read from the query result. An `sql`-mode model SHALL get the same finding and the same in-place re-type on re-ingest, each result column typed from the live schema of the tables its query reads, else from the type the database reports for the result column; a result column neither yields as a list is not detected.

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

#### Scenario: A stale column in an sql-mode model
- **WHEN** an `sql`-mode model declares `scores` as `DOUBLE` and its query selects the live `INTEGER[]` column `scores`
- **THEN** `validate_models` reports a re-type finding for `scores` with the re-ingest remedy and no deletion, a query failing on `scores` raises `SchemaDriftError` carrying it, and re-ingesting re-types `scores` to a list of `INT` with only `count` allowed

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

### Requirement: A bare sql_table resolves in the connection's default schema
Schema-drift checks — explicit validation (`validate_models`, ingest validation,
`validate-models --force-clean`) and query-time drift attribution — SHALL resolve a model
whose `sql_table` is unqualified against the connection's default schema, however many other
schemas the database holds. The default schema SHALL be the one the connection itself uses
for unqualified names; on BigQuery that is the dataset in the connection URL
(`bigquery://<project>/<dataset>`). An unqualified `sql_table` SHALL NOT resolve to an object
in any other schema.

#### Scenario: A bare model in the URL dataset survives a second dataset
- **WHEN** a BigQuery datasource's connection URL names dataset `a`, the project also holds dataset `b`, and a model's `sql_table` is the bare name of a table in `a`
- **THEN** `validate_models` reports no `WholeModelDelete` for that model

#### Scenario: Column drift on a bare model is still reported
- **WHEN** the default schema is known from the connection, the database holds a second schema, and a column of the live table behind a bare-named model was dropped
- **THEN** `validate_models` reports an edit removing that column, not a `WholeModelDelete`

#### Scenario: A bare name never resolves to another schema
- **WHEN** a model's bare `sql_table` names an object that exists only in a non-default schema
- **THEN** `validate_models` reports a `WholeModelDelete` for that model

#### Scenario: A same-named object elsewhere does not shadow the default
- **WHEN** an object named like a model's bare `sql_table` exists in both the default schema and another schema, and only the other schema's object has the model's columns
- **THEN** drift checks compare the model against the default schema's object, and a model whose `sql_table` is qualified with the other schema compares against that schema's object

#### Scenario: Query-time attribution resolves bare names the same way
- **WHEN** the default schema is known from the connection, the database holds a second schema, and a query reading a bare-named model whose live table is unchanged fails for an unrelated reason
- **THEN** the original database error propagates, not `SchemaDriftError`

#### Scenario: A schema list that cannot be read falls back to the default
- **WHEN** the database's schema list cannot be read during query-time attribution and the default schema is known only from the connection
- **THEN** a bare-named model whose live table is unchanged is not reported as deleted
