# models/save-validation Specification

## Purpose
Defines how a model's source is validated at save time — before it is persisted —
so that a raw-`sql` model whose SQL is invalid for its datasource dialect is
rejected at the create/edit door rather than failing silently on first query.

## Requirements

### Requirement: Raw-sql model source is trial-executed at save time

When a model whose source is a raw `sql` expression (not `sql_table` and not
`source_queries`) is saved, the system SHALL trial-execute that SQL against the
model's live datasource with a zero-row guard before persisting it. If the
reachable datasource rejects the SQL, the save SHALL fail with an error that
names the model and its datasource, and the model SHALL NOT be persisted.

#### Scenario: Valid SQL is accepted and persisted

- **WHEN** a raw-`sql` model whose SQL selects existing columns from an existing
  table is saved against a reachable datasource
- **THEN** the trial-execute succeeds and the model is persisted

#### Scenario: SQL rejected by a reachable backend blocks the save

- **WHEN** a raw-`sql` model whose SQL has a syntax error, references a
  non-existent table, or references a non-existent column is saved against a
  reachable datasource
- **THEN** the save fails with an error naming the model and datasource
- **AND** the model is not persisted

#### Scenario: A trailing statement terminator does not cause a false rejection

- **WHEN** a raw-`sql` model whose otherwise-valid SQL ends with a trailing `;`
  is saved
- **THEN** the terminator is stripped before wrapping and the model is persisted

### Requirement: Inconclusive validation warns and saves

When the save-time check cannot obtain a definite verdict from the backend —
the datasource is unreachable, a transient error occurs, credentials are
rejected, or no datasource is configured for the model — the system SHALL log a
warning and persist the model anyway, rather than blocking the save. Only a
definite rejection from a reachable datasource blocks the save.

#### Scenario: Unreachable datasource does not block the save

- **WHEN** a raw-`sql` model is saved and the datasource cannot be reached
  (connection refused / host resolution failure / connection timeout)
- **THEN** a warning is logged and the model is persisted

#### Scenario: Transient error does not block the save

- **WHEN** a raw-`sql` model is saved and the trial-execute fails with a
  transient error (dropped connection, lock/deadlock)
- **THEN** a warning is logged and the model is persisted

#### Scenario: Credential rejection does not block the save

- **WHEN** a raw-`sql` model is saved and the datasource rejects the connection
  credentials
- **THEN** a warning is logged and the model is persisted

#### Scenario: Missing datasource configuration does not block the save

- **WHEN** a raw-`sql` model whose `data_source` is unset or not configured is
  saved
- **THEN** a warning is logged and the model is persisted

#### Scenario: Permission denied on a reachable datasource blocks the save

- **WHEN** a raw-`sql` model is saved against a reachable datasource whose valid
  credentials lack the privilege to read a referenced object
- **THEN** the save fails with an error and the model is not persisted, because
  the SQL is non-functional under that datasource's credentials

### Requirement: Parameterized source SQL is not trial-executed

When a raw-`sql` model's `sql` itself contains `{var}` or `{? ?}` placeholders,
the system SHALL skip the trial-execute (logging at informational level) and
persist the model, because a placeholder's value is not known at save time. A placeholder that appears only in a column expression or
filter, and not in the model's `sql`, SHALL NOT cause the model `sql` check to be
skipped.

#### Scenario: Parameterized model SQL is skipped and saved

- **WHEN** a raw-`sql` model whose `sql` contains a `{var}` placeholder is saved
- **THEN** the trial-execute is skipped and the model is persisted

#### Scenario: A placeholder only in a column expression does not skip the model-SQL check

- **WHEN** a raw-`sql` model whose `sql` is static but a `DatasetField.sql` contains a
  `{var}` placeholder is saved, and the static `sql` is invalid for the backend
- **THEN** the model `sql` is still trial-executed and the invalid SQL is
  rejected

### Requirement: Non-sql-mode models are not trial-executed

The save-time SQL check SHALL apply only to raw-`sql` models. A `sql_table`-backed
model and a query-backed (`source_queries`) model SHALL NOT trigger the
trial-execute, preserving their existing save behavior.

#### Scenario: sql_table-backed model is not trial-executed

- **WHEN** a `sql_table`-backed model is saved
- **THEN** no raw-SQL trial-execute is performed and existing save behavior is
  unchanged

#### Scenario: Query-backed model is not affected

- **WHEN** a query-backed (`source_queries`) model is saved
- **THEN** the raw-SQL trial-execute is not performed and the existing
  query-backed cache validation is unchanged

### Requirement: Validation applies at every create and edit door

The save-time SQL check SHALL run at every door that creates or edits a model
through the engine — the REST create/update endpoints, the CLI create command,
and the MCP `create_dataset` and `edit_dataset` tools. Direct ingestion / YAML-load
persistence SHALL remain out of scope and unaffected.

#### Scenario: REST create with invalid SQL is rejected

- **WHEN** a raw-`sql` model with SQL invalid for a reachable datasource is
  submitted to the REST create endpoint
- **THEN** the request fails with a client error and the model is not persisted

#### Scenario: MCP create_model with invalid SQL is rejected

- **WHEN** the MCP `create_dataset` tool is called with a raw-`sql` model whose SQL
  is invalid for a reachable datasource
- **THEN** the tool returns an error message and the model is not persisted

#### Scenario: MCP edit_model changing sql to invalid is rejected and leaves the original intact

- **WHEN** the MCP `edit_dataset` tool changes a model's `sql` to SQL invalid for a
  reachable datasource
- **THEN** the tool returns an error message and the previously-persisted model
  is left unchanged

#### Scenario: Ingestion / YAML-load persistence is not subject to the check

- **WHEN** a model is persisted through the ingestion / YAML-load path
- **THEN** the save-time trial-execute is not performed

### Requirement: Aggregation formulas are parse-checked at save time

When a model is saved through any engine create or edit door, every model-defined
aggregation `expression` SHALL be parsed with its placeholders replaced, in the model's
datasource dialect (sqlglot's generic dialect when the datasource cannot be resolved —
the check is never skipped). A formula that does not parse, or that places a placeholder
in a non-expression position, SHALL fail the save with an error naming the model and the
aggregation, before anything is persisted or trial-executed. Placeholder names SHALL NOT
be checked at save time, since query-time arguments may legitimately supply them; but a
declared parameter the rendered formula never references (for a formula-less built-in, one
that is not among its own parameters), or a formula on the ranked `first`/`last`, SHALL
fail the save with a typed error naming the model, the aggregation and the parameter.
Ingestion / YAML-load persistence SHALL remain out of scope.

#### Scenario: Unparseable formula blocks the save

- **WHEN** a model declaring an aggregation with formula `SUM({value}` is saved through the
  MCP `create_dataset` tool
- **THEN** the tool returns an error naming the model and the aggregation
- **AND** the model is not persisted

#### Scenario: Query-time-only placeholder is accepted

- **WHEN** a model declaring an aggregation with formula `SUM({value} * {scale})` and no
  declared `scale` parameter is saved
- **THEN** the save succeeds

#### Scenario: A declared parameter the formula never references blocks the save

- **WHEN** a model declaring an aggregation with formula `SUM({value}) * {k}` and parameters
  `k` and `unused` is saved
- **THEN** the save fails with a typed error naming the model, the aggregation and `unused`
- **AND** the model is not persisted

#### Scenario: Edit introducing a broken formula leaves the original intact

- **WHEN** the MCP `edit_dataset` tool changes an aggregation's formula to `SUM({value}`
- **THEN** the tool returns an error and the previously-persisted model is unchanged

### Requirement: MCP create_model accepts model-defined aggregations

The MCP `create_dataset` tool SHALL accept an optional `aggregations` list of aggregation
definitions (the same shape `edit_dataset` upserts), persisting them on the created model
subject to the same save-time checks. Combining `aggregations` with `query` SHALL be
rejected like the other table-model parameters.

#### Scenario: Custom aggregation declared at creation

- **WHEN** `create_dataset` is called with a table model and an aggregation
  `{"name": "sum_sq", "expression": "SUM({value} * {value})"}`
- **THEN** the model is persisted with that aggregation and a query using
  `sum_sq(amount)` succeeds

### Requirement: Self-referencing query-backed models are rejected at save time
Saving a query-backed model whose `source_queries` reference the model itself, directly or
through other query-backed models, SHALL fail with the query-backed cycle error naming the ordered
cycle path. **BREAKING**: such models previously saved through a cached-SQL short-circuit.

#### Scenario: Direct self-reference
- **WHEN** a query-backed model `A` whose stage sources `A` is saved
- **THEN** the save fails with the cycle error naming `A -> A`

#### Scenario: Transitive self-reference
- **WHEN** query-backed model `A` sources `B` and `B` sources `A`, and `A` is saved
- **THEN** the save fails with the cycle error naming the path

### Requirement: Model source SQL must be read-only and parse with its placeholders as literals

A raw-`sql` model's source SHALL be a read-only query. Before any trial-execution
the source SHALL be statically parsed and admitted only when its root is a query
(SELECT / set-operation / subquery) with no data-modifying statement nested (e.g.
a data-modifying CTE). Anything else — a non-query statement (INSERT / UPDATE /
DELETE / MERGE / DDL / COPY / GRANT / CALL / …) or SQL that cannot be parsed at
all — SHALL be rejected with no database round-trip, so persisting a model can
never mutate the datasource and SQL the query generator could not run either
fails fast. A parameterized source (`{var}` / `{? ?}`) SHALL be parsed with each
placeholder standing for a literal, so a parameterized non-read-only statement is
rejected, a parameterized source that does not parse is rejected, and a placeholder
in a non-literal position is rejected with the literal-values error
(`queries/variables` › Mode-A surfaces bind variables as literals). The trial-execute that follows for a parsed read-only query
SHALL run read-only where the dialect supports it and SHALL always roll back.

#### Scenario: Data-modifying model SQL is rejected without executing

- **WHEN** a raw-`sql` model whose `sql` holds a data-modifying statement (e.g.
  `WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x`) is saved
- **THEN** the save fails with an error and the SQL is never executed against
  the datasource

#### Scenario: Unparseable (non-parameterized) model SQL is rejected without a database call

- **WHEN** a raw-`sql` model whose `sql` carries no placeholders and cannot be
  parsed is saved
- **THEN** the save fails with an error and no query is run against the datasource

#### Scenario: An identifier-position placeholder is rejected

- **WHEN** a raw-`sql` model whose `sql` has an identifier-position placeholder
  (e.g. `SELECT * FROM {table}`) is saved
- **THEN** the save fails with the literal-values error and no query is run against
  the datasource

#### Scenario: Parameterized data-modifying SQL is rejected

- **WHEN** a raw-`sql` model whose `sql` is data-modifying but carries a
  placeholder (e.g. `DELETE FROM orders WHERE id = {id}`) is saved
- **THEN** the save fails with an error and no query is run against the datasource

#### Scenario: The trial-execute cannot persist a mutation

- **WHEN** a raw-`sql` model is trial-executed at save time
- **THEN** the probe runs in a rolled-back transaction (read-only where the
  dialect supports it), so nothing it runs is committed

### Requirement: Saves judge what they add or change
A save through SLayer SHALL apply its save-time checks to every definition the save adds or changes, and SHALL run the broken-dependents analysis over every stored definition that depends on what changed, as saves do today. A definition the save neither adds nor changes, and which depends on nothing the save changes, SHALL NOT be judged, so a stored definition that fails every query (allowed in a user-written document, `models/semantic-model-documents` › Entities SLayer cannot evaluate yet still load) never blocks an unrelated save. A definition SLayer is asked to add or change that is guaranteed to fail every query SHALL be refused, whether or not Ossie defines it.

#### Scenario: An unrelated edit beside a windowed metric
- **WHEN** a stored document holds a model-scoped metric using `OVER`, and `edit_dataset` changes the description of one of its datasets
- **THEN** the save succeeds and the windowed metric is unchanged

#### Scenario: Authoring a windowed metric is refused
- **WHEN** `edit_semantic_model` adds a model-scoped metric whose expression uses `OVER`
- **THEN** the save fails with a typed error naming the metric and the window function

#### Scenario: Renaming a referenced field warns about dependents
- **WHEN** a save renames the document field `orders.amount` that a model-scoped metric reads
- **THEN** the save reports the metric as a broken dependent, exactly as a save of a standalone dataset reports broken dependents today

### Requirement: Saves re-validate the whole dataset or datasource

Saving a dataset, saving a document dataset's SLayer extensions, and saving a datasource SHALL first validate the object in full, every nested part included, with the checks construction applies, before any canonicalization, dialect conversion, peer loading or write. An object these checks refuse SHALL be refused with the construction error and nothing SHALL be written. Runtime-only state the object carries SHALL take part in the check as it does at construction, and the saved object SHALL NOT be modified by the check.

#### Scenario: A dataset changed in place is refused on save
- **WHEN** a dataset is saved after a field duplicating another's name was appended to its fields, after a key was appended to one relationship's `from_columns` only, after a non-field value was appended to its fields, or after `"bogus"` was appended to one field's `allowed_aggregations`
- **THEN** each save is refused with the error construction raises for that state, and the store's write was never called

#### Scenario: Document dataset extensions are re-validated
- **WHEN** a document-declared dataset whose fields gained a duplicate name in place has its SLayer extensions saved
- **THEN** the save is refused with the construction error and the document is unchanged

#### Scenario: A datasource changed in place is refused on save
- **WHEN** a datasource is saved after a value that is not a granularity was appended to its granularities
- **THEN** the save is refused with the construction error and the store's write was never called

#### Scenario: A valid dataset with runtime-only state saves as before
- **WHEN** a dataset carrying a metric that loaded with a load warning, and a document-declared dataset, are saved unchanged
- **THEN** both saves succeed exactly as before this change

### Requirement: Enforced keys name declared keys of a table-backed dataset
Saving a dataset SHALL refuse an `enforced_keys` entry that is not one of its declared key
sets (its primary key, or a unique field), one naming a field that is not a base column,
and any `enforced_keys` on a dataset backed by `sql` or a backing query. A save that changes
a dataset's `sql_table`, `sql`, backing query or datasource SHALL drop its `enforced_keys`;
one that makes a field an enforced key names no longer a base column, or a different one,
SHALL drop that key.

#### Scenario: An undeclared enforced key is refused
- **WHEN** a dataset with primary key `id` is saved with `enforced_keys: [[email]]` and `email`
  is not unique
- **THEN** the save fails naming the entry

#### Scenario: A query-backed dataset cannot carry enforced keys
- **WHEN** a dataset backed by `sql` is saved with an enforced key
- **THEN** the save fails

#### Scenario: Repointing drops enforced keys
- **WHEN** a dataset recording `[customer_id]` as enforced is saved with a different
  `sql_table`
- **THEN** the stored dataset has no enforced keys

#### Scenario: An enforced key on a computed field is refused
- **WHEN** a dataset is saved with `enforced_keys: [[customer_id]]` and field `customer_id`
  has the expression `lower(email)`
- **THEN** the save fails naming the entry

#### Scenario: Re-pointing a key field drops its enforced key
- **WHEN** a dataset recording `[customer_id]` as enforced is saved with field `customer_id`
  reading another column, or a computed expression
- **THEN** the stored dataset no longer records `[customer_id]` as enforced
