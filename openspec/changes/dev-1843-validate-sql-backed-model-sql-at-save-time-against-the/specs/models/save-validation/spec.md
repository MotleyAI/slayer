## Purpose

Defines how a model's source is validated at save time — before it is persisted —
so that a raw-`sql` model whose SQL is invalid for its datasource dialect is
rejected at the create/edit door rather than failing silently on first query.

## ADDED Requirements

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
persist the model, because a placeholder in identifier position cannot be safely
filled for a trial run. A placeholder that appears only in a column expression or
filter, and not in the model's `sql`, SHALL NOT cause the model `sql` check to be
skipped.

#### Scenario: Parameterized model SQL is skipped and saved

- **WHEN** a raw-`sql` model whose `sql` contains a `{var}` placeholder is saved
- **THEN** the trial-execute is skipped and the model is persisted

#### Scenario: A placeholder only in a column expression does not skip the model-SQL check

- **WHEN** a raw-`sql` model whose `sql` is static but a `Column.sql` contains a
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
and the MCP `create_model` and `edit_model` tools. Direct ingestion / YAML-load
persistence SHALL remain out of scope and unaffected.

#### Scenario: REST create with invalid SQL is rejected

- **WHEN** a raw-`sql` model with SQL invalid for a reachable datasource is
  submitted to the REST create endpoint
- **THEN** the request fails with a client error and the model is not persisted

#### Scenario: MCP create_model with invalid SQL is rejected

- **WHEN** the MCP `create_model` tool is called with a raw-`sql` model whose SQL
  is invalid for a reachable datasource
- **THEN** the tool returns an error message and the model is not persisted

#### Scenario: MCP edit_model changing sql to invalid is rejected and leaves the original intact

- **WHEN** the MCP `edit_model` tool changes a model's `sql` to SQL invalid for a
  reachable datasource
- **THEN** the tool returns an error message and the previously-persisted model
  is left unchanged

#### Scenario: Ingestion / YAML-load persistence is not subject to the check

- **WHEN** a model is persisted through the ingestion / YAML-load path
- **THEN** the save-time trial-execute is not performed

### Requirement: Model source SQL must be read-only

A raw-`sql` model's source SHALL be a read-only query. Before any trial-execution
the source SHALL be statically parsed and admitted only when its root is a query
(SELECT / set-operation / subquery) with no data-modifying statement nested (e.g.
a data-modifying CTE). Anything else — a non-query statement (INSERT / UPDATE /
DELETE / MERGE / DDL / COPY / GRANT / CALL / …) or SQL that cannot be parsed at
all — SHALL be rejected with no database round-trip, so persisting a model can
never mutate the datasource and SQL the query generator could not run either
fails fast. Parameterized SQL (`{var}` / `{? ?}`) SHALL still be classified after
its placeholders are normalized to parse-safe tokens, so a parameterized
statement that is not a read-only query — or is unparseable — is rejected even
though it is never trial-run. The
trial-execute that follows for a parsed read-only query SHALL run read-only where
the dialect supports it and SHALL always roll back.

#### Scenario: Data-modifying model SQL is rejected without executing

- **WHEN** a raw-`sql` model whose `sql` holds a data-modifying statement (e.g.
  `WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x`) is saved
- **THEN** the save fails with an error and the SQL is never executed against
  the datasource

#### Scenario: Unparseable model SQL is rejected without a database call

- **WHEN** a raw-`sql` model whose `sql` cannot be parsed (after normalizing any
  `{var}` / `{? ?}` placeholders to parse-safe tokens) is saved
- **THEN** the save fails with an error and no query is run against the datasource

#### Scenario: Parameterized data-modifying SQL is rejected

- **WHEN** a raw-`sql` model whose `sql` is data-modifying but carries a
  placeholder (e.g. `DELETE FROM orders WHERE id = {id}`) is saved
- **THEN** the save fails with an error and no query is run against the datasource

#### Scenario: The trial-execute cannot persist a mutation

- **WHEN** a raw-`sql` model is trial-executed at save time
- **THEN** the probe runs in a rolled-back transaction (read-only where the
  dialect supports it), so nothing it runs is committed
