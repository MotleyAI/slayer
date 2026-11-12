## ADDED Requirements

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

## MODIFIED Requirements

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

- **WHEN** a raw-`sql` model whose `sql` is static but a `Column.sql` contains a
  `{var}` placeholder is saved, and the static `sql` is invalid for the backend
- **THEN** the model `sql` is still trial-executed and the invalid SQL is
  rejected

## REMOVED Requirements

### Requirement: Model source SQL must be read-only
**Reason**: A placeholder now binds only as a literal, so a parameterized source that parses only once an identifier is filled can no longer exist, and the rule that skips one contradicts `queries/variables` › Mode-A surfaces bind variables as literals; replaced by "Model source SQL must be read-only and parse with its placeholders as literals".
**Migration**: Replace an identifier-position placeholder in model `sql` with the identifier itself, or with one stored model per variant.
