# queries/identifier-fitting Specification (delta)

## Purpose

Governs how SLayer-minted SQL identifiers — outermost projection aliases and internal
CTE column aliases alike — are fitted to each dialect's identifier byte limit at
emission, how user-authored identifiers are exempted from fitting, and how unfittable
or colliding names fail closed instead of truncating silently.

## ADDED Requirements

### Requirement: Every SLayer-minted identifier fits the dialect limit

On a dialect with a finite identifier byte limit, every identifier SLayer mints into
the emitted SQL — internal CTE column aliases included, not just outermost projection
aliases — SHALL be at most the dialect's limit in UTF-8 bytes, shortened deterministically
to a `<head>_<hash8>_<tail>` form. All occurrences of one pre-fit name (definition and
every reference, across CTE boundaries) SHALL be rewritten to the same fitted form.

#### Scenario: Over-limit internal CTE column alias is fitted everywhere

- **WHEN** a query compiles to a producer CTE whose canonical output column name exceeds
  the dialect's identifier byte limit (e.g. the 298-byte `lift/nested_attach` alias on
  postgres)
- **THEN** the emitted SQL contains no SLayer-minted identifier over the limit, and the
  fitted name is byte-identical at its definition and at every reference

#### Scenario: Under-limit query is byte-identical

- **WHEN** every identifier in a query's emitted SQL is within the dialect's limit
- **THEN** the emitted SQL is byte-identical to the SQL emitted before this change

#### Scenario: Unbounded dialect never fits

- **WHEN** the same over-limit query is emitted on a dialect without an identifier limit
  (e.g. SQLite, ClickHouse)
- **THEN** the canonical over-limit aliases appear unchanged in the emitted SQL

### Requirement: Distinct names stay distinct

Two distinct pre-fit identifiers SHALL never be emitted as one identifier. In
particular, two internal aliases that agree on their first `limit` bytes and differ
only beyond it SHALL emit as two distinct fitted identifiers, and the query SHALL
return the same results as on an unlimited dialect.

#### Scenario: 63-byte-prefix collision is prevented

- **WHEN** a query mints two internal aliases identical up to the dialect's byte limit
  and differing only after it (e.g. two partitioned aggregates whose partition-key
  suffixes differ), on a dialect with that limit in force
- **THEN** the emitted SQL contains two distinct fitted identifiers and executing the
  query yields the same rows as executing it with the limit lifted

#### Scenario: Fitted-form collision fails closed

- **WHEN** two distinct pre-fit names would fit to the same emitted form, or a fitted
  form equals an identifier already present in the statement
- **THEN** generation raises `IdentifierCollisionError` naming both parties, and no SQL
  is emitted

### Requirement: User-authored identifiers pass through byte-identical

Identifiers originating from user-authored surfaces — model `sql`, `sql_table`, model
`filters`, column `name`, column `sql`, column `filter`, on stored models, inline
models, `ModelExtension`-added columns, and per-stage source models — SHALL never be
rewritten by the length-fitting pass, even when they exceed the dialect limit (the
target database resolves them by its own truncation, as today). When one spelling is
both user-authored and SLayer-minted, exemption SHALL win: the name passes through
unfitted everywhere.

#### Scenario: Over-limit physical column in user Column.sql survives

- **WHEN** a model column's `sql` references a physical column whose quoted name exceeds
  the dialect limit
- **THEN** that identifier appears byte-identical in the emitted SQL

#### Scenario: Over-limit physical column in a non-root stage survives

- **WHEN** a multi-stage query's non-root named stage binds a concrete user model whose
  SQL references an over-limit physical identifier
- **THEN** that identifier appears byte-identical in the emitted SQL

### Requirement: Fitting never rewrites literals or comments

The length-fitting pass SHALL NOT alter the content of string literals or SQL comments,
even when a literal or comment contains text equal to an identifier being fitted.

#### Scenario: Alias text inside a string literal survives

- **WHEN** the emitted SQL contains a string literal whose content includes the quoted
  form of an over-limit identifier that is fitted elsewhere in the statement
- **THEN** the literal's content is byte-identical after fitting while identifier
  occurrences are fitted

### Requirement: Surviving over-limit identifiers fail closed

After the emission rewrite, if the final SQL still contains an identifier-shaped token
over the dialect limit that is neither exempt nor fitted, generation SHALL raise a typed
error rather than hand the statement to the database for silent truncation. This check
SHALL be active in production, not only under a test harness.

#### Scenario: Unaccounted over-limit token is rejected

- **WHEN** the post-rewrite SQL contains a non-exempt over-limit identifier token
- **THEN** generation raises a typed error naming the token and the dialect limit

### Requirement: Fitting composes with dialect alias mangling

On dialects that mangle dotted aliases (BigQuery, T-SQL), the fitted form SHALL be sized
against the post-mangle byte length, and the mangling SHALL apply after fitting, so the
final emitted identifier is within the limit. Result-key decoding SHALL keep returning
canonical keys. The pre-existing dot-mangling contract for user-authored dotted
identifiers is unchanged by this capability (fitting-pass exemption does not extend to
the mangle pass).

#### Scenario: Fitted then mangled stays within budget

- **WHEN** an over-limit dotted internal alias is emitted on BigQuery or T-SQL
- **THEN** the final identifier (after fitting and dot-mangling) is within the dialect's
  byte limit and the query's result keys are the canonical dotted names
