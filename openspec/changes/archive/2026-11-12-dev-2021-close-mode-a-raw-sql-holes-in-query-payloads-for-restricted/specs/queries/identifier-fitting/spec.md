## MODIFIED Requirements

### Requirement: User-authored identifiers pass through byte-identical

Identifiers originating from user-authored surfaces — model `sql`, `sql_table`, model
`filters`, column `name`, column `sql`, column `filter`, on stored models and
per-stage source models — SHALL never be
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
