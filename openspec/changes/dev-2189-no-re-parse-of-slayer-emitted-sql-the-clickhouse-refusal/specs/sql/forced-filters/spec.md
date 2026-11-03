## ADDED Requirements

### Requirement: Forced filters rewrite the statement before its single render

A session policy SHALL scope every statement SLayer builds and runs under it — the data
query of execute, dry run and explain, the refresh-key scan, the join-cardinality side
statistics and the column-type probe — by rewriting the statement's syntax tree before the
statement is rendered, never by parsing rendered SQL. The rewrite SHALL scope exactly the
physical tables, with exactly the predicates, that a rewrite of the same statement's
rendered SQL would, on every dialect. Every fail-closed refusal SHALL be preserved: a
non-query statement, a table whose tenant column cannot be confirmed, a table the policy
does not cover, or an invalid join path SHALL be refused before any SQL for that statement
reaches the database; each statement kind keeps its existing outcome for a refusal (the
column-type probe yields no types, every other kind raises the forced-filter error).

#### Scenario: Scoped SQL matches the previous rewrite

- **WHEN** a query under a column ruleset (pass or block) or a join ruleset (one hop,
  multi-hop, whitelist) is generated on any supported dialect, for plain, joined,
  multi-stage, query-backed, sql-mode, semi-join-filtered, list-unnesting, time-spine,
  cross-model and paginated shapes
- **THEN** the scoped SQL is equivalent, up to formatting, to rewriting the unscoped
  rendered SQL
- **AND** on SQLite, DuckDB and Postgres the returned rows are identical

#### Scenario: Dotted result names survive scoping

- **WHEN** a policy-scoped query whose result names contain dots runs on BigQuery or
  T-SQL
- **THEN** the scoped SQL carries the dialect's mangled result names and the response
  carries the canonical dotted names

#### Scenario: Unconfirmable column fails closed on every statement kind

- **WHEN** a column-ruleset policy cannot confirm its column on a table read by the data
  query, the refresh-key scan, the side statistics or the column-type probe
- **THEN** no SQL for that statement reaches the database
- **AND** the data query, refresh-key scan and side statistics raise the forced-filter
  error, while the column-type probe returns no types

#### Scenario: Uncovered table fails closed

- **WHEN** a join-ruleset policy meets a table that is neither its anchor, a join target
  nor whitelisted
- **THEN** the statement is refused with the forced-filter error naming that table and
  nothing executes

#### Scenario: Refresh-key and profiling statements are tenant-scoped

- **WHEN** a cached query's refresh-key scan or a join-cardinality detection runs under a
  session policy
- **THEN** the statement sent to the database reads only the tenant's rows
