# sql/forced-filters Specification

## Purpose
Defines how a session policy's join rules scope the tables they target, so that
tenant scoping holds on every supported database, user permission level, and
server version.

## Requirements

### Requirement: Join rules scope without changing server settings
On a database whose correlated subqueries need a server setting the user may be
unable to change (ClickHouse), a join rule SHALL scope its target table with a
non-correlated semi-join that sends no setting and needs no minimum server
version. The scoped rows SHALL be exactly those whose join path reaches an anchor
row matching the tenant value: a target row whose join key is NULL, or whose key
matches only a NULL-keyed anchor-side row, SHALL never be admitted, whatever NULL
comparison semantics the user's profile configures. Other databases SHALL keep
their existing correlated semi-join unchanged.

#### Scenario: Readonly user on a server with correlated subqueries off
- **WHEN** a `readonly = 1` user on ClickHouse 25.4 runs a query under a join-based
  session policy whose join target is `orders`
- **THEN** the query succeeds and returns only the orders whose customer belongs to
  the policy's tenant

#### Scenario: Server without correlated subqueries
- **WHEN** a query under a join-based session policy runs on a ClickHouse server
  older than 25.4
- **THEN** the query succeeds with correctly scoped rows instead of failing closed

#### Scenario: No setting is sent
- **WHEN** a join-based session policy rewrites a query for ClickHouse
- **THEN** the rewritten SQL carries no `allow_experimental_correlated_subqueries`
  setting and no correlated subquery

#### Scenario: Multi-hop join path
- **WHEN** a policy scopes `line_items` through `line_items → orders → customers` on
  ClickHouse
- **THEN** only the line items whose order's customer belongs to the tenant are
  returned

#### Scenario: NULL keys never admit a row
- **WHEN** a ClickHouse user whose profile sets `transform_null_in = 1` queries a
  join target containing a row with a NULL join key, and the anchor side holds a
  tenant row with a NULL key
- **THEN** the query succeeds, returns the tenant's non-NULL-keyed rows, and does not
  return the NULL-keyed target row

#### Scenario: Distributed tables
- **WHEN** a join-based policy scopes a ClickHouse `Distributed` target table whose
  join path reaches a `Distributed` anchor table, with related rows on different shards
- **THEN** the query succeeds with correctly scoped rows

#### Scenario: Other databases keep the correlated semi-join
- **WHEN** a join-based session policy rewrites a query for Postgres or DuckDB
- **THEN** the rewritten SQL is identical to the SQL emitted before this change

### Requirement: Forced filters rewrite the statement before its single render

A session policy SHALL scope every statement SLayer builds and runs under it — the data
query of execute, dry run and explain, the refresh-key scan, the join-cardinality side
statistics and the column-type probe — by rewriting the statement's syntax tree before the
statement is rendered, never by parsing rendered SQL. The rewrite SHALL scope exactly the
physical tables, with exactly the predicates, that a rewrite of the same statement's
rendered SQL would, on every dialect, except that a schema- or catalog-qualified table
sharing a CTE's name is scoped too. Every fail-closed refusal SHALL be preserved: a
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
