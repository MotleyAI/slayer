## Purpose

Defines how a session policy's join rules scope the tables they target, so that
tenant scoping holds on every supported database, user permission level, and
server version.

## ADDED Requirements

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
