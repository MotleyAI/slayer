## MODIFIED Requirements

### Requirement: A query is one span tree
A query SHALL produce a `slayer.query` span with the children `slayer.query.normalize`, `slayer.query.prepare` and `slayer.query.run`; an EXPLAIN query has no `slayer.query.run`, and its database call is a child of `slayer.query`. The policy rewrite SHALL be `slayer.query.policy`. The result cache SHALL be `slayer.query.cache.get` and `slayer.query.cache.put`. Each database call SHALL be a child span: `slayer.db.execute`, `slayer.db.explain` or `slayer.db.metadata`.

#### Scenario: Query span tree
- **WHEN** a query runs on a SQLite datasource
- **THEN** `slayer.db.execute` is a child of `slayer.query.run`, and `slayer.query.run` is a child of `slayer.query`

#### Scenario: EXPLAIN query
- **WHEN** a query without a session policy runs with `explain=True` and without `dry_run=True`
- **THEN** exactly one database span exists, `slayer.db.explain`, a child of `slayer.query`, and no `slayer.query.run` span exists
