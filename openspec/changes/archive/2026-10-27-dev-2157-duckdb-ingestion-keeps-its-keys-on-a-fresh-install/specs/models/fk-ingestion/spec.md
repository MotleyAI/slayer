## ADDED Requirements

### Requirement: Ingestion reads every declared key on every supported SQLAlchemy version
Ingestion SHALL read each table's declared primary key, foreign keys, UNIQUE constraints and unique
indexes that constrain the whole table over plain columns, and SHALL use them for column primary-key
and uniqueness flags, joins and join cardinality, on every SQLAlchemy release the package's
dependency range allows. On DuckDB the keys SHALL be read from DuckDB's own catalog, never through
another database's catalog queries, scoped to the table's catalog and schema so a same-named table in
another attached catalog or schema contributes nothing. A partial unique index, or a unique index with
an expression member, SHALL NOT count as a key.

#### Scenario: DuckDB demo keeps its joins on a fresh install
- **WHEN** the bundled Jaffle Shop demo is ingested with the newest SQLAlchemy release the dependency
  range allows
- **THEN** `items`, `orders`, `supplies` and `tweets` carry their FK joins (items → orders,
  items → products, orders → customers, orders → stores, supplies → products, tweets → customers),
  every model's primary key is set (`supplies` composite `id, sku`), and the README query
  `{"source_model": "orders", "measures": ["sum(order_total)", "change_pct(sum(order_total))"], "dimensions": ["stores.name", "month(ordered_at)"]}`
  returns rows

#### Scenario: DuckDB UNIQUE constraints mark columns unique
- **WHEN** a DuckDB table declares `code VARCHAR UNIQUE` and `UNIQUE (a, b)`, and another table's FK
  references `code`
- **THEN** the ingested `code` column is unique, `(a, b)` counts as a composite key for join
  cardinality, and the FK join referencing `code` is inferred as to-one

#### Scenario: DuckDB composite and cross-schema keys
- **WHEN** a DuckDB table in a non-default schema declares a composite primary key and a composite FK
  to another table in that schema, and an attached second catalog holds same-named tables with
  different keys
- **THEN** the composite primary key and the composite join are ingested from the table's own catalog
  and schema only

#### Scenario: DuckDB unique indexes
- **WHEN** a DuckDB table has `CREATE UNIQUE INDEX ON t ("Mixed Case")` and
  `CREATE UNIQUE INDEX ON t (lower(name))`
- **THEN** `"Mixed Case"` counts as a single-column key and the expression index counts as no key

#### Scenario: The host database's generic catalog queries are not consulted on DuckDB
- **WHEN** SQLAlchemy's generic key reflection raises for every DuckDB table
- **THEN** DuckDB ingestion still produces every primary key, unique key and join

## MODIFIED Requirements

### Requirement: FK reflection failure loses only that table's joins
When reading one table's foreign keys fails, ingestion SHALL still ingest that table, with no joins
declared on it, and SHALL ingest every other table and join unaffected. Each failed key read (primary
key, foreign keys, unique keys) SHALL be logged at WARNING level, naming the table, the read that
failed and the error.

#### Scenario: One table's FK reflection fails
- **WHEN** reading the foreign keys of `orders` raises during ingestion
- **THEN** `orders` is ingested with its columns and no declared joins, and is not reported as skipped

#### Scenario: A failed key read is visible at the default log level
- **WHEN** reading the foreign keys of `orders` raises during ingestion
- **THEN** a WARNING log record names `orders`, the foreign-key read and the error message
