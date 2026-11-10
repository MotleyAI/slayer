## MODIFIED Requirements

### Requirement: FK reflection failure loses only that table's joins
When reading one table's foreign keys fails, ingestion SHALL still ingest that table, with no joins
declared on it, and SHALL ingest every other table and join unaffected. Each failed key read (primary
key, foreign keys, unique constraints, unique indexes) SHALL be logged at WARNING level, naming the
table, the read that failed and the error. Each failed key read SHALL also be reported on the ingest
result, naming the table's location (schema, and catalog where the database has one), the read and the
error, once per location and read in one ingest; every ingest surface (MCP, CLI, REST) SHALL show it.
A failed key read SHALL NOT change the ingest's exit status or HTTP status, and a later ingest whose
read succeeds SHALL fill the missing key.

#### Scenario: One table's FK reflection fails
- **WHEN** reading the foreign keys of `orders` raises during ingestion
- **THEN** `orders` is ingested with its columns and no declared joins, and is not reported as skipped

#### Scenario: A failed key read is visible at the default log level
- **WHEN** reading the foreign keys of `orders` raises during ingestion
- **THEN** a WARNING log record names `orders`, the foreign-key read and the error message

#### Scenario: A failed key read is reported on every ingest surface
- **WHEN** reading the primary key of `customers` raises during an ingest run through MCP
  `create_datasource`, MCP `ingest_datasource_models`, the CLI and the REST API
- **THEN** each result names `customers` with its schema, the primary-key read and the error, the table
  is ingested without a primary key, and the CLI exit status and HTTP status are those of the same
  ingest without the failure

#### Scenario: Same-named tables in two schemas are reported apart
- **WHEN** a multi-schema ingest fails to read the primary key of `orders` in schema `a` but not in
  schema `b`
- **THEN** exactly one failure is reported, naming `orders` in schema `a`

#### Scenario: A re-ingest fills a lost key
- **WHEN** a table was ingested without its primary key because the read failed, and the datasource is
  re-ingested with the read succeeding
- **THEN** the model gains the primary key and the joins into it gain their cardinality

### Requirement: Ingestion reads every declared key on every supported SQLAlchemy version
Ingestion SHALL read each table's declared primary key, foreign keys, UNIQUE constraints and unique
indexes that constrain the whole table over plain columns, and SHALL use them for column primary-key
and uniqueness flags, joins and join cardinality, on every SQLAlchemy release the package's
dependency range allows. On DuckDB the keys SHALL be read from DuckDB's own catalog, never through
another database's catalog queries, scoped to the table's catalog and schema so a same-named table in
another attached catalog or schema contributes nothing. On SQLite the keys SHALL be read from SQLite's
own catalog, scoped to the table's schema (`main`, `temp` or an attached database), and no SQLite key
read SHALL run a statement that fails. A partial unique index, or a unique index with an expression
member, SHALL NOT count as a key.

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

#### Scenario: SQLite keys are read without a failing statement
- **WHEN** a SQLite database holding a composite primary key, a `WITHOUT ROWID` table, a foreign key
  naming no parent columns, a composite foreign key, a UNIQUE constraint, a unique index, a partial
  unique index, a unique index over a column and an expression, a keyless table and a view is ingested
- **THEN** every declared key is ingested — the implicit foreign key resolving to the parent's primary
  key — the partial and the mixed column-and-expression index count as no key, the keyless table and
  the view have none, and no statement executed while reading keys fails

#### Scenario: SQLite keys are scoped to the table's schema
- **WHEN** a SQLite database has an attached database holding a same-named table with different keys,
  and a temporary table with keys
- **THEN** each location's keys are its own
