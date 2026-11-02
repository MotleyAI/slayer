## ADDED Requirements

### Requirement: Every live database access goes through an engine-owned client

Every read of or statement against a user datasource — query and explain statements, column-type probes, cache refresh scans, join-cardinality profiling, server-profile probes, schema-drift listing and reflection, row-level-security column checks, ingestion, legacy type refinement, the MCP datasource tools and the dbt/OSI importers' table introspection — SHALL go through a client obtained from a query engine. A query engine SHALL build its clients only through its client factory when one is given, else as the default client, and SHALL reuse one client per datasource name and connection identity. No call order SHALL let a client from any other source serve a later access. Without a client factory, observable behaviour SHALL be unchanged.

#### Scenario: Query statements reach the factory client
- **WHEN** an engine built with a client factory executes a query and an explain query
- **THEN** both statements reach the factory's client verbatim, and no direct database connection is opened

#### Scenario: Engine-internal statements reach the factory client
- **WHEN** that engine runs a column-type probe, a cache refresh scan, join-cardinality profiling, and a query on a ClickHouse datasource that needs the server profile
- **THEN** every statement and the profile request reach the factory's client

#### Scenario: A gateway with no direct access serves every feature
- **WHEN** every direct connection path is made to fail, and an engine whose factory returns a gateway client answering statements and metadata from a fixture runs: ingestion of a datasource, model validation, a query under a column-scoped session policy, the load of a model stored below version 8, the MCP create-datasource and describe-datasource tools, and dbt and OSI imports that introspect tables
- **THEN** each succeeds with the results the default client gives over the same fixture

#### Scenario: Validation before the first query cannot bypass the factory
- **WHEN** an engine with a client factory validates the models of a datasource that has a SQL-backed model, and then executes a query on that datasource
- **THEN** the query runs on the factory's client

#### Scenario: One client per datasource and connection identity
- **WHEN** an engine with a client factory runs several queries on one datasource, and one query on each of two datasources with different names that share one connection URL
- **THEN** the factory is called once for the first datasource and once for each of the other two

#### Scenario: A factory need not extend the default client
- **WHEN** the factory returns an object that implements the client contract without extending the default client
- **THEN** queries and metadata reads run through it

#### Scenario: Only the default client touches the database driver
- **WHEN** the package source is inspected
- **THEN** database engines, inspectors and driver connections for user datasources are created or used only inside the default client and the connection and dialect code it owns; SLayer's own storage databases, facade-local catalogs and demo data generation are exempt

### Requirement: The query engine owns the clients it builds

A query engine SHALL be the only party that closes the clients it built; code that borrows a client SHALL NOT close it. Closing a query engine, synchronously or asynchronously, SHALL close every client it built, including factory clients, and the clients SHALL remain usable afterwards. Every synchronous entry point SHALL run its whole workflow in one bridged event loop and SHALL release the engine's loop-bound client resources at the end of that loop, also when the workflow raises or is cancelled.

#### Scenario: Join profiling leaves the shared client open
- **WHEN** join-cardinality profiling runs and a query on the same datasource follows on the same engine
- **THEN** profiling never closes the client, and the query reuses it

#### Scenario: Closing the engine reaches factory clients
- **WHEN** an engine whose factory built two clients is closed asynchronously, then synchronously, and then executes a query
- **THEN** both clients were closed each time, and the query succeeds on a usable client

#### Scenario: A multi-step CLI command uses one event loop
- **WHEN** `slayer validate-models --cardinality` runs on a Postgres datasource reached through an asynchronous driver
- **THEN** drift validation and join profiling both complete, and no join is reported as failed to scan

#### Scenario: Synchronous calls repeat safely
- **WHEN** a synchronous client method is called three times in a row, once from inside a running event loop, and once with a workflow that raises
- **THEN** each call returns or raises its own result, and the engine's loop-bound client resources are released after each

### Requirement: Live database access never blocks the event loop

Live database access reached from asynchronous code SHALL run without blocking the event loop.

#### Scenario: Legacy refinement keeps the loop responsive
- **WHEN** a model stored below version 8 is loaded on a non-SQLite datasource whose client answers metadata slowly, and separately on a SQLite datasource whose integer-affinity probe is slow
- **THEN** a concurrent task keeps advancing while each load runs

#### Scenario: MCP datasource tools keep the loop responsive
- **WHEN** the MCP create-datasource, describe-datasource and empty-result ingest tools run against a slow client
- **THEN** a concurrent task keeps advancing while each runs

### Requirement: Entry points accept the client factory

`SlayerQueryEngine`, `SlayerClient` in local mode, the REST and MCP server builders, and the Postgres-facade and Flight engine builders SHALL accept a client factory and SHALL pass it to every query engine they build. `SlayerClient` in remote mode SHALL reject a client factory with a `ValueError`. CLI commands SHALL use the default client.

#### Scenario: The local Python client passes the factory through
- **WHEN** `SlayerClient` is built with storage and a client factory and runs a query
- **THEN** the query runs on the factory's client

#### Scenario: Remote mode rejects a factory
- **WHEN** `SlayerClient` is built with a URL and a client factory
- **THEN** construction raises a `ValueError`

#### Scenario: Server builders pass the factory to every engine
- **WHEN** the REST, MCP, Postgres-facade and Flight builders are given a client factory and serve a query and a datasource-describing request
- **THEN** every engine they build uses the factory's clients
