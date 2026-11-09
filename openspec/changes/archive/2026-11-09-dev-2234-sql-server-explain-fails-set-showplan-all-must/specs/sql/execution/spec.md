## MODIFIED Requirements

### Requirement: Every live database access goes through an engine-owned client

Every read of or statement against a user datasource — query and explain statements, column-type probes, cache refresh scans, join-cardinality profiling, server-profile probes, schema-drift listing and reflection, row-level-security column checks, ingestion, legacy type refinement, the MCP datasource tools and the dbt/OSI importers' table introspection — SHALL go through a client obtained from a query engine. A query engine SHALL build its clients only through its client factory when one is given, else as the default client, and SHALL reuse one client per datasource name and connection identity. No call order SHALL let a client from any other source serve a later access. Without a client factory, observable behaviour SHALL be unchanged.

#### Scenario: Query statements reach the factory client
- **WHEN** an engine built with a client factory executes a query and an explain query
- **THEN** the query's statement reaches the factory client's statement execution and the explain's statements reach its explain operation, each verbatim, and no direct database connection is opened

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
