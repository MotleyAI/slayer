## MODIFIED Requirements

### Requirement: Which CLI, Flight and Postgres calls open a request span
On the CLI, only `slayer query`, `slayer ingest` and `slayer datasources create` SHALL open a request span, with the command's usage token (`query`, `ingest`, `datasources.create`) as the operation. The other commands SHALL only count usage. In Flight SQL, each RPC that runs a query SHALL open one request span. This includes the schema query that `GetFlightInfo` and prepared statements run. A Postgres-facade metadata probe, such as `SHOW application_name`, runs no query. It SHALL open no request span, as usage reporting does not count it.

#### Scenario: CLI query
- **WHEN** `slayer query` runs
- **THEN** one `slayer.request` span has `slayer.surface=cli` and `slayer.operation=query`, and the `slayer.query` span is its child

#### Scenario: CLI datasource create with ingestion
- **WHEN** `slayer datasources create <url> --ingest` runs
- **THEN** one `slayer.request` span has `slayer.surface=cli` and `slayer.operation=datasources.create`, and the `slayer.ingest` span is its child

#### Scenario: Flight schema query
- **WHEN** a Flight SQL client calls `GetFlightInfo` for a query
- **THEN** the `slayer.query` span of the schema query is a child of a `slayer.request` span with `slayer.surface=flight`

#### Scenario: Dry run
- **WHEN** a query without a session policy runs with `dry_run=True`
- **THEN** a `slayer.query` span exists, and no span name starts with `slayer.db.`

### Requirement: An ingest is one span tree with its outcome
Every surface call that ingests a datasource — `slayer ingest`, `slayer datasources create --ingest` (the demo included), the MCP `ingest_datasource_models` tool, the MCP `create_datasource` tool with auto-ingestion, REST `/ingest` and the startup ingest — SHALL produce exactly one `slayer.ingest` span per ingested datasource, with the children `slayer.ingest.scan.schema`, `slayer.ingest.merge`, `slayer.ingest.drift` and `slayer.ingest.embeddings`. The ingest span SHALL carry the datasource name and type and its outcome: `slayer.ingest.datasets.added`, `.updated`, `.skipped` and `slayer.ingest.drift.entries`. `.updated` SHALL count only the existing models with a reported change, not the unchanged ones. Each ingest span SHALL record one `slayer.ingest.duration` point and add its non-zero dataset counts to `slayer.ingest.datasets`.

#### Scenario: Ingest of one changed model
- **WHEN** an idempotent ingest updates one model
- **THEN** the `slayer.ingest` span has `slayer.ingest.datasets.updated=1`, and the four step spans are its children

#### Scenario: CLI create with ingestion
- **WHEN** `slayer datasources create <url> --ingest` creates a datasource whose database has three tables
- **THEN** exactly one `slayer.ingest` span exists, with `slayer.ingest.datasets.added=3`, the datasource name and type, and the four step spans as its children; `slayer.ingest.duration` has one point and `slayer.ingest.datasets` counts 3 with `slayer.ingest.change=added`

#### Scenario: MCP create with ingestion
- **WHEN** the MCP `create_datasource` tool creates a datasource with auto-ingestion over a database with three tables
- **THEN** exactly one `slayer.ingest` span exists, with `slayer.ingest.datasets.added=3`; `slayer.ingest.duration` has one point and `slayer.ingest.datasets` counts 3 with `slayer.ingest.change=added`

#### Scenario: slayer ingest exports one ingest span
- **WHEN** `slayer ingest` ingests one datasource
- **THEN** exactly one span is named `slayer.ingest`, and `slayer.ingest.duration` has one point
