## ADDED Requirements

### Requirement: One request span per surface call
Each call on a surface SHALL open one span named `slayer.request`. It SHALL carry `slayer.surface` (`cli`, `rest`, `mcp`, `flight` or `pg`) and `slayer.operation`. The operation SHALL come from the fixed vocabulary that usage reporting uses, never from argument values. Flight SQL and the Postgres facade SHALL use `query`. Their request spans SHALL have kind `SERVER`, and other request spans kind `INTERNAL`.

#### Scenario: Span kinds
- **WHEN** request stages run for the surfaces `pg`, `flight` and `cli`
- **THEN** their spans have the kinds `SERVER`, `SERVER` and `INTERNAL`

### Requirement: Which CLI, Flight and Postgres calls open a request span
On the CLI, only `slayer query` and `slayer ingest` SHALL open a request span, with the command as the operation. The other commands SHALL only count usage. In Flight SQL, each RPC that runs a query SHALL open one request span. This includes the schema query that `GetFlightInfo` and prepared statements run. A Postgres-facade metadata probe, such as `SHOW application_name`, runs no query. It SHALL open no request span, as usage reporting does not count it.

#### Scenario: CLI query
- **WHEN** `slayer query` runs
- **THEN** one `slayer.request` span has `slayer.surface=cli` and `slayer.operation=query`, and the `slayer.query` span is its child

#### Scenario: Flight schema query
- **WHEN** a Flight SQL client calls `GetFlightInfo` for a query
- **THEN** the `slayer.query` span of the schema query is a child of a `slayer.request` span with `slayer.surface=flight`

### Requirement: REST requests are named by their route template
A REST request span SHALL use the route template as its operation. A path that matches no route SHALL be `other`. The MCP app that the REST server mounts SHALL NOT open a REST request span.

#### Scenario: REST query
- **WHEN** a client posts a query to `/query`
- **THEN** one `slayer.request` span has `slayer.surface=rest` and `slayer.operation=/query`, and the `slayer.query` span is its child

#### Scenario: Route template
- **WHEN** a client gets `/models/orders`
- **THEN** the request span has `slayer.operation=/models/{name}`

#### Scenario: Unknown path
- **WHEN** a client gets a path that matches no route
- **THEN** the request span has `slayer.operation=other`

### Requirement: MCP requests follow the OTel MCP conventions
An MCP tool call span SHALL carry the tool name as `slayer.operation` and `gen_ai.tool.name`. It SHALL also carry `mcp.method.name=tools/call` and `gen_ai.operation.name=execute_tool`. An unknown tool SHALL be named `other`.

#### Scenario: Tool call
- **WHEN** an MCP client calls the `query` tool
- **THEN** the request span has `slayer.operation=query`, `mcp.method.name=tools/call`, `gen_ai.operation.name=execute_tool` and `gen_ai.tool.name=query`

#### Scenario: Unknown tool
- **WHEN** an MCP client calls a tool that does not exist
- **THEN** the request span has `gen_ai.tool.name=other`

### Requirement: A query is one span tree
A query SHALL produce a `slayer.query` span with the children `slayer.query.normalize`, `slayer.query.prepare` and `slayer.query.run`. The policy rewrite SHALL be `slayer.query.policy`. The result cache SHALL be `slayer.query.cache.get` and `slayer.query.cache.put`. Each database call SHALL be a child span: `slayer.db.execute`, `slayer.db.explain` or `slayer.db.metadata`.

#### Scenario: Query span tree
- **WHEN** a query runs on a SQLite datasource
- **THEN** `slayer.db.execute` is a child of `slayer.query.run`, and `slayer.query.run` is a child of `slayer.query`

### Requirement: A dry run runs no query
A dry run SHALL produce no `slayer.db.execute` or `slayer.db.explain` span. Without a session policy, it SHALL produce no `slayer.db.*` span. A column session policy can read the column catalog once, which produces a `slayer.db.metadata` span.

#### Scenario: Dry run
- **WHEN** a query without a session policy runs with `dry_run=True`
- **THEN** a `slayer.query` span exists, and no span name starts with `slayer.db.`

### Requirement: An ingest is one span tree with its outcome
An ingest SHALL produce a `slayer.ingest` span with the children `slayer.ingest.scan.schema`, `slayer.ingest.merge`, `slayer.ingest.drift` and `slayer.ingest.embeddings`. The ingest span SHALL carry its outcome: `slayer.ingest.models.added`, `.updated`, `.skipped` and `slayer.ingest.drift.entries`. `.updated` SHALL count only the existing models with a reported change, not the unchanged ones.

#### Scenario: Ingest of one changed model
- **WHEN** an idempotent ingest updates one model
- **THEN** the `slayer.ingest` span has `slayer.ingest.models.updated=1`, and the four step spans are its children

### Requirement: Spans nest under the caller's span
If a span is current at the call into SLayer, SLayer's top-level span SHALL be its child. SLayer's other spans keep the tree that the other requirements define.

#### Scenario: Nesting under a host span
- **WHEN** a host opens the span `host.handler` and runs a query inside it
- **THEN** `slayer.query` is a child of `host.handler`

### Requirement: Standard names keep their names, other names get the slayer prefix
An attribute that an OTel semantic convention defines SHALL keep its standard name: `error.type`, `db.*`, `server.*`, `mcp.*` and `gen_ai.*`. The standard metric `db.client.operation.duration` and the standard `exception` event SHALL also keep their names. Every other span, attribute, event and metric name SHALL start with `slayer.`.

#### Scenario: Exported names
- **WHEN** a stage sets `query.rows`, `error.type` and `db.system.name`
- **THEN** they are exported as `slayer.query.rows`, `error.type` and `db.system.name`

### Requirement: Database spans follow the OTel database conventions
Database spans SHALL have kind `CLIENT`. If these values are known, they SHALL carry `db.system.name`, `db.namespace`, `server.address`, `server.port` and `db.response.returned_rows`. `db.system.name` SHALL be a value from the OTel convention. A datasource type without a standard value SHALL be `other_sql`. Credentials SHALL never be an attribute.

#### Scenario: Database system names
- **WHEN** a database span runs for the datasource types `postgres`, `mssql` and `duckdb`
- **THEN** `db.system.name` is `postgresql`, `microsoft.sql_server` and `other_sql`

#### Scenario: SQLite query
- **WHEN** a query returns 2 rows from a SQLite datasource
- **THEN** the `slayer.db.execute` span has kind `CLIENT`, `db.system.name=sqlite` and `db.response.returned_rows=2`

### Requirement: Errors are reported by class
A stage that raises SHALL set its span status to `ERROR` and SHALL set `error.type` to the exception class name. The exception SHALL propagate unchanged. Without the content capture, no exception message, exception event or stack trace SHALL be recorded.

#### Scenario: Unknown model
- **WHEN** a query names a model that does not exist
- **THEN** the `slayer.query` span has status `ERROR` and `error.type=ModelNotFoundError`, and no `exception` event

### Requirement: Duration histograms in seconds
SLayer SHALL record durations in seconds. `db.client.operation.duration` SHALL use the buckets of the OTel database convention, from 1 ms to 10 s. `slayer.request.duration`, `slayer.query.duration` and `slayer.query.prepare.duration` SHALL use buckets from 1 ms to 2 minutes. `slayer.ingest.duration` SHALL use buckets from 0.1 s to 30 minutes.

#### Scenario: Query duration
- **WHEN** a query succeeds on the datasource `test` of type `sqlite`
- **THEN** `slayer.query.duration` records one value in seconds with the labels `slayer.datasource.name=test` and `slayer.datasource.type=sqlite`

#### Scenario: Failed query
- **WHEN** a query fails with `ModelNotFoundError`
- **THEN** `slayer.query.duration` records one value labeled `error.type=ModelNotFoundError`

### Requirement: Counters for retries, cache and ingest
SLayer SHALL count retried transient database errors in `slayer.db.retries`. It SHALL count result-cache lookups in `slayer.cache.lookups`, labeled with `slayer.cache.hit`. It SHALL count ingested models in `slayer.ingest.models`, labeled with `slayer.ingest.change` (`added`, `updated` or `skipped`). It SHALL count drift entries in `slayer.ingest.drift.entries`.

#### Scenario: Cache lookups
- **WHEN** the same cached query runs twice
- **THEN** `slayer.cache.lookups` counts one miss and one hit

#### Scenario: Retry
- **WHEN** the database client retries a transient error
- **THEN** the database span gets a `slayer.db.retry` event with `slayer.db.attempt` and `error.type`, and `slayer.db.retries` counts 1

### Requirement: Metric labels carry no captured text
Metric labels SHALL come only from the datasource name and type, the surface and operation, the database system, namespace and server, `error.type`, `slayer.cache.hit` and `slayer.ingest.change`. A label SHALL never carry SQL or exception text, also with the content capture.

#### Scenario: Content stays off metrics
- **WHEN** a query runs with `SLAYER_OTEL_CAPTURE_CONTENT=on`
- **THEN** no metric point has the label `db.query.text`

### Requirement: Content needs the content capture
Content is the executed SQL and the text of exceptions: messages and stack traces. Without `SLAYER_OTEL_CAPTURE_CONTENT=on`, no span, event, metric or log record SHALL carry SQL text or a value derived from query input. With it, database spans SHALL carry the executed SQL as `db.query.text`, including `slayer.db.explain` spans. A failed span SHALL record its exception as an `exception` event with the complete message and stack trace.

#### Scenario: Default keeps content in the process
- **WHEN** a query with the filter `region = '<sentinel>'` runs without the content capture
- **THEN** no exported span, event or metric contains the sentinel, and no span has `db.query.text`

#### Scenario: Content capture sends the SQL
- **WHEN** the same query runs with `SLAYER_OTEL_CAPTURE_CONTENT=on`
- **THEN** the `slayer.db.execute` span has `db.query.text`

#### Scenario: Content capture sends the exception
- **WHEN** a query fails with `SLAYER_OTEL_CAPTURE_CONTENT=on`
- **THEN** the `slayer.query` span has one `exception` event, its `exception.message` contains the message, and its `exception.stacktrace` is not empty

#### Scenario: A failed statement keeps its text
- **WHEN** a statement fails with `SLAYER_OTEL_CAPTURE_CONTENT=on`
- **THEN** the `exception.message` contains the complete statement and its parameters

### Requirement: Only the slayer logger is exported
When SLayer configures OTel itself, it SHALL attach one log handler to the `slayer` logger only. Records from other loggers SHALL NOT be exported. An export error SHALL never reach the code that logged.

#### Scenario: Host loggers stay untouched
- **WHEN** the loggers `uvicorn.error` and root log a warning
- **THEN** no record is exported

### Requirement: Log records leave as templates
A record SHALL leave as its message template, with its logger name, level, code location and the current trace and span ids. Its arguments, `extra` fields, exception message and stack trace SHALL stay in the process. An exception SHALL leave as its class in `error.type`.

#### Scenario: Template only
- **WHEN** `slayer.pg_facade` logs `"pg facade: cannot translate %r: %s"` with a sentinel argument
- **THEN** the exported body is the template, and no exported field contains the sentinel

### Requirement: Full log records need the content capture
With `SLAYER_OTEL_CAPTURE_CONTENT=on`, the full record SHALL leave: the formatted message, the `extra` fields, and the exception message and stack trace.

#### Scenario: Retry warning by default
- **WHEN** the database client logs its retry warning, with the SQL as an argument, without the content capture
- **THEN** the exported body is the template, and no exported field contains the SQL

#### Scenario: Full record with the content capture
- **WHEN** a record with an exception is logged with `SLAYER_OTEL_CAPTURE_CONTENT=on`
- **THEN** the exported body contains the argument values, and the attributes contain `exception.stacktrace`

### Requirement: Log export keeps stderr output and the level
If the `slayer` logger has no handler on its path, SLayer SHALL also attach `logging.lastResort`, so stderr output stays. If `OTEL_PYTHON_LOG_LEVEL` names a known level, it SHALL set the level of the `slayer` logger. Detaching SHALL restore the previous level and remove every added handler.

#### Scenario: Level from OTEL_PYTHON_LOG_LEVEL
- **WHEN** `OTEL_PYTHON_LOG_LEVEL=debug` is set, and the handler is attached and then detached
- **THEN** the `slayer` logger has level DEBUG while attached, and its previous level after detach
