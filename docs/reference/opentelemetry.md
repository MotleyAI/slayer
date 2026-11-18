# OpenTelemetry

SLayer can send traces, metrics and logs to any OpenTelemetry (OTel) backend over OTLP, the OTel protocol. SLayer sends OTLP over HTTP/protobuf, not gRPC, so the backend must accept OTLP/HTTP. It is off by default, and it sends no SQL text, query values or row data unless you opt in.

You configure it with the standard `OTEL_*` environment variables, plus two SLayer variables: `SLAYER_OTEL` to turn it on, and `SLAYER_OTEL_CAPTURE_CONTENT` to opt in to content, such as SQL text.

## Install

The `otel` extra holds the OTel packages. The `all` extra includes it.

```bash
pip install 'motley-slayer[otel]'
```

## Turn it on

### The `slayer` command and Docker

Set `SLAYER_OTEL=on` and the endpoint of your backend. This works for every `slayer` command, for example `slayer serve`, `slayer mcp`, `slayer query` and `slayer ingest`.

```bash
export SLAYER_OTEL=on
export OTEL_SERVICE_NAME=slayer
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
slayer serve
```

```bash
docker run -e SLAYER_OTEL=on -e OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 -p 5143:5143 <slayer image>
```

The first rule that matches decides:

| # | Rule | Effect |
|---|------|--------|
| 1 | `OTEL_SDK_DISABLED=true` | off, even with `SLAYER_OTEL=on` |
| 2 | `SLAYER_OTEL=on` (or `1`, `true`, `yes`) | on |
| 3 | `SLAYER_OTEL=off`, or any other value | off |
| 4 | not set | off |

When it is on, SLayer configures the OTel SDK from the standard variables, unless a tracer provider already exists.

### With `opentelemetry-instrument`

If you already run your services under `opentelemetry-instrument`, keep it. Set `SLAYER_OTEL=on` as well. SLayer then uses the providers that `opentelemetry-instrument` created, and configures nothing itself.

```bash
SLAYER_OTEL=on opentelemetry-instrument slayer serve
```

In this mode, SLayer sends traces and metrics through those providers, and it does not export logs itself. If you turn on log export in `opentelemetry-instrument`, with `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true`, it exports every log record in full, with its values. SLayer's template rule does not apply to those records.

`opentelemetry-instrument` also controls the flush at exit. If the backend is down, a command such as `slayer query` waits for the export timeout before it exits, about 13 seconds with the default. To shorten this, set `OTEL_EXPORTER_OTLP_TIMEOUT`, for example `1`.

### In your own Python application

Library use never turns OTel on by itself, and it never configures OTel providers. Configure your providers as usual, then call `otel.instrument()`:

```python
from slayer import otel

otel.instrument()  # uses your global tracer and meter providers
```

You can also pass providers and opt-ins directly:

```python
otel.instrument(tracer_provider=my_tracer_provider, meter_provider=my_meter_provider, capture_content=True)
```

In library use, SLayer exports traces and metrics. It does not attach a log handler. Your application's logging setup decides what happens to SLayer's log records.

## Check the setup

`slayer otel status` prints what SLayer does at start, without a network call:

```text
$ slayer otel status
OTel is on (decided by: SLAYER_OTEL).
otel extra: installed
Service: slayer 2.0.0
traces:  otlp -> http://localhost:4318/v1/traces
metrics: otlp -> http://localhost:4318/v1/metrics
logs:    otlp -> http://localhost:4318/v1/logs
Headers: Authorization (values hidden)
Protocol: http/protobuf
Sampler: parentbased_always_on (default)
Content capture: off (SLAYER_OTEL_CAPTURE_CONTENT)
Log level: Python logging (OTEL_PYTHON_LOG_LEVEL not set)
```

`slayer otel test` checks the connection to your backend. It sends one empty OTLP request to each signal's endpoint, then one span named `slayer.otel.test`. If an endpoint fails, the command exits with code 1, so you can use it in scripts and health checks. Two cases send fewer requests and still exit 0: with `OTEL_SDK_DISABLED=true` the command sends nothing, and a signal whose exporter is `none` is skipped, so `OTEL_TRACES_EXPORTER=none` also skips the test span. Exit code 0 alone does not prove that the backend works. With an exporter value that SLayer does not support, such as `console`, a signal is not probed either. Without the `otel` extra, the probes can pass, but no test span is sent. For a health check, look for `ok` on each signal you need, and for `Test span: sent` if the span matters.

```text
$ slayer otel test
traces:  ok, HTTP 200  http://localhost:4318/v1/traces
metrics: ok, HTTP 200  http://localhost:4318/v1/metrics
logs:    ok, HTTP 200  http://localhost:4318/v1/logs
Test span: sent; search your backend for the span slayer.otel.test from service slayer.
All endpoints accepted OTLP.
```

A failure prints the cause. For example, `HTTP 401` means that the backend rejected the credentials in `OTEL_EXPORTER_OTLP_HEADERS`. `HTTP 404` usually means a wrong endpoint path.

There is no `slayer otel enable` or `disable` command. The environment variables are the only switch, so that no saved setting can contradict them. That is also how container platforms configure OTel.

## Send to your backend

Set two variables. SLayer adds `/v1/traces`, `/v1/metrics` and `/v1/logs` to `OTEL_EXPORTER_OTLP_ENDPOINT`.

| Backend | `OTEL_EXPORTER_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_HEADERS` |
|---|---|---|
| OpenTelemetry Collector | `http://<host>:4318` | none |
| SigNoz, self-hosted | `http://<host>:4318` | none |
| Grafana `otel-lgtm` | `http://<host>:4318` | none |
| OpenObserve | `http://<host>:5080/api/<org>` | `Authorization=Basic%20<base64 of email:password>` |
| Honeycomb | `https://api.honeycomb.io` | `x-honeycomb-team=<api key>` |

To send one signal somewhere else, set its own endpoint, for example `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. SLayer uses that URL exactly as given.

The repository has a ready example in `examples/otel`: SLayer, a Postgres database and OpenObserve, started with one `docker compose` command.

## What SLayer sends

### Traces

One REST query produces this trace:

```text
POST /query                   FastAPI's own span, only with FastAPI instrumentation
└─ slayer.request             surface=rest, operation=/query
   └─ slayer.query            slayer.datasource.name=demo
      ├─ slayer.query.normalize
      ├─ slayer.query.prepare
      │  └─ slayer.query.policy
      └─ slayer.query.run
         └─ slayer.db.execute db.system.name=postgresql, db.namespace=slayer_demo
```

| Span | What it covers |
|---|---|
| `slayer.request` | one request from the CLI (`query`, `ingest`), REST, MCP, Flight SQL or the Postgres facade |
| `slayer.query` | one query |
| `slayer.query.normalize`, `.prepare`, `.policy`, `.run` | the phases of a query |
| `slayer.query.cache.get`, `.put` | the result cache, which library callers can turn on |
| `slayer.db.execute`, `slayer.db.explain`, `slayer.db.metadata` | each database call, including catalog reads |
| `slayer.ingest` | one ingest run, with `slayer.ingest.scan.schema`, `.merge`, `.drift` and `.embeddings` inside |

Database spans have kind `CLIENT`. Request spans from Flight SQL and the Postgres facade have kind `SERVER`. All other spans are `INTERNAL`. A REST request contains FastAPI's own spans only when FastAPI instrumentation is active, for example through `opentelemetry-instrument`. In that case, SLayer's spans sit inside them. When SLayer configures OTel itself, its providers stay private. A FastAPI version with native tracing then exports nothing through SLayer, so its URL paths and query values stay in the process.

A failed operation has status `ERROR` and an `error.type` attribute with the exception class. A transient database error that SLayer retries adds a `slayer.db.retry` event to the database span.

SLayer uses the OTel standard attribute names where they exist:

| Attribute | Value |
|---|---|
| `error.type` | the exception class |
| `db.system.name` | the standard name of the database, for example `postgresql`, `mysql`, `clickhouse` or `microsoft.sql_server`. Types without a standard name, such as DuckDB, Snowflake and BigQuery, are `other_sql`. |
| `db.namespace` | the database name |
| `server.address`, `server.port` | the database host and port, never credentials |
| `db.response.returned_rows` | the rows that the statement returned |
| `mcp.method.name`, `gen_ai.operation.name`, `gen_ai.tool.name` | on MCP requests: `tools/call`, `execute_tool` and the tool name |
| `slayer.surface`, `slayer.operation` | the surface and the operation, on every request |
| `slayer.datasource.name`, `slayer.datasource.type` | the datasource from your configuration |
| `slayer.query.rows`, `slayer.query.dry_run`, `slayer.query.explain` | about the query |
| `slayer.ingest.models.added`, `.updated`, `.skipped`, `slayer.ingest.drift.entries` | the outcome of an ingest run |

The resource has `service.name` (default `slayer`) and `service.version` (the SLayer version), unless you set them.

### Metrics

| Metric | Type | Labels |
|---|---|---|
| `db.client.operation.duration` | histogram, seconds | `db.system.name`, `db.namespace`, `server.address`, `server.port`, `slayer.datasource.name`, `error.type` |
| `slayer.request.duration` | histogram, seconds | `slayer.surface`, `slayer.operation`, `error.type` |
| `slayer.query.duration` | histogram, seconds | datasource, `error.type` |
| `slayer.query.prepare.duration` | histogram, seconds | datasource, `error.type` |
| `slayer.ingest.duration` | histogram, seconds | datasource, `error.type` |
| `slayer.db.retries` | counter | datasource, `error.type` |
| `slayer.cache.lookups` | counter | datasource, `slayer.cache.hit` |
| `slayer.ingest.models` | counter | datasource, `slayer.ingest.change` (`added`, `updated`, `skipped`) |
| `slayer.ingest.drift.entries` | counter | datasource |

The duration histograms use buckets in seconds. `db.client.operation.duration` runs from 1 ms to 10 s, as the OTel database conventions recommend. The request and query histograms run from 1 ms to 2 minutes, and the ingest histogram from 0.1 s to 30 minutes. Metrics are cumulative, so a later successful export recovers the counts of a failed one. If the last export of a short command fails, its counts are lost, as Performance explains. For a backend that needs delta values, set `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta`.

Some numbers are on both a metric and a span, for example retries and ingest results. The metric is never sampled and is cheap to keep and to alert on. The span shows the single request.

### Logs

When SLayer configures OTel itself, in processes started by the `slayer` command with `SLAYER_OTEL=on`, it also exports its own log records. To turn this off, set `OTEL_LOGS_EXPORTER=none`.

- Only SLayer's loggers are exported. Your application's logs and other libraries' logs are not.
- A record leaves as its message template, for example `Transient DB error on attempt %d, retrying in %.1fs: %s | sql: %s`. The values, such as SQL text, stay in the process. An exception appears as its class in `error.type`.
- Each record carries the trace and span ids of the request that wrote it, so you can go from a trace to its logs.
- The level follows Python logging. Plain commands, such as `slayer query`, export WARNING and above. `slayer serve` and `slayer mcp` export INFO and above, because their MCP server configures logging at INFO. To set the level, use `OTEL_PYTHON_LOG_LEVEL`, for example `info` or `warning`.
- Your terminal output does not change.

## Sensitive data

By default, SLayer exports none of these: SQL text, query variables, filter values, row values, exception messages, stack traces and connection strings. This covers what SLayer exports. If your application adds its own log instrumentation, for example with `OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true`, that instrumentation decides what it exports from SLayer's log records.

To debug with full detail on your own backend, turn on content capture:

```bash
export SLAYER_OTEL_CAPTURE_CONTENT=on
```

It adds all content at once:

- `db.query.text`, the executed SQL, on database spans.
- An `exception` event with `exception.message` and `exception.stacktrace` on failed spans.
- Full log records: the formatted message, the `extra` fields, and the exception message and stack trace.

Metrics never carry content.

!!! warning
    Content contains the literal values of filters and of row-level-security policies, for example tenant ids. Error messages from database drivers and from the SQL parser often repeat the SQL or a value, for example `DETAIL: Key (tenant_id)=(42)`. Turn on content capture only for a backend that is allowed to store this data.

Captured SQL and stack traces can be long. To cut long values, set the standard `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT`, for example `4096`.

## Configuration reference

| Variable | Kind | Effect |
|---|---|---|
| `SLAYER_OTEL` | SLayer | `on` turns OTel on for the `slayer` command |
| `SLAYER_OTEL_CAPTURE_CONTENT` | SLayer | `on` sends content: SQL text, exception text and full log records |
| `OTEL_SDK_DISABLED` | standard | `true` keeps OTel off |
| `OTEL_SERVICE_NAME` | standard | the service name, default `slayer` |
| `OTEL_RESOURCE_ATTRIBUTES` | standard | extra resource attributes, for example `deployment.environment.name=prod` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | standard | the backend base URL, default `http://localhost:4318` |
| `OTEL_EXPORTER_OTLP_HEADERS` | standard | headers such as credentials, as `key=value` pairs |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `..._METRICS_...`, `..._LOGS_...` | standard | a full URL for one signal |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | standard | seconds per export request, default 10 |
| `OTEL_EXPORTER_OTLP_COMPRESSION` | standard | `gzip` or `deflate` |
| `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`, `OTEL_LOGS_EXPORTER` | standard | `otlp` (default) or `none` |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | standard | sampling, for example `parentbased_traceidratio` and `0.1` |
| `OTEL_METRIC_EXPORT_INTERVAL` | standard | milliseconds between metric exports, default 60000 |
| `OTEL_BSP_SCHEDULE_DELAY`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`, `OTEL_BSP_MAX_QUEUE_SIZE` | standard | span batching: every 5000 ms, 512 spans, queue of 2048 |
| `OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT` | standard | maximum length of attribute values |
| `OTEL_PYTHON_LOG_LEVEL` | OTel Python | the level of SLayer's loggers in processes started by the `slayer` command |

SLayer supports two values of the standard variables only partly:

- Exporters: `otlp` and `none`. The `console` exporter writes to stdout, which breaks command output such as `slayer query --format json`. SLayer ignores it.
- Protocol: OTLP over HTTP/protobuf. SLayer does not support `grpc`.

`slayer otel status` lists each ignored value.

## Performance

- Off: each stage costs about 0.1 µs, and a query has about 10 stages. This is far below the normal variation of a query's time.
- On: each stage costs about 9 µs, measured with the OTel SDK. A query on a local database takes a few milliseconds, so this adds a few percent. For a query on a remote warehouse, the cost is too small to see.
- No network call happens during a request. Three background threads, one per signal, send the data.
- At exit, SLayer waits at most 1 second to send what remains, when SLayer configured OTel itself. Each signal flushes on its own thread, so a slow trace backend does not hold back metrics and logs. If the backend does not answer in time, that last batch is lost, and the command still exits on time. Under `opentelemetry-instrument`, the export timeout decides this wait.

## Troubleshooting

If no data arrives:

1. Run `slayer otel status`. Make sure that OTel is on and that the endpoints are what you expect.
2. Run `slayer otel test`. It shows whether the backend accepts the requests, and why not.
3. If OTel is off with `SLAYER_OTEL=on`, look for `OTEL_SDK_DISABLED=true` in the environment.
4. If the extra is missing, install it with `pip install 'motley-slayer[otel]'`.
5. If `OTEL_EXPORTER_OTLP_ENDPOINT` ends with `/v1/traces`, remove that path. SLayer adds it.
6. For a command that exits quickly, such as `slayer query`, SLayer waits at most 1 second at exit. A slow backend can miss that last batch.
7. If a command takes many seconds to exit under `opentelemetry-instrument`, the backend is probably down. Set `OTEL_EXPORTER_OTLP_TIMEOUT` to a few seconds.

If the backend is down, the OTel exporter writes its own error messages to stderr. Your commands still work.
