# OpenTelemetry + Docker Compose Example

Run SLayer with OpenTelemetry on, and send its traces and metrics to an OTLP backend.

SLayer only speaks OTLP. The backend is an endpoint, set with the standard `OTEL_*` variables in `example.env`. This example uses [OpenObserve](https://openobserve.ai) as the default backend, because it runs as one container. Any other OTLP backend works the same way.

## Quick Start

```bash
cd examples/otel
docker compose up -d --build
```

This starts the services below. Their ports listen on `127.0.0.1` only, so other hosts cannot reach the test credentials.

| Service | Host port | Purpose |
|---|---|---|
| OpenObserve | 5080 | OTLP backend and UI |
| SLayer API | 5143 | SLayer with `SLAYER_OTEL=on` |
| Postgres | 5434 | sample e-commerce data |

At start, the SLayer container runs `slayer ingest` once, which sends an ingest trace. Then it runs `slayer serve`.

## Send Traffic

```bash
docker compose run --rm traffic
```

The traffic service sends 20 rounds of five REST queries: three normal queries, one dry run and one query on a model that does not exist. It exits 1 if a normal query or the dry run fails. You can also run it from the host:

```bash
python traffic.py http://localhost:5143 50
```

## See The Traces

Open http://localhost:5080 and log in as `root@example.com` with the password `Complexpass#123`. These test credentials are in `example.env` and exist only in this local container.

In Traces, select the service `slayer`.

| Span | Shows |
|---|---|
| `slayer.request` | one REST, MCP, Flight, Postgres-facade or CLI request, with `slayer.surface` and `slayer.operation`. MCP requests also carry the standard `mcp.method.name` and `gen_ai.tool.name`. |
| `slayer.query` | one query, with its phases `normalize`, `prepare`, `policy` and `run` |
| `slayer.db.execute`, `slayer.db.metadata` | each database call, with the standard `db.system.name`, `db.namespace`, `server.address`, `server.port` and `db.response.returned_rows` |
| `slayer.ingest` | the startup ingest, with schema, merge, drift and embeddings phases |

A failed query has status `ERROR` and an `error.type` attribute. SLayer follows the OTel semantic conventions where they define a name, so backends with database views recognize the Postgres datasource as `postgresql`.

In Metrics, look for the standard `db.client.operation.duration`, and for `slayer.request.duration`, `slayer.query.duration` and `slayer.ingest.duration`. The counters are `slayer.db.retries`, `slayer.cache.lookups`, `slayer.ingest.datasets` and `slayer.ingest.drift.entries`. OpenObserve replaces each `.` with `_`, for example `slayer_query_duration`. SLayer exports metrics every 10 seconds in this example.

A REST trace starts at `slayer.request`. SLayer keeps its tracer provider private, so FastAPI's own spans, such as `POST /query`, do not appear. They appear only with FastAPI instrumentation, for example through `opentelemetry-instrument`.

## See The Logs

In Logs, the records come from SLayer's own loggers, such as `slayer.sql.client`. Each record is the message template only, for example `Transient DB error on attempt %d, retrying in %.1fs: %s | sql: %s`. The values in a message, such as SQL, stay in the process. An exception appears as its class, in `error.type`. Each record carries the trace id of the request that wrote it, so you can go from a trace to its logs.

Your terminal output does not change. SLayer still prints its full warnings to stderr.

Logs export by default, because `otlp` is the standard default of `OTEL_LOGS_EXPORTER`. Set it to `none` to switch logs off. The level follows Python logging. This example sets the OTel Python variable `OTEL_PYTHON_LOG_LEVEL=info`, which sets the level of SLayer's loggers.

To see a log record, stop Postgres during a query. The SQL client then logs a retry warning:

```bash
curl -s -X POST localhost:5143/query -H "Content-Type: application/json" \
  -d '{"source_model": "orders", "measures": ["count(*)"]}'
docker compose stop postgres
curl -s -X POST localhost:5143/query -H "Content-Type: application/json" \
  -d '{"source_model": "orders", "measures": ["count(*)"]}'
docker compose start postgres
```

## Use Another Backend

Change two variables in `example.env`, then run `docker compose up -d slayer`. If you do not use OpenObserve, you can remove the `openobserve` service from `docker-compose.yml`. Also remove the `openobserve` entry under `depends_on` in the `slayer` service, and keep the `seed` entry.

| Backend | `OTEL_EXPORTER_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_HEADERS` |
|---|---|---|
| OpenObserve | `http://<host>:5080/api/<org>` | `Authorization=Basic%20<base64 of email:password>` |
| SigNoz, self-hosted | `http://<host>:4318` | none |
| Grafana `otel-lgtm` | `http://<host>:4318` | none |
| OpenTelemetry Collector | `http://<host>:4318` | none |
| Honeycomb | `https://api.honeycomb.io` | `x-honeycomb-team=<api key>` |

SLayer sends OTLP over HTTP/protobuf. The SDK adds `/v1/traces`, `/v1/metrics` and `/v1/logs` to the endpoint. The exporter values `console` and the `grpc` protocol are not supported.

## Capture Sensitive Data

By default, no SQL text, exception message or stack trace leaves SLayer. To debug with full detail on your own backend, set `SLAYER_OTEL_CAPTURE_CONTENT=on` in `example.env`, then run `docker compose up -d slayer`.

Content capture adds `db.query.text`, the executed SQL, on database spans. It adds exception messages and stack traces on failed spans, and full records in logs. All of it can contain row-level-security values and filter literals.

## Configuration

`example.env` holds the settings for every service in `docker-compose.yml`. It holds the local test credentials for OpenObserve (`ZO_ROOT_USER_EMAIL`, `ZO_ROOT_USER_PASSWORD`) and Postgres (`POSTGRES_PASSWORD`). These are the settings for the SLayer service:

| Variable | Value |
|---|---|
| `SLAYER_OTEL` | `on` |
| `OTEL_SERVICE_NAME` | `slayer` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the backend URL |
| `OTEL_EXPORTER_OTLP_HEADERS` | backend authentication, empty for a backend without it |
| `OTEL_METRIC_EXPORT_INTERVAL` | `10000` (milliseconds) |
| `OTEL_PYTHON_LOG_LEVEL` | `info` in this example. Without it, the level follows Python logging. |
| `SLAYER_OTEL_CAPTURE_CONTENT` | not set, so no content leaves. See "Capture Sensitive Data". |
| `SLAYER_USAGE_REPORTING` | `off`, so test traffic sends no usage reports |

At start, `start.sh` runs `prepare.py`, which waits up to two minutes for the endpoint in `OTEL_EXPORTER_OTLP_ENDPOINT` and registers the demo datasource. If the endpoint stays down, SLayer serves anyway and drops its spans. SLayer keeps its models in the Docker volume `slayer-data`.

## Stop

```bash
docker compose down
```

To also delete the volumes, the OpenObserve data and SLayer's models:

```bash
docker compose down -v
```
