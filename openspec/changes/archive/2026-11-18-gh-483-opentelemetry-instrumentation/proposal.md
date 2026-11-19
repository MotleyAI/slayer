## Why

SLayer often runs as a shared service between agents or BI tools and one or more warehouses. The operators of that service cannot see where a query spends its time or why a query fails. They also cannot see how often the database client retries, or what an ingest run did. Log lines carry no request id, so operators cannot separate two concurrent requests. They also cannot follow one user request from the agent through SLayer to the warehouse. Issue [#483](https://github.com/MotleyAI/slayer/issues/483) asks for this visibility.

OpenTelemetry (OTel) is the vendor-neutral standard for traces, metrics and logs. This change adds opt-in OTel support. When an operator turns it on, SLayer reports each request, query phase, database call and ingest phase to the operator's own OTel backend. When it is off, SLayer behaves and performs as it does today.

## What Changes

- A new dependency-free module, `slayer/core/probes.py`, marks the orchestration points of SLayer as named probes, through the `@probe` decorator and the `scope()` context manager. A probe with no listener takes a fast path that adds about 0.1 µs. `core.timing` and `SLAYER_PROFILE_TIMING` do not change.
- A new package, `slayer.otel`, turns probes into OTel spans and metrics. It is installed by the new `otel` extra (`opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http`, version 1.45 or later), which `all` includes.
- Processes started by the `slayer` command turn OTel on with `SLAYER_OTEL=on` and configure it from the standard `OTEL_*` variables. `OTEL_SDK_DISABLED=true` keeps it off. A tracer provider that already exists, for example from `opentelemetry-instrument`, is kept.
- Library callers turn it on with `otel.instrument()`, which uses the caller's providers and never configures any.
- Every surface opens one `slayer.request` span: the CLI (`query`, `ingest`), REST, MCP, Flight SQL and the Postgres facade. Queries, database calls, the result cache and ingest runs open child spans.
- Spans and metrics use the OTel standard names where they exist (`error.type`, `db.system.name`, `db.client.operation.duration`, `mcp.method.name`, `gen_ai.tool.name`). Other names carry the `slayer.` prefix.
- SLayer's own log records are exported with trace correlation. Without content capture, they leave as their message templates, and their argument values stay in the process.
- SQL text and exception text leave the process only with content capture: `SLAYER_OTEL_CAPTURE_CONTENT=on` for the `slayer` command, or `otel.instrument(capture_content=True)` for a library caller.
- New commands: `slayer otel status` prints the configuration that a start uses, without a network call. `slayer otel test` sends one empty OTLP request per signal and one test span. If an endpoint fails, it exits 1.
- At exit, SLayer waits at most 1 second for the final export of the providers it created; a host's own providers follow the host's shutdown. A down, slow or rejecting backend never fails a command.
- Usage reporting gains the usage keys `cli:otel.status` and `cli:otel.test` and the extra token `otel`.

## Capabilities

### New Capabilities

- `otel/controls`: how OTel is switched on and off, the opt-in for sensitive data, and the `slayer otel status` and `slayer otel test` commands.
- `otel/collection`: which spans, attributes, metrics and log records SLayer produces, and what stays in the process.
- `otel/delivery`: how the data leaves the process, how SLayer shares OTel with a host, and the bounds on its cost to the host.

### Modified Capabilities

None. `usage-reporting/collection` already requires one fixed usage key per leaf command. It also requires a report of the installed extras. The new keys and the new token follow these requirements.

## Impact

- New code: `slayer/core/probes.py`, `slayer/otel/` (`settings`, `recorder`, `adapter`, `exporters`, `logs`, `middleware`, `control`).
- Hook points, with added lines only:
  - `slayer/cli.py`: start, shutdown, the `otel` commands, and the `query` and `ingest` requests.
  - `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/flight/handlers.py`, `slayer/pg_facade/connection.py`: one request per call.
  - `slayer/engine/query_engine.py`, `slayer/engine/cache.py`: query phases, policy and cache.
  - `slayer/database/client.py`: execute, explain, metadata, and the retry event.
  - `slayer/live_schema/ingestion.py`, `slayer/live_schema/schema_drift.py`: ingest phases.
  - `slayer/usage_reporting/payload.py`: the usage vocabulary.
- Packaging: the `otel` extra in `pyproject.toml`, and `poetry.lock`. The base install gains no dependency.
- Architecture: a new node `otel` in `architecture/model.c4` that claims `slayer.otel`, a nested element `core.probes`, and the edges `protocols -> otel`, `surfaces -> otel` and `otel -> core.probes`. The `otel` spec group is cross-cutting in `architecture/index.yaml`. A new `architecture/python.otel.arc42.md` states the node's principles, and the arc42 diagram blocks are regenerated.
- Docs: `docs/reference/opentelemetry.md` (in the `zensical.toml` nav), a README section, and `examples/otel` (SLayer, Postgres and OpenObserve under Docker Compose).
- Tests: `tests/test_probes*.py`, `tests/test_otel_*.py`, `tests/_otel_helpers.py`, `tests/perf/test_otel_bench.py`. An autouse fixture keeps OTel off for the rest of the suite.
- Results, generated SQL, errors and warnings do not change, with OTel on or off.
