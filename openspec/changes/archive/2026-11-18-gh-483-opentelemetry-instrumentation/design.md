## Context

See proposal.md for the problem. SLayer is async-first. Sync callers reach the engine through `run_sync`, which copies context variables onto the bridge loop.

The query path starts at `SlayerQueryEngine.execute`. It normalizes the input, prepares the pipeline, and then takes one of four branches: dry run, cache hit, explain, or a data query. Every database access goes through `SlayerSQLClient` (database principle 1). Every full ingest goes through `ingest_datasource_idempotent`.

`slayer.usage_reporting` already hooks into the five surfaces: the CLI, MCP, REST, Flight SQL and the Postgres facade. It activates only in processes started by the `slayer` command, it fails silently, and it reports errors by class. This change follows the same pattern, but for a different audience. Usage reporting sends anonymous counts to MotleyAI. OTel sends detailed timings to the operator's own backend.

`core.timing` measures pipeline phases for `SLAYER_PROFILE_TIMING`. It has no listener interface, and its output is part of a user contract.

## Goals / Non-Goals

**Goals:**

- Results, generated SQL, errors and warnings stay the same, with OTel on or off.
- Existing files gain added lines only: an import, a decorator, or a statement next to an existing hook.
- The base install gains no dependency.
- `engine`, `database`, `live_schema`, `sql` and `core` gain no import of `slayer.otel`.
- OTel off has no measurable cost.
- OTel on never fails or delays a command beyond a fixed bound.

**Non-Goals:**

- Changes to `core.timing` or to the `SLAYER_PROFILE_TIMING` output.
- A `/metrics` endpoint, a Prometheus exporter, or dashboards.
- OTLP over gRPC, and the `console` exporter.
- Configuring OTel providers in library use.
- Inbound trace context for Flight SQL and the Postgres facade.
- Rewriting existing log lines so that their values are safe to export.
- Context propagation into worker threads that SLayer starts.

## Decisions

1. **A new `core.probes` module, not an extension of `core.timing`.** `core.probes` holds the `@probe` decorator, the `scope()` context manager, `current()` and a listener protocol. It is not named for stages, because a stage is a step of a multi-stage query. It imports nothing outside the standard library and Pydantic, so `core` stays a leaf node. A change to `core.timing` instead changes existing code and the profile output for no gain. A later change can merge the two.

2. **Decorators place the probes.** A decorator adds one line and re-indents nothing. A reviewer reads each file as the same code plus marked probes. Upstream changes rarely conflict with an added decorator line. The decorator refuses generator functions and unknown parameter names at import time. The one statement inside a function body is the retry event in the database client. A retry is a point in time, not a call.

3. **The fast path costs about 0.1 µs per probe.** With no listener, a probe takes no timestamp, binds no arguments and allocates nothing. A query opens about 10 probes, so OTel off adds about 1 µs per query. The fastest query in `tests/perf` takes 1.9 ms, and its run-to-run noise is 9%. With OTel on, a probe costs about 9 µs through the SDK. The measurements are in `tests/perf/test_otel_bench.py`.

4. **Short names in code, the `slayer.` prefix on export.** Code names a probe `ingest.scan.schema` and an attribute `datasource.name`. The OTel adapter exports them as `slayer.ingest.scan.schema` and `slayer.datasource.name`. Keys that OTel standardizes keep their standard name: `error.type`, `db.*`, `server.*`, `mcp.*` and `gen_ai.*`. This follows the OTel naming rules for library-specific names.

5. **Standard conventions where they exist.** Database spans have kind `CLIENT` and carry `db.system.name`, `db.namespace`, `server.address`, `server.port` and `db.response.returned_rows`. Database time goes to the standard histogram `db.client.operation.duration`, with its recommended buckets. A datasource type without a standard `db.system.name` value is exported as `other_sql`. MCP requests carry `mcp.method.name`, `gen_ai.operation.name` and `gen_ai.tool.name`, although those conventions still have Development status.

6. **Activation by construction, as in usage reporting.** Only the `slayer` command calls `otel.start()`. The first matching rule decides: `OTEL_SDK_DISABLED=true` keeps OTel off, then `SLAYER_OTEL` decides, and the default is off. Library use never turns OTel on by itself. A library caller calls `otel.instrument()` with its own providers. There is no `slayer otel enable` command, because a saved setting next to the environment variables creates a second source of truth.

7. **The standard `OTEL_*` variables configure everything else.** SLayer reads them through the OTel SDK, so operators reuse the configuration of their other services. If no tracer provider exists, SLayer creates its own providers. Under `opentelemetry-instrument`, SLayer uses the existing providers and exports no logs itself.

8. **Sensitive data needs an explicit opt-in.** By default, these stay in the process: SQL text, query and filter values, and row values. Exception messages, stack traces and connection strings also stay. `SLAYER_OTEL_CAPTURE_CONTENT=on` adds all of it at once: `db.query.text` on database spans, exception events, and full log records. One switch replaces a split between SQL and exception text, because the two are equally sensitive: driver and parser messages repeat SQL fragments and values, and no text matching removes them all. Executed SQL contains the literal values of filters and of row-level-security policies, so the switch is for a backend that may store them. Metrics never carry content.

9. **Logs: SLayer's loggers only, templates by default.** The handler attaches to the `slayer` logger only, so the host's logs and other libraries' logs are never exported. A record leaves as its message template. SLayer logs every value as a lazy `%` argument, so the template carries no SQL, literals or credentials. The handler writes to the OTel Logs API directly. The SDK's `LoggingHandler` is deprecated, and its replacement, `opentelemetry-instrumentation-logging`, pins exact beta versions of other OTel packages. Writing to the API directly needs OTel 1.39 or later. The floor of the extra is 1.45, because from 1.45 the OTLP exporter treats an empty per-signal variable as unset. In older versions, an empty `OTEL_EXPORTER_OTLP_<SIGNAL>_CERTIFICATE` turns off certificate checks, and an empty endpoint or header variable differs from what `slayer otel test` checks.

10. **Bounded cost to the host.** Export runs on the SDK's background threads, so no network call happens during a request. At exit, SLayer flushes the providers it created in a daemon thread and waits at most 1 second, as usage reporting does. Every SLayer entry point into OTel swallows its own errors and logs them at DEBUG.

11. **OTLP over HTTP with protobuf only.** It needs no gRPC dependency and works through HTTP proxies. `slayer otel status` lists every unsupported value of the standard variables, for example `grpc` or `console`. The `console` exporter writes to stdout and breaks command output such as `slayer query --format json`.

12. **A new architecture node `otel`.** It claims `slayer.otel`. `surfaces` and `protocols` import it to open request probes. It imports only `core.probes`, a nested element of `core`, and the arrow `otel -> core.probes` enforces that. No other node imports it, so the engine, the database client and ingestion know only `core.probes`. The `otel` spec is cross-cutting: it touches `otel`, `core.probes`, `engine`, `database`, `live_schema`, `surfaces` and `protocols`, where the probes sit. `python.otel.arc42.md` states the node's principles.
13. **Warnings keep their location.** A decorator adds a stack frame, so a warning that names its caller's line walks to the first frame outside SLayer instead of counting frames.

## Risks / Trade-offs

- [The OTel Logs API is still experimental] → Log export sits behind `OTEL_LOGS_EXPORTER`, and traces and metrics do not depend on it.
- [The MCP and GenAI conventions can be renamed] → The names sit in one table in `slayer/otel/adapter.py`.
- [One extra frame per decorated function in tracebacks] → Error types and messages do not change.
- [A slow backend can lose the final batch at exit] → The docs state the 1-second bound. Metrics are cumulative, so a later successful export from the same process recovers the counts of a failed one; a process that exits after a failed final export loses them.
- [Worker threads that SLayer starts do not inherit the trace context] → Their probes start new traces. A later change can copy the context.

## Migration Plan

No migration. OTel is off by default, and no stored format changes. To roll back, unset `SLAYER_OTEL`.

## Open Questions

- Can a later change replace SQL text in WARNING log lines with a statement fingerprint, so that full log records are safe to export?
- Can usage reporting and `slayer.otel` share one request scope in a later change?
