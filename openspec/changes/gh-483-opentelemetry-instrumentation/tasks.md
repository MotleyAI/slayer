## 1. Tests

- [x] 1.1 `tests/test_stages.py`, `tests/test_stages_decorator.py`: the inactive fast path, the stage lifecycle, nesting, attributes and events. Non-scalar values are dropped. Errors and a broken listener are handled. The listener registry works, and asyncio tasks keep their own context. The decorator keeps behavior, `__wrapped__` and the signature, and refuses unknown parameters and generator functions at import time.
- [x] 1.2 `tests/test_otel_controls.py`: the precedence table, `SLAYER_OTEL_CAPTURE` parsing, endpoints, headers and exporters. `slayer otel status` names the deciding rule, hides header values and lists unsupported values. `slayer otel test` against a capture server: 200, 401, 404, 413, a refused endpoint and a skipped signal.
- [x] 1.3 `tests/test_otel_guards.py`: no SLayer module imports OTel while it is off. OTel is off by default, and `OTEL_SDK_DISABLED` wins. A missing extra and a failing configure stay silent. Shutdown is bounded and runs in a daemon thread.
- [x] 1.4 `tests/test_otel_collection.py`: the span tree of a query and of an ingest, with their attributes. The dry run and the error status. The duration histograms and their buckets, and the cache and ingest counters. The `slayer.` prefix rule, and the `db.system.name` table against the semconv values. Span kinds, the retry event and counter, and nesting under a host span.
- [x] 1.5 `tests/test_otel_privacy.py`: no SQL or exception text leaves by default. `capture=sql` adds `db.query.text` to spans but not to metrics. `capture=exceptions` adds the exception event only.
- [x] 1.6 `tests/test_otel_logs.py`: records leave as templates, an exception leaves as `error.type`, and `full` sends the whole record. The host's loggers are not exported. `lastResort` output survives. `OTEL_PYTHON_LOG_LEVEL` sets and restores the level. A failing export never reaches the caller.
- [x] 1.7 `tests/test_otel_scope.py`: one request span per REST, MCP and CLI call, named by the route template. Unknown paths and tools are `other`. The MCP mount opens no REST request.
- [x] 1.8 `tests/test_otel_isolation.py`, in subprocesses: command output is the same with OTel on. Spans reach `/v1/traces` as OTLP protobuf. A host tracer provider is kept. A refused, stalled or rejecting backend adds less than 5 seconds and no traceback.
- [x] 1.9 `tests/perf/test_otel_bench.py`: a plain call, a stage with OTel off, and a stage with OTel on.
- [x] 1.10 `tests/conftest.py`: an autouse fixture sets `SLAYER_OTEL=off` for the suite, and the `otel_env` fixture gives in-memory providers.

## 2. Implementation

- [x] 2.1 `slayer/core/stages.py`: `Stage`, `stage()`, `@staged`, `current()`, `add_listener` and `remove_listener`, with the no-listener fast path.
- [x] 2.2 `slayer/otel/settings.py`: `DecidingRule`, `OtelState`, `resolve_state()`, and the readers for capture, exporters, endpoints, headers and protocol, without importing OTel.
- [x] 2.3 `slayer/otel/recorder.py`: `start()`, `instrument()`, `uninstrument()`, `shutdown()` with the 1-second bound, `reset()` and the `request()` decorator, all fail-silent.
- [x] 2.4 `slayer/otel/adapter.py`: `OtelStageListener`, the exported-name rule, span kinds, the `DB_SYSTEMS` table, the bucket boundaries, the metrics and the capture filter.
- [x] 2.5 `slayer/otel/exporters.py`: OTLP HTTP providers from the standard variables, unless a tracer provider exists.
- [x] 2.6 `slayer/otel/logs.py`: `TemplateLogHandler` on the OTel Logs API, attached to the `slayer` logger only.
- [x] 2.7 `slayer/otel/middleware.py` and `slayer/otel/control.py`: the REST request middleware, and `slayer otel status` and `slayer otel test`.
- [x] 2.8 Hook points, with added lines only. Surfaces: `cli.py`, `api/server.py`, `mcp/server.py`, `flight/handlers.py`, `pg_facade/connection.py`. Engine: `engine/query_engine.py`, `engine/cache.py`. Database: `database/client.py`. Ingest: `live_schema/ingestion.py`, `live_schema/schema_drift.py`. Usage vocabulary: `usage_reporting/payload.py`.
- [x] 2.9 `pyproject.toml` and `poetry.lock`: the `otel` extra, version 1.39 or later, also in `all`.
- [x] 2.10 `architecture/model.c4`: the `otel` node and its three edges, after maintainer sign-off. Regenerate the diagrams with `la-arch-diagrams`.

## 3. Docs and gates

- [x] 3.1 `docs/reference/opentelemetry.md` and its `zensical.toml` nav entry, the README section, and `examples/otel`.
- [x] 3.2 Run the unit and integration suites with the CI commands. Run `ruff check` and `basedpyright`, with no new errors against the baseline. Run `la-arch-check` and `npx -y likec4@1.47.0 validate architecture`. Run `openspec validate gh-483-opentelemetry-instrumentation --strict`.
- [ ] 3.3 Open the PR against `main` after the user's go-ahead.
