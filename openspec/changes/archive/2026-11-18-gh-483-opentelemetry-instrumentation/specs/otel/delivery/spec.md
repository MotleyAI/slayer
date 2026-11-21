## ADDED Requirements

### Requirement: OTLP over HTTP with protobuf
When SLayer configures OTel itself, it SHALL export each signal over OTLP HTTP with protobuf. A signal's own `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` SHALL be used as given. Otherwise the endpoint SHALL be `OTEL_EXPORTER_OTLP_ENDPOINT` plus `/v1/<signal>`. The default base SHALL be `http://localhost:4318`.

#### Scenario: Spans reach the backend
- **WHEN** `slayer query` runs with `SLAYER_OTEL=on` and `OTEL_EXPORTER_OTLP_ENDPOINT` set to a capture server
- **THEN** the server receives a POST on `/v1/traces` with content type `application/x-protobuf` that contains `slayer.query`

#### Scenario: Per-signal endpoint
- **WHEN** `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318/custom` is set
- **THEN** traces go to `http://collector:4318/custom` without an added path

### Requirement: The standard variables configure the export
The standard `OTEL_*` variables SHALL configure the exporters, the resource, the sampler and the batching. A signal whose exporter is `none` SHALL NOT be exported. SLayer SHALL ignore the `console` exporter and the `grpc` protocol. The default service name SHALL be `slayer`, and `service.version` SHALL be the SLayer version.

#### Scenario: Signal turned off
- **WHEN** `OTEL_LOGS_EXPORTER=none` is set
- **THEN** SLayer exports no logs

#### Scenario: Default service name
- **WHEN** neither `OTEL_SERVICE_NAME` nor a `service.name` resource attribute is set
- **THEN** the service name is `slayer`

### Requirement: SLayer's providers stay private
When SLayer configures OTel itself, its tracer, meter and logger providers SHALL stay private: SLayer SHALL NOT set a global provider. A library that traces through the global provider, such as a FastAPI version with native tracing, then exports nothing through SLayer. If the setup fails part way, SLayer SHALL shut down the providers it created. A later `otel.start()` in the same process SHALL create new providers.

#### Scenario: Private providers
- **WHEN** `otel.start()` runs with `SLAYER_OTEL=on`, then `otel.shutdown()` and `otel.start()` run again
- **THEN** the global tracer provider is still the default proxy, and the second start has new providers

#### Scenario: Late setup failure
- **WHEN** the log exporter raises after the trace and metric providers exist
- **THEN** OTel stays off, and SLayer shuts down the providers it created

### Requirement: A host's providers are kept
If a tracer provider is already set, for example by `opentelemetry-instrument`, `otel.start()` SHALL NOT replace it. SLayer SHALL send its spans and metrics through the existing global providers, and SHALL attach no log handler of its own.

#### Scenario: Host tracer provider
- **WHEN** a process sets its own tracer provider, then calls `otel.start()` with `SLAYER_OTEL=on`
- **THEN** OTel is active, and the global tracer provider is still the host's provider

### Requirement: OTel never changes command results
With OTel on, every command SHALL produce the same stdout and exit status as with OTel off. The `slayer otel` diagnostics are outside this rule, because their output reports on OTel. Generated SQL, errors and warnings SHALL also stay the same. A failure inside SLayer's OTel code SHALL be logged at DEBUG and SHALL NOT reach the caller.

#### Scenario: Same output
- **WHEN** the same `slayer query` runs with OTel off and with OTel on
- **THEN** stdout and the exit status are the same

### Requirement: A broken backend never delays exit beyond a bound
No export SHALL run on the request path. At exit, SLayer SHALL wait at most 1 second for the providers it created. A refused, stalled or rejecting backend SHALL NOT fail an ordinary command, delay it beyond that bound, or print a traceback. The bound applies only to the providers that SLayer created. A host that set its own providers controls their export and shutdown.

#### Scenario: Refused backend
- **WHEN** `slayer query` runs with `SLAYER_OTEL=on` and an endpoint that refuses connections
- **THEN** the command exits 0 with its normal output, and takes less than 5 seconds longer than with OTel off

#### Scenario: Stalled backend
- **WHEN** the backend waits 30 seconds before it answers
- **THEN** the command exits 0 and takes less than 5 seconds longer than with OTel off

#### Scenario: Rejecting backend
- **WHEN** the backend answers HTTP 401
- **THEN** the command exits 0, and stderr contains no `Traceback`

### Requirement: Inactive probes cost no measurable time
A probe with no listener SHALL take no timestamp, bind no arguments and allocate no objects. The decorator SHALL keep the function's name, signature, return value and exceptions, and the file and line that its warnings name. At decoration time, it SHALL refuse a generator function and an unknown parameter name.

#### Scenario: Behavior unchanged
- **WHEN** a decorated function returns a value or raises, with no listener
- **THEN** the caller gets the same value or the same exception as without the decorator

#### Scenario: Warning location unchanged
- **WHEN** a query run through `engine.execute` emits a deprecation warning, with or without a listener
- **THEN** the warning names the caller's file and line

#### Scenario: Unknown parameter
- **WHEN** `@probe` names a parameter that the function does not have
- **THEN** the decoration raises at import time

### Requirement: Each provider flushes on its own thread
At exit, SLayer SHALL flush each provider it created on its own daemon thread, so that a slow backend for one signal does not hold back the others. The 1-second wait SHALL be shared by all the threads together.

#### Scenario: One-second wait
- **WHEN** SLayer shuts down two providers that each take 3 seconds
- **THEN** the joins together wait at most 1 second. The subprocess scenarios above stay as a guard against a hang.

#### Scenario: Slow trace backend
- **WHEN** the trace provider takes 3 seconds to shut down
- **THEN** the other providers still shut down within the 1-second wait
