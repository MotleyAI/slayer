## ADDED Requirements

### Requirement: Off by default with a fixed precedence
OTel SHALL be off by default. In a process started by the `slayer` command, the first matching rule SHALL decide its state:
1. `OTEL_SDK_DISABLED=true` → off.
2. `SLAYER_OTEL` set to `on`, `1`, `true` or `yes` → on. Any other non-empty value → off.
3. Default → off.

When OTel is off, no SLayer module SHALL import an OpenTelemetry package. SLayer SHALL create no provider, attach no log handler and make no OTel request. `slayer otel test` is the one exception.

#### Scenario: OTEL_SDK_DISABLED beats SLAYER_OTEL
- **WHEN** `OTEL_SDK_DISABLED=true` and `SLAYER_OTEL=on` are both set
- **THEN** OTel is off, decided by `OTEL_SDK_DISABLED`

#### Scenario: SLAYER_OTEL turns it on
- **WHEN** only `SLAYER_OTEL=on` is set
- **THEN** OTel is on, decided by `SLAYER_OTEL`

#### Scenario: Unset means off
- **WHEN** neither variable is set
- **THEN** OTel is off, decided by the default

#### Scenario: No SLayer module imports OTel
- **WHEN** a process imports the CLI, the engine, the REST and MCP servers and `slayer.otel` with OTel off
- **THEN** no `slayer` module imports an `opentelemetry` module

### Requirement: Library use never activates OTel by itself
Outside processes started by the `slayer` command, SLayer SHALL NOT turn on OTel by itself. It SHALL configure no provider and attach no log handler, whatever the environment. A library caller SHALL turn it on with `otel.instrument()`. That call SHALL use the given providers or the global ones, and SHALL configure none. `otel.instrument()` SHALL be idempotent, and `otel.uninstrument()` SHALL remove the listener.

#### Scenario: Library caller instruments with its own providers
- **WHEN** a caller passes its own tracer and meter providers to `otel.instrument()` and runs a query
- **THEN** the query's spans and metrics reach those providers

#### Scenario: Instrument twice
- **WHEN** a caller calls `otel.instrument()` twice
- **THEN** each stage produces one span

### Requirement: A missing extra or a failing setup stays silent
If OTel is on but the `otel` extra is not installed, or if the OTel setup raises, SLayer SHALL stay off. It SHALL log the cause at DEBUG only. The command SHALL run with unchanged output and exit status.

#### Scenario: Extra missing
- **WHEN** `SLAYER_OTEL=on` is set and the OTel packages cannot be imported
- **THEN** the command succeeds, prints nothing about OTel, and OTel stays off

#### Scenario: Setup fails
- **WHEN** the exporter setup raises an exception
- **THEN** the command succeeds and OTel stays off

### Requirement: Sensitive data needs an explicit opt-in
`SLAYER_OTEL_CAPTURE_CONTENT` SHALL be one switch for content: the executed SQL, exception messages and stack traces, and full log records. `on`, `1`, `true` and `yes`, in any case, SHALL turn it on. Any other value, or no value, SHALL leave it off. The switch SHALL apply to the `slayer` command. A library caller SHALL pass `otel.instrument(capture_content=True)`.

#### Scenario: Switch values
- **WHEN** `SLAYER_OTEL_CAPTURE_CONTENT=" TRUE "` is set
- **THEN** content capture is on, and with `sql` it is off

### Requirement: slayer otel status reports the start configuration
`slayer otel status` SHALL print, without a network call, the facts that a start of the `slayer` command uses. These facts SHALL be the state and its deciding rule, whether the extra is installed, and the service name and version. They SHALL also include each signal's exporter and endpoint, and the protocol, sampler, content capture and log level. The command SHALL exit 0.

#### Scenario: Deciding rule
- **WHEN** `OTEL_SDK_DISABLED=true` and `SLAYER_OTEL=on` are set and `slayer otel status` runs
- **THEN** the first line reads `OTel is off (decided by: OTEL_SDK_DISABLED).`

### Requirement: slayer otel status hides secrets and names ignored values
`slayer otel status` SHALL print the names of the OTLP headers, never their values. `slayer otel status` and `slayer otel test` SHALL print each endpoint with the password in its user information and every query value replaced by `***`. The probe and the export SHALL use the unmasked URL. `status` SHALL list every value of the standard variables that SLayer does not support and ignores. It SHALL work without the `otel` extra, and then report the extra as not installed.

#### Scenario: Header values stay hidden
- **WHEN** `OTEL_EXPORTER_OTLP_HEADERS=Authorization=Basic%20c2VjcmV0` is set and `slayer otel status` runs
- **THEN** the output contains `Authorization (values hidden)` and does not contain `c2VjcmV0`

#### Scenario: URL secrets stay hidden
- **WHEN** `OTEL_EXPORTER_OTLP_ENDPOINT=http://user:s3cret@collector:4318` is set and `slayer otel status` runs
- **THEN** the output contains `user:***@collector` and does not contain `s3cret`

#### Scenario: Extra not installed
- **WHEN** the OTel packages cannot be imported and `slayer otel status` runs
- **THEN** the command exits 0 and reports `otel extra: not installed`

#### Scenario: Unsupported values are named
- **WHEN** `OTEL_TRACES_EXPORTER=console` and `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` are set
- **THEN** the output names `console` as an ignored exporter value and `grpc` as an unsupported protocol

### Requirement: slayer otel test checks each endpoint
`slayer otel test` SHALL send one empty OTLP request, as `application/x-protobuf`, to the endpoint of each exported signal. It SHALL send the configured headers, and use the configured CA and client certificate files. It SHALL print the result for each signal. For HTTP 401, 403, 404, 405 and 413 it SHALL print a hint about the cause.

If any endpoint fails, it SHALL exit 1. A signal whose exporter is `none` SHALL be reported as skipped.

#### Scenario: All endpoints accept
- **WHEN** the backend answers HTTP 200 on `/v1/traces`, `/v1/metrics` and `/v1/logs`
- **THEN** the command prints `ok, HTTP 200` for each signal, sends the test span and exits 0

#### Scenario: Rejected credentials
- **WHEN** the backend answers HTTP 401
- **THEN** the command prints the status, a hint to check `OTEL_EXPORTER_OTLP_HEADERS`, and exits 1

#### Scenario: Backend down
- **WHEN** the endpoint refuses the connection
- **THEN** the command prints `cannot connect` and exits 1

#### Scenario: Skipped signal
- **WHEN** `OTEL_LOGS_EXPORTER=none` is set
- **THEN** the logs line reads `skipped, not exported`, and no request goes to `/v1/logs`

### Requirement: slayer otel test follows no redirect
`slayer otel test` SHALL NOT follow a redirect, because a redirect resends the headers to another URL. It SHALL report a redirect as a failure, with a hint to set the final URL.

#### Scenario: Redirect
- **WHEN** the endpoint answers HTTP 302
- **THEN** the command sends no second request, prints a hint about the redirect, and exits 1

### Requirement: slayer otel test sends one test span
If every endpoint accepts and traces are exported, `slayer otel test` SHALL send one span named `slayer.otel.test`, whatever the configured sampler. If the backend rejects the span, it SHALL exit 1. Otherwise it SHALL exit 0.

#### Scenario: Traces not exported
- **WHEN** `OTEL_TRACES_EXPORTER=none` is set and the other endpoints accept
- **THEN** the command sends no request to `/v1/traces`, sends no test span, and exits 0

#### Scenario: Rejected test span
- **WHEN** every endpoint accepts the empty request, but the backend rejects the test span
- **THEN** the command exits 1

### Requirement: slayer otel test runs while OTel is off
`slayer otel test` SHALL run while `SLAYER_OTEL` is off, so an operator can test a backend before turning OTel on. With `OTEL_SDK_DISABLED=true`, it SHALL send nothing, say so, and exit 0.

#### Scenario: SLAYER_OTEL off
- **WHEN** `SLAYER_OTEL` is not set and `slayer otel test` runs
- **THEN** the command probes each exported signal

#### Scenario: SDK disabled
- **WHEN** `OTEL_SDK_DISABLED=true` is set
- **THEN** the command sends no request and exits 0
