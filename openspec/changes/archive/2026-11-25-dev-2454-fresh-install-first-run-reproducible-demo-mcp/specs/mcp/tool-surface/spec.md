## ADDED Requirements

### Requirement: Startup preparation never delays the handshake
`slayer mcp` SHALL answer the MCP handshake (`initialize` and `tools/list`) without waiting for startup
preparation. Startup preparation is the demo setup requested by `--demo` followed by the startup ingest
requested by `--ingest-on-startup` or `SLAYER_INGEST_ON_STARTUP`; it SHALL run in the background once the
server starts. Every tool call SHALL wait until preparation has finished and then run normally. While a call
waits, the server SHALL send it progress notifications when the request carries a progress token. A
cancelled call SHALL NOT stop preparation for other calls. When the demo setup fails, the server SHALL keep
running and every tool call SHALL return the error `Failed to set up the Jaffle Shop demo: <reason>`, which
is also written to stderr. A startup-ingest failure — of a datasource or of individual tables — SHALL be
reported on stderr and SHALL NOT fail tool calls. Preparation SHALL write nothing to stdout. Without
startup preparation, tool calls SHALL NOT wait. `slayer serve` SHALL keep finishing its preparation before
it starts serving.

#### Scenario: The handshake answers during the demo build
- **WHEN** `slayer mcp --demo` starts on empty storage and the client sends `initialize` and `tools/list`
  while the demo is still being built
- **THEN** both answer without waiting for the build

#### Scenario: A tool call waits for the demo
- **WHEN** the client calls `query` on the demo while the demo is still being built
- **THEN** the call returns the query's rows once the build has finished

#### Scenario: Progress while waiting
- **WHEN** a tool call carrying a progress token waits for preparation
- **THEN** the client receives progress notifications for that call until it runs

#### Scenario: A cancelled call does not stop preparation
- **WHEN** one of two waiting tool calls is cancelled
- **THEN** preparation continues and the other call returns its result

#### Scenario: A failed demo setup becomes a tool error
- **WHEN** the demo setup fails
- **THEN** the server keeps running, every tool call returns `Failed to set up the Jaffle Shop demo:`
  followed by the reason, and stderr holds the reason

#### Scenario: Startup ingest runs behind the gate
- **WHEN** `slayer mcp --ingest-on-startup` starts and one configured datasource fails to ingest
- **THEN** the handshake answers without waiting for the ingest, stderr reports the failed datasource,
  and tool calls succeed once the ingest has finished
