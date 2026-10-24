## Purpose

Defines how the `slayer` server commands (`serve`, `mcp`, `pg-serve`, `flight-serve`) react to termination signals: exit status, output, graceful and forced stop.

## ADDED Requirements

### Requirement: A termination signal stops a server command
From the moment a server command starts, SIGTERM or SIGINT SHALL stop it, whatever the process is doing when the signal arrives — starting up, waiting for connections, or blocked serving a request. The process SHALL exit with status 128 + the signal number (143 for SIGTERM, 130 for SIGINT) and SHALL NOT print a traceback. A handler for the signal installed before the command started SHALL still be called; a signal the process inherited as ignored SHALL stay ignored.

#### Scenario: Signal as soon as the server is reachable
- **WHEN** a server command receives SIGTERM or SIGINT the moment it accepts connections
- **THEN** it exits with 143 or 130 and stderr carries no traceback

#### Scenario: Signal during startup
- **WHEN** a server command receives SIGTERM before it accepts connections
- **THEN** it exits with 143

#### Scenario: Pre-installed handler
- **WHEN** a handler for SIGTERM was installed before the command started and SIGTERM arrives
- **THEN** that handler is called and the command still exits with 143

#### Scenario: Server failure is not a signal stop
- **WHEN** a server command fails on its own, without a signal
- **THEN** it exits with that failure's status and diagnostics, not 130 or 143

### Requirement: Graceful stop, forced by a second signal
On the first SIGTERM or SIGINT a server command SHALL stop accepting work and stop gracefully, closing its client connections; open idle client connections SHALL NOT keep it running. A second SIGTERM or SIGINT during the graceful stop SHALL end the process at once, after writing its usage spool, with status 128 + that signal's number.

#### Scenario: Idle client connected
- **WHEN** `slayer pg-serve` with an idle client connection receives SIGTERM
- **THEN** it exits with 143

#### Scenario: Stuck graceful stop
- **WHEN** a server command's graceful stop does not finish and a second SIGTERM or SIGINT arrives
- **THEN** the process exits at once with 143 or 130 and its counters are spooled

### Requirement: stdio MCP exits without waiting for its input
Stdio `slayer mcp` SHALL exit on SIGTERM or SIGINT without waiting for further input or end of input on stdin. End of input SHALL end it normally with status 0.

#### Scenario: Signal with stdin still open
- **WHEN** an MCP client keeps stdin open and sends SIGTERM to `slayer mcp`
- **THEN** the process exits with 143 within seconds

#### Scenario: End of input
- **WHEN** an MCP client closes stdin
- **THEN** `slayer mcp` exits with status 0
