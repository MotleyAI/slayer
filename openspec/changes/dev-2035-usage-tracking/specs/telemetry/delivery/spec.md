## Purpose

Defines how recorded usage is stored locally, when and where it is sent, and the guarantee that telemetry never affects the process it runs in.

## ADDED Requirements

### Requirement: Telemetry never affects the host process
No telemetry failure — file I/O, network, timeout, non-2xx response, corrupt or foreign files — SHALL change a command's result, exit code or stdout, or raise into the host process. Telemetry SHALL NOT write to stdout, and SHALL NOT block the process except for the bounded exit wait defined below.

#### Scenario: Endpoint down
- **WHEN** the telemetry endpoint refuses connections, times out or returns 500 during a command
- **THEN** the command's output and exit code are identical to a run with telemetry off

#### Scenario: Corrupt spool file
- **WHEN** a spool file contains invalid JSON
- **THEN** it is skipped and the command runs normally

### Requirement: Usage is spooled locally at process end
Each process SHALL keep its counters in memory and write them to its own spool file when it ends — normal exit, SIGINT, server shutdown, end of input or broken pipe on stdio MCP, and SIGTERM for stdio `slayer mcp` (chaining any handler already installed) — with no network request at that point. A process killed with SIGKILL loses its counts. A process that forks SHALL start the child with empty counters.

#### Scenario: stdio MCP terminated
- **WHEN** an MCP client sends SIGTERM to a `slayer mcp` process that handled tool calls
- **THEN** its counters are written to a spool file before it exits

#### Scenario: Concurrent processes
- **WHEN** several `slayer` processes end at the same time
- **THEN** each writes its own spool file and none overwrites another

#### Scenario: Forked child
- **WHEN** a process with recorded counts forks
- **THEN** the child's report does not repeat the parent's counts

### Requirement: Send at most once per 24 hours, only when SLayer runs
A report SHALL be sent only when a `slayer` process starts or records an action, at least 24 hours have passed since the last successful send, and telemetry is on. The report SHALL merge all spool files and the sending process's live counters. An idle process SHALL send nothing. A last-send time in the future SHALL count as due.

#### Scenario: Idle server
- **WHEN** a server records no action for 48 hours
- **THEN** it sends nothing

#### Scenario: Due on activity
- **WHEN** a server records an action 25 hours after the last successful send
- **THEN** exactly one report is sent, merging the spool and its live counters

#### Scenario: Not yet due
- **WHEN** a command starts 3 hours after the last successful send
- **THEN** nothing is sent and the command's counters are spooled at exit

#### Scenario: Clock moved backwards
- **WHEN** the recorded last-send time lies in the future
- **THEN** a send is due

### Requirement: Background send with a bounded exit wait
A due send SHALL run in a background thread started when the process starts (or, in a running server, when the action that made it due is recorded). At exit, the process SHALL wait for an in-flight send at most about one second; with no send in flight it SHALL not wait.

#### Scenario: Slow network at exit
- **WHEN** a short command exits while its send has been stalled for 10 seconds
- **THEN** the process exits within about one second of finishing its work, and the unsent data stays in the spool

### Requirement: Exactly-once-per-batch delivery bookkeeping
Only one process SHALL send at a time. Spool files included in a send SHALL be deleted, and the last-send time recorded, only after a 2xx response; otherwise they SHALL remain for a later send. Files claimed by a sender that died SHALL be recovered by a later send. Each report SHALL carry a random batch ID so the backend can drop duplicates. The spool SHALL be capped (about 100 files / 1 MB, oldest dropped first); spool files SHALL be readable only by the owner, and entries that are not regular files SHALL be ignored.

#### Scenario: Two simultaneous senders
- **WHEN** two processes find a send due at the same moment
- **THEN** one report is sent

#### Scenario: Failed send retained
- **WHEN** a send gets a 500 response
- **THEN** the spool files remain and the last-send time is unchanged

#### Scenario: Sender crash recovered
- **WHEN** a sender dies after claiming spool files
- **THEN** a later send includes those files

### Requirement: Destination and request shape
A report SHALL be sent as one event to the PostHog EU capture API (`https://eu.i.posthog.com/i/v0/e/`), with event name `slayer_usage`, the install ID as distinct ID, the batch ID as event UUID, GeoIP enrichment disabled and person-profile processing off. `SLAYER_TELEMETRY_ENDPOINT` SHALL override the URL.

#### Scenario: Endpoint override
- **WHEN** `SLAYER_TELEMETRY_ENDPOINT` points to a local test server and a send is due
- **THEN** the report is posted to that server with the event name `slayer_usage` and GeoIP disabled

### Requirement: Install identity and rotation
The install ID SHALL be a random UUID created on first use and stored in the platform config directory (not in SLayer's model storage). It SHALL be replaced by a new random UUID at the first start after it is 13 months old.

#### Scenario: Rotation
- **WHEN** a process starts and the install ID was created 14 months ago
- **THEN** a new install ID is created and used for subsequent reports
