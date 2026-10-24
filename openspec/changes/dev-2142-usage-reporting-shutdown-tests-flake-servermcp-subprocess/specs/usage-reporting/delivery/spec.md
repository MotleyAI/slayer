## MODIFIED Requirements

### Requirement: Usage is spooled locally at process end
Each process SHALL keep its counters in memory and write them to its own spool file when it ends — normal exit, SIGINT, server shutdown, end of input or broken pipe on stdio MCP, and SIGTERM for every server command (`serve`, `mcp`, `pg-serve`, `flight-serve`, chaining any handler already installed) — with no network request at that point. A server command stopped by SIGTERM or SIGINT SHALL count as a successful run; any other command interrupted by a signal SHALL count as failed. A process killed with SIGKILL loses its counts. A process that forks SHALL start the child with empty counters.

#### Scenario: stdio MCP terminated
- **WHEN** an MCP client sends SIGTERM to a `slayer mcp` process that handled tool calls
- **THEN** its counters are written to a spool file before it exits

#### Scenario: Server terminated right after startup
- **WHEN** a `slayer serve`, `pg-serve` or `flight-serve` process receives SIGTERM or SIGINT as soon as it accepts connections
- **THEN** it exits and its counters are written to a spool file

#### Scenario: Signal-stopped server counts as ok
- **WHEN** a server command is stopped by SIGTERM or SIGINT
- **THEN** the report counts that command once as ok, with no error

#### Scenario: Concurrent processes
- **WHEN** several `slayer` processes end at the same time
- **THEN** each writes its own spool file and none overwrites another

#### Scenario: Forked child
- **WHEN** a process with recorded counts forks
- **THEN** the child's report does not repeat the parent's counts
