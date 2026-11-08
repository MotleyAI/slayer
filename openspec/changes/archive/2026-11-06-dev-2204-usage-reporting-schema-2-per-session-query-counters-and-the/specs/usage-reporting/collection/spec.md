## ADDED Requirements

### Requirement: Per-session MCP query outcomes
A usage report SHALL carry a histogram of MCP sessions: each row counts the sessions whose successful and whose failed `query` tool calls fall into a given pair of buckets (`0` / `1` / `2-5` / `6-20` / `21+`). A session SHALL be every MCP client session that makes at least one tool call — the one session of a stdio `slayer mcp` process, and each client session of the MCP endpoint mounted on `slayer serve`. A `query` call SHALL count as successful or failed exactly as it is counted under the `mcp:query` usage counter, except that a dry run SHALL be counted neither way. A session without counted `query` calls SHALL be reported in the `0` / `0` row. Each session SHALL be reported exactly once, with its totals, in the counters its process spools when it ends; a send made while the process runs SHALL NOT include it. A forked child SHALL start without sessions. Sessions in equal bucket pairs SHALL be reported as one row, and rows SHALL be ordered by bucket rank.

#### Scenario: Successful and failed queries in one session
- **WHEN** one MCP session makes two `query` calls that succeed and one that fails, then the process ends
- **THEN** the report holds one session row with successful bucket `2-5`, failed bucket `1` and session count 1

#### Scenario: Session without queries
- **WHEN** an MCP session calls only `list_datasources`, then the process ends
- **THEN** the report holds one session row with buckets `0` / `0`

#### Scenario: Sessions with equal buckets share a row
- **WHEN** two MCP sessions on one `slayer serve` each make one successful `query` call and no failing one
- **THEN** the report holds one row with buckets `1` / `0` and session count 2

#### Scenario: Dry run not counted
- **WHEN** an MCP session makes one `query` call with `dry_run` and nothing else
- **THEN** the session is reported with buckets `0` / `0`

#### Scenario: Database failure counted as failed
- **WHEN** a session's `query` call fails in the database
- **THEN** it is counted in the session's failed bucket and under the `mcp:query` errors with a database error token

#### Scenario: Session spanning a send
- **WHEN** a session makes one successful `query` call, a send happens while the process runs, and the session then makes a second successful `query` call before the process ends
- **THEN** the send carries no row for that session, and the counters spooled at process end hold it once with successful bucket `2-5`

#### Scenario: Session totals survive a termination signal
- **WHEN** a `slayer mcp` process whose session made a successful `query` call is stopped by SIGTERM
- **THEN** its spooled counters hold that session with successful bucket `1`

#### Scenario: Forked child does not repeat sessions
- **WHEN** a process with a recorded session forks
- **THEN** the child's counters hold no session rows

### Requirement: First successful query date
A usage report SHALL carry the date of the install's first successful query, or none. A query SHALL count when it succeeds on a query surface — the MCP `query` tool, `slayer query`, the REST `/query` route, a Flight SQL query or a PostgreSQL-wire statement — and is not a dry run; probes and catalog queries SHALL NOT count. The date SHALL be stored with the install ID, set by the first counting query of the current install ID and not changed by later ones, and sent with every report of that install ID. Only a process started by the `slayer` CLI with usage reporting on SHALL set it.

#### Scenario: First successful query sets the date
- **WHEN** no date is stored and a `slayer query` succeeds
- **THEN** the stored date is today and the next report carries it

#### Scenario: Every query surface counts
- **WHEN** no date is stored and the first successful query arrives through MCP, the REST `/query` route, Flight SQL or the PostgreSQL wire
- **THEN** the date is set to today

#### Scenario: Failures, dry runs and probes do not count
- **WHEN** no date is stored and a query fails, a query runs as a dry run, or a Flight SQL or PostgreSQL probe succeeds
- **THEN** no date is stored

#### Scenario: Later queries keep the date
- **WHEN** a date is stored and a later query succeeds on another day
- **THEN** the stored date is unchanged

#### Scenario: Another process rotates the install ID
- **WHEN** a running server has seen the date stored, another process then rotates the install ID, and the server then runs a successful query
- **THEN** the date is stored again for the new install ID

#### Scenario: Library use stores nothing
- **WHEN** user code runs a successful query through the query engine without the `slayer` CLI
- **THEN** no date is stored

## MODIFIED Requirements

### Requirement: Report identity and time resolution
A usage report SHALL carry the install ID, a random batch ID, the report schema version (2), the first and last calendar dates it covers, and the install's first-successful-query date. It SHALL NOT carry timestamps finer than a date.

#### Scenario: Dates only
- **WHEN** a report covering activity on two days is built
- **THEN** its period holds two dates and no time-of-day values

#### Scenario: Schema version 2
- **WHEN** a report is built
- **THEN** its schema version is 2
