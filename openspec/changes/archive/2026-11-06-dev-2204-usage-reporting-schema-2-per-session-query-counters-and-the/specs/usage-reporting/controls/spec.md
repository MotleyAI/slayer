## MODIFIED Requirements

### Requirement: The slayer usage-reporting command
`slayer usage-reporting` SHALL provide:
- `status`: the effective state, the rule that decided it, and the current install ID (if any).
- `enable` / `disable`: persist the setting; `disable` also deletes the spool (the files of every schema version), the install ID and the first-successful-query date.
- `show`: print exactly the report the next send would contain, without sending it.

#### Scenario: Status names the deciding rule
- **WHEN** `CI` is set and the user runs `slayer usage-reporting status`
- **THEN** the output says usage reporting is off because of `CI`

#### Scenario: Disable removes local data
- **WHEN** the user runs `slayer usage-reporting disable`
- **THEN** the spool, the install ID and the first-successful-query date are deleted and no later process sends

#### Scenario: Show does not send
- **WHEN** the user runs `slayer usage-reporting show`
- **THEN** the pending report is printed and no request is made

### Requirement: One-time disclosure notice
When usage reporting is on, SLayer SHALL print a notice once, on stderr only, naming what is collected, linking the usage-reporting docs page and giving the opt-out. The notice SHALL be printed again when the recorded notice was shown for an older report schema version, and SHALL NOT be printed for a recorded newer one; a process SHALL NOT lower the recorded schema version. It SHALL never be written to stdout.

#### Scenario: Notice once
- **WHEN** two `slayer` commands run in sequence with usage reporting on for the first time
- **THEN** only the first prints the notice, on stderr

#### Scenario: stdio MCP stays clean
- **WHEN** `slayer mcp` starts with usage reporting on for the first time
- **THEN** the notice goes to stderr and stdout carries only MCP protocol messages

#### Scenario: Schema major bump
- **WHEN** the recorded notice was shown for an older report schema major version
- **THEN** the notice is printed again once

#### Scenario: Newer recorded schema
- **WHEN** the recorded notice was shown for a newer report schema version
- **THEN** no notice is printed and the recorded version is unchanged
