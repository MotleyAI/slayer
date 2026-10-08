## Purpose

Defines how usage telemetry is switched on and off, how users inspect it, and how its existence is disclosed on first use.

## ADDED Requirements

### Requirement: Enabled by default with an opt-out precedence
Telemetry SHALL be enabled by default. Its effective state SHALL be decided by the first matching rule, highest first:
1. `DO_NOT_TRACK` set to a truthy value → off.
2. `SLAYER_TELEMETRY` set to `on` or `off` → that value.
3. Automatic off: the `CI` variable is set; SLayer is an editable install; the telemetry config directory cannot be written.
4. The persisted setting from `slayer telemetry enable|disable`.
5. Default → on.

When off, telemetry SHALL record nothing, write no telemetry file, print no notice and make no telemetry request.

#### Scenario: DO_NOT_TRACK beats explicit enable
- **WHEN** `DO_NOT_TRACK=1` and `SLAYER_TELEMETRY=on` are both set
- **THEN** telemetry is off

#### Scenario: Explicit enable beats CI
- **WHEN** `CI=true` and `SLAYER_TELEMETRY=on` are set
- **THEN** telemetry is on

#### Scenario: CI auto-off
- **WHEN** `CI` is set and no higher rule applies
- **THEN** telemetry is off

#### Scenario: Editable install auto-off
- **WHEN** SLayer is installed in editable mode and no higher rule applies
- **THEN** telemetry is off

#### Scenario: Unwritable config directory
- **WHEN** the telemetry config directory cannot be created or written and no higher rule applies
- **THEN** telemetry is off, with no notice and no network request

#### Scenario: Persisted disable
- **WHEN** the user ran `slayer telemetry disable` and no environment rule applies
- **THEN** telemetry is off in later processes

### Requirement: The slayer telemetry command
`slayer telemetry` SHALL provide:
- `status`: the effective state, the rule that decided it, and the current install ID (if any).
- `enable` / `disable`: persist the setting; `disable` also deletes the spool and the install ID.
- `show`: print exactly the report the next send would contain, without sending it.

#### Scenario: Status names the deciding rule
- **WHEN** `CI` is set and the user runs `slayer telemetry status`
- **THEN** the output says telemetry is off because of `CI`

#### Scenario: Disable removes local data
- **WHEN** the user runs `slayer telemetry disable`
- **THEN** the spool and the install ID are deleted and no later process sends

#### Scenario: Show does not send
- **WHEN** the user runs `slayer telemetry show`
- **THEN** the pending report is printed and no request is made

### Requirement: One-time disclosure notice
When telemetry is on, SLayer SHALL print a notice once, on stderr only, naming what is collected, linking the telemetry docs page and giving the opt-out. The notice SHALL be printed again when the report schema's major version changes. It SHALL never be written to stdout.

#### Scenario: Notice once
- **WHEN** two `slayer` commands run in sequence with telemetry on for the first time
- **THEN** only the first prints the notice, on stderr

#### Scenario: stdio MCP stays clean
- **WHEN** `slayer mcp` starts with telemetry on for the first time
- **THEN** the notice goes to stderr and stdout carries only MCP protocol messages

#### Scenario: Schema major bump
- **WHEN** the recorded notice was shown for an older report schema major version
- **THEN** the notice is printed again once
