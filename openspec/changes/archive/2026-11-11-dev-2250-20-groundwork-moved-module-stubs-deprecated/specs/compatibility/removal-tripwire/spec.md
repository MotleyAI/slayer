## MODIFIED Requirements

### Requirement: One marker for code scheduled for removal

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use, attributed to the line that used it (for an input form, the first line outside SLayer that supplied it), and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, the Mode-B grammar's `slayer.engine.syntax` and `slayer.core.formula`, and the OSI importer's `slayer.osi.expression`, each removed in 3.0; and the `strict` query input form, removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked
- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with its pinned removal version, and none of the excluded paths

#### Scenario: A marker without a literal removal version is rejected
- **WHEN** a marked site gives its removal version as anything but a string literal
- **THEN** the tripwire test fails naming that site

#### Scenario: A deprecation names its caller's line
- **WHEN** code calls a function that emits through the shared mechanism
- **THEN** exactly one `FutureWarning` is recorded, attributed to the calling line

#### Scenario: The CLI prints a deprecation on one line
- **WHEN** a deprecation is emitted while a CLI command runs
- **THEN** stderr carries it as exactly one `Deprecated: <message>` line

## ADDED Requirements

### Requirement: Deprecated input forms convert with a structured warning

A deprecated input form SHALL be converted to its replacement only through the shared marker. Each conversion SHALL emit exactly one `FutureWarning` that carries a structured payload of kind `deprecated_input` naming the old form, its replacement and the removal version, and SHALL keep that payload with the converted input. Every query run that consumes converted input — as the query, a stage, a nested query or a refinement, in process or through the HTTP client — SHALL report each of its payloads exactly once on the response's `warnings`, on every run, without emitting the payload again as a Python warning. A save whose input carried a deprecated form SHALL report the payload on that save's response: as a structured entry on MCP and REST model saves, and as its message on memory saves. Converted input SHALL be persisted only in its replacement form.

#### Scenario: Conversion warns once and every run reports it
- **WHEN** a query object carrying `strict: true` is built once and run three times
- **THEN** exactly one `FutureWarning` is emitted, at construction, carrying a payload naming `strict`, `to_many_handling` and 3.0; and each of the three responses carries exactly one `deprecated_input` warning with that payload and no further Python warning for it

#### Scenario: Each converted query object reports its own payload
- **WHEN** a multi-stage query whose two named stages each carry `strict` and whose root stage carries none is run
- **THEN** the response carries exactly two `deprecated_input` warnings

#### Scenario: A converted refinement is reported
- **WHEN** a saved query is run by name with `refine={"strict": true}`, in process, through REST and through the MCP `query` tool
- **THEN** each run executes under `to_many_handling: "error"` and its response carries one `deprecated_input` warning

#### Scenario: The HTTP client reports conversions it made locally
- **WHEN** the HTTP client runs a mapping, a list of stages or a refinement carrying `strict`
- **THEN** its returned response carries one `deprecated_input` warning per converted object

#### Scenario: A save reports the deprecation and persists the replacement
- **WHEN** a query-backed model whose source query carries `strict: true` is saved through REST and through MCP, and a memory whose query carries `strict: true` is saved
- **THEN** the REST response's `warnings` and the MCP response each carry the `deprecated_input` payload, the memory save's `warnings` carry its message, and every stored query holds `to_many_handling: "error"` and no `strict`
