## MODIFIED Requirements

### Requirement: Deprecated input forms convert with a structured warning

A deprecated input form SHALL be converted to its replacement only through the shared marker; a deprecated field given explicitly as `null` is still that form. Each conversion SHALL emit exactly one `FutureWarning` that carries a structured payload of kind `deprecated_input` naming the old form, its replacement and the removal version, and SHALL keep that payload with the converted input. Every query run that consumes converted input — as the query, a stage, a nested query or a refinement, executed or as a dry run, in process, through REST, through the MCP `query` tool or through the HTTP client — SHALL report each of its payloads exactly once on the response's `warnings`, on every run, without emitting the payload again as a Python warning. A save whose input carried a deprecated form SHALL report the payload on that save's response: as a structured entry on REST model saves, and as its message on MCP model saves and on memory saves. Converted input SHALL be persisted only in its replacement form.

#### Scenario: Conversion warns once and every run reports it
- **WHEN** a query object carrying `strict: true` is built once and run three times
- **THEN** exactly one `FutureWarning` is emitted, at construction, carrying a payload naming `strict`, `to_many_handling` and 3.0; and each of the three responses carries exactly one `deprecated_input` warning with that payload and no further Python warning for it

#### Scenario: Each converted query object reports its own payload
- **WHEN** a multi-stage query whose two named stages each carry `strict` and whose root stage carries none is run
- **THEN** the response carries exactly two `deprecated_input` warnings

#### Scenario: Two deprecated forms in one query are each reported
- **WHEN** a query carrying both `strict: true` and an extension object in `source_model` is run in process and through REST
- **THEN** exactly one `FutureWarning` is emitted per form, and each response carries exactly two `deprecated_input` warnings, one naming `strict` and one naming the extension object

#### Scenario: An explicit null strict is reported over REST
- **WHEN** a `POST /query` body carries `"strict": null`
- **THEN** the query runs as it would without `strict`, and the response carries exactly one `deprecated_input` warning naming `strict` and 3.0

#### Scenario: A run-by-name body carrying a deprecated form is refused
- **WHEN** a `POST /query` body names a saved query-backed model and carries `"strict": null`
- **THEN** the request is rejected with status 400, as for any query clause next to `name`

#### Scenario: A dry run reports the conversion
- **WHEN** a query carrying `strict: true` is run with `dry_run` through REST and through the MCP `query` tool
- **THEN** each response carries the generated SQL and exactly one `deprecated_input` warning

#### Scenario: A converted refinement is reported
- **WHEN** a saved query is run by name with `refine={"strict": true}`, in process, through REST and through the MCP `query` tool
- **THEN** each run executes under `to_many_handling: "error"` and its response carries one `deprecated_input` warning

#### Scenario: The HTTP client reports conversions it made locally
- **WHEN** the HTTP client runs a mapping, a list of stages or a refinement carrying `strict`
- **THEN** its returned response carries one `deprecated_input` warning per converted object

#### Scenario: A save reports the deprecation and persists the replacement
- **WHEN** a query-backed model whose source query carries `strict: true` is saved through REST and through MCP, and a memory whose query carries `strict: true` is saved
- **THEN** the REST response's `warnings` carry the `deprecated_input` payload, the MCP response and the memory save's `warnings` each carry its message exactly once, and every stored query holds `to_many_handling: "error"` and no `strict`
