## MODIFIED Requirements

### Requirement: One marker for code scheduled for removal

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use (a deprecated Mode-B spelling: per spelling per query or save), attributed to the line that used it (for an input form, the first line outside SLayer that supplied it), and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead; a marked informational notice SHALL emit no `FutureWarning` — it is carried only as its structured response warning. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, the Mode-B grammar's `slayer.engine.syntax` and `slayer.core.formula`, and the OSI importer's `slayer.osi.expression`; the `strict` query input form, the extension object in `population` and a field's `sql` input form (`models/field-expressions` › A field's sql converts to its expression); the `broadcast` value of `to_many_handling` (`queries/attribution-modes` › Broadcast is a deprecated mode); the deprecated Mode-B spellings `==`, `is None` / `is not None`, bare `None`, a `[…]` list after `IN` and the method-call aggregation form (`queries/expression-grammar` › Legacy spellings keep their meaning); the root-relative reference spellings converted in a query that names its population (a bare field or dataset metric, `count(*)`, and a path whose first segment is not a dataset, `queries/references` › Pre-2.0 root-relative spellings convert when a population is named); the date-only comparison notice (`queries/time-points` › A date-only comparison that changed meaning is announced); and the changed-reference notice (`queries/references` › A reference whose meaning changed announces it); each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

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

#### Scenario: A deprecated spelling used twice in one query
- **WHEN** a query has the filters `status == 'paid'` and `region == 'EU'`
- **THEN** it emits exactly one `FutureWarning` naming `=` and "SLayer 3.0"

#### Scenario: A marked notice emits no FutureWarning
- **WHEN** a query triggers the date-only comparison notice, and another triggers the changed-reference notice
- **THEN** no `FutureWarning` is recorded, each response carries its notice, and the tripwire enumerates both notices' sites with removal version 3.0

#### Scenario: A root-relative spelling used twice in one query
- **WHEN** a query with population `orders` measures `count(*)` and filters `count(*) > 1`
- **THEN** it emits exactly one `FutureWarning` naming `count(*)`, `count(orders.*)` and "SLayer 3.0"

#### Scenario: The broadcast mode is a marked site
- **WHEN** the tripwire's version is patched to 3.0.0
- **THEN** it fails naming the `broadcast` value of `to_many_handling` among the pinned sites

### Requirement: Deprecated input forms convert with a structured warning

A deprecated input form SHALL be converted to its replacement only through the shared marker; a deprecated field given explicitly as `null` is still that form. A deprecated input form that no conversion can replace without changing its results (a *kept form*) SHALL instead be kept as given and reported through the same marker. Each conversion, and each fresh use of a kept form, SHALL emit exactly one `FutureWarning` that carries a structured payload of kind `deprecated_input` naming the old form, its replacement and the removal version, and SHALL keep that payload with the converted input. Every query run that consumes converted input — as the query, a stage, a nested query or a refinement, executed or as a dry run, in process, through REST, through the MCP `query` tool or through the HTTP client — SHALL report each of its payloads exactly once on the response's `warnings`, on every run, without emitting the payload again as a Python warning. A kept form SHALL be reported on every run that uses it, from stored documents too, and SHALL emit its `FutureWarning` only as fresh input. A save whose input carried a deprecated form SHALL report the payload on that save's response: as a structured entry on REST model saves, and as its message on MCP model saves and on memory saves. Converted input SHALL be persisted only in its replacement form; a kept form SHALL be persisted as given.

#### Scenario: Conversion warns once and every run reports it
- **WHEN** a query object carrying `strict: true` is built once and run three times
- **THEN** exactly one `FutureWarning` is emitted, at construction, carrying a payload naming `strict`, `to_many_handling` and 3.0; and each of the three responses carries exactly one `deprecated_input` warning with that payload and no further Python warning for it

#### Scenario: Each converted query object reports its own payload
- **WHEN** a multi-stage query whose two named stages each carry `strict` and whose root stage carries none is run
- **THEN** the response carries exactly two `deprecated_input` warnings

#### Scenario: Two deprecated forms in one query are each reported
- **WHEN** a query carrying both `strict: true` and an extension object in `population` is run in process and through REST
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

#### Scenario: A kept form is honoured, persisted as given and reported from storage
- **WHEN** a query carrying `to_many_handling: "broadcast"` is built fresh, run, saved as a query-backed model's source query, reloaded and run again
- **THEN** both runs return the same values and each carries exactly one `deprecated_input` warning, exactly one `FutureWarning` is recorded (at the fresh construction, none at reload), and the stored query holds `"broadcast"`
