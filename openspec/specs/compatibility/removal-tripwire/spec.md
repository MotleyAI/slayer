# compatibility/removal-tripwire Specification

## Purpose
Makes every piece of compatibility code scheduled for removal in a future major version identifiable (warning while it still works, raising with its new location where its module moved), and stops that version's release while any of it remains.

## Requirements

### Requirement: Moved module paths fail with their new location

Importing a module path that SLayer 2.0 moved SHALL raise an `ImportError` whose message names the old path, the new path and SLayer 2.0. Where the old path is now a package, importing a public name of the moved module through it SHALL raise the same kind of `ImportError`, naming the name's new module. Importing a new path, or any name a package at an old path still provides, SHALL succeed and SHALL NOT warn.

#### Scenario: An old module path names its new location

- **WHEN** `slayer.sql.client` is imported
- **THEN** an `ImportError` is raised whose message names `slayer.sql.client`, `slayer.database.client` and SLayer 2.0

#### Scenario: An old name through a path that is now a package

- **WHEN** `infer_population` is imported from `slayer.engine.population`
- **THEN** an `ImportError` is raised whose message names `slayer.engine.population.inference`

#### Scenario: New paths are unaffected

- **WHEN** `SqlClient` is imported from `slayer.database.client`, and the `inference` submodule from `slayer.engine.population`
- **THEN** both imports succeed and no warning is emitted

#### Scenario: Every moved path is covered

- **WHEN** every module path moved in 2.0 is imported, and every public name of a module whose old path is now a package is imported through that path
- **THEN** each raises an `ImportError` naming its new location

### Requirement: One marker for code scheduled for removal

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use (a deprecated Mode-B spelling: per spelling per query or save), attributed to the line that used it (for an input form, the first line outside SLayer that supplied it), and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, the Mode-B grammar's `slayer.engine.syntax` and `slayer.core.formula`, and the OSI importer's `slayer.osi.expression`; the `strict` query input form and the extension object in `source_model`; and the deprecated Mode-B spellings `==`, `is None` / `is not None`, bare `None`, a `[…]` list after `IN` and the method-call aggregation form (`queries/expression-grammar` › Legacy spellings keep their meaning); each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

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

### Requirement: The tripwire stops the release of a removal's major version

A unit test SHALL read the package version and SHALL fail while any marked site's removal major version is less than or equal to the package's major version, naming every such site by file, line and message. It SHALL pass while the package's major version is below every marked site's removal version.

#### Scenario: The current version passes
- **WHEN** the tripwire runs with the real package version
- **THEN** it passes

#### Scenario: At the next major the tripwire names every pinned site
- **WHEN** the version the tripwire reads is patched to the next major version's `.0.0`
- **THEN** it fails naming exactly the pinned covered sites

#### Scenario: Aliased use is still found
- **WHEN** a module emits through the shared mechanism under an import alias
- **THEN** the tripwire finds that site

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
