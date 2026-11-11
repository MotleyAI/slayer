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

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked and the only emitter of `FutureWarning` in the package. A marked form that still works SHALL emit exactly one `FutureWarning` per use, attributed to the line that used it, and the CLI SHALL print it as one `Deprecated: …` line on stderr; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, each removed in 3.0; no form that still works is marked at 2.0.0.dev0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

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
