## MODIFIED Requirements

### Requirement: One marker for code removed in 2.0

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked. A marked form that still works SHALL emit a `FutureWarning`; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be: the 1.5.0 Python call forms (ingestion, drift validation, converters with `sa_engine`), the 0.9.x storage-backend adapters, the CLI `--models-dir` flag, the `SLAYER_MODELS_DIR` environment variable, the MCP `inspect_model` tool, `MemoryNotFoundError.identifier`, the 1.5.0 column-type probes, the 1.6.0 SQL client's `get_column_types` probe and the drivers a SLayer extra installs besides the type's default (psycopg2, asyncpg, aiomysql), each removed in 2.0; and every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked

- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with its pinned removal version (2.0 for the warning workarounds, 3.0 for the moved module paths), and none of the excluded paths

#### Scenario: A marker without a literal removal version is rejected

- **WHEN** a marked site gives its removal version as anything but a string literal
- **THEN** the tripwire test fails naming that site

### Requirement: The tripwire stops a 2.0 release

A unit test SHALL read the package version and SHALL fail while any marked site's removal major version is less than or equal to the package's major version, naming every such site by file, line and message. It SHALL pass while the package's major version is below every marked site's removal version.

#### Scenario: On 1.x the tripwire passes

- **WHEN** the tripwire runs with the real 1.x package version
- **THEN** it passes

#### Scenario: At 2.0.0 the tripwire names every site

- **WHEN** the version the tripwire reads is patched to 2.0.0
- **THEN** it fails, naming every pinned covered site whose removal version is 2.0 and none whose removal version is 3.0

#### Scenario: At 3.0.0 the moved module paths are named

- **WHEN** the version the tripwire reads is patched to 3.0.0
- **THEN** it fails, naming every pinned moved-module-path site

#### Scenario: Aliased use is still found

- **WHEN** a module emits through the shared mechanism under an import alias
- **THEN** the tripwire finds that site

## ADDED Requirements

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
