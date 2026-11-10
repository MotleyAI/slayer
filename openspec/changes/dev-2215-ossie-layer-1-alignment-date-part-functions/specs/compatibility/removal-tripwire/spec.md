## MODIFIED Requirements

### Requirement: One marker for code removed in 2.0

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked. A marked form that still works SHALL emit a `FutureWarning`; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be: the 1.5.0 Python call forms (ingestion, drift validation, converters with `sa_engine`), the 0.9.x storage-backend adapters, the CLI `--models-dir` flag, the `SLAYER_MODELS_DIR` environment variable, the MCP `inspect_model` tool, `MemoryNotFoundError.identifier`, the 1.5.0 column-type probes, the 1.6.0 SQL client's `get_column_types` probe, the drivers a SLayer extra installs besides the type's default (psycopg2, asyncpg, aiomysql), the dialect's single-text EXPLAIN builder and the route running an explain query through the statement execution of a client without `explain`, each removed in 2.0; every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, each removed in 3.0; and the deprecated Mode-B spellings `==`, `is None` / `is not None`, bare `None`, a `[…]` list after `IN` and the method-call aggregation form (`queries/expression-grammar` › Legacy spellings keep their meaning), each removed in 3.0. Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked

- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with its pinned removal version (2.0 for the warning workarounds, 3.0 for the moved module paths and the deprecated Mode-B spellings), and none of the excluded paths

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
- **THEN** it fails, naming every pinned moved-module-path site and every pinned deprecated Mode-B spelling site

#### Scenario: Aliased use is still found

- **WHEN** a module emits through the shared mechanism under an import alias
- **THEN** the tripwire finds that site

### Requirement: Deprecated surfaces warn once per use

Each use of a newly warning workaround SHALL emit exactly one `FutureWarning` naming the replacement and "SLayer 2.0"; its replacement SHALL emit none. `--models-dir` SHALL warn only when it decides the storage location (no `--storage`); `SLAYER_MODELS_DIR` only when it decides it (no `--storage`, `--models-dir` or `SLAYER_STORAGE`). Storage precedence SHALL be `--storage`, `--models-dir`, `SLAYER_STORAGE`, `SLAYER_MODELS_DIR`, then the default. The CLI SHALL print a deprecation warning as one line on stderr. Each call of the MCP `inspect_model` tool and each read of `MemoryNotFoundError.identifier` SHALL warn; `inspect` and `memory_id` SHALL NOT. Each deprecated Mode-B spelling SHALL emit exactly one `FutureWarning` per spelling per query or save, naming its canonical twin and "SLayer 3.0"; its canonical twin SHALL emit none.

#### Scenario: --models-dir decides the storage

- **WHEN** a CLI command runs with `--models-dir` and no `--storage`
- **THEN** it uses that directory and prints exactly one deprecation line on stderr naming `--storage`

#### Scenario: A shadowed deprecated input is silent

- **WHEN** a CLI command runs with `--storage` together with `--models-dir`, or with `SLAYER_STORAGE` and `SLAYER_MODELS_DIR` both set
- **THEN** the replacement decides the storage and no deprecation warning is emitted

#### Scenario: SLAYER_MODELS_DIR decides the storage

- **WHEN** only `SLAYER_MODELS_DIR` is set and the CLI resolves its storage
- **THEN** that directory is used with exactly one warning naming `SLAYER_STORAGE`

#### Scenario: Deprecated MCP tool

- **WHEN** `inspect_model` is called, and separately `inspect` with `entity_type="model"`
- **THEN** the first emits exactly one `FutureWarning` naming `inspect`; the second emits none

#### Scenario: Deprecated error attribute

- **WHEN** `MemoryNotFoundError.identifier` is read, and separately `memory_id`
- **THEN** the first returns the memory id with exactly one `FutureWarning` naming `memory_id`; the second warns nothing

#### Scenario: A deprecated spelling used twice in one query

- **WHEN** a query has the filters `status == 'paid'` and `region == 'EU'`
- **THEN** it emits exactly one `FutureWarning` naming `=` and "SLayer 3.0"
