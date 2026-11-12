## MODIFIED Requirements

### Requirement: One marker for code removed in 2.0

Every piece of compatibility code scheduled for removal in a future major version SHALL emit its deprecation through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked. Each emission SHALL be a `FutureWarning`. The covered workarounds SHALL be: the 1.5.0 Python call forms (ingestion, drift validation, converters with `sa_engine`), the 0.9.x storage-backend adapters, the CLI `--models-dir` flag, the `SLAYER_MODELS_DIR` environment variable, the MCP `inspect_model` tool, `MemoryNotFoundError.identifier`, the 1.5.0 column-type probes and the drivers a SLayer extra installs besides the type's default (psycopg2, asyncpg, aiomysql). Stored-data upgrade paths, the colon aggregation spelling, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked

- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with removal version 2.0, and none of the excluded paths

#### Scenario: A marker without a literal removal version is rejected

- **WHEN** a marked site gives its removal version as anything but a string literal
- **THEN** the tripwire test fails naming that site
