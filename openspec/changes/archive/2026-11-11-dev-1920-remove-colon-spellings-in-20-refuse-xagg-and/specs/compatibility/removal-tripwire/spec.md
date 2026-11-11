## MODIFIED Requirements

### Requirement: One marker for code removed in 2.0

Every piece of compatibility code scheduled for removal in a future major version SHALL be marked through one shared mechanism that records the removal version as a literal, and that mechanism SHALL be the only way such code is marked. A marked form that still works SHALL emit a `FutureWarning`; a marked module path that moved SHALL raise an `ImportError` instead. The covered workarounds SHALL be: the 1.5.0 Python call forms (ingestion, drift validation, converters with `sa_engine`), the 0.9.x storage-backend adapters, the CLI `--models-dir` flag, the `SLAYER_MODELS_DIR` environment variable, the MCP `inspect_model` tool, `MemoryNotFoundError.identifier`, the 1.5.0 column-type probes, the 1.6.0 SQL client's `get_column_types` probe, the drivers a SLayer extra installs besides the type's default (psycopg2, asyncpg, aiomysql), the dialect's single-text EXPLAIN builder and the route running an explain query through the statement execution of a client without `explain`, each removed in 2.0; and every module path moved in 2.0 by the database-access, live-schema, row-supply and cardinality carve-outs, each removed in 3.0. Stored-data upgrade paths, the Flight pyarrow adapter modules and the pg facade's `token=` argument SHALL NOT be marked.

#### Scenario: Every covered workaround is marked

- **WHEN** the marked sites in the package are enumerated
- **THEN** they are exactly the pinned set of covered workarounds, each with its pinned removal version (2.0 for the warning workarounds, 3.0 for the moved module paths), and none of the excluded paths

#### Scenario: A marker without a literal removal version is rejected

- **WHEN** a marked site gives its removal version as anything but a string literal
- **THEN** the tripwire test fails naming that site
