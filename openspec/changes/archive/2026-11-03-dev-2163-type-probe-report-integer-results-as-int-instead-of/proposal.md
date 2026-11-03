## Why

The column-type probe classifies every result into four coarse categories (`number` / `string` / `time` / `boolean`) and every consumer maps `number` to DOUBLE, so no probed expression is ever INT. The categories are a second type vocabulary with their own driver tables that disagree with reflection, and the dbt / Cube importers on the DEV-2101 stack need the database's declared type names from the probe — through the engine's client — which the categories cannot carry.

## What Changes

- The probe returns, per result column, a SLayer `DataType` plus the database's own type name (`ResultColumnType`), under a new client method; integer results are INT, decimals by scale, floats DOUBLE, containers and unrecognised names as reflection maps them, opaque types UNKNOWN.
- One type-name mapper is shared by reflection and the probe; each dialect owns its driver's cursor description (Postgres OIDs, MySQL field codes, Snowflake `FieldType`, SQL Server via `sp_describe_first_result_set`, SQLite via the integer-affinity sampler).
- Postgres and Redshift probes run on the synchronous driver, so NUMERIC scale is known on every path.
- A datasource URL naming an async-only driver gets a synchronous engine on the type's default sync driver, so its metadata reads work.
- A cast to INT renders as 64-bit `BIGINT`; row-level derived columns over exact-numeric database types keep their native type instead of being cast.
- The 1.5.0 probe entry points (`get_column_types_sync`, `SlayerSQLClient.get_column_types`, `SlayerQueryEngine.get_column_types`) keep returning categories, derived from the typed result, and warn that they are deprecated.
- Consumers (schema drift, OSI importer, model inspection, conformance suite) read the typed result; the per-consumer category maps are deleted.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `sql/client-contract`: column-type probes report SLayer types and database type names; the 1.5.0 category entry points are deprecated.
- `sql/execution`: the synchronous engine for an async-only driver URL uses the type's default synchronous driver.
- `sql/statement-assembly`: a cast to INT is 64-bit.
- `aggregations/native-type-preservation`: row-level derived columns over exact-numeric types are not cast.
- `importers/osi-import`: field datatypes follow the typed probe (integer expressions INT, exact decimals keep their database type).

## Impact

- `slayer/sql/client.py`, `slayer/sql/dialects/` (base, reflection, postgres, `_tier2` Redshift, mysql, snowflake, tsql, sqlite), `slayer/sql/sqlite_introspect.py`, `slayer/sql/engine_factory.py`, `slayer/sql/conformance.py`, `slayer/sql/render/value_expr.py`.
- `slayer/engine/schema_drift.py`, `slayer/engine/query_engine.py`, the projection / bind path that sets native-type preservation, `slayer/osi/converter.py`, `slayer/inspect/model_render.py`.
- Public API: new typed probe method on `SqlClient` (unreleased protocol); three 1.5.0 entry points deprecated (tagged for 2.0 removal).
- Generated SQL: `CAST(… AS INT)` becomes `CAST(… AS BIGINT)`; exact-numeric derived dimensions lose their inferred cast (golden baselines re-blessed).
- Saved SQL-mode models: columns persisted from a wrong earlier probe answer (opaque as TEXT, SQL Server decimal/datetime as TEXT, MySQL `BIT` / arrays as DOUBLE) surface as type-mismatch drift; INT vs DOUBLE is never drift.
- Docs: `docs/configuration/sql-clients.md`, schema-drift docs.
