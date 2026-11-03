## Context

See proposal.md — Why. Today `slayer/sql/client.py` owns `_map_type_code` with Postgres-OID, MySQL and ODBC tables behind `if "mysql" in db_type` branches (against `architecture/python.sql.arc42.md` §3.2), plus a string-substring classifier that disagrees with reflection's `info_schema_type` / `_sa_type_to_data_type` (DuckDB `INTEGER[]` and Trino `array(integer)` match `INT`; ClickHouse `DATE32` falls to string). Every consumer maps the categories back by hand: `schema_drift._CURSOR_CATEGORY_TO_DATATYPE` (unknown → TEXT), the OSI importer through the legacy-alias coercion, `model_render` by string compare. `_ODBC_SQL_TYPE_MAP` is dead: pyodbc reports Python types in `description[1]`, so SQL Server `Decimal` / `datetime` probe as `"string"` today. SQLAlchemy's asyncpg adapter hard-codes precision and scale to `None` (asyncpg keeps the typmod in a `cdef` field). A datasource URL naming an async-only driver cannot build a working synchronous engine (`MissingGreenlet` on connect), so its metadata reads already fail.

Applicable principles: sql §3.1 (AST end to end; dialect hooks take typed operands), §3.2 (dialect quirks only in `dialects/`), §3.9 (fail closed), §3.13 (verbatim execution), §3.14 (one engine owner); system §7 (async-first via the sanctioned bridges), §16, §17 (one database door). No arc42 or `.c4` edit is needed. Spec attachment: `sql/*` → `python.sql` (`specs ['sql']`), `importers/osi-import` → `importers`, `aggregations/native-type-preservation` → `cross_cutting_specs.aggregations`.

## Goals / Non-Goals

**Goals:**
- One type vocabulary end to end: the probe speaks `DataType` + database type name, and no category map survives outside the deprecated 1.5.0 wrappers.
- One type-name→`DataType` mapper shared by reflection and the probe; one cursor-description reader per dialect.

**Non-Goals:**
- `element_type` / list typing: `DataType.ARRAY` exists only on the DEV-2101 branch, which adds the field when it merges this change.
- Re-typing saved SQL-mode models: persisted DOUBLE integer columns stay DOUBLE (no drift: same bucket); columns persisted from a wrong old answer surface through drift. No storage migration.
- Fixing the type of saved table-mode models (reflection already typed them).

## Decisions

**Shape.** `ResultColumnType(type: DataType, db_type: str | None = None)` lives in `slayer/sql/dialects/reflection.py` next to `ReflectedColumn` (dialect hooks return it; `dialects/` cannot import `client.py`) and is re-exported from `slayer.sql.client`. The typed probe is `SqlClient.get_result_column_types(sql) -> dict[str, ResultColumnType]` (working name), with the module-level `get_result_column_types_sync` and the engine's `SlayerQueryEngine.get_result_column_types(model_name, data_source=None)`. `DelegatingSqlClient` forwards it; the conformance suite checks it.

**One name mapper.** `_INFO_SCHEMA_TYPE_MAP` and `_SA_TYPE_MAP` merge into one name table behind the catalog-string mapper (`info_schema_type` / `column_from_type_string`); `_sa_type_to_data_type` looks names up in the same table after its class-sensitive overrides (SQL Server `TIMESTAMP` = rowversion, `sa.Uuid`, the opaque allow-list). Order: peel ClickHouse wrappers → dialect-sensitive overrides → container shapes (`…[]`, `LIST`, `ARRAY(…)`, `STRUCT`/`ROW`/`MAP(…)`, recognised before any `INT` substring test; typed as reflection types them, TEXT on `main`) → the explicit opaque allow-list → UNKNOWN → exact numerics by scale (0 → INT; >0 or unknown → DOUBLE) → the name table → substring fallbacks → TEXT. Unrecognised names stay TEXT; only the allow-list is UNKNOWN. Info-schema reflection gains the same fixes, so reflection and the probe agree name for name.

**Dialect-owned cursor descriptions.** `SqlDialect.map_cursor_type_code(type_code) -> str | None` is replaced by `describe_cursor_column(description_row) -> ResultColumnType | None`, receiving the whole DB-API description row (name, type code, display size, internal size, precision, scale, null_ok). The base default maps a string code (DuckDB `DuckDBPyType` str, BigQuery, Trino, ClickHouse names) through the name mapper with `db_type` = that name. Per column, a `None` from the hook falls to the base mapper, then TEXT. `client.py` keeps no per-driver tables or `db_type` substring branches.
- Postgres (and Redshift): OID table with canonical names (`smallint`, `integer`, `bigint`, `oid`, `real`, `double precision`, `money`, `numeric`, `boolean`, `text`, `character varying`, `character`, `"char"`, `date`, `time`, `timestamp without time zone`, `timestamp with time zone`, `interval`, `json`, `jsonb`, `uuid`, …), each typed through the name mapper; `numeric` by scale with `db_type` `numeric(p,s)` when precision and scale are reported; unknown OIDs → TEXT with `db_type=None`.
- MySQL / MariaDB: field-type codes with names; `NEWDECIMAL`/`DECIMAL` by scale, `db_type` `DECIMAL(p,s)`; `TINY` with declared length 1 → BOOLEAN, other `TINY` → INT; `BIT` with length 1 → BOOLEAN, `BIT(n>1)` → TEXT; `LONG`/`LONGLONG`/`SHORT`/`INT24`/`YEAR` → INT; `FLOAT`/`DOUBLE` → DOUBLE; `DATE`/`NEWDATE` → DATE; `DATETIME`/`TIMESTAMP` → TIMESTAMP; `TIME` as reflection maps it.
- Snowflake: `FieldType` codes; `FIXED` by scale with `db_type` `NUMBER(p,s)`; `REAL` → DOUBLE `FLOAT`; `TEXT`; `DATE`; `TIMESTAMP_*`; `BOOLEAN`; `VARIANT`/`OBJECT`/`ARRAY`/`BINARY` as reflection maps them.
- SQL Server: see below. SQLite: see below.

**Postgres probes on the synchronous driver.** A dialect flag (`type_probe_uses_sync_driver`, on for Postgres and Redshift) makes the client run the probe through its own synchronous engine via `_run_sync_in_thread`, so psycopg2 reports precision and scale. Async and sync callers then agree. Alternative rejected (user): treat NUMERIC as scale-unknown everywhere — loses `numeric(p,0)` INT parity with reflection and the importers' `(p,s)`.

**Async-only driver URLs get a synchronous engine.** The synchronous engine for a URL naming the type's async-only driver (`postgresql+asyncpg`, `mysql+aiomysql`, `mariadb+aiomysql`) is built on the type's default synchronous driver — the inverse of the existing async derivation (`_async_connection_string`), owned by `engine_factory` (§3.14) from the dialect's driver facts. Every metadata read and the Postgres probe then work for such datasources. Alternatives rejected (user, codex): keep the async probe for those URLs (answer depends on URL spelling); document the exception.

**SQL Server describes the probe.** The tsql dialect returns, from a typed `exp.National` literal of the probe SQL built by the client, the AST of `EXEC sp_describe_first_result_set @tsql = N'…'`; the client renders it once and runs it verbatim on the same read-only, timeout-wrapped connection, reading `name`, `system_type_name`, `precision`, `scale`. `system_type_name` goes through the name mapper with the tsql overrides (`timestamp`/`rowversion` → TEXT). Only the proc's "metadata could not be determined" errors (11500–11541) fall back to the `TOP 0` probe, typed from pyodbc's Python `type_code` plus scale (`bool` → BOOLEAN, `int` → INT, `float` → DOUBLE, `Decimal` by scale, `datetime` → TIMESTAMP, `date` → DATE, `time` as reflection maps it, `str`/`bytes`/`UUID` → TEXT) with `db_type=None`; every other error propagates. `_ODBC_SQL_TYPE_MAP` is deleted.

**SQLite certifies integers.** SQLite's cursor reports no types. The client runs the `LIMIT 1` probe for column names, then one `typeof`-count statement over at most `PROBE_SCAN_CAP + 1` rows of the probe. The count select-list is built as AST by the SQLite dialect from the column identifiers (typed operands); the client splices the probe SQL as the relation through its existing probe wrapper, never parsing it. Verdict per column: all non-NULL `integer`, unsaturated → INT; any `real` or non-integral → DOUBLE; any `text`/`blob` → TEXT (no coercion — a result column has no affinity declaration); no non-NULL value → TEXT; saturated all-integer → DOUBLE. `db_type=None`. The counting statement and its classification are shared with `sqlite_introspect.probe_sqlite_integer_column`, whose own SQL is rebuilt as AST (touched-file compliance with §3.1).

**Generic first-row fallback.** A non-SQLite driver whose description has no type code for a column types it from the first row: `bool` → BOOLEAN; `int`/`float`/`Decimal` → DOUBLE (uncertified, §3.9); `datetime` → TIMESTAMP; `date` → DATE; `str`/`None`/other → TEXT; `db_type=None`.

**INT casts are 64-bit.** One logical-INT → `BIGINT` target in `slayer/sql/render/value_expr.py` serves both `_wrap_cast_for_type`'s insertion and its idempotence check (and the boolean-restore cast's `DataType` path). The fixed-purpose `INT` casts (date-add counts, boolean 1/0 reads) stay.

**Native-type preservation reaches row-level derived columns.** The flag that suppresses an inferred cast for measures over exact-numeric columns is also set for a row-level derived column whose own `db_type` is exact-numeric (non-explicit type only; SQLite keeps its cast, as for measures). So wide exact integers (`NUMBER(38,0)`) never pass through `BIGINT`.

**1.5.0 entry points stay, deprecated.** `get_column_types_sync`, `SlayerSQLClient.get_column_types` and `SlayerQueryEngine.get_column_types` keep their signatures and category returns, computed from the typed probe by one `DataType`→category function (INT/DOUBLE → `number`, TEXT/UNKNOWN → `string`, DATE/TIMESTAMP → `time`, BOOLEAN → `boolean`), emit a `DeprecationWarning` naming the typed method, and are tagged for 2.0 removal. The `SqlClient` protocol (unreleased) carries only the typed method.

**Consumers.** `schema_drift._live_columns_for_sql_model` reads `.type` (`_CURSOR_CATEGORY_TO_DATATYPE` deleted; bucket rule unchanged). OSI: SQL-mode columns, derived fields and hidden helpers take `.type`, and store `db_type` when exact-numeric or opaque (the existing `Column.db_type` rule). `model_render._choose_sample_agg` reads `.type in (INT, DOUBLE)`. The conformance suite asserts `{id: INT, name: TEXT}` by `.type`, and `db_type` a non-empty string or `None`.

### Decisions log

1. The typed result replaces the categories; scope per the 2026-11-03 issue comment. (user)
2. Structural fix: one name mapper shared with reflection, dialect-owned cursor descriptions, no category maps. (default: structural-fix rule, sql §3.2)
3. No `element_type` on `main`; DEV-2101 adds it at merge. (default: `DataType.ARRAY` absent on `main`)
4. `ResultColumnType` lives in `dialects/reflection.py`, re-exported from `client.py`. (default: layering)
5. INT casts render as `BIGINT`, over leaving the 32-bit overflow. (user)
6. SQL Server via `sp_describe_first_result_set` with a `TOP 0` / Python-type fallback, over Python types with `db_type=None`. (user)
7. Postgres probes on the synchronous driver for the typmod, over scale-unknown NUMERIC. (user)
8. SQLite saturated all-integer sample → DOUBLE, over INT or an uncapped scan. (user)
9. SQLite text storage is never coerced to DOUBLE in the result probe. (default: no affinity declaration)
10. Generic first-row fallback types numbers DOUBLE. (default: §3.9, as decision 8)
11. MySQL `TINY` length 1 → BOOLEAN, other `TINY` → INT; `BIT(1)` → BOOLEAN, `BIT(n>1)` → TEXT — refines the issue comment's "TINY stays boolean". (user, codex)
12. Containers as reflection maps them (TEXT on `main`); UNKNOWN only for the explicit opaque allow-list; unrecognised names TEXT. (default: sql-mode parity; codex)
13. No compatibility path for SQL-mode columns persisted from a wrong old answer; they surface through drift. Rejected: storage-version bump with load-time re-typing. (user)
14. Saved SQL-mode DOUBLE integer columns are not re-typed. (default: follows 13; same drift bucket)
15. Callers apply "`db_type` only where `DataType` is lossy" when storing a `Column`. (user, issue comment)
16. The SQLite sampler is shared with `sqlite_introspect` and built as AST. (default: touched-file compliance, sql §3.1)
17. Async-only driver URLs get a synchronous engine on the type's default sync driver, over keeping the async probe for them or documenting an exception. (user, codex)
18. Native-type preservation extends to row-level derived columns with an exact-numeric `db_type`, over accepting a loud overflow past 2⁶³. (user, codex)
19. Typed API under a new name; the three 1.5.0 entry points stay as deprecated category wrappers, over an in-place return-type change. (user, codex)
20. `db_type` is absent only when the driver supplies no type name (SQLite, unknown Postgres OIDs, the SQL Server fallback, the generic fallback); per-backend tests assert it present on DuckDB, Postgres and SQL Server describe. (codex)
21. SQL Server falls back only on the proc's metadata-undeterminable errors (11500–11541); others propagate. (codex)
22. Dialect hooks take typed operands (`exp.National` literal, column identifiers); the inner probe SQL is spliced only by the client's existing probe wrapper, never parsed. (codex)
23. The merged name table keeps dialect- and class-sensitive overrides ahead of it (SQL Server `timestamp` = rowversion). (codex)
24. Cursor fallback is per column. (codex)
25. One INT → `BIGINT` target for cast insertion and the idempotence check; rendering snapshots per dialect. (codex)
26. Rejected (codex): SQLite saturation as "no verdict" — a probe has no prior type, the type is persisted at save, and INT vs DOUBLE share a drift bucket (SQLite skips type comparison), so row growth causes no drift. Decision 8 stands.
27. Rejected (codex): a precision cutoff for scale-0 exact numerics — Snowflake stores every integer as `NUMBER(38,0)`; decision 18 covers the overflow instead.
28. DuckDB `JSON` is TEXT in reflection and the probe (VARCHAR-backed and comparable), so the opaque allow-list is dialect-overridable, as SQL Server's `timestamp` already is. Rejected: UNKNOWN everywhere, which drops grouping and makes saved DuckDB JSON columns report false drift. (user)

## Risks / Trade-offs

- [Postgres probes take a sync pooled connection in a worker thread] → the client already owns that engine for metadata reads; probes are save-/import-/inspect-time, not per-query.
- [`sp_describe_first_result_set` coverage is CI-only (no local ODBC driver)] → unit tests drive the tsql hook and fallback classifier with fakes; the SQL Server integration leg covers the live path.
- [Golden SQL churn from `AS BIGINT` and dropped exact-numeric dimension casts] → re-bless the baselines in one commit, reviewing each diff for only those two shapes.
- [OSI info-warning set changes (Integer over integer stops warning; Decimal/Float over integer starts)] → intended; follows the database type.
- [Old OSI imports with opaque / SQL Server decimal / MySQL BIT / array columns get delete proposals] → drift reports the persisted and live types; docs say how to re-type or re-import.
