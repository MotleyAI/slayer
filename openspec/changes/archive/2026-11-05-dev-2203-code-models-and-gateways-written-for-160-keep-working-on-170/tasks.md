## 0. 1.6.0 client type probe (done before planning)

- [x] 0.1 `tests/test_legacy_client_type_probe.py`: from-scratch 1.6.0 client, a `DelegatingSqlClient` subclass overriding only `get_column_types`, and one deferring to `super().get_column_types`, behind an engine's client factory; OSI `collect_metadata` and `validate_datasource(client=...)` through a 1.6.0 client; the category mapping; the deprecated `DelegatingSqlClient.get_column_types`; current clients pass through unchanged. Verified failing with the adapter disabled.
- [x] 0.2 Two new sites pinned in `tests/test_law_removal_tripwire.py`.
- [x] 0.3 `adapt_client` / `_CategoryProbeClient` in `slayer/sql/client.py`, applied in `SlayerQueryEngine.client_for`, OSI `collect_metadata` and `validate_datasource`; deprecated `DelegatingSqlClient.get_column_types`.
- [x] 0.4 `docs/configuration/sql-clients.md`: a 1.6.0 client keeps working until 2.0.

## 1. Tests (pr-tests stage; all fail before implementation)

- [x] 1.1 Conformance: `run_conformance` on a 1.6.0-only client fails its `get_result_column_types` check (passes today; pins the decision).
- [x] 1.2 `tests/test_list_retype_dependents.py` (DuckDB, unit): every scenario of the list-columns delta — measure, derived column (incl. a chain), model filter, saved query, a to-one joined model's measure, `count(tags)` not reported, a definition broken before the re-type not reported, string-literal / unrelated same-named column not checked, an unchecked dependant reported with its reason, structured warnings, CLI ingest lines, MCP `ingest_datasource_models` text, REST field, the re-type still saved, a caller's client usable afterwards.
- [x] 1.3 `tests/test_builtin_aggregation_exact_case.py`: every scenario of the aggregation-names delta, over functional / colon / method spellings, stored measures and saved queries, `Sum` + `SUM` without ambiguity, exact `sum` override, `upper` / `Total` unchanged.
- [x] 1.4 `tests/test_retype_finding.py`: every scenario of the schema-drift delta. Table-backed models typed by 1.6.0 reflection over DuckDB (`INTERVAL`, `JSON`, `BIT`, `YEAR`-like names via info-schema) and pytest-postgresql Postgres (`interval`, `json`, `oid`, `money`); an `sql`-mode model over Postgres `json`; force-clean keeps the columns and their measures and prints `RE-TYPE:`; a genuinely retyped column; the list case's `retype` kind and re-ingest remedy; missing column still deleted with cascade; query-time `SchemaDriftError` carrying the finding; `live_db_type` present when reported and `None` when not; one serialized shape over REST, MCP and Python. Align existing tests that pin a type-mismatch delete or `list_retype` (needs the user's approval per file).
- [x] 1.5 `tests/test_legacy_category_parity.py`: 1.6.0's tables copied as fixtures (Postgres OIDs, MySQL field codes, ODBC SQL types, Snowflake field types, DuckDB name rule, SQLite value inference, unknown and absent codes); each cursor description run through HEAD's dialect description and `type_categories` equals 1.6.0's category; the interval / time-with-time-zone / YEAR / BIT scenarios through `SlayerSQLClient.get_column_types` on DuckDB and Postgres; `tests/test_law_removal_tripwire.py`: the dialects' 1.6.0 classification tables are referenced only from `type_categories`, and `type_categories` only from the tagged deprecated probes.
- [x] 1.6 `tests/test_money_exact_numeric.py`: `is_exact_numeric_db_type` and `lossy_db_type` for `money`, `MONEY`, `SMALLMONEY` as raw names, SQLAlchemy types, info-schema strings and cursor descriptions; executed Postgres `sum(amount)` and `amount * 2` exact with no float cast; SQL Server emission. Executed SQL Server case in `tests/integration/test_integration_sqlserver.py` (CI-only).
- [x] 1.7 Codex review of the tests against this plan.

## 2. Implementation

- [x] 2.1 `aggregation_definition`: a built-in name in any case matches only the exact lowercase definition.
- [x] 2.2 Dialect 1.6.0 category declarations (Postgres, MySQL, SQL Server, Snowflake, DuckDB, SQLite as data in each dialect file, base default from the SLayer type); `type_categories(types, dialect)` and every caller (`get_column_types_sync`, `SlayerSQLClient.get_column_types`, `SlayerQueryEngine.get_column_types`, `DelegatingSqlClient.get_column_types`); extend a dialect's cursor description with the type name where parity needs it; no other reader of the 1.6.0 tables.
- [x] 2.3 MONEY / SMALLMONEY: out of `_FLOAT_LIKE_TYPE_NAMES`, recognised by `is_exact_numeric_db_type`.
- [x] 2.4 Drift: `LiveTable` keeps every column's type name; `sql`-mode live columns keep `ResultColumnType`; `RetypeFinding` replaces `ListRetypeFinding` in `REPORT_ONLY_FINDINGS`, attribution, `findings_out`, CLI (`RE-TYPE:`), MCP and REST; `_diff_sql_table_columns` / `_diff_sql_model_columns` and the SQLite storage probe emit a finding instead of a delete for a type conflict, no cascade.
- [x] 2.5 Dependant check in idempotent ingestion: collect re-typed columns before saving; dependants from the cascade reference walk; a read-only model-overlay storage view; a non-owning client wrapper; plan before/after via a short-lived engine; `IdempotentIngestResult.broken_dependents`; structured warning in `slayer/core/warnings.py`; CLI and MCP rendering.

## 3. Docs

- [x] 3.1 `docs/concepts/schema-drift.md`: re-type finding for type mismatches (lists and scalars), drop the two-pass invariant, update the `sql` bullet and output shape; one sentence each.
- [x] 3.2 Re-ingest's `broken_dependents` in the ingestion docs; the built-in-name rule wherever `docs/concepts/` states aggregation case resolution; MONEY in any exact-numeric list in docs.
- [x] 3.3 Release notes (`RELEASE_NOTES_1.7.0.md`, untracked) for items 1–5.
