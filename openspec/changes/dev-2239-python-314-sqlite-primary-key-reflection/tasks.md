## 1. Tests (pr-tests stage; each fails before its fix)

- [x] 1.1 SQLite catalog corpus fixture (composite PK, `WITHOUT ROWID`, FK naming no parent columns, composite FK, UNIQUE constraint, unique index, partial unique index, unique index over a column and an expression, keyless table, view, generated column, every affinity-distinguishing declared-type spelling incl. empty, virtual table with hidden columns, attached database with a same-named table with different keys, temp table); verify it builds in the default unit suite
- [x] 1.2 No-failing-statement test: an engine `handle_error` listener records every statement error during the default client's key and column reads over the corpus; assert none (fails today on `sqlite_temp_master`)
- [x] 1.3 Parity test: over the corpus, the default client's keys and column types equal what SQLAlchemy's Inspector yields (taken via `sa.inspect` on a 3.12-safe path), plus the documented differences (partial and mixed expression indexes count as no key); missing table is not-found; virtual table excludes hidden columns
- [x] 1.4 `tests/test_law_one_thread_per_connection.py`: tracking `sqlite3` connection factory + slow-statement listener; `asyncio.gather` of many statements on an in-memory and on a file SQLite client → no overlap and each statement's own rows (in-memory half fails today); a nested same-thread checkout of the in-memory engine fails at once naming the nesting; structural check that every engine SLayer builds per Tier 1 database (sync, and async where the type has one) uses a pool granting each connection to one caller at a time — a type whose engine cannot be built without connecting is a failure, never a skip
- [x] 1.5 Reporting tests: a monkeypatched failing primary-key read appears on the key-read result (`failed_reads`), on the scan report and the idempotent ingest result with the table's location, in MCP `create_datasource` and `ingest_datasource_models` output, CLI ingest output and the REST body; the table is ingested without the key; exit status and HTTP status unchanged; same-named tables in two schemas reported apart; a gateway client listing nothing reports nothing; a re-ingest with the read succeeding fills the key and the join cardinality
- [x] 1.6 Align `tests/test_engine_factory.py:833` and `:840` (assert `StaticPool`) to assert the exclusive one-slot in-memory pool (approved in pr-plan); update StaticPool mentions in docstrings of `tests/test_sql_client_in_memory_async.py`, `tests/test_mcp_engine_teardown.py`, `tests/test_engine_ownership.py`, `tests/test_async_engine_disposal.py`, `tests/test_sqlite_probe.py`, `tests/integration/test_in_memory_sqlite.py` (wording only, no logic change)

## 2. SQLite own-catalog reads

- [ ] 2.1 `slayer/sql/dialects/sqlite.py`: `reflect_table_keys` from `pragma_table_xinfo` / `pragma_foreign_key_list` / `pragma_index_list` / `pragma_index_xinfo` as sqlglot AST via `reflection.fetch`, schema-scoped, each read through `best_effort`; whole index discarded on any expression member; NULL FK `to` resolved to the referred table's PK; drop `primary_key_inspector_only`; verify 1.2, 1.3 key parts pass
- [ ] 2.2 `reflect_columns` from `pragma_table_xinfo` (`hidden = 1` skipped, generated kept), types via the SQLAlchemy dialect's `_resolve_type_affinity` + `column_from_sa_type`; empty → existing not-found; verify 1.2, 1.3 column parts pass

## 3. One thread per connection

- [ ] 3.1 `slayer/database/engine_factory.py`: in-memory builder uses a one-slot `QueuePool` subclass recording its holder thread, failing a same-thread re-checkout at once; disposal, `release_idle` / `holds_local_file`, per-datasource isolation and cross-thread coherence unchanged; update `slayer/database/client.py:888` docstring; verify 1.4 and existing in-memory tests pass
- [ ] 3.2 `architecture/python.database.arc42.md`: add principle 6 and reword principle 3 exactly as quoted in design.md decisions 4–5 (user, approved wording); verify `poetry run la-arch-check` passes

## 4. Reporting lost key reads

- [ ] 4.1 `slayer/sql/dialects/reflection.py`: `KeyReadFailure(read, error)`; `TableKeys.failed_reads` default empty; `best_effort` returns the failure record alongside the default (all dialects, DuckDB included); verify 1.5 key-read-result part
- [ ] 4.2 `slayer/live_schema/ingestion.py` / `schema_drift.py`: scan returns its failures explicitly (location + read + error, deduped), incl. tables later skipped; `key_read_failures` on `IngestionScanReport` and `IdempotentIngestResult`; verify 1.5 result parts
- [ ] 4.3 Surfaces: MCP `create_datasource` + `ingest_datasource_models` list each lost key read with the re-ingest hint; CLI ingest prints them; REST body carries the field; statuses unchanged; verify 1.5 surface parts
- [ ] 4.4 `docs/concepts/ingestion.md`: one sentence — a key that cannot be read is reported in the ingest output and filled by a re-ingest

## 5. Finish

- [ ] 5.1 Full unit suite `poetry run pytest -m "not integration"`, ruff, basedpyright (no new errors vs baseline), `poetry run la-arch-check`, `npx likec4 validate` (or build) all green
- [ ] 5.2 Run `tests/test_gateway_features.py` and the new tests on Python 3.14 (scratch venv) in a loop of ≥25 runs; no `returned NULL` and no failure
