## Context

See proposal.md (Why). Live-database access today: statements go through `SlayerSQLClient` (cached per engine in `_sql_clients`), but metadata is read through `engine_factory.get_engine` + `sa.inspect` at ~30 sites across `engine` (ingestion, schema_drift, schema_scope, introspect_utils, query_engine `_column_present`), `storage` (type_refinement), `sql/dialects` (reflection hooks), importers (dbt/OSI converters), MCP and CLI. `schema_drift._sql_client_for` writes direct clients into the engine's cache (reproduced: after `validate_models`, `_client_for` returns a plain `SlayerSQLClient` despite a factory). `_detect_datasource_joins` closes the client it uses. Several async paths call blocking reflection directly (storage refinement, MCP datasource tools, RLS column check).

Reference intent: external PR MotleyAI/slayer#530 — `SlayerQueryEngine(client_factory=...)` routing statements through a cached `_client_for`.

Applicable principles: system §3.1 layering, §3.7 async-first, §3.12 Pydantic, §3.15 RLS fails closed, new §3.17; sql §3.13 (amended), §3.14 one engine owner; storage §3.2 access is a view. Spec attaches to `sql/execution`, new `sql/client-contract` (sql node, `specs ['sql']`) and `models/stored-upgrade` (cross-cutting `models`). No `model.c4` / `index.yaml` change.

## Goals / Non-Goals

**Goals:** one door for all live database access; per-engine factory; engine-owned client lifecycle; typed client failures; a documented, executable client contract with a minimal (statements-only) and a full gateway path; no event-loop blocking.

**Non-Goals:**
- Relocating system principles / carving database-access and live-schema nodes (DEV-2182, lands after this; it will move §3.17 and sql §3.13–3.14 into the new node).
- Gateway-side work; changes to SQL generation.
- SLayer-internal databases (storage SQLite, facade-local DuckDB, demo data) — not user datasources.

## Decisions

### Shape
1. **`SqlClient` protocol** (`slayer/sql/client.py`, `typing.Protocol`, async-only). Statements: `execute(sql) -> ExecutionResult`, `get_column_types(sql) -> dict[str, str]`, `server_profile() -> ServerProfile`. Metadata: `ping()`, `list_schemas()`, `default_schema()`, `current_catalog()`, `list_objects(schema) -> ObjectListing` (tables / views / materialized views), `reflect_columns(location) -> list[ReflectedColumn]`, `reflect_table_keys(location) -> TableKeys`, `table_comment(location)`, `schema_description(schema, configured_schema)`. Lifecycle: `aclose()` releases loop-bound resources and leaves the client usable; `close()` idempotent. Attribute `datasource`.
2. **Records carry SLayer types only.** `ReflectedColumn.type: DataType` (+ `is_float`, `db_type`, `comment`); SA `TypeEngine` leaves the contract. `_sa_type_to_data_type`, `_raw_db_type_str`, `_sa_type_is_float`, `_sa_type_is_exact_numeric`, `_unwrap_clickhouse_wrappers` move from `engine/ingestion.py` to `slayer/sql/dialects/reflection.py` (sql must not import engine). New Pydantic record `ObjectListing`.
3. **Default client** `SlayerSQLClient` implements everything by absorbing today's reflection: dialect hooks (`reflect_columns`, `reflect_table_keys`, `reflect_default_schema`, `reflect_schema_description`, `_column_comments`, `fetch`), inspector listing (`get_schema_names`, `get_table_names`, `get_view_names`, `get_materialized_view_names`, `get_table_comment`), `schema_scope`'s live reads (DuckDB `current_database()`, `connection_default_schema`). Sync SA reflection runs in a worker thread. Pure scope logic (`resolve_ingest_scope`, `_enumerate_own_schemas` filtering) stays in `engine`, fed by client facts.
4. **SQLite integer-affinity probe** rebuilt on the client's `execute`, keeping the exact algorithm: the same bounded `typeof()` count statement (`LIMIT 100001`) and distinct-text statement (`LIMIT 1001`), saturation → `None`, any failure → `None` (no change), finite-number checks, quoting of attached-schema and quoted identifiers.
5. **`DelegatingSqlClient(inner: SqlClient)`** — pure forwarder of every member incl. lifecycle; no state beyond `inner`, no method calls another. Composition over subclassing `SlayerSQLClient`: the default client's internal self-calls (probe, catalog SQL on `execute`) never route through a gateway's override (fragile base class).
6. **Typed errors** in `slayer/sql/client.py`: `SqlClientError` base; `DatabaseUnavailable`, `AuthenticationRefused`, `ObjectNotFound`. Other statement failures propagate unchanged. The default client maps SA/driver exceptions (existing `_is_auth_failure` / `_is_unreachable_db_error` classifiers) and keeps discarding engines on auth failure. Drift's `IntrospectionUnavailable` is raised from `DatabaseUnavailable`.
7. **Drift verdicts**: deletion only from absence in a successful `list_objects`; a reflection failure on a listed object (incl. `ObjectNotFound` from a race, permission, unsupported reflection) = unreadable, no verdict — preserving the existing schema-drift spec.

### Ownership and construction
8. **One construction point.** `SlayerQueryEngine(client_factory: ClientFactory | None = None)`; `_client_for(ds)` is the only builder. Client cache key = `(datasource.name, _sql_client_cache_key(ds))` for every client (one scheme): distinct logical datasources sharing a URL never share a factory client. The key never resolves credentials (unresolved URL + `credentials_json` digest). `ClientFactory = Callable[[DatasourceConfig], SqlClient]` alias lives in `slayer/engine/query_engine.py`, so protocols/surfaces annotate against `engine` (no new model arrow).
9. **Required client argument** for every database-touching function: `validate_datasource` & drift helpers (replacing the `sql_clients` dict and `_sql_client_for`), `_LiveConnection`/`DriftSnapshot`, ingestion (`ingest_datasource_report`, `ingest_datasource_idempotent`, `list_ingestable_objects`, `introspect_table_to_model`, `_get_schemas`, …), `introspect_utils`, type refinement, MCP `_test_connection` / `_fetch_tables`. No fallback default client anywhere outside an engine.
10. **Borrowers never close**: remove `finally: await client.aclose()` from `_detect_datasource_joins`. `engine.aclose()` / `close()` reach every cached client.
11. **One sync bridge**: `SlayerQueryEngine.run_sync(coro)` runs the coroutine then `await self.aclose()` in the same loop (`try/finally`, cleanup shielded from cancellation). `execute_sync`, every CLI command and every `SlayerClient` sync method run their WHOLE workflow in one `run_sync` (e.g. `validate-models --cardinality` = drift + profiling in one coroutine).
12. **RLS**: `_apply_policy` becomes async: collect candidate (scoped table, column) pairs from the statement AST, resolve presence via `client.reflect_columns` (cached as today; `None` = unconfirmable → fail closed), then run the pure synchronous `apply_session_policy` over a lookup of resolved facts.
13. **Importers**: dbt and OSI converters split into an async metadata-collection step (through an engine client) and pure synchronous conversion; CLI/API callers run collection inside the engine bridge.
14. **Storage (S3)**: storage never builds a client. The per-engine storage view supplies its client source (`Callable[[DatasourceConfig], SqlClient]`) to every load it delegates through a context variable that `_apply_refinement_or_raise` reads; no storage method signature changes, so third-party backends keep working. The engine exposes `engine.storage` as a non-owning per-engine view (pattern of `AccessFilteredStorage`, but its `aclose()` never closes the inner storage) supplying `engine._client_for`. Refinement reads only the model's table (`reflect_columns(location)`; SQLite → the probe). "Datasource entry missing" keeps today's behaviour; "entry present, no client source" raises a typed error naming loading through a query engine. CLI/MCP/REST model loads go through `engine.storage`; `slayer storage migrate-types` runs through an engine.
15. **Entry points**: `SlayerQueryEngine`, `SlayerClient` local mode (remote + factory → `ValueError`, like `policy=`), `create_app`, `create_mcp_server`, pg-facade and Flight engine builders accept `client_factory` and pass it to every engine they build (incl. `api/server.py`'s second engine). CLI always default.

### Contract tooling and docs
16. **Conformance suite** `slayer/sql/conformance.py` (sql node, no pytest dependency): `run_conformance(client, *, capabilities) -> ConformanceReport`; per-dialect fixture setup (DDL + data: tables, a view, PK, FKs, unique constraint, typed columns; comments / materialized views only where the dialect supports them — capability-gated). Two parts: protocol correctness (statement round-trips incl. verbatim `:name`/`%` literals, metadata vs fixture facts, typed errors, lifecycle) and routing (used by our own tests: a full-gateway fake while `engine_factory.get_engine`, `sa.inspect` and raw driver connects are patched to raise). SLayer's unit suite runs protocol conformance on `SlayerSQLClient` over SQLite, DuckDB and Postgres (pytest-postgresql).
17. **Door law test** `tests/test_law_database_door.py`: AST-scans `slayer/` — `engine_factory.get_engine`/builders, `sa.create_engine`/`create_async_engine`, `sa.inspect`, `.connect()`/`exec_driver_sql` on user-datasource engines appear only in the trusted boundary (`sql/client.py`, `sql/engine_factory.py`, `sql/dialects/`, `sql/sqlite_introspect.py`); exempt storage's own SQLite, facade-local DuckDB, demo data generation. Reachability is proven by the routing scenarios (decision 16).
18. **Implementer guide** `docs/configuration/sql-clients.md` (+ `zensical.toml` nav beside `storage.md`): (1) contract — methods, records, type mapping, locations, typed errors, lifecycle/concurrency, server-profile fail-closed rule; (2) minimal gateway (original PR scope): `DelegatingSqlClient` over a default client overriding `execute` + `get_column_types`, metadata via the direct inner client, and what fails without direct credentials (drift refuses, policy queries refused, ingestion / legacy refinement / MCP describe fail; queries still work); (3) full gateway without direct credentials; (4) passing `client_factory` per entry point (routes per datasource, carries caller identity per engine); (5) verifying with the fixture + `run_conformance`, minimal gateways being partial coverage. One sentence each in `docs/reference/python-client.md`, `docs/interfaces/python-client.md`, `docs/getting-started/python.md`.

### Architecture edits (approved wording, apply verbatim)
- `architecture/python.sql.arc42.md` §3.13 → "13. **Rendered SQL executes verbatim**: every statement reaches its executing client unchanged; `client.py` hands it to the DBAPI through one door (`exec_driver_sql`, no parameters); SQLAlchemy `text()` never touches rendered SQL — `:name` is never a bind parameter, `%` never a format directive. [enforced: test:tests/test_dev1933_verbatim_execution.py]" (user, approved wording)
- `architecture/system.arc42.md` §3 new item → "17. **One database door**: every live database access — statement or metadata — goes through a client owned by a query engine. [enforced: test:tests/test_law_database_door.py]" (user, approved wording)

### Decisions log
1. Single connection abstraction covering statements AND introspection (not statements-only). (user)
2. Seam level L1: SLayer-level protocol with typed metadata methods, not a SQLAlchemy/DB-API hook — an explicit, readable gateway contract. (user)
3. Holder H-engine: factory per engine, not process-wide — governed gateways need per-caller identity. (user)
4. Entry points: engine + `SlayerClient` local mode (user, Q2 b); extended to REST/MCP/pg-facade/Flight builders because H-engine requires their database-touching tools to use the engine's factory. (default)
5. Storage S3: legacy refinement through the loading engine's client via a per-engine storage view; no engine → typed error. Chosen over a storage-level factory (two config points, silent bypass) and default-client-only (gateway never reaches legacy upgrade). (user)
6. Fold DEV-2117 in, adopting its scenarios; DEV-2117 closed on merge with the user's yes then. (user)
7. Architecture relocations and node carve-outs deferred to DEV-2182; this change keeps existing structure. (user)
8. sql §3.13 wording as above. (user, approved wording)
9. system §3.17 wording as above. (user, approved wording)
10. Seam is a `typing.Protocol`, not the concrete class. (default)
11. Names `SqlClient`, `client_factory`, `ClientFactory` (contributor's naming; matches approved §3.13 term). (default)
12. Async-only interface; one engine-owned sync bridge `run_sync`. (default, system §3.7)
13. One construction point; required client argument everywhere; drift/ingestion/importer tests updated mechanically to pass an engine-owned client, no logic change. (default, follows from 9)
14. Borrowers never close; the engine owns its cache. (default)
15. Records carry SLayer types only; SA type mapping moves into sql. (default)
16. SQLite affinity probe rebuilt on `execute`. (default)
17. Explicit `ping()`; `server_profile()` with fail-closed empty profile. (default)
18. `ClientFactory` alias in engine → no `model.c4` change. (default)
19. CLI always uses the default client. (default)
20. Typed client errors; other statement errors propagate unchanged. (user)
21. Executable conformance suite in `slayer/sql/conformance.py`, no pytest dependency, run against the default client. (user)
22. Implementer guide covering a minimal gateway (statements only, metadata delegated to a direct default client) and a full one; Python-client pages one sentence each — agreed exception to one-sentence docs. (user)
23. `DelegatingSqlClient` pure forwarder; composition so internal self-calls never route through gateway overrides. (user)
24. RLS policy preparation async: pre-resolve column presence, then pure sync rewrite. (codex)
25. dbt/OSI converters split into async metadata collection + pure sync conversion. (codex)
26. Client cache key `(datasource name, connection identity)` for every client. The "unresolved credentials" part rejected (codex): the key never resolves credentials. (codex)
27. Drift deletes only on absence from a successful listing; listed-but-unreadable = no verdict. (codex)
28. Each sync operation runs its whole workflow in one `run_sync`; cancellation-safe cleanup at its end. (codex)
29. Per-engine storage view is non-owning. (codex)
30. "Entry missing" keeps today's behaviour; "entry present, no engine" = new typed error; 0.10.2 corpus regression through raw and engine storage. (codex)
31. SQLite probe keeps its exact algorithm. (codex)
32. Conformance: mandatory core + capability-gated checks, per-dialect fixtures; protocol vs routing parts; minimal delegating client documented as partial coverage. (codex)
33. Dialect regression scenarios (Snowflake overrides, BigQuery URL dataset, DuckDB attached catalogs, ClickHouse profile denial, in-memory SQLite per owner). (codex)
34. Door-law boundary = default client + engine_factory + dialects + sqlite_introspect; internal DBs exempt; routing scenarios prove reachability. (codex)
35. Client source reaches storage through a context variable set by the engine's view, not a keyword, to keep the 0.9.x backend-compat promise. (user)

## Risks / Trade-offs

- [Large refactor across ~30 sites] → required client arguments make a missed site a type error; the door law test and routing scenarios catch any residual direct access.
- [Moving SA type mapping could change ingested types] → "Default reflection keeps today's types" scenario plus the existing ingestion suites unchanged.
- [`run_sync` closing clients after each sync workflow costs reconnects] → one bridge per whole workflow, not per call; same cost model as today's `execute_sync`.
- [Third-party clients may not survive `aclose()` + reuse] → contract states it; conformance checks it.
- [Sync converter APIs (dbt/OSI) change shape] → internal-facing; CLI/API callers updated in the same change.
- [Raw-storage callers loading pre-v8 models now fail] → typed error names the fix; all in-repo loaders go through `engine.storage`.

## Migration Plan

No stored-data migration. Hosts using only `SlayerQueryEngine(storage=...)` see no change. Internal callers of drift/ingestion/introspection functions must pass a client (obtain via an engine). Rollback = revert the PR.
