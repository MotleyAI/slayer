# database — database access

## 1. Purpose & context

`slayer/database` is SLayer's door to live databases: the SQL client contract
and its default client (`client.py`), the engine factory (`engine_factory.py`),
SQLite introspection (`sqlite_introspect.py`) and the executable client
conformance check (`conformance.py`). It executes the SQL `sql` renders and
builds none.

## 2. Building blocks

The `database_focus` view ([views.c4](views.c4)):

<!-- likec4:database_focus -->
<!-- /likec4:database_focus -->

## 3. Principles

1. **One database door**: every live database access — statement or metadata —
   goes through a client owned by a query engine.
   [enforced: test:tests/test_law_database_door.py]
2. **Rendered SQL executes verbatim**: every statement reaches its executing
   client unchanged; `client.py` hands it to the DBAPI through one door
   (`exec_driver_sql`, no parameters); SQLAlchemy `text()` never touches
   rendered SQL — `:name` is never a bind parameter, `%` never a format
   directive. [enforced: test:tests/test_dev1933_verbatim_execution.py]
3. **One engine owner**: every SQLAlchemy engine is disposed by exactly one
   owner — `engine_factory` for the engines it caches (eviction and
   `reset_cache` always dispose; `:memory:` is built by its single StaticPool
   builder, keyed per datasource), `SlayerSQLClient` for its private in-memory
   engine (via `close()`, with a finalizer backstop) and its async engines, one
   per event loop, each released only on its own loop. `create_engine` runs only
   in the factory and the dialects' build hooks; `create_async_engine` only in
   the client's async builder. [enforced: test:tests/test_law_resource_ownership.py]
   [enforced: test:tests/test_sql_client_event_loops.py]
4. **One driver per client**: a client reaches its database through one driver
   only — its caller's engine's, else its URL's (the one named, else the type's
   default); no URL is rewritten to another driver.
   [enforced: test:tests/test_one_driver_per_client.py]

## 4. Rationale

The client is the one place SLayer holds a live connection, so every rule about
statements, engines and drivers binds there and nowhere else (archived changes
`2026-09-19-dev-1933-slayer-satext-misreads-word-in-rendered-sql-as-a-bind`,
`2026-09-21-dev-1943-executed-test-fixtures-leak-undisposed-engines-unclosed`,
`2026-10-20-dev-2116-a-connection-url-that-names-no-driver-always-uses-slayers`,
`2026-11-02-dev-2181-injectable-sql-client-factory-so-the-query-engine-executes`).
