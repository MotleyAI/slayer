# SQL Clients

Every live database access SLayer makes — query statements, type probes, schema-drift checks,
ingestion, row-level-security column checks, legacy model upgrades, the MCP datasource tools and
the dbt / OSI importers — goes through one **SQL client** per datasource, owned by a query engine.
By default that client is `SlayerSQLClient`, which connects with the datasource's own credentials.
A host can instead pass a **client factory** and route everything through its own client, for
example a gateway that holds the database credentials and enforces its own governance.

## The contract

A client implements `slayer.sql.client.SqlClient`. Every method is `async` except `close()`.

| Member | Returns | Meaning |
|---|---|---|
| `datasource` | `DatasourceConfig` | The datasource the client serves. |
| `execute(sql)` | `ExecutionResult(rows=[{column: value}, ...])` | Run a rendered statement **verbatim** — `:name` and `%` are literal text. A statement that returns no rows (DDL) yields `rows=[]`. |
| `get_result_column_types(sql)` | `{column: ResultColumnType(type, db_type)}` | Each result column's SLayer `type` (`DataType`) and the database's type name (`db_type`, `None` only when the driver names none), without fetching data beyond what certifying integers needs. |
| `server_profile()` | `ServerProfile` | Server facts SLayer gates features on (ClickHouse); `ServerProfile()` when there are none. |
| `ping()` | `None` | Raise unless the database answers. |
| `list_schemas()` | `list[str]` | Visible schema tokens (catalog-qualified, `catalog.schema`, on DuckDB). |
| `default_schema()` | `str \| None` | The connection's default schema. |
| `current_catalog()` | `str \| None` | The current catalog where schema tokens are catalog-qualified, else `None`. |
| `list_objects(schema)` | `ObjectListing(tables, views, materialized_views)` | One schema's relations by kind; `None` is the connection default. |
| `reflect_columns(location)` | `list[ReflectedColumn]` | A table's columns in order: `name`, SLayer `type` (`DataType`), `is_float`, `db_type` (set only for opaque and exact-numeric types), `comment`, `type_name` (the database's name for the type, where known). |
| `reflect_table_keys(location)` | `TableKeys` | Primary key, foreign keys and unique key sets (never repeating the primary key). |
| `table_comment(location)` | `str \| None` | A table's comment. |
| `schema_description(schema, configured_schema)` | `str \| None` | A schema's own description (a BigQuery dataset's). |
| `aclose()` | `None` | Release resources bound to the current event loop; the client stays usable. |
| `close()` | `None` | Release everything; idempotent. |

A client written for 1.6.0, whose type probe is `get_column_types(sql)` returning `number` / `string` / `time` / `boolean`, keeps working until SLayer 2.0: each probe reads those categories as DOUBLE, TEXT, TIMESTAMP and BOOLEAN and warns, as does `DelegatingSqlClient.get_column_types`.

A `TableLocation(name, schema_name=None, catalog=None)` names a table; an unset part is the
connection's current one.

**Failures.** Raise `DatabaseUnavailable` when the database cannot be reached,
`AuthenticationRefused` when it rejects the credentials, and `ObjectNotFound` when a metadata read
names a schema or table that does not exist (all subclass `SqlClientError`). Any other statement
failure must reach the caller as the database's own error, message unchanged. SLayer treats these
strictly: drift deletes a model only when its table is absent from a *successful* listing, a
listed-but-unreadable table yields no verdict, and a session-policy column it cannot confirm
refuses the query.

**Lifecycle and concurrency.** The engine builds one client per datasource name and connection
identity, calls it concurrently from one event loop, and closes it — code that borrows a client
never does. Synchronous SLayer calls run each whole workflow in one event loop and call `aclose()`
at its end, so a client must keep working after `aclose()`.

## A statements-only gateway

The smallest gateway routes only the statements and keeps the default client for metadata. Extend
`DelegatingSqlClient`, which forwards every operation to the client it wraps:

```python
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import DelegatingSqlClient, ExecutionResult, ResultColumnType, SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage


class GatewayClient(DelegatingSqlClient):
    async def execute(self, sql: str) -> ExecutionResult:
        # Send ``sql`` to your gateway here; this one runs it on the wrapped client.
        return await self.inner.execute(sql)

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        return await self.inner.get_result_column_types(sql)


def client_factory(datasource: DatasourceConfig) -> GatewayClient:
    return GatewayClient(inner=SlayerSQLClient(datasource=datasource))


engine = SlayerQueryEngine(storage=YAMLStorage(base_dir="./slayer_data"), client_factory=client_factory)
```

The wrapped client's own work (its catalog queries) never routes through
your overrides. Metadata still needs the database credentials: without them, queries keep working
but schema-drift checks refuse, session-policy queries are refused, and ingestion, legacy model
upgrades and the MCP `describe_datasource` tool fail.

## A full gateway

To run with no direct database credentials at all, implement every member of the contract against
your gateway (extend nothing, or extend `DelegatingSqlClient` and override everything). SLayer then
opens no connection of its own for that datasource.

## Passing the factory

`client_factory` is accepted by `SlayerQueryEngine`, `SlayerClient` in local mode (remote mode
rejects it), `create_app`, `create_mcp_server`, the Postgres facade (`PgConnection` / `serve`) and
the Flight builder (`run_flight_serve`); each passes it to every engine it builds. The factory is
called with the `DatasourceConfig` of each datasource the engine touches, so it can route per
datasource, and because it is per engine it can carry the calling user's identity. CLI commands
always use the default client.

## Verifying a client

`slayer.sql.conformance` checks a client against the contract without a test framework. Load the
fixture into the database behind your client, then run the suite:

```python
from slayer.sql.conformance import capabilities_for, fixture_statements, run_conformance

for statement in fixture_statements(dialect="postgres"):
    ...  # run it on the database your client reaches

report = await run_conformance(client, capabilities=capabilities_for(dialect="postgres"))
print(report.failed, report.failures)
```

`fixture_statements` and `capabilities_for` cover `sqlite`, `duckdb` and `postgres`; for another
database, adapt the fixture's DDL and pass a `Capabilities(...)` stating which metadata it can
represent — checks for the rest are reported as not applicable. A statements-only gateway passes
the suite only as far as its wrapped client does.
