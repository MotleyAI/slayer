"""Executable ``SqlClient`` contract: run it against a client over a database loaded with ``fixture_statements``."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from pydantic import BaseModel, Field

from slayer.core.enums import DataType
from slayer.database.client import ObjectNotFound, SqlClient, SqlClientError
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.reflection import TableLocation

_ORDERS, _CUSTOMERS, _PAID, _TOTALS = "conf_orders", "conf_customers", "conf_paid", "conf_totals"
_MISSING = "conf_no_such_table"

_EXPECTED_ORDER_COLUMNS = [
    ("id", DataType.INT), ("customer_id", DataType.INT), ("amount", DataType.DOUBLE),
    ("placed_at", DataType.TIMESTAMP), ("note", DataType.TEXT),
]


class Capabilities(BaseModel):
    """Metadata the fixture's database can represent; checks of the rest are not applicable."""

    table_comments: bool = False
    column_comments: bool = False
    materialized_views: bool = False
    catalogs: bool = False


class ConformanceReport(BaseModel):
    passed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    not_applicable: list[str] = Field(default_factory=list)
    #: Why each failed check failed, by name.
    failures: dict[str, str] = Field(default_factory=dict)


# The orders table, its ``amount`` in each backend's float type.
_ORDERS_DDL = {
    "sqlite": "CREATE TABLE conf_orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES conf_customers(id), "
              "amount REAL, placed_at TIMESTAMP, note VARCHAR(100))",
    "duckdb": "CREATE TABLE conf_orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES conf_customers(id), "
              "amount DOUBLE, placed_at TIMESTAMP, note VARCHAR(100))",
    "postgres": "CREATE TABLE conf_orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES conf_customers(id), "
                "amount DOUBLE PRECISION, placed_at TIMESTAMP, note VARCHAR(100))",
}
_CAPABILITIES = {
    "sqlite": Capabilities(),
    "duckdb": Capabilities(column_comments=True, catalogs=True),
    "postgres": Capabilities(table_comments=True, column_comments=True, materialized_views=True),
}


def capabilities_for(*, dialect: str) -> Capabilities:
    """What the fixture represents on one of SLayer's local backends (``sqlite`` / ``duckdb`` / ``postgres``)."""
    return _CAPABILITIES[dialect]


def fixture_statements(*, dialect: str) -> list[str]:
    """DDL and data of the conformance fixture for ``dialect``, in execution order."""
    statements = [
        "CREATE TABLE conf_customers (id INTEGER PRIMARY KEY, email VARCHAR(100), name VARCHAR(100), UNIQUE (email))",
        _ORDERS_DDL[dialect],
        "CREATE VIEW conf_paid AS SELECT id, amount FROM conf_orders",
        "INSERT INTO conf_customers VALUES (1, 'a@x', 'Ann'), (2, 'b@x', 'Bob')",
        "INSERT INTO conf_orders VALUES (1, 1, 10.5, '2024-01-01 00:00:00', 'first'), "
        "(2, 2, 20.0, '2024-01-02 00:00:00', NULL)",
    ]
    caps = capabilities_for(dialect=dialect)
    if caps.column_comments:
        statements.append("COMMENT ON COLUMN conf_orders.amount IS 'Order amount'")
    if caps.table_comments:
        statements.append("COMMENT ON TABLE conf_orders IS 'Orders'")
    if caps.materialized_views:
        statements.append("CREATE MATERIALIZED VIEW conf_totals AS SELECT customer_id, SUM(amount) AS total "
                          "FROM conf_orders GROUP BY customer_id")
    return statements


class _Failed(Exception):
    pass


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise _Failed(message)


async def _expect_not_found(read: Callable[[], Awaitable[object]]) -> None:
    try:
        result = await read()
    except ObjectNotFound:
        return
    raise _Failed(f"expected ObjectNotFound, got {result!r}")


def _checks(client: SqlClient, caps: Capabilities) -> list[tuple[str, bool, Callable[[], Awaitable[None]]]]:
    orders, customers = TableLocation(name=_ORDERS), TableLocation(name=_CUSTOMERS)

    async def execute_rows() -> None:
        rows = (await client.execute("SELECT id FROM conf_customers ORDER BY id")).rows
        _expect([r["id"] for r in rows] == [1, 2], f"rows {rows!r}")

    async def execute_verbatim_literals() -> None:
        rows = (await client.execute("SELECT ':name' AS a, '50%' AS b")).rows
        _expect(rows == [{"a": ":name", "b": "50%"}], f"rows {rows!r}")

    async def execute_error_unchanged() -> None:
        try:
            await client.execute("SELEC 1")
        except SqlClientError as exc:
            raise _Failed(f"a syntax error surfaced as {type(exc).__name__}") from exc
        except Exception:  # noqa: BLE001 — the database's own error is the contract
            return
        raise _Failed("an invalid statement succeeded")

    async def get_result_column_types() -> None:
        types = await client.get_result_column_types("SELECT id, name FROM conf_customers")
        _expect({k: t.type for k, t in types.items()} == {"id": DataType.INT, "name": DataType.TEXT}, f"types {types!r}")
        _expect(
            all(t.db_type is None or (isinstance(t.db_type, str) and t.db_type) for t in types.values()),
            f"type names {types!r}",
        )

    async def server_profile() -> None:
        _expect(isinstance(await client.server_profile(), ServerProfile), "not a ServerProfile")

    async def ping() -> None:
        await client.ping()

    async def default_schema_listed() -> None:
        default = await client.default_schema()
        listed = [s.casefold() for s in await client.list_schemas()]
        _expect(bool(default) and any(s.rpartition(".")[2] == default.casefold() for s in listed),
                f"default schema {default!r} not among {listed!r}")

    async def current_catalog() -> None:
        _expect(isinstance(await client.current_catalog(), str), "no current catalog")

    async def list_objects() -> None:
        for schema in (None, await client.default_schema()):
            listing = await client.list_objects(schema)
            _expect({_ORDERS, _CUSTOMERS} <= set(listing.tables), f"tables {listing.tables!r}")
            _expect(_PAID in listing.views, f"views {listing.views!r}")

    async def list_materialized_views() -> None:
        listing = await client.list_objects(None)
        _expect(_TOTALS in listing.materialized_views, f"materialized views {listing.materialized_views!r}")

    async def reflect_columns() -> None:
        columns = await client.reflect_columns(orders)
        _expect([(c.name, c.type) for c in columns] == _EXPECTED_ORDER_COLUMNS, f"columns {columns!r}")

    async def reflect_column_comments() -> None:
        comments = {c.name: c.comment for c in await client.reflect_columns(orders)}
        _expect(comments.get("amount") == "Order amount", f"comments {comments!r}")

    async def reflect_table_keys() -> None:
        keys = await client.reflect_table_keys(orders)
        _expect(keys.primary_key == ["id"], f"primary key {keys.primary_key!r}")
        fks = [(fk.columns, fk.referred_table, fk.referred_columns) for fk in keys.foreign_keys]
        _expect(fks == [(["customer_id"], _CUSTOMERS, ["id"])], f"foreign keys {fks!r}")
        unique = (await client.reflect_table_keys(customers)).unique_key_sets
        _expect(unique == [["email"]], f"unique keys {unique!r}")

    async def table_comment() -> None:
        comment = await client.table_comment(orders)
        _expect(comment == "Orders", f"comment {comment!r}")

    async def schema_description() -> None:
        await client.schema_description(schema=None, configured_schema=None)

    async def reflect_columns_not_found() -> None:
        await _expect_not_found(lambda: client.reflect_columns(TableLocation(name=_MISSING)))

    async def reflect_table_keys_not_found() -> None:
        await _expect_not_found(lambda: client.reflect_table_keys(TableLocation(name=_MISSING)))

    async def list_objects_not_found() -> None:
        await _expect_not_found(lambda: client.list_objects("conf_no_such_schema"))

    async def aclose_then_execute() -> None:
        await client.aclose()
        await execute_rows()

    async def close_idempotent() -> None:  # NOSONAR(S7503) — every check is awaited by run_conformance
        client.close()
        client.close()

    return [
        ("execute_rows", True, execute_rows),
        ("execute_verbatim_literals", True, execute_verbatim_literals),
        ("execute_error_unchanged", True, execute_error_unchanged),
        ("get_result_column_types", True, get_result_column_types),
        ("server_profile", True, server_profile),
        ("ping", True, ping),
        ("default_schema_listed", True, default_schema_listed),
        ("current_catalog", caps.catalogs, current_catalog),
        ("list_objects", True, list_objects),
        ("list_materialized_views", caps.materialized_views, list_materialized_views),
        ("reflect_columns", True, reflect_columns),
        ("reflect_column_comments", caps.column_comments, reflect_column_comments),
        ("reflect_table_keys", True, reflect_table_keys),
        ("table_comment", caps.table_comments, table_comment),
        ("schema_description", True, schema_description),
        ("reflect_columns_not_found", True, reflect_columns_not_found),
        ("reflect_table_keys_not_found", True, reflect_table_keys_not_found),
        ("list_objects_not_found", True, list_objects_not_found),
        ("aclose_then_execute", True, aclose_then_execute),
        ("close_idempotent", True, close_idempotent),
    ]


async def run_conformance(client: SqlClient, *, capabilities: Capabilities) -> ConformanceReport:
    """Every check against ``client``; checks for metadata ``capabilities`` lacks are reported not applicable."""
    report = ConformanceReport()
    for name, applicable, check in _checks(client, capabilities):
        if not applicable:
            report.not_applicable.append(name)
            continue
        try:
            await check()
        except Exception as exc:  # noqa: BLE001 — every failure is a named finding
            report.failed.append(name)
            report.failures[name] = f"{type(exc).__name__}: {exc}"
        else:
            report.passed.append(name)
    return report
