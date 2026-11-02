"""The SQL client contract: typed failures, SLayer-typed metadata, delegation (spec: sql/client-contract)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict
from sqlalchemy.exc import OperationalError, ProgrammingError

from slayer.core.enums import DataType
from slayer.core.errors import ForcedFilterError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import DatasourceConfig
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.engine.ingestion import ingest_datasource_report
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import WholeModelDelete
from slayer.sql import engine_factory
from slayer.sql.client import (
    AuthenticationRefused,
    DatabaseUnavailable,
    DelegatingSqlClient,
    ObjectListing,
    ObjectNotFound,
    SlayerSQLClient,
    SqlClientError,
)
from slayer.sql.dialects.reflection import TableLocation
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import (
    DS_NAME,
    GatewayFactory,
    RecordingClient,
    backend_of,
    customers_model,
    orders_model,
    paid_sql_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}]}


class Shop(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    db: str
    datasource: DatasourceConfig
    storage: YAMLStorage


@pytest.fixture
async def shop(tmp_path: Path) -> Shop:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=ds, models=[orders_model(), customers_model(), paid_sql_model()],
    )
    return Shop(db=db, datasource=ds, storage=storage)


def _chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


class _Unavailable(DelegatingSqlClient):
    """Every database read reports the database unavailable."""

    async def _down(self, *_args: Any, **_kwargs: Any) -> Any:
        raise DatabaseUnavailable("database is down")

    execute = get_column_types = server_profile = _down
    ping = list_schemas = default_schema = current_catalog = list_objects = _down
    reflect_columns = reflect_table_keys = table_comment = schema_description = _down


class _HidesTable(DelegatingSqlClient):
    """Listings omit ``hidden``; everything else is the wrapped client's."""

    hidden = "customers"

    async def list_objects(self, *args: Any, **kwargs: Any) -> ObjectListing:
        listing = await self.inner.list_objects(*args, **kwargs)
        return listing.model_copy(update={"tables": [t for t in listing.tables if t != self.hidden]})


def _unreadable_customers(error: Exception) -> type[DelegatingSqlClient]:
    class _Unreadable(DelegatingSqlClient):
        async def reflect_columns(self, location: TableLocation, *args: Any, **kwargs: Any) -> Any:
            if location.name == "customers":
                raise error
            return await self.inner.reflect_columns(location, *args, **kwargs)

    return _Unreadable


def _wrapping(cls: type[DelegatingSqlClient]) -> Callable[[DatasourceConfig], DelegatingSqlClient]:
    return lambda ds: cls(inner=SlayerSQLClient(datasource=ds))


class TestTypedFailures:
    async def test_an_unreachable_client_refuses_validation(self, shop: Shop) -> None:
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=_wrapping(_Unavailable))
        with pytest.raises(Exception) as excinfo:  # NOSONAR(S5958) — the cause chain is the contract
            await engine.validate_models(DS_NAME)
        assert any(isinstance(e, DatabaseUnavailable) for e in _chain(excinfo.value))

    async def test_a_table_missing_from_the_listing_is_drift(self, shop: Shop) -> None:
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=_wrapping(_HidesTable))
        entries = await engine.validate_models(DS_NAME)
        whole = [e for e in entries if isinstance(e, WholeModelDelete)]
        assert [e.model_name for e in whole] == ["customers"]

    @pytest.mark.parametrize("error", [
        ObjectNotFound("customers vanished"),
        ProgrammingError("SELECT", {}, Exception("permission denied for table customers")),
        NotImplementedError("reflection unsupported"),
    ], ids=["not_found", "permission", "unsupported"])
    async def test_a_listed_but_unreadable_table_yields_no_verdict(self, shop: Shop, error: Exception) -> None:
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=_wrapping(_unreadable_customers(error)))
        entries = await engine.validate_models(DS_NAME)
        assert [e for e in entries if e.model_name == "customers"] == []

    async def test_an_unconfirmable_policy_column_refuses_the_query(self, shop: Shop) -> None:
        class _ColumnsDown(DelegatingSqlClient):
            async def reflect_columns(self, *_args: Any, **_kwargs: Any) -> Any:
                raise DatabaseUnavailable("database is down")

        engine = SlayerQueryEngine(
            storage=shop.storage, client_factory=_wrapping(_ColumnsDown),
            policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="acme")),
        )
        with pytest.raises(ForcedFilterError):
            await engine.execute(COUNT_ORDERS)

    async def test_a_policy_column_is_checked_through_the_engine_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(
            storage=shop.storage, client_factory=factory,
            policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="acme")),
        )
        response = await engine.execute(COUNT_ORDERS)
        assert response.data[0]["orders.n"] == 3
        assert "reflect_columns" in factory.clients[0].ops()

    async def test_a_database_error_reaches_the_caller_unchanged(self, shop: Shop) -> None:
        client = SlayerSQLClient(datasource=shop.datasource)
        try:
            with pytest.raises(Exception) as excinfo:  # NOSONAR(S5958) — the driver's own error type
                await client.execute("SELEC 1")
        finally:
            client.close()
        assert not isinstance(excinfo.value, SqlClientError)
        assert 'near "SELEC": syntax error' in str(excinfo.value)

    async def test_rejected_credentials_are_reported_and_connections_discarded(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        rejected = OperationalError("SELECT 1", {}, Exception('password authentication failed for user "u"'))
        fake = MagicMock(spec=sa.Engine)
        fake.connect.side_effect = rejected
        invalidated: list[str] = []
        monkeypatch.setattr(engine_factory, "get_engine", lambda _ds: fake)
        monkeypatch.setattr(engine_factory, "invalidate_engine", lambda ds: invalidated.append(ds.name) or True)
        client = SlayerSQLClient(datasource=shop_datasource(str(tmp_path / "x.db")))
        with pytest.raises(AuthenticationRefused):
            await client.execute("SELECT 1")
        assert invalidated == [DS_NAME]

    async def test_an_unreachable_database_is_reported_unavailable(self, tmp_path: Path) -> None:
        client = SlayerSQLClient(datasource=shop_datasource(str(tmp_path / "missing" / "dir" / "x.db")))
        try:
            with pytest.raises(DatabaseUnavailable):
                await client.ping()
            with pytest.raises(DatabaseUnavailable):
                await client.list_schemas()
        finally:
            client.close()

    async def test_a_missing_table_or_schema_is_not_found(self, shop: Shop) -> None:
        client = SlayerSQLClient(datasource=shop.datasource)
        try:
            with pytest.raises(ObjectNotFound):
                await client.reflect_columns(TableLocation(name="no_such_table"))
            with pytest.raises(ObjectNotFound):
                await client.reflect_table_keys(TableLocation(name="no_such_table"))
            with pytest.raises(ObjectNotFound):
                await client.list_objects("no_such_schema")
        finally:
            client.close()

    def test_typed_errors_share_one_base(self) -> None:
        for cls in (DatabaseUnavailable, AuthenticationRefused, ObjectNotFound):
            assert issubclass(cls, SqlClientError)


# What ingestion produced before this change (recorded): (type, format, db_type) per column.
_INT = NumberFormat(type=NumberFormatType.INTEGER)
_FLOAT = NumberFormat(type=NumberFormatType.FLOAT)
_DUCKDB_KINDS = {
    "id": (DataType.INT, _INT, None),
    "qty": (DataType.INT, _INT, None),
    "price": (DataType.DOUBLE, _FLOAT, "DECIMAL(12,2)"),
    "ratio": (DataType.DOUBLE, _FLOAT, None),
    "label": (DataType.TEXT, None, None),
    "created_at": (DataType.TIMESTAMP, None, None),
    "flag": (DataType.BOOLEAN, None, None),
    "uid": (DataType.TEXT, None, None),
    "doc": (DataType.TEXT, None, None),
}
_SQLITE_KINDS = {
    "id": (DataType.INT, _INT, None),
    "qty": (DataType.INT, _INT, None),
    "price": (DataType.DOUBLE, _FLOAT, "NUMERIC(12, 2)"),
    "ratio": (DataType.DOUBLE, _FLOAT, None),
    "label": (DataType.TEXT, None, None),
    "created_at": (DataType.TIMESTAMP, None, None),
    "flag": (DataType.BOOLEAN, None, None),
    "uid": (DataType.DOUBLE, _FLOAT, "NUMERIC"),
    "payload": (DataType.TEXT, None, None),
}


def _kinds_duckdb(path: str) -> None:
    con = duckdb.connect(path)
    try:
        con.execute(
            "CREATE TABLE kinds (id INTEGER PRIMARY KEY, qty BIGINT, price DECIMAL(12,2), ratio DOUBLE, "
            "label VARCHAR, created_at TIMESTAMP, flag BOOLEAN, uid UUID, doc JSON)"
        )
        con.execute(
            "INSERT INTO kinds VALUES (1, 2, 3.50, 0.5, 'a', '2024-01-01 00:00:00', true, gen_random_uuid(), '{}')"
        )
    finally:
        con.close()


def _kinds_sqlite(path: str) -> None:
    with transaction(path) as cur:
        cur.execute(
            "CREATE TABLE kinds (id INTEGER PRIMARY KEY, qty BIGINT, price NUMERIC(12,2), ratio REAL, "
            "label TEXT, created_at TIMESTAMP, flag BOOLEAN, uid UUID, payload BLOB)"
        )
        cur.execute("INSERT INTO kinds VALUES (1, 2, 3.5, 0.5, 'a', '2024-01-01 00:00:00', 1, 'u', x'00')")


class TestReflectedMetadataUsesSlayerTypes:
    @pytest.mark.parametrize(("dialect", "seed", "expected"), [
        ("duckdb", _kinds_duckdb, _DUCKDB_KINDS),
        ("sqlite", _kinds_sqlite, _SQLITE_KINDS),
    ], ids=["duckdb", "sqlite"])
    async def test_default_reflection_keeps_todays_types(
        self, tmp_path: Path, dialect: str, seed: Callable[[str], None], expected: dict,
    ) -> None:
        path = str(tmp_path / f"kinds.{dialect}")
        seed(path)
        ds = DatasourceConfig(name="kinds", type=dialect, database=path)
        engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
        try:
            client = engine._client_for(ds)
            report = await ingest_datasource_report(datasource=ds, client=client)
            reflected = await client.reflect_columns(TableLocation(name="kinds"))
        finally:
            await engine.aclose()
            engine.close()
            engine_factory.invalidate_engine(ds)
        (model,) = report.models
        assert {c.name: (c.type, c.format, c.db_type) for c in model.columns} == expected
        assert all(isinstance(c.type, DataType) for c in reflected)
        assert {c.name: c.type for c in reflected} == {name: t for name, (t, _f, _d) in expected.items()}


class TestDelegatingClient:
    async def test_a_statements_only_gateway(self, shop: Shop) -> None:
        backend = backend_of(shop.datasource)
        inners: list[RecordingClient] = []
        overridden: list[str] = []

        class StatementsGateway(DelegatingSqlClient):
            async def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                overridden.append(sql)
                return await self.inner.execute(sql, *args, **kwargs)

            async def get_column_types(self, sql: str) -> dict[str, str]:
                overridden.append(sql)
                return await self.inner.get_column_types(sql)

        def factory(ds: DatasourceConfig) -> StatementsGateway:
            inners.append(RecordingClient(inner=SlayerSQLClient(datasource=backend), datasource=ds))
            return StatementsGateway(inner=inners[-1])

        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        response = await engine.execute(COUNT_ORDERS)
        await engine.validate_models(DS_NAME)
        engine.close()
        engine_factory.invalidate_engine(backend)
        assert response.sql in overridden
        assert {"list_objects", "reflect_columns"} <= set(inners[0].ops())
        assert set(inners[0].statements()) <= set(overridden)

    async def test_the_wrapped_clients_own_statements_never_reach_the_override(self, tmp_path: Path) -> None:
        db = str(tmp_path / "comments.duckdb")
        con = duckdb.connect(db)
        try:
            con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v DOUBLE)")
            con.execute("COMMENT ON COLUMN t.v IS 'the value'")
        finally:
            con.close()
        ds = DatasourceConfig(name="comments", type="duckdb", database=db)
        overridden: list[str] = []

        class StatementsGateway(DelegatingSqlClient):
            async def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
                overridden.append(sql)
                return await self.inner.execute(sql, *args, **kwargs)

        gateway = StatementsGateway(inner=SlayerSQLClient(datasource=ds))
        try:
            columns = await gateway.reflect_columns(TableLocation(name="t"))
        finally:
            gateway.close()
            engine_factory.invalidate_engine(ds)
        assert [(c.name, c.comment) for c in columns] == [("id", None), ("v", "the value")]
        assert overridden == []

    async def test_closing_a_delegating_client_closes_the_wrapped_client(self, shop: Shop) -> None:
        inner = RecordingClient(inner=SlayerSQLClient(datasource=shop.datasource), datasource=shop.datasource)
        client = DelegatingSqlClient(inner=inner)
        await client.aclose()
        client.close()
        assert (inner.acloses, inner.closes) == (1, 1)

    async def test_every_operation_is_forwarded(self, shop: Shop) -> None:
        inner = RecordingClient(inner=SlayerSQLClient(datasource=shop.datasource), datasource=shop.datasource)
        client = DelegatingSqlClient(inner=inner)
        location = TableLocation(name="orders")
        try:
            await client.execute("SELECT 1 AS one")
            await client.get_column_types("SELECT 1 AS one")
            await client.server_profile()
            await client.ping()
            await client.list_schemas()
            await client.default_schema()
            await client.current_catalog()
            await client.list_objects(None)
            await client.reflect_columns(location)
            await client.reflect_table_keys(location)
            await client.table_comment(location)
            await client.schema_description(schema=None, configured_schema=None)
        finally:
            client.close()
        assert inner.ops() == [
            "execute", "get_column_types", "server_profile", "ping", "list_schemas", "default_schema",
            "current_catalog", "list_objects", "reflect_columns", "reflect_table_keys", "table_comment",
            "schema_description",
        ]
        assert client.datasource is shop.datasource
