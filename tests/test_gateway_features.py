"""A gateway with no direct database access serves every feature, and no live access blocks the loop.

Spec: database/execution. Each parity test runs a feature once on a default engine and once, with
every direct path refused, on an engine whose factory returns a gateway; the results must match.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy import event
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject, DbtRegularModel
from slayer.live_schema.ingestion import ingest_datasource_idempotent
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import (
    DS_NAME,
    GatewayFactory,
    backend_of,
    customers_model,
    no_direct_access,
    orders_model,
    paid_sql_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)
from tests._stored_upgrade_fixtures import raw_store

REVENUE_BY_STATUS = {
    "source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "sum(amount)", "name": "rev"}],
}
ORDERS_PROJECT = DbtProject(
    semantic_models=[], metrics=[],
    regular_models=[DbtRegularModel(name="orders", schema_name="main", alias="orders", description="Orders")],
)
ORDERS_OSI = OSIDocument.model_validate({
    "version": "0.2.0.dev0", "name": "shop",
    "datasets": [{
        "name": "orders", "source": "orders",
        "fields": [{"name": "status", "expression": {"sql": "status"}},
                   {"name": "big", "expression": {"sql": "amount * 2"}}],
    }],
})

Run = Callable[[SlayerQueryEngine, GatewayFactory | None], Awaitable[Any]]


class Twin(BaseModel):
    """One feature's two runs: ``default`` (direct access) and ``gateway`` (guarded, factory)."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    default: Any
    gateway: Any
    attempts: list[str]


@pytest.fixture
def shop_db(tmp_path: Path) -> str:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    return db


async def _twin(
    *, datasource: DatasourceConfig, build: Callable[[str], Awaitable[StorageBackend]], base: Path,
    run: Run, policy: SessionPolicy | None = None,
) -> Twin:
    default_engine = SlayerQueryEngine(storage=await build(str(base / "default")), policy=policy)
    try:
        default = await run(default_engine, None)
    finally:
        await default_engine.aclose()
        default_engine.close()
        engine_factory.invalidate_engine(datasource)
    factory = GatewayFactory()
    gateway_engine = SlayerQueryEngine(storage=await build(str(base / "gateway")), policy=policy, client_factory=factory)
    try:
        with no_direct_access(backends=[backend_of(datasource)]) as guard:
            gateway = await run(gateway_engine, factory)
    finally:
        await gateway_engine.aclose()
        gateway_engine.close()
    return Twin(default=default, gateway=gateway, attempts=guard.attempts)


def _dumps(models: list[SlayerModel]) -> list[dict]:
    return [m.model_dump(mode="json") for m in sorted(models, key=lambda m: m.name)]


async def _call(server: Any, name: str, arguments: dict[str, Any]) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


class TestGatewayServesEveryFeature:
    async def test_ingestion(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)

        async def build(base: str) -> StorageBackend:
            return await shop_storage(base, datasource=ds)

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> list[dict]:
            result = await ingest_datasource_idempotent(datasource=ds, storage=engine.storage, client=engine.client_for(ds))
            assert result.errors == []
            models, _ = await engine.storage.load_models(data_source=DS_NAME)
            return _dumps(models)

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        assert twin.default
        assert twin.gateway == twin.default

    async def test_model_validation(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)
        drifted = orders_model()
        drifted.columns.append(Column(name="ghost", sql="ghost", type=DataType.TEXT))

        async def build(base: str) -> StorageBackend:
            return await shop_storage(base, datasource=ds, models=[drifted, customers_model(), paid_sql_model()])

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> list[dict]:
            return [e.model_dump(mode="json") for e in await engine.validate_models(DS_NAME)]

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        assert twin.default
        assert twin.gateway == twin.default

    async def test_query_under_a_column_scoped_policy(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)

        async def build(base: str) -> StorageBackend:
            return await shop_storage(base, datasource=ds, models=[orders_model(), customers_model()])

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> list[dict]:
            rows = (await engine.execute(REVENUE_BY_STATUS)).data
            return sorted(rows, key=lambda r: r["orders.status"])

        twin = await _twin(
            datasource=ds, build=build, base=tmp_path, run=run,
            policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="acme")),
        )
        assert twin.attempts == []
        assert twin.default == [
            {"orders.status": "new", "orders.rev": 20.0}, {"orders.status": "paid", "orders.rev": 40.5},
        ]
        assert twin.gateway == twin.default

    async def test_load_of_a_model_stored_below_version_8(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)
        v7_orders = {
            "version": 7, "name": "orders", "sql_table": "orders", "data_source": DS_NAME,
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "quantity", "type": "DOUBLE"},
                {"name": "amount", "type": "DOUBLE"},
            ],
        }

        async def build(base: str) -> StorageBackend:
            return await raw_store(backend="yaml", base=base, datasource=ds, models=[v7_orders])

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> dict:
            model = await engine.storage.get_model("orders", data_source=DS_NAME)
            assert model is not None
            return model.model_dump(mode="json")

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        types = {c["name"]: c["type"] for c in twin.default["columns"]}
        assert (types["quantity"], types["amount"]) == ("INT", "DOUBLE")
        assert twin.gateway == twin.default

    async def test_mcp_create_and_describe_datasource(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)

        async def build(base: str) -> StorageBackend:
            return YAMLStorage(base_dir=base)

        async def run(engine: SlayerQueryEngine, factory: GatewayFactory | None) -> tuple[str, str, list[dict]]:
            server = create_mcp_server(storage=engine.storage, client_factory=factory, _seed_help=False)
            created = await _call(server, "create_datasource", {"name": DS_NAME, "type": "sqlite", "database": shop_db})
            described = await _call(server, "describe_datasource", {"name": DS_NAME})
            models, _ = await engine.storage.load_models(data_source=DS_NAME)
            return created, described, _dumps(models)

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        created, described, models = twin.default
        assert "Ingested" in created
        assert "Connection: OK" in described
        assert models
        assert twin.gateway == twin.default

    async def test_dbt_import_introspecting_a_table(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)

        async def build(base: str) -> StorageBackend:
            return await shop_storage(base, datasource=ds)

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> list[dict]:
            converter = DbtToSlayerConverter(project=ORDERS_PROJECT, data_source=DS_NAME, include_hidden_models=True)
            await converter.collect_metadata(client=engine.client_for(ds))
            return _dumps(converter.convert().models)

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        assert [m["name"] for m in twin.default] == ["orders"]
        assert twin.gateway == twin.default

    async def test_osi_import_introspecting_a_table(self, tmp_path: Path, shop_db: str) -> None:
        ds = shop_datasource(shop_db)

        async def build(base: str) -> StorageBackend:
            return await shop_storage(base, datasource=ds)

        async def run(engine: SlayerQueryEngine, _factory: GatewayFactory | None) -> list[dict]:
            converter = OsiToSlayerConverter(documents=[ORDERS_OSI], data_source=DS_NAME, target_dialect="sqlite")
            await converter.collect_metadata(client=engine.client_for(ds))
            return _dumps(converter.convert().models)

        twin = await _twin(datasource=ds, build=build, base=tmp_path, run=run)
        assert twin.attempts == []
        assert [m["name"] for m in twin.default] == ["orders"]
        assert twin.gateway == twin.default


def test_importer_metadata_collects_through_the_bridge_inside_a_running_loop(tmp_path: Path, shop_db: str) -> None:
    ds = shop_datasource(shop_db)
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    converter = DbtToSlayerConverter(project=ORDERS_PROJECT, data_source=DS_NAME, include_hidden_models=True)

    async def _from_a_loop() -> None:
        engine.run_sync(converter.collect_metadata(client=engine.client_for(ds)))

    try:
        asyncio.run(_from_a_loop())
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)
    assert [m.name for m in converter.convert().models] == ["orders"]


# --------------------------------------------------------------------------- #
# Live database access never blocks the event loop
# --------------------------------------------------------------------------- #
_SLOW_STATEMENT_S = 0.15


def _slow_driver(datasource: DatasourceConfig) -> tuple[list[int], list[int]]:
    """Every statement on the datasource's engine blocks its thread (a slow driver).

    Returns the ids of the threads that checked out a connection, and of those that ran a statement.
    """
    checkouts: list[int] = []
    statements: list[int] = []
    engine = engine_factory.get_engine(datasource)

    def _checkout(*_args: Any, **_kwargs: Any) -> None:
        checkouts.append(threading.get_ident())

    def _sleep(*_args: Any, **_kwargs: Any) -> None:
        statements.append(threading.get_ident())
        time.sleep(_SLOW_STATEMENT_S)

    event.listen(engine, "checkout", _checkout)
    event.listen(engine, "before_cursor_execute", _sleep)
    return checkouts, statements


def _seed_items(db: str, *, dialect: str) -> None:
    ddl = "CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)"
    if dialect == "duckdb":
        con = duckdb.connect(db)
        try:
            con.execute(ddl)
            con.execute("INSERT INTO items VALUES (1, 10), (2, 20)")
        finally:
            con.close()
        return
    with transaction(db) as cur:
        cur.execute(ddl)
        cur.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])


class TestTheLoopStaysResponsive:
    @pytest.mark.parametrize("dialect", ["duckdb", "sqlite"])
    async def test_legacy_refinement(self, tmp_path: Path, dialect: str) -> None:
        db = str(tmp_path / f"live.{dialect}")
        _seed_items(db, dialect=dialect)
        ds = DatasourceConfig(name=DS_NAME, type=dialect, database=db)
        doc = {
            "version": 7, "name": "items", "sql_table": "items", "data_source": DS_NAME,
            "columns": [{"name": "id", "type": "INT", "primary_key": True}, {"name": "amount", "type": "DOUBLE"}],
        }
        engine = SlayerQueryEngine(
            storage=await raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=ds, models=[doc]),
        )
        checkouts, statements = _slow_driver(ds)
        try:
            model = await engine.storage.get_model("items", data_source=DS_NAME)
        finally:
            await engine.aclose()
            engine.close()
            engine_factory.invalidate_engine(ds)
        assert model is not None
        assert {c.name: c.type for c in model.columns}["amount"] is DataType.INT
        assert statements
        assert threading.get_ident() not in checkouts + statements

    @pytest.mark.parametrize("tool", ["create_datasource", "describe_datasource", "ingest_datasource_models"])
    async def test_mcp_datasource_tools(self, tmp_path: Path, shop_db: str, tool: str) -> None:
        db = shop_db
        if tool == "ingest_datasource_models":
            db = str(tmp_path / "empty.db")
            with transaction(db) as cur:
                cur.execute("SELECT 1")
        ds = shop_datasource(db)
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        if tool != "create_datasource":
            await storage.save_datasource(ds)
        server = create_mcp_server(storage=storage, _seed_help=False)
        arguments = {
            "create_datasource": {"name": DS_NAME, "type": "sqlite", "database": db},
            "describe_datasource": {"name": DS_NAME},
            "ingest_datasource_models": {"datasource_name": DS_NAME},
        }[tool]
        checkouts, statements = _slow_driver(ds)
        try:
            await _call(server, tool, arguments)
        finally:
            engine_factory.invalidate_engine(ds)
        assert statements
        assert threading.get_ident() not in checkouts + statements
