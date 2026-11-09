"""Engine-owned clients built by an injectable factory (spec: sql/execution)."""

from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

import slayer.api.server as api_server
import slayer.mcp.server as mcp_server
from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.client.slayer_client import SlayerClient
from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig, ModelMeasure
from slayer.engine.cache import CacheConfig
from slayer.engine.cardinality import CardinalityVerdict
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.pg_facade.connection import PgConnection
from slayer.sql.client import ExecutionResult, ResultColumnType, SlayerSQLClient
from slayer.sql.dialects.base import ServerProfile
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1840_fixtures import dev1840_models, q
from tests._gateway_harness import (
    DS_NAME,
    GatewayFactory,
    RecordingClient,
    backend_of,
    customers_model,
    no_direct_access,
    orders_model,
    paid_sql_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)
from tests.pg_facade.test_connection import _FakeWriter, _messages, _query, _startup, _terminate

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}]}
REVENUE_BY_STATUS = {
    "source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "amount:sum", "name": "rev"}],
}


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
        str(tmp_path / "store"), datasource=ds,
        models=[orders_model(), customers_model(), paid_sql_model()],
    )
    return Shop(db=db, datasource=ds, storage=storage)


def _n(response: Any) -> int:
    return response.data[0]["orders.n"]


class TestStatementsReachTheFactoryClient:
    async def test_query_and_explain_statements_reach_the_factory_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            response = await engine.execute(COUNT_ORDERS)
            explained = await engine.execute(COUNT_ORDERS, explain=True)
        assert guard.attempts == []
        assert _n(response) == 4
        sent = factory.all_statements()
        assert response.sql in sent
        assert any(sql != response.sql and response.sql in sql for sql in sent), sent
        assert explained.data

    async def test_column_type_probe_reaches_the_factory_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            types = await engine.get_result_column_types("orders", data_source=DS_NAME)
        assert guard.attempts == []
        assert types
        assert "get_result_column_types" in factory.clients[0].ops()

    async def test_cache_refresh_scan_reaches_the_factory_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(
            storage=shop.storage, client_factory=factory,
            cache_config=CacheConfig(refresh_keys=(("orders", "COUNT(*)"),)),
        )
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            await engine.execute(COUNT_ORDERS, cache=True)
            before = len(factory.all_statements())
            await engine.refresh()
        assert guard.attempts == []
        scans = [sql for sql in factory.all_statements()[before:] if "COUNT(*)" in sql.upper()]
        assert scans, factory.all_statements()

    async def test_join_profiling_reaches_the_factory_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            report = await engine.detect_join_cardinality(data_source=DS_NAME)
        assert guard.attempts == []
        assert report.findings
        assert all(f.verdict is not CardinalityVerdict.SCAN_FAILED for f in report.findings), report.findings
        assert any("customers" in sql for sql in factory.all_statements())

    async def test_server_profile_request_reaches_the_factory_client(self, tmp_path: Path) -> None:
        built: list[ProfileClient] = []
        engine = await _clickhouse_engine(
            tmp_path, profile=ServerProfile(version=(25, 8), readonly=1, correlated_subqueries=True), built=built,
        )
        with no_direct_access() as guard:
            await engine.execute(_PUSHED)
        assert guard.attempts == []
        assert "server_profile" in built[0].ops
        assert built[0].executed
        assert "EXISTS" in built[0].executed[0].upper()

    async def test_a_factory_profile_denying_correlated_subqueries_is_honoured(self, tmp_path: Path) -> None:
        built: list[ProfileClient] = []
        engine = await _clickhouse_engine(
            tmp_path, profile=ServerProfile(version=(25, 4), readonly=1, correlated_subqueries=False), built=built,
        )
        with pytest.raises(SlayerError, match="allow_experimental_correlated_subqueries"):
            await engine.execute(_PUSHED)
        assert "server_profile" in built[0].ops
        assert built[0].executed == []


_PUSHED = q(
    dimensions=["customers.tier"],
    measures=[ModelMeasure(formula="amount:sum", name="m"), ModelMeasure(formula="customers.spend:sum", name="cm")],
    filters=["channel = 'app'"],
)


async def _clickhouse_engine(tmp_path: Path, *, profile: ServerProfile, built: list[ProfileClient]) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="clickhouse"))
    for model in dev1840_models():
        await storage.save_model(model, _validate=False)

    def factory(datasource: DatasourceConfig) -> ProfileClient:
        built.append(ProfileClient(datasource=datasource, profile=profile))
        return built[-1]

    return SlayerQueryEngine(storage=storage, client_factory=factory)


class ProfileClient:
    """A protocol-only client for a ClickHouse datasource: a fixed profile, empty results."""

    def __init__(self, *, datasource: DatasourceConfig, profile: ServerProfile) -> None:
        self.datasource = datasource
        self.profile = profile
        self.ops: list[str] = []
        self.executed: list[str] = []

    async def server_profile(self) -> ServerProfile:
        self.ops.append("server_profile")
        return self.profile

    async def execute(self, sql: str, *_args: Any, **_kwargs: Any) -> ExecutionResult:
        self.ops.append("execute")
        self.executed.append(sql)
        return ExecutionResult(rows=[])

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        self.ops.append("get_result_column_types")
        return {}

    async def _no_metadata(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no metadata read expected")

    ping = list_schemas = default_schema = current_catalog = list_objects = _no_metadata
    reflect_columns = reflect_table_keys = table_comment = schema_description = _no_metadata

    async def aclose(self) -> None:
        self.ops.append("aclose")

    def close(self) -> None:
        self.ops.append("close")


class TestOneConstructionPoint:
    async def test_validation_before_the_first_query_cannot_bypass_the_factory(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            await engine.validate_models(DS_NAME)
            response = await engine.execute(COUNT_ORDERS)
        assert guard.attempts == []
        assert isinstance(engine.client_for(shop.datasource), RecordingClient)
        assert response.sql in factory.all_statements()

    async def test_one_client_per_datasource_name_and_connection_identity(self, tmp_path: Path) -> None:
        first_db, twin_db = str(tmp_path / "a.db"), str(tmp_path / "b.db")
        seed_shop(first_db)
        seed_shop(twin_db)
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        for name, db in (("shop", first_db), ("twin1", twin_db), ("twin2", twin_db)):
            await storage.save_datasource(shop_datasource(db, name=name))
            await storage.save_model(orders_model(data_source=name), _validate=False)
            await storage.save_model(customers_model(data_source=name), _validate=False)
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=storage, client_factory=factory)
        for _ in range(3):
            await engine.execute(COUNT_ORDERS, data_source="shop")
        await engine.execute(COUNT_ORDERS, data_source="twin1")
        await engine.execute(COUNT_ORDERS, data_source="twin2")
        assert [ds.name for ds in factory.built] == ["shop", "twin1", "twin2"]

    async def test_a_factory_need_not_extend_the_default_client(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            response = await engine.execute(COUNT_ORDERS)
            entries = await engine.validate_models(DS_NAME)
        assert guard.attempts == []
        assert _n(response) == 4
        assert entries == []
        client = factory.clients[0]
        assert not isinstance(client, SlayerSQLClient)
        assert {"execute", "list_objects", "reflect_columns"} <= set(client.ops())


class TestEngineOwnsItsClients:
    async def test_join_profiling_leaves_the_shared_client_open(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        await engine.detect_join_cardinality(data_source=DS_NAME)
        assert len(factory.clients) == 1
        client = factory.clients[0]
        assert (client.acloses, client.closes) == (0, 0)
        response = await engine.execute(COUNT_ORDERS)
        assert len(factory.clients) == 1
        assert response.sql in client.statements()

    async def test_closing_the_engine_reaches_every_factory_client(self, tmp_path: Path, shop: Shop) -> None:
        twin_db = str(tmp_path / "twin.db")
        shutil.copy(shop.db, twin_db)
        await shop.storage.save_datasource(shop_datasource(twin_db, name="twin"))
        await shop.storage.save_model(orders_model(data_source="twin"), _validate=False)
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        await engine.execute(COUNT_ORDERS, data_source=DS_NAME)
        await engine.execute(COUNT_ORDERS, data_source="twin")
        assert len(factory.clients) == 2
        await engine.aclose()
        assert [c.acloses for c in factory.clients] == [1, 1]
        engine.close()
        assert [c.closes for c in factory.clients] == [1, 1]
        assert _n(await engine.execute(COUNT_ORDERS, data_source=DS_NAME)) == 4

    def test_synchronous_calls_repeat_safely(self, shop: Shop) -> None:
        """A custom client has no end-of-workflow release: sync calls leave it open."""
        factory = GatewayFactory()
        client = SlayerClient(storage=shop.storage, client_factory=factory)

        def acloses() -> int:
            return sum(c.acloses for c in factory.clients)

        for _ in range(3):
            assert client.query_sync(COUNT_ORDERS).data[0]["orders.n"] == 4

        async def _inside_a_loop() -> int:
            return client.query_sync(COUNT_ORDERS).data[0]["orders.n"]

        assert asyncio.run(_inside_a_loop()) == 4
        with pytest.raises(SlayerError):
            client.query_sync({"source_model": "no_such_model", "measures": [{"formula": "*:count"}]})
        assert acloses() == 0

    def test_run_sync_leaves_custom_clients_open_when_cancelled(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)

        async def _cancelled() -> None:
            await engine.execute(COUNT_ORDERS)
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(0)

        workflow = _cancelled()
        with pytest.raises(asyncio.CancelledError):
            engine.run_sync(workflow)
        assert factory.clients[0].acloses == 0

    def test_run_sync_returns_the_workflow_result_and_leaves_custom_clients_open(self, shop: Shop) -> None:
        factory = GatewayFactory()
        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        response = engine.run_sync(engine.execute(COUNT_ORDERS))
        assert _n(response) == 4
        assert factory.clients[0].acloses == 0

    def test_a_failing_client_release_neither_skips_others_nor_masks_the_result(
        self, tmp_path: Path, shop: Shop, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        twin_db = str(tmp_path / "twin.db")
        shutil.copy(shop.db, twin_db)
        run_sync(shop.storage.save_datasource(shop_datasource(twin_db, name="twin")))
        run_sync(shop.storage.save_model(orders_model(data_source="twin"), _validate=False))
        released: list[str] = []

        async def _release(self: SlayerSQLClient) -> None:
            released.append(self.datasource.name)
            if self.datasource.name == DS_NAME:
                raise RuntimeError("release failed")

        monkeypatch.setattr(SlayerSQLClient, "release_local_connections", _release)
        engine = SlayerQueryEngine(storage=shop.storage)

        async def _both() -> int:
            await engine.execute(COUNT_ORDERS, data_source=DS_NAME)
            return _n(await engine.execute(COUNT_ORDERS, data_source="twin"))

        try:
            assert engine.run_sync(_both()) == 4
        finally:
            engine.close()
        assert released.count("twin") == 1

    def test_create_model_from_query_sync_releases_clients(self, shop: Shop, monkeypatch: pytest.MonkeyPatch) -> None:
        engine = SlayerQueryEngine(storage=shop.storage)
        released: list[bool] = []

        async def _release() -> None:
            released.append(True)

        monkeypatch.setattr(engine, "release_local_connections", _release)
        try:
            model = engine.create_model_from_query_sync(REVENUE_BY_STATUS, name="rev_by_status")
        finally:
            engine.close()
        assert model.name == "rev_by_status"
        assert released == [True]

    async def test_the_local_python_client_passes_the_factory_through(self, shop: Shop) -> None:
        factory = GatewayFactory()
        client = SlayerClient(storage=shop.storage, client_factory=factory)
        response = await client.query(COUNT_ORDERS)
        assert response.data[0]["orders.n"] == 4
        assert response.sql in factory.all_statements()

    def test_remote_mode_rejects_a_factory(self) -> None:
        factory = GatewayFactory()
        with pytest.raises(ValueError, match="client_factory"):
            SlayerClient(url="http://localhost:5143", client_factory=factory)

    def test_rest_builder_passes_the_factory_to_every_engine(self, shop: Shop) -> None:
        factory = GatewayFactory()
        app = create_app(storage=shop.storage, client_factory=factory)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            http = TestClient(app)
            queried = http.post("/query", json=COUNT_ORDERS)
            validated = http.post("/validate-models", json={"data_source": DS_NAME})
            ingested = http.post("/ingest", json={"datasource": DS_NAME})
        assert guard.attempts == []
        assert queried.status_code == 200, queried.text
        assert validated.status_code == 200, validated.text
        assert ingested.status_code == 200, ingested.text
        assert {"execute", "list_objects", "reflect_columns"} <= {c.op for c in factory.all_calls()}

    def test_rest_requests_share_the_app_engine(self, shop: Shop, monkeypatch: pytest.MonkeyPatch) -> None:
        built: list[SlayerQueryEngine] = []

        class _Counting(SlayerQueryEngine):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                built.append(self)

        monkeypatch.setattr(api_server, "SlayerQueryEngine", _Counting)
        http = TestClient(create_app(storage=shop.storage, client_factory=GatewayFactory()))
        recommended = http.post("/recommend-root-model", json={"items": ["orders.amount", "customers.id"]})
        assert recommended.status_code == 200, recommended.text
        assert len(built) == 1

    async def test_mcp_builder_passes_the_factory_to_every_engine(self, shop: Shop) -> None:
        factory = GatewayFactory()
        server = create_mcp_server(storage=shop.storage, client_factory=factory, _seed_help=False)
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            queried = await _call(server, name="query", arguments={"query": COUNT_ORDERS})
            described = await _call(server, name="describe_datasource", arguments={"name": DS_NAME})
        assert guard.attempts == []
        assert "4" in queried
        assert "Connection: OK" in described
        assert "orders" in described
        ops = {c.op for c in factory.all_calls()}
        assert {"execute", "ping", "list_objects"} <= ops

    async def test_pg_facade_builder_passes_the_factory_to_every_engine(self, shop: Shop) -> None:
        factory = GatewayFactory()

        async def provider(_principal: object) -> YAMLStorage:
            return shop.storage

        reader = asyncio.StreamReader()
        reader.feed_data(_startup(user="u", database="acme") + _query("SELECT status FROM orders") + _terminate())
        reader.feed_eof()
        writer = _FakeWriter()
        with no_direct_access(backends=[backend_of(shop.datasource)]) as guard:
            await PgConnection(
                reader, cast(asyncio.StreamWriter, writer), engine=None, storage=shop.storage,
                storage_provider=provider, client_factory=factory,
            ).run()
        assert guard.attempts == []
        assert not any(t == "E" for t, _ in _messages(writer.buffer))
        assert "execute" in {c.op for c in factory.all_calls()}


class TestEntryPointsCloseTheirClients:
    def test_rest_app_shutdown_closes_the_factory_clients(self, shop: Shop) -> None:
        factory = GatewayFactory()
        with TestClient(create_app(storage=shop.storage, client_factory=factory)) as http:
            assert http.post("/query", json=COUNT_ORDERS).status_code == 200
        assert factory.clients
        assert all(c.closes == 1 for c in factory.clients)

    def test_rest_shutdown_closes_every_engine_when_one_release_fails(
        self, shop: Shop, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        closed: list[str] = []

        class _FailingRelease(SlayerQueryEngine):
            async def aclose(self) -> None:
                raise RuntimeError("release failed")

            def close(self) -> None:
                closed.append("app")
                super().close()

        class _Recording(SlayerQueryEngine):
            def close(self) -> None:
                closed.append("mcp")
                super().close()

        monkeypatch.setattr(api_server, "SlayerQueryEngine", _FailingRelease)
        monkeypatch.setattr(mcp_server, "SlayerQueryEngine", _Recording)
        app = create_app(storage=shop.storage)
        with pytest.raises(RuntimeError, match="release failed"), TestClient(app):
            pass
        assert sorted(closed) == ["app", "mcp"]

    async def test_pg_facade_static_catalog_reads_the_engine_view_of_the_same_store(
        self, tmp_path: Path, shop: Shop,
    ) -> None:
        engine = SlayerQueryEngine(storage=shop.storage)

        def _conn(storage: Any) -> PgConnection:
            return PgConnection(
                asyncio.StreamReader(), cast(asyncio.StreamWriter, _FakeWriter()), engine=engine, storage=storage,
            )

        other = YAMLStorage(base_dir=str(tmp_path / "other"))
        assert _conn(shop.storage)._read_storage is engine.storage
        assert _conn(engine.storage)._read_storage is engine.storage
        assert _conn(other)._read_storage is other

    async def test_pg_facade_connection_end_closes_its_engine_clients(self, shop: Shop) -> None:
        factory = GatewayFactory()

        async def provider(_principal: object) -> YAMLStorage:
            return shop.storage

        reader = asyncio.StreamReader()
        reader.feed_data(_startup(user="u", database="acme") + _query("SELECT status FROM orders") + _terminate())
        reader.feed_eof()
        await PgConnection(
            reader, cast(asyncio.StreamWriter, _FakeWriter()), engine=None, storage=shop.storage,
            storage_provider=provider, client_factory=factory,
        ).run()
        assert factory.clients
        assert all(c.closes == 1 for c in factory.clients)


async def _call(server: Any, *, name: str, arguments: dict[str, Any]) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text
