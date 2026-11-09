"""Delegation forwards ``explain``; a client written before ``explain`` keeps working and warns (spec: sql/client-contract)."""

from __future__ import annotations

import re
import tempfile
import warnings
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.errors import ExplainUnsupportedError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import (
    DelegatingSqlClient,
    ExecutionResult,
    ResultColumnType,
    SlayerSQLClient,
    SqlClient,
    adapt_client,
)
from slayer.sql.dialects.base import ExplainStatements, ServerProfile
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import (
    DS_NAME,
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
DEPRECATION = r"A SQL client without explain\(\).*SLayer 2\.0"
_PLAN = [{"plan": "scan"}]


def _explain_deprecations(caught: list[warnings.WarningMessage]) -> list[str]:
    return [
        str(w.message) for w in caught
        if issubclass(w.category, FutureWarning) and "explain" in str(w.message)
    ]


class Shop(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    datasource: DatasourceConfig
    storage: YAMLStorage


@pytest.fixture
async def shop(tmp_path: Path) -> AsyncIterator[Shop]:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=ds, models=[orders_model(), customers_model(), paid_sql_model()],
    )
    try:
        yield Shop(datasource=ds, storage=storage)
    finally:
        engine_factory.invalidate_engine(ds)
        engine_factory.invalidate_engine(backend_of(ds))


async def _explain_through(shop: Shop, factory: Callable[[DatasourceConfig], Any]) -> tuple[Any, list[str]]:
    """Explain ``COUNT_ORDERS`` through an engine on ``factory``; return the response and the explain deprecations."""
    engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            response = await engine.execute(COUNT_ORDERS, explain=True)
    finally:
        await engine.aclose()
        engine.close()
    return response, _explain_deprecations(caught)


class ScratchClient:
    """Every contract member but ``explain``, written from scratch over a default client."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self._inner = inner
        self.executed: list[str] = []

    def __getattr__(self, name: str) -> Any:
        if name == "explain":
            raise AttributeError(name)
        return getattr(self._inner, name)

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self._inner.execute(sql)


class StatementsOnly(DelegatingSqlClient):
    """A gateway written before ``explain``: it overrides statement execution only."""

    executed: list[str]

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.executed = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self.inner.execute(sql)


class Intermediate(StatementsOnly):
    """Overrides nothing itself."""


class ViaIntermediate(DelegatingSqlClient):
    pass


class StatementsOnlyBelowAPassThrough(ViaIntermediate):
    executed: list[str]

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.executed = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self.inner.execute(sql)


class ExplainingGateway(StatementsOnly):
    """Overrides statement execution and ``explain``."""

    explained: list[ExplainStatements]

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.explained = []

    async def explain(self, statements: ExplainStatements) -> ExecutionResult:
        self.explained.append(statements)
        return await self.inner.explain(statements)


class InnerSpy(DelegatingSqlClient):
    """Records what reaches the wrapped client's ``explain`` and ``execute``."""

    explained: list[ExplainStatements]
    executed: list[str]

    def __init__(self, inner: SqlClient) -> None:
        super().__init__(inner)
        self.explained = []
        self.executed = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return await self.inner.execute(sql)

    async def explain(self, statements: ExplainStatements) -> ExecutionResult:
        self.explained.append(statements)
        return await self.inner.explain(statements)


class ScratchExplainClient(ScratchClient):
    """Written from scratch, with its own ``explain``."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        super().__init__(inner)
        self.explained: list[ExplainStatements] = []

    async def explain(self, statements: ExplainStatements) -> ExecutionResult:
        self.explained.append(statements)
        return await self._inner.explain(statements)


class ProtocolStubClient(SqlClient):
    """Subclasses the protocol, so it inherits ``explain``'s stub."""

    def __init__(self, datasource: DatasourceConfig) -> None:
        self._datasource = datasource
        self.executed: list[str] = []

    @property
    def datasource(self) -> DatasourceConfig:
        return self._datasource

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return ExecutionResult(rows=_PLAN)


class TestDelegationForwardsExplain:
    async def test_a_plain_delegating_client_forwards_to_the_wrapped_explain(self, shop: Shop) -> None:
        spies: list[InnerSpy] = []

        def factory(ds: DatasourceConfig) -> DelegatingSqlClient:
            spies.append(InnerSpy(SlayerSQLClient(datasource=backend_of(ds))))
            return DelegatingSqlClient(spies[-1])

        response, deprecations = await _explain_through(shop, factory)
        assert [s.statement for s in spies[0].explained] == [f"EXPLAIN QUERY PLAN {response.sql}"]
        assert spies[0].executed == []
        assert response.data
        assert deprecations == []

    async def test_delegating_explain_returns_the_wrapped_result(self) -> None:
        class Answer(DelegatingSqlClient):
            async def explain(self, statements: ExplainStatements) -> ExecutionResult:
                return ExecutionResult(rows=[{"echo": statements.statement}])

        client = DelegatingSqlClient(Answer(inner=None))  # pyright: ignore[reportArgumentType]
        result = await client.explain(ExplainStatements(statement="EXPLAIN SELECT 1"))
        assert result.rows == [{"echo": "EXPLAIN SELECT 1"}]

    async def test_a_statements_only_gateway_serves_query_explain_and_probe(self, shop: Shop) -> None:
        backend = backend_of(shop.datasource)
        inners: list[RecordingClient] = []
        overridden: list[str] = []

        class StatementsGateway(DelegatingSqlClient):
            async def execute(self, sql: str) -> ExecutionResult:
                overridden.append(sql)
                return await self.inner.execute(sql)

            async def explain(self, statements: ExplainStatements) -> ExecutionResult:
                overridden.append(statements.statement)
                return await self.inner.explain(statements)

            async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
                overridden.append(sql)
                return await self.inner.get_result_column_types(sql)

        def factory(ds: DatasourceConfig) -> StatementsGateway:
            inners.append(RecordingClient(inner=SlayerSQLClient(datasource=backend), datasource=ds))
            return StatementsGateway(inner=inners[-1])

        engine = SlayerQueryEngine(storage=shop.storage, client_factory=factory)
        try:
            response = await engine.execute(COUNT_ORDERS)
            explained = await engine.execute(COUNT_ORDERS, explain=True)
            await engine.validate_models(DS_NAME)
        finally:
            engine.close()
        assert response.sql in overridden
        assert f"EXPLAIN QUERY PLAN {explained.sql}" in overridden
        assert "explain" in inners[0].ops()
        assert {"list_objects", "reflect_columns"} <= set(inners[0].ops())
        assert set(inners[0].statements()) <= set(overridden)


class TestAClientWrittenBeforeExplain:
    async def test_a_client_built_from_scratch_runs_explain_through_execute(self, shop: Shop) -> None:
        clients: list[ScratchClient] = []

        def factory(ds: DatasourceConfig) -> ScratchClient:
            clients.append(ScratchClient(SlayerSQLClient(datasource=backend_of(ds))))
            return clients[-1]

        response, deprecations = await _explain_through(shop, factory)
        assert clients[0].executed == [f"EXPLAIN QUERY PLAN {response.sql}"]
        assert response.data
        assert any(_matches(m) for m in deprecations), deprecations

    @pytest.mark.parametrize("gateway", [StatementsOnly, Intermediate, StatementsOnlyBelowAPassThrough],
                             ids=["direct", "inherited-override", "below-a-pass-through"])
    async def test_a_statements_only_gateway_runs_explain_through_its_override(
        self, shop: Shop, gateway: type[StatementsOnly] | type[StatementsOnlyBelowAPassThrough],
    ) -> None:
        gateways: list[Any] = []
        spies: list[InnerSpy] = []

        def factory(ds: DatasourceConfig) -> Any:
            spies.append(InnerSpy(SlayerSQLClient(datasource=backend_of(ds))))
            gateways.append(gateway(spies[-1]))
            return gateways[-1]

        response, deprecations = await _explain_through(shop, factory)
        assert gateways[0].executed == [f"EXPLAIN QUERY PLAN {response.sql}"]
        assert spies[0].explained == []
        assert any(_matches(m) for m in deprecations), deprecations

    async def test_an_inherited_protocol_stub_is_no_explain(self) -> None:
        client = ProtocolStubClient(  # pyright: ignore[reportAbstractUsage]
            DatasourceConfig(name="lite", type="sqlite", database=":memory:"),
        )
        adapted = adapt_client(client)
        with pytest.warns(FutureWarning, match=DEPRECATION):
            result = await adapted.explain(ExplainStatements(statement="EXPLAIN QUERY PLAN SELECT 1"))
        assert result.rows == _PLAN
        assert client.executed == ["EXPLAIN QUERY PLAN SELECT 1"]

    async def test_a_session_mode_explain_is_refused_before_any_statement(self) -> None:
        client = ProtocolStubClient(  # pyright: ignore[reportAbstractUsage]
            DatasourceConfig(name="lite", type="sqlite", database=":memory:"),
        )
        adapted = adapt_client(client)
        statements = ExplainStatements(statement="SELECT 1", session_on="SET A ON", session_off="SET A OFF")
        with pytest.raises(ExplainUnsupportedError):
            await adapted.explain(statements)
        assert client.executed == []

    async def test_the_1_6_0_probe_adapter_composes(self, shop: Shop) -> None:
        class Probe160NoExplain(ScratchClient):
            def __getattr__(self, name: str) -> Any:
                if name == "get_result_column_types":
                    raise AttributeError(name)
                return super().__getattr__(name)

            async def get_column_types(self, sql: str) -> dict[str, str]:  # noqa: ARG002 — 1.6.0 signature
                return {"id": "number"}

        client = Probe160NoExplain(SlayerSQLClient(datasource=backend_of(shop.datasource)))
        adapted = adapt_client(client)  # pyright: ignore[reportArgumentType]
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            types = await adapted.get_result_column_types("SELECT id FROM orders")
            result = await adapted.explain(ExplainStatements(statement="EXPLAIN QUERY PLAN SELECT id FROM orders"))
        await adapted.aclose()
        adapted.close()
        assert types == {"id": ResultColumnType(type=DataType.DOUBLE)}
        assert client.executed == ["EXPLAIN QUERY PLAN SELECT id FROM orders"]
        assert result.rows
        assert any(_matches(m) for m in _explain_deprecations(caught))


def _matches(message: str) -> bool:
    return re.search(DEPRECATION, message) is not None


class _LegacyStub:
    """A from-scratch client without ``explain`` that answers no statement."""

    def __init__(self, datasource: DatasourceConfig) -> None:
        self.datasource = datasource
        self.executed: list[str] = []

    async def execute(self, sql: str) -> ExecutionResult:
        self.executed.append(sql)
        return ExecutionResult(rows=_PLAN)

    async def server_profile(self) -> ServerProfile:
        return ServerProfile()

    async def aclose(self) -> None:
        return None

    def close(self) -> None:
        return None


async def test_a_sql_server_explain_on_a_client_without_explain_is_refused() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        ds = DatasourceConfig(name="mssql", type="mssql", database="d")
        await storage.save_datasource(ds)
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_t", data_source="mssql",
            columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        ), _validate=False)
        stubs: list[_LegacyStub] = []

        def factory(datasource: DatasourceConfig) -> _LegacyStub:
            stubs.append(_LegacyStub(datasource))
            return stubs[-1]

        engine = SlayerQueryEngine(storage=storage, client_factory=factory)  # pyright: ignore[reportArgumentType]
        with pytest.raises(ExplainUnsupportedError):
            await engine.execute({"source_model": "orders", "dimensions": ["id"]}, explain=True)
    assert all(stub.executed == [] for stub in stubs)


class TestACurrentClientIsUsedUnchanged:
    @pytest.mark.parametrize("make", [
        lambda ds: SlayerSQLClient(datasource=ds),
        lambda ds: DelegatingSqlClient(SlayerSQLClient(datasource=ds)),
        lambda ds: ExplainingGateway(SlayerSQLClient(datasource=ds)),
        lambda ds: ScratchExplainClient(SlayerSQLClient(datasource=ds)),
    ], ids=["default", "delegating", "explain-override", "from-scratch-with-explain"])
    def test_adapt_client_leaves_it_alone(self, shop: Shop, make: Callable[[DatasourceConfig], Any]) -> None:
        client = make(shop.datasource)
        assert adapt_client(client) is client

    async def test_an_explain_override_serves_without_warning(self, shop: Shop) -> None:
        gateways: list[ExplainingGateway] = []

        def factory(ds: DatasourceConfig) -> ExplainingGateway:
            gateways.append(ExplainingGateway(SlayerSQLClient(datasource=backend_of(ds))))
            return gateways[-1]

        response, deprecations = await _explain_through(shop, factory)
        assert [s.statement for s in gateways[0].explained] == [f"EXPLAIN QUERY PLAN {response.sql}"]
        assert gateways[0].executed == []
        assert deprecations == []

    async def test_a_from_scratch_explain_serves_without_warning(self, shop: Shop) -> None:
        clients: list[ScratchExplainClient] = []

        def factory(ds: DatasourceConfig) -> ScratchExplainClient:
            clients.append(ScratchExplainClient(SlayerSQLClient(datasource=backend_of(ds))))
            return clients[-1]

        response, deprecations = await _explain_through(shop, factory)
        assert [s.statement for s in clients[0].explained] == [f"EXPLAIN QUERY PLAN {response.sql}"]
        assert clients[0].executed == []
        assert deprecations == []

    async def test_the_default_client_explains_without_warning(self, shop: Shop) -> None:
        response, deprecations = await _explain_through(shop, lambda ds: SlayerSQLClient(datasource=backend_of(ds)))
        assert response.data
        assert deprecations == []
