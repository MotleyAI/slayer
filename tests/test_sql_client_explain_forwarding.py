"""A client's own ``explain`` serves EXPLAIN, and delegation forwards it to the wrapped client (spec: database/client-contract)."""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.database.client import (
    DelegatingSqlClient,
    ExecutionResult,
    ResultColumnType,
    SlayerSQLClient,
    SqlClient,
)
from slayer.sql.dialects.base import ExplainStatements
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

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}]}


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
    """Overrides statement execution only."""

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


class TestACurrentClientIsUsedUnchanged:
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
