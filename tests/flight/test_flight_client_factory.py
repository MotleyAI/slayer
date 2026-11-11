"""The Flight engine builder passes the client factory to the engine it builds (spec: database/execution)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

import slayer.flight.server as flight_server
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.flight.cli import run_flight_serve
from slayer.flight.handlers import FlightHandlers
from slayer.facade.translator import QueryResult
from tests._gateway_harness import (
    GatewayFactory,
    customers_model,
    orders_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)

COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}]}


class _Args(BaseModel):
    port: int = 0
    host: str | None = "127.0.0.1"
    token: str | None = "t"
    tls_cert: str | None = None
    tls_key: str | None = None
    demo: bool = False


class _StubServer:
    def wait(self) -> None:
        return None

    def shutdown(self) -> None:
        return None


async def test_flight_builder_passes_the_factory_to_its_engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=shop_datasource(db), models=[orders_model(), customers_model()],
    )
    captured: dict[str, Any] = {}

    def _build_server(**kwargs: Any) -> _StubServer:
        captured.update(kwargs)
        return _StubServer()

    monkeypatch.setattr(flight_server, "build_server", _build_server)
    factory = GatewayFactory()
    run_flight_serve(
        _Args(), resolve_storage=lambda _args: storage, prepare_demo=lambda *_a: None,
        serve_until_terminated=lambda **_kw: None, client_factory=factory,
    )
    engine = captured["handlers"]._engine
    response = await engine.execute(COUNT_ORDERS)
    assert response.data[0]["orders.n"] == 4
    assert response.sql in factory.all_statements()


async def test_flight_server_stop_closes_the_factory_clients(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=shop_datasource(db), models=[orders_model(), customers_model()],
    )
    captured: dict[str, Any] = {}

    def _build_server(**kwargs: Any) -> _StubServer:
        captured.update(kwargs)
        return _StubServer()

    def _serve(**_kw: Any) -> None:
        captured["handlers"].do_get_for_sql("SELECT status, amount_sum FROM orders")

    monkeypatch.setattr(flight_server, "build_server", _build_server)
    factory = GatewayFactory()
    run_flight_serve(
        _Args(), resolve_storage=lambda _args: storage, prepare_demo=lambda *_a: None,
        serve_until_terminated=_serve, client_factory=factory,
    )
    assert factory.clients
    assert all(c.closes == 1 for c in factory.clients)


async def test_flight_query_execution_leaves_custom_clients_open(tmp_path: Path) -> None:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    storage = await shop_storage(
        str(tmp_path / "store"), datasource=shop_datasource(db), models=[orders_model(), customers_model()],
    )
    factory = GatewayFactory()
    handlers = FlightHandlers(engine=SlayerQueryEngine(storage=storage, client_factory=factory))
    sql = "SELECT status, amount_sum FROM orders"
    result = handlers._translate(sql)
    assert isinstance(result, QueryResult), result
    handlers._derive_query_schema(result)
    assert [c.acloses for c in factory.clients] == [0]
    handlers._execute_full(result)
    assert [c.acloses for c in factory.clients] == [0]
