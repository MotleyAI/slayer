"""A query carries no Mode A: every route from a query payload to dialect SQL or an unmodelled table is refused.

Spec: queries/extensions › Queries carry no Mode A; semantics §4 P2.3.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator
from typing import Any

import duckdb
import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ValidationError

from slayer.api.server import create_app
from slayer.core.enums import DataType
from slayer.core.errors import UnanalyzableAggregationParameterError
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import owned_query_engine

DS = "shop"
_DDL = (
    "CREATE TABLE customers (id INTEGER, name TEXT, tier TEXT)",
    "CREATE TABLE orders (id INTEGER, customer_id INTEGER, status TEXT, amount DOUBLE, cost DOUBLE)",
    "CREATE TABLE secrets (id INTEGER, pw TEXT)",
)
_CUSTOMERS = [(1, "Ann", "gold"), (2, "Bob", "silver"), (3, "Cid", "gold")]
_ORDERS = [(1, 1, "ok", 10.0, 1.0), (2, 1, "new", 20.0, 2.0), (3, 2, "ok", 30.0, 3.0), (4, 3, "ok", 40.0, 4.0)]
_SECRETS = [(1, "hunter2"), (2, "swordfish")]
_LEAKS = ("hunter2", "swordfish")


def _seed(db_path: str, dialect: str) -> None:
    inserts = (("customers", _CUSTOMERS), ("orders", _ORDERS), ("secrets", _SECRETS))
    if dialect == "duckdb":
        con = duckdb.connect(db_path)
        try:
            for stmt in _DDL:
                con.execute(stmt)
            for table, rows in inserts:
                con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        finally:
            con.close()
        return
    with transaction(db_path) as cur:
        for stmt in _DDL:
            cur.execute(stmt)
        for table, rows in inserts:
            cur.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)


def _models() -> list[SlayerModel]:
    customers = SlayerModel(
        name="customers", sql_table="customers", data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
        ],
    )
    orders = SlayerModel(
        name="orders", sql_table="orders", data_source=DS,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="scaled", sql="amount * {k}", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )
    return [customers, orders]


class _Shop:
    def __init__(self, *, storage: YAMLStorage, engine: SlayerQueryEngine) -> None:
        self.storage = storage
        self.engine = engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def shop(request) -> AsyncIterator[_Shop]:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, f"shop.{request.param}")
        _seed(db_path, request.param)
        storage = YAMLStorage(base_dir=os.path.join(tmp, "store"))
        ds = DatasourceConfig(name=DS, type=request.param, database=db_path)
        async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
            for model in _models():
                await storage.save_model(model)
            yield _Shop(storage=storage, engine=engine)


@pytest.fixture
def executed(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Every SQL text the client is asked to execute."""
    seen: list[str] = []
    real = SlayerSQLClient.execute

    async def _spy(self, sql: str, *args: Any, **kwargs: Any):  # noqa: ANN202
        seen.append(sql)
        return await real(self, sql, *args, **kwargs)

    monkeypatch.setattr(SlayerSQLClient, "execute", _spy)
    return seen


def _ext_column(column: dict[str, Any], *, source_name: str = "orders") -> dict[str, Any]:
    return {
        "source_model": "orders",
        "extensions": [{"source_name": source_name, "columns": [column]}],
        "measures": [{"formula": "count(*)"}],
    }


_CONSTRUCTION_ROUTES = {
    "extension-sql-subquery": _ext_column({"name": "leak", "sql": "(SELECT group_concat(pw) FROM secrets)"}),
    "extension-filter-oracle": _ext_column({"name": "oracle", "sql": "amount", "filter": "EXISTS (SELECT 1 FROM secrets)"}),
    "extension-sql-less-hidden-column": _ext_column({"name": "cost", "type": "DOUBLE"}),
    "inline-sql-table": {
        "source_model": {"name": "s", "sql_table": "secrets", "data_source": DS,
                         "columns": [{"name": "pw", "type": "TEXT"}]},
        "dimensions": ["pw"],
    },
    "inline-sql": {
        "source_model": {"name": "s", "sql": "SELECT pw FROM secrets", "data_source": DS,
                         "columns": [{"name": "pw", "type": "TEXT"}]},
        "dimensions": ["pw"],
    },
}


class TestEachRouteIsRefused:
    @pytest.mark.parametrize("payload", list(_CONSTRUCTION_ROUTES.values()), ids=list(_CONSTRUCTION_ROUTES))
    def test_refused_at_construction(self, payload: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery.model_validate(payload)

    def test_legacy_root_extension_with_columns_is_refused(self) -> None:
        legacy = {"source_name": "orders", "columns": [{"name": "leak", "sql": "(SELECT max(pw) FROM secrets)"}]}
        with pytest.raises(ValidationError, match="columns"):
            SlayerQuery.model_validate({"source_model": legacy, "measures": ["count(*)"]})

    @pytest.mark.parametrize(("column", "replacement"), [
        ({"name": "q", "sql": "amount", "filter": "status = 'ok'", "type": "DOUBLE"},
         "q → CAST(iif(orders.status = 'ok', orders.amount, NULL) AS DOUBLE)"),
        ({"name": "t", "sql": "upper(orders.status)"}, "t → upper(orders.status)"),
    ])
    def test_the_suggested_replacement_keeps_the_columns_meaning(self, column: dict, replacement: str) -> None:
        with pytest.raises(ValidationError) as exc:
            SlayerQuery.model_validate({"source_model": "orders", "measures": ["count(*)"],
                                        "extensions": [{"source_name": "orders", "columns": [column]}]})
        assert replacement in str(exc.value)

    def test_a_slayer_model_instance_is_refused(self) -> None:
        inline = SlayerModel(name="s", sql_table="secrets", data_source=DS, columns=[Column(name="pw", type=DataType.TEXT)])
        with pytest.raises(ValidationError, match="inline"):
            SlayerQuery(source_model=inline, dimensions=["pw"])  # pyright: ignore[reportArgumentType]  # the removed form must fail

    async def test_a_hostile_variable_is_refused_before_sql_runs(self, shop: _Shop, executed: list[str]) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "sum(scaled)"}],
            "variables": {"k": "(SELECT count(*) FROM secrets)"},
        })
        with pytest.raises(ValueError, match="literal values"):
            await shop.engine.execute(query)
        assert not any("secrets" in sql or "sqlite_version" in sql for sql in executed)

    async def test_a_dialect_function_argument_is_refused_before_sql_runs(
        self, shop: _Shop, executed: list[str],
    ) -> None:
        query = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "weighted_avg(amount, weight='length(sqlite_version())')"}],
        })
        with pytest.raises(UnanalyzableAggregationParameterError):
            await shop.engine.execute(query)
        assert not any("secrets" in sql or "sqlite_version" in sql for sql in executed)


class TestSurfaces:
    @pytest.mark.parametrize("payload", list(_CONSTRUCTION_ROUTES.values()), ids=list(_CONSTRUCTION_ROUTES))
    def test_rest_responds_422(self, payload: dict[str, Any]) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            client = TestClient(create_app(storage=YAMLStorage(base_dir=tmp)))
            resp = client.post("/query", json={**payload, "dry_run": True})
        assert resp.status_code == 422, resp.text
        assert not any(leak in resp.text for leak in _LEAKS)

    @pytest.mark.parametrize("payload", list(_CONSTRUCTION_ROUTES.values()), ids=list(_CONSTRUCTION_ROUTES))
    async def test_mcp_query_tool_refuses(self, shop: _Shop, payload: dict[str, Any]) -> None:
        server = create_mcp_server(storage=shop.storage)
        with pytest.raises(ToolError) as exc:
            await server.call_tool(name="query", arguments={"query": payload})
        assert not any(leak in str(exc.value) for leak in _LEAKS)

    async def test_mcp_query_tool_refuses_a_hostile_variable(self, shop: _Shop, executed: list[str]) -> None:
        server = create_mcp_server(storage=shop.storage)
        with pytest.raises(ToolError, match="literal values"):
            await server.call_tool(name="query", arguments={"query": {
                "source_model": "orders", "measures": [{"formula": "sum(scaled)"}],
                "variables": {"k": "(SELECT count(*) FROM secrets)"},
            }})
        assert not any("secrets" in sql or "sqlite_version" in sql for sql in executed)


class TestModeBSpellingsRun:
    async def test_an_expression_dimension_replaces_the_extension_column(self, shop: _Shop) -> None:
        resp = await shop.engine.execute(SlayerQuery.model_validate({
            "source_model": "orders",
            "dimensions": [{"expression": "upper(customers.tier)", "name": "tier_label"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
        }))
        assert {r["orders.tier_label"]: r["orders.rev"] for r in resp.data} == {"GOLD": 70.0, "SILVER": 30.0}

    async def test_a_literal_variable_binds_into_stored_mode_a(self, shop: _Shop) -> None:
        resp = await shop.engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "sum(scaled)", "name": "s"}], "variables": {"k": 2},
        }))
        assert resp.data[0]["orders.s"] == pytest.approx(200.0)

    @pytest.mark.parametrize("formula", [
        "weighted_avg(amount, weight='id * 2')",
        "weighted_avg(amount, weight=id * 2)",
    ])
    async def test_a_string_argument_is_mode_b(self, shop: _Shop, formula: str) -> None:
        resp = await shop.engine.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": formula, "name": "w"}],
        }))
        # sum(amount * id * 2) / sum(id * 2) = 600 / 20
        assert resp.data[0]["orders.w"] == pytest.approx(30.0)
