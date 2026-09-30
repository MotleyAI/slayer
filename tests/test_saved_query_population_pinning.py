"""A saved query's inferred populations are pinned at save (spec: queries/population)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any, Dict, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.mcp.server import create_mcp_server
from slayer.storage.sqlite_conn import transaction
from tests._dev1994_fixtures import dev1994_engine
from tests._engine_helpers import seeded_exec_engine

REVENUE = {"formula": "sum(orders.amount)", "name": "revenue"}
# Rootless: infers ``customers``; with an ``orders.status`` filter it would infer ``orders``.
BY_CUSTOMER: Dict[str, Any] = {"dimensions": ["customers.name"], "measures": [REVENUE]}
OK_FILTER = {"filters": ["orders.status = 'ok'"]}
N = {"formula": "count(*)", "name": "n"}
# The final stage infers model ``orders``, which a sibling stage is named.
COLLIDING: List[Dict[str, Any]] = [
    {"name": "orders", "source_model": "customers", "dimensions": ["name"], "measures": [N]},
    {"dimensions": ["customers.name", "products.title"], "measures": [N]},
]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev1994_engine(request.param) as eng:
        yield eng


@pytest.fixture
async def sqlite_engine() -> AsyncIterator[SlayerQueryEngine]:
    async with dev1994_engine("sqlite") as eng:
        yield eng


def _seed_products(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        con.executemany("INSERT INTO customers VALUES (?,?)", [(1, "Ann"), (2, "Ben")])
        con.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)")
        con.executemany("INSERT INTO products VALUES (?,?)", [(1, "pen"), (2, "cup")])
        con.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, product_id INTEGER, amount REAL)")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?)", [(1, 1, 1, 5.0), (2, 1, 2, 7.0), (3, 2, 1, 3.0)])


def _products_models() -> List[SlayerModel]:
    return [
        SlayerModel(name="customers", data_source="test", sql_table="customers", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)]),
        SlayerModel(name="products", data_source="test", sql_table="products", columns=[
            Column(name="id", type=DataType.INT, primary_key=True), Column(name="title", type=DataType.TEXT)]),
        SlayerModel(name="orders", data_source="test", sql_table="orders", columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="product_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ], joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="products", join_pairs=[["product_id", "id"]]),
        ]),
    ]


@pytest.fixture
async def products_engine() -> AsyncIterator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect="sqlite", seed=_seed_products, models=_products_models()) as (eng, _):
        yield eng


def rows(resp: SlayerResponse) -> List[tuple]:
    return sorted(tuple(str(r[c]) for c in resp.columns) for r in resp.data)


async def stored_stages(engine: SlayerQueryEngine, name: str) -> List[SlayerQuery]:
    model = await engine.storage.get_model(name)
    assert model is not None
    return list(model.source_queries or [])


class TestPinnedOnSave:
    async def test_create_model_from_query(self, engine: SlayerQueryEngine) -> None:
        before = await engine.execute(BY_CUSTOMER)
        await engine.create_model_from_query(query=BY_CUSTOMER, name="by_customer")
        (stage,) = await stored_stages(engine, "by_customer")
        assert stage.source_model == "customers"
        after = await engine.execute("by_customer")
        assert after.columns == before.columns
        assert rows(after) == rows(before)
        assert (after.population, after.population_inferred) == ("customers", False)

    async def test_save_model(self, engine: SlayerQueryEngine) -> None:
        await engine.save_model(SlayerModel(
            name="by_customer", data_source="test", source_queries=[SlayerQuery.model_validate(BY_CUSTOMER)],
        ))
        (stage,) = await stored_stages(engine, "by_customer")
        assert stage.source_model == "customers"

    async def test_edit_model_source_queries(self, engine: SlayerQueryEngine) -> None:
        await engine.create_model_from_query(
            query={"source_model": "orders", "measures": [N]}, name="edited",
        )
        server = create_mcp_server(storage=engine.storage)
        blocks, _ = await server.call_tool(
            name="edit_model", arguments={"model_name": "edited", "source_queries": [BY_CUSTOMER]},
        )
        assert json.loads(blocks[0].text)["success"] is True
        (stage,) = await stored_stages(engine, "edited")
        assert stage.source_model == "customers"

    async def test_every_rootless_stage_pinned_and_rooted_stages_verbatim(
        self, sqlite_engine: SlayerQueryEngine,
    ) -> None:
        extension = {"source_name": "orders", "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}]}
        await sqlite_engine.create_model_from_query(query=[
            {"name": "per_customer", **BY_CUSTOMER},
            {"name": "ext", "source_model": extension, "measures": [{"formula": "sum(double_amount)", "name": "d"}]},
            {"source_model": "per_customer", "measures": [{"formula": "sum(revenue)", "name": "total"}]},
        ], name="staged")
        stages = {s.name: s for s in await stored_stages(sqlite_engine, "staged")}
        assert stages["per_customer"].source_model == "customers"
        assert stages["ext"].source_model == SlayerQuery.model_validate({"source_model": extension}).source_model
        assert stages[None].source_model == "per_customer"


class TestRefinementKeepsPopulation:
    async def test_refined_run_keeps_pinned_population(self, engine: SlayerQueryEngine) -> None:
        await engine.create_model_from_query(query=BY_CUSTOMER, name="by_customer")
        refined = await engine.execute("by_customer", refine=OK_FILTER)
        hand_written = await engine.execute({"source_model": "customers", **BY_CUSTOMER, **OK_FILTER})
        assert refined.population == "customers"
        assert refined.columns == hand_written.columns == ["customers.name", "customers.revenue"]
        assert rows(refined) == rows(hand_written)


class TestNameCollision:
    async def test_create_model_from_query_refused(self, products_engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match=r"(?i)rename") as info:
            await products_engine.create_model_from_query(query=COLLIDING, name="colliding")
        assert "orders" in str(info.value)
        assert await products_engine.storage.get_model("colliding") is None

    async def test_save_model_refused(self, products_engine: SlayerQueryEngine) -> None:
        model = SlayerModel(
            name="colliding", data_source="test",
            source_queries=[SlayerQuery.model_validate(q) for q in COLLIDING],
        )
        with pytest.raises(ValueError, match=r"(?i)rename"):
            await products_engine.save_model(model)
        assert await products_engine.storage.get_model("colliding") is None


class TestLegacyUnpinnedModel:
    async def _store_legacy(self, engine: SlayerQueryEngine, name: str, stages: List[Dict[str, Any]]) -> None:
        await engine.storage.save_model(SlayerModel(
            name=name, data_source="test", source_queries=[SlayerQuery.model_validate(q) for q in stages],
        ))

    async def test_refined_run_pins_from_unrefined_stage(self, engine: SlayerQueryEngine) -> None:
        await self._store_legacy(engine, "legacy", [BY_CUSTOMER])
        refined = await engine.execute("legacy", refine=OK_FILTER)
        hand_written = await engine.execute({"source_model": "customers", **BY_CUSTOMER, **OK_FILTER})
        assert refined.population == "customers"
        assert refined.columns == hand_written.columns
        assert rows(refined) == rows(hand_written)
        (stage,) = await stored_stages(engine, "legacy")
        assert stage.source_model is None

    async def test_unrefined_run_unchanged(self, engine: SlayerQueryEngine) -> None:
        await self._store_legacy(engine, "legacy", [BY_CUSTOMER])
        by_name = await engine.execute("legacy")
        direct = await engine.execute(BY_CUSTOMER)
        assert (by_name.population, by_name.population_inferred) == ("customers", True)
        assert by_name.columns == direct.columns
        assert rows(by_name) == rows(direct)

    async def test_refined_collision_refused(self, products_engine: SlayerQueryEngine) -> None:
        await self._store_legacy(products_engine, "colliding", COLLIDING)
        with pytest.raises(ValueError, match=r"(?i)rename"):
            await products_engine.execute("colliding", refine={"filters": ["n > 0"]})

    async def test_unrefined_collision_runs_as_before(self, products_engine: SlayerQueryEngine) -> None:
        await self._store_legacy(products_engine, "colliding", COLLIDING)
        by_name = await products_engine.execute("colliding")
        assert by_name.population == "orders"
        assert rows(by_name) == rows(await products_engine.execute(COLLIDING))
