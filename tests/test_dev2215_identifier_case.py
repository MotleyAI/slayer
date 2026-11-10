"""Unquoted names fold, quoted names match exactly, the exact spelling wins, and saves never introduce a case
collision (spec: queries/identifier-case)."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousReferenceError, SlayerError, UnknownReferenceError
from slayer.core.models import Aggregation, Column, DatasourceConfig, ModelJoin, ModelMeasure, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import (
    BACKENDS,
    TIER1,
    dev2215_engine,
    dev2215_models,
    ledger_model,
    m,
    messages,
    num,
    orders_model,
    q,
    recorded_warnings,
    single,
    sorted_rows,
    warnings_mentioning,
)
from tests._engine_helpers import _engine_generate

STORAGES = ("yaml", "sqlite")


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=STORAGES)
async def storage(request, tmp_path) -> StorageBackend:
    store: StorageBackend = (YAMLStorage(base_dir=str(tmp_path)) if request.param == "yaml"
                             else SQLiteStorage(db_path=os.path.join(tmp_path, "store.db")))
    await store.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    return store


# --------------------------------------------------------------------------- #
# Unquoted names fold, quoted names match exactly
# --------------------------------------------------------------------------- #
class TestFolding:
    async def test_any_case_reaches_a_column(self, engine: SlayerQueryEngine) -> None:
        folded = await engine.execute(q(dimensions=["ORDERS.Status"], measures=[m("SUM(Amount)")]))
        plain = await engine.execute(q(dimensions=["status"], measures=[m("sum(amount)")]))
        assert folded.columns == plain.columns
        assert sorted_rows(folded) == sorted_rows(plain)

    async def test_quoted_names_are_exact(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(UnknownReferenceError):
            await engine.execute(q(measures=[m('sum("Amount")')]))
        assert num(single(await engine.execute(q(measures=[m('sum("amount")')])))) == 160.0

    @pytest.mark.parametrize("dimension", ['"customers".REGION', 'CUSTOMERS."region"'])
    async def test_quoting_per_segment(self, engine: SlayerQueryEngine, dimension: str) -> None:
        resp = await engine.execute(q(dimensions=[dimension], measures=[m("count(*)", "n")]))
        assert "orders.customers.region" in resp.columns

    async def test_quoted_segment_with_wrong_case_fails(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(UnknownReferenceError):
            await engine.execute(q(dimensions=['"Customers".region'], measures=[m("count(*)", "n")]))

    async def test_join_names_fold(self, engine: SlayerQueryEngine) -> None:
        folded = await engine.execute(q(dimensions=["CUSTOMERS.region"], measures=[m("count(*)", "n")]))
        plain = await engine.execute(q(dimensions=["customers.region"], measures=[m("count(*)", "n")]))
        assert folded.columns == plain.columns
        assert sorted_rows(folded) == sorted_rows(plain)

    async def test_stage_names_fold(self, engine: SlayerQueryEngine) -> None:
        stages: list[Any] = [
            {"name": "Monthly", "source_model": "orders",
             "time_dimensions": [{"dimension": "created_at", "granularity": "month"}], "measures": [m("count(*)", "n")]},
            {"source_model": "monthly", "measures": [m("sum(n)", "t")]},
        ]
        resp = await engine.execute(stages)
        assert [num(v) for r in resp.data for k, v in r.items() if k.endswith(".t")] == [6.0]

    async def test_model_names_fold(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(source_model="ORDERS", measures=[m("count(*)", "n")]))
        assert resp.columns == ["orders.n"]

    @pytest.mark.parametrize(("folded", "plain"), [
        ("RANK(SUM(amount), direction = 'desc')", "rank(sum(amount), direction = 'desc')"),
        ("CumSum(sum(amount))", "cumsum(sum(amount))"),
    ])
    async def test_transform_names_fold(self, engine: SlayerQueryEngine, folded: str, plain: str) -> None:
        td = [{"dimension": "created_at", "granularity": "month"}]
        a = await engine.execute(q(time_dimensions=td, measures=[m(folded)]))
        b = await engine.execute(q(time_dimensions=td, measures=[m(plain)]))
        assert a.sql == b.sql
        assert sorted_rows(a) == sorted_rows(b)

    async def test_model_aggregation_parameter_names_fold(self, engine: SlayerQueryEngine) -> None:
        a = await engine.execute(q(measures=[m("TRIMMED(amount, cutoff = 25)")]))
        b = await engine.execute(q(measures=[m("trimmed(amount, cutoff = 25)")]))
        assert num(single(a)) == num(single(b))


# --------------------------------------------------------------------------- #
# The exact spelling wins; otherwise one match or a typed ambiguity
# --------------------------------------------------------------------------- #
class TestExactSpellingWins:
    async def test_exact_spelling_in_a_colliding_model(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(source_model="ledger", measures=[m("sum(Revenue)", "a"), m("sum(revenue)", "b")]))
        assert (num(single(resp, "a", model="ledger")), num(single(resp, "b", model="ledger"))) == (3.0, 30.0)

    async def test_ambiguous_spelling(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(AmbiguousReferenceError) as err:
            await engine.execute(q(source_model="ledger", measures=[m("sum(REVENUE)")]))
        text = str(err.value)
        assert "Revenue" in text and "revenue" in text
        assert '"Revenue"' in text and '"revenue"' in text

    async def test_quoting_resolves_the_ambiguity(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(source_model="ledger", measures=[m('sum("Revenue")')]))
        assert num(single(resp, model="ledger")) == 3.0


# --------------------------------------------------------------------------- #
# Declared spellings are canonical
# --------------------------------------------------------------------------- #
class TestCanonicalKeys:
    async def test_case_variant_references_share_one_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=["sum(AMOUNT)", "sum(amount)"]))
        assert resp.columns == ["orders.amount_sum"]
        assert num(resp.data[0]["orders.amount_sum"]) == 160.0

    async def test_dimension_key_uses_the_declared_spelling(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["CUSTOMERS.REGION"], measures=[m("count(*)", "n")]))
        assert "orders.customers.region" in resp.columns


# --------------------------------------------------------------------------- #
# Saves never introduce a case collision
# --------------------------------------------------------------------------- #
def _with_measure(model: SlayerModel, name: str) -> SlayerModel:
    return model.model_copy(update={"measures": [*model.measures, ModelMeasure(name=name, formula="sum(amount)")]})


class TestSaveCollisions:
    async def test_a_new_column_measure_collision_is_refused(self, storage: StorageBackend) -> None:
        await storage.save_model(orders_model())
        with pytest.raises(SlayerError) as err:
            await storage.save_model(_with_measure(orders_model(), "Amount"))
        assert "amount" in str(err.value) and "Amount" in str(err.value)

    async def test_a_new_aggregation_collision_is_refused(self, storage: StorageBackend) -> None:
        model = orders_model(aggregations=[Aggregation(name="Share", formula="AVG({value})")])
        with pytest.raises(SlayerError) as err:
            await storage.save_model(model)
        assert "Share" in str(err.value) and "share" in str(err.value)

    async def test_a_new_join_name_collision_is_refused(self, storage: StorageBackend) -> None:
        await storage.save_model(dev2215_models()[1])
        model = orders_model().model_copy(update={"joins": [
            ModelJoin(target_model="customers", name="buyer", join_pairs=[["customer_id", "id"]]),
            ModelJoin(target_model="customers", name="Buyer", join_pairs=[["customer_id", "id"]]),
        ]})
        with pytest.raises(SlayerError) as err:
            await storage.save_model(model)
        assert "buyer" in str(err.value) and "Buyer" in str(err.value)

    @pytest.mark.parametrize("source_queries", [
        [{"name": "Monthly", "source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}]},
         {"name": "monthly", "source_model": "Monthly", "measures": [{"formula": "sum(n)", "name": "t"}]},
         {"source_model": "monthly", "measures": [{"formula": "sum(t)", "name": "u"}]}],
        [{"source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "Total"},
                                                 {"formula": "count(*)", "name": "total"}]}],
    ], ids=["stage-names", "result-names"])
    async def test_a_saved_query_collision_is_refused(self, storage: StorageBackend,
                                                      source_queries: list[dict[str, Any]]) -> None:
        await storage.save_model(orders_model())
        model = SlayerModel.model_validate({"name": "saved", "data_source": "test", "source_queries": source_queries})
        with pytest.raises(SlayerError):
            await storage.save_model(model)

    async def test_model_names_in_a_datasource(self, storage: StorageBackend) -> None:
        await storage.save_model(orders_model())
        with pytest.raises(SlayerError) as err:
            await storage.save_model(orders_model().model_copy(update={"name": "Orders"}))
        assert "orders" in str(err.value) and "Orders" in str(err.value)

    async def test_a_grandfathered_collision_keeps_working(self, storage: StorageBackend) -> None:
        await storage.save_model(ledger_model(), _validate=False)
        with recorded_warnings() as caught:
            await storage.save_model(ledger_model())
            await storage.save_model(ledger_model().model_copy(update={"description": "edited"}))
        assert len(messages(caught, "Revenue", "revenue")) >= 2

    async def test_removing_and_reintroducing_a_collision(self, storage: StorageBackend) -> None:
        await storage.save_model(ledger_model(), _validate=False)
        without = ledger_model().model_copy(update={"columns": [c for c in ledger_model().columns if c.name != "revenue"]})
        await storage.save_model(without)
        with pytest.raises(SlayerError) as err:
            await storage.save_model(ledger_model())
        assert "Revenue" in str(err.value) and "revenue" in str(err.value)

    async def test_a_grandfathered_collision_warns_on_load(self, storage: StorageBackend) -> None:
        await storage.save_model(ledger_model(), _validate=False)
        with recorded_warnings() as caught:
            await storage.get_model("ledger", data_source="test")
        assert messages(caught, "Revenue", "revenue")

    async def test_a_query_reading_the_model_returns_the_warning(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(source_model="ledger", measures=[m('sum("Revenue")')]))
        assert warnings_mentioning(resp, "Revenue", "revenue")

    async def test_a_facade_query_reading_the_model_returns_the_warning(self, engine: SlayerQueryEngine) -> None:
        catalog = build_catalog(models_by_datasource={"test": dev2215_models()})
        result = translate("SELECT id FROM ledger", catalog)
        assert isinstance(result, QueryResult)
        assert warnings_mentioning(await engine.execute(result.query), "Revenue", "revenue")

    async def test_a_query_not_reading_the_model_is_silent(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("count(*)", "n")]))
        assert not warnings_mentioning(resp, "Revenue", "revenue")


# --------------------------------------------------------------------------- #
# A query's result names never collide by case
# --------------------------------------------------------------------------- #
COLLIDING = [m("sum(amount)", "Total"), m("count(*)", "total")]


async def test_result_names_colliding_by_case_are_refused(engine: SlayerQueryEngine) -> None:
    with pytest.raises(SlayerError) as err:
        await engine.execute(q(measures=COLLIDING))
    assert "Total" in str(err.value) and "total" in str(err.value)


@pytest.mark.parametrize("dialect", TIER1)
async def test_result_names_colliding_by_case_are_refused_on_every_dialect(dialect: str) -> None:
    first, *rest = dev2215_models()
    with pytest.raises(SlayerError):
        await _engine_generate(query=q(measures=COLLIDING), model=first, extra_models=rest, dialect=dialect,
                               validate=False)


async def test_a_stored_query_with_colliding_names_keeps_running() -> None:
    saved = SlayerModel.model_validate({"name": "saved", "data_source": "test", "source_queries": [
        {"source_model": "orders", "measures": [{"formula": "sum(amount)", "name": "Total"},
                                                {"formula": "count(*)", "name": "total"}]}]})
    async with dev2215_engine("sqlite", models=[*dev2215_models(), saved]) as eng:
        resp = await eng.execute(q(source_model="saved", measures=[m("sum(Total)", "a")]))
    assert num(single(resp, "a", model="saved")) == 160.0


def test_model_lookups_fold() -> None:
    model = SlayerModel(name="x", sql_table="orders", data_source="test",
                        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
                        measures=[ModelMeasure(name="Total", formula="count(*)")])
    column, measure = model.get_column("ID"), model.get_measure("total")
    assert column is not None and column.name == "id"
    assert measure is not None and measure.name == "Total"
