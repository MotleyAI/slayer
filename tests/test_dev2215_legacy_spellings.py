"""``==``, ``is [not] None``, bare ``None``, ``[…]`` after ``IN`` and the method-call form are deprecated until 3.0:
same meaning, one warning per spelling, stored as their canonical twin; SLayer never emits them (specs:
queries/expression-grammar, queries/expression-keywords, aggregations/functional-form,
compatibility/removal-tripwire)."""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.syntax import canonical_measure_text, parse_expr
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.facade.catalog import build_catalog
from slayer.facade.translator import QueryResult, translate
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2215_fixtures import (
    BACKENDS,
    dev2215_engine,
    dev2215_models,
    future_warnings,
    m,
    num,
    orders_model,
    q,
    recorded_warnings,
    single,
    warnings_mentioning,
)

SLAYER_3 = "3.0"


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with dev2215_engine(request.param) as eng:
        yield eng


@pytest.fixture(params=("yaml", "sqlite"))
async def storage(request, tmp_path) -> StorageBackend:
    store: StorageBackend = (YAMLStorage(base_dir=str(tmp_path)) if request.param == "yaml"
                             else SQLiteStorage(db_path=os.path.join(tmp_path, "store.db")))
    await store.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
    for model in dev2215_models()[1:3]:
        await store.save_model(model)
    return store


def deprecations(resp: SlayerResponse) -> list[Any]:
    return warnings_mentioning(resp, SLAYER_3)


async def run(engine: SlayerQueryEngine, query: Any) -> tuple[SlayerResponse, list[str]]:
    with recorded_warnings() as caught:
        resp = await engine.execute(query)
    return resp, future_warnings(caught, SLAYER_3)


# --------------------------------------------------------------------------- #
# Fresh input keeps its meaning and warns once per spelling
# --------------------------------------------------------------------------- #
class TestFreshInput:
    async def test_python_spellings(self, engine: SlayerQueryEngine) -> None:
        legacy, python_warned = await run(engine, q(measures=[m("count(*)", "n")],
                                                    filters=["status == 'paid' and region is not None"]))
        canonical, sql_warned = await run(engine, q(measures=[m("count(*)", "n")],
                                                    filters=["status = 'paid' AND region IS NOT NULL"]))
        assert legacy.sql == canonical.sql
        assert legacy.data == canonical.data
        assert len(deprecations(legacy)) == 2
        assert len(warnings_mentioning(legacy, SLAYER_3, "IS NOT NULL")) == 1
        assert len(python_warned) == 2
        assert len([w for w in python_warned if "IS NOT NULL" in w]) == 1
        assert deprecations(canonical) == [] and sql_warned == []

    @pytest.mark.parametrize(("legacy", "canonical"), [
        ("sum(iif(region IN ['EU', 'US'], amount, None))", "sum(iif(region IN ('EU', 'US'), amount, NULL))"),
        ("customers.count_distinct(status)", "count_distinct(customers.status)"),
        ("customers.count(*)", "count(customers.*)"),
        ("customers.count_distinct(status || name)", "count_distinct(customers.status || customers.name)"),
        ("customers.percentile(score, 0.5)", "percentile(customers.score, 0.5)"),
        ("sum(iif(region is None, 1, 0))", "sum(iif(region IS NULL, 1, 0))"),
    ])
    async def test_twins_share_sql_key_and_value(self, engine: SlayerQueryEngine, legacy: str,
                                                 canonical: str) -> None:
        old, old_warned = await run(engine, q(measures=[legacy]))
        new, new_warned = await run(engine, q(measures=[canonical]))
        assert old.columns == new.columns
        assert old.sql == new.sql
        assert old.data == new.data
        assert deprecations(old) and old_warned
        assert deprecations(new) == [] and new_warned == []

    async def test_method_call_warning_names_the_functional_spelling(self, engine: SlayerQueryEngine) -> None:
        resp, warned = await run(engine, q(measures=["customers.count_distinct(status)"]))
        assert warnings_mentioning(resp, SLAYER_3, "count_distinct(customers.status)")
        assert any("count_distinct(customers.status)" in w for w in warned)

    async def test_a_date_part_method_call_shows_its_functional_spelling(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError, match=re.escape("month(customers.updated_at)")):
            await engine.execute(q(dimensions=[{"expression": "customers.month(updated_at)", "name": "mo"}],
                                   measures=[m("count(*)", "n")]))

    async def test_bare_none_list_and_method_call_warn_per_spelling(self, engine: SlayerQueryEngine) -> None:
        resp, warned = await run(engine, q(measures=[
            m("sum(iif(region IN ['EU', 'US'], amount, None))", "a"), m("customers.count_distinct(status)", "b")]))
        assert len(deprecations(resp)) == 3
        assert len(warned) == 3

    async def test_a_spelling_used_twice_warns_once(self, engine: SlayerQueryEngine) -> None:
        resp, warned = await run(engine, q(measures=[m("count(*)", "n")],
                                           filters=["status == 'paid'", "region == 'EU'"]))
        assert len(warned) == 1
        assert len(deprecations(resp)) == 1
        assert int(single(resp, "n")) == 3

    async def test_true_false_and_not_equal_are_not_deprecated(self, engine: SlayerQueryEngine) -> None:
        resp, warned = await run(engine, q(measures=[m("count(*)", "n")],
                                           filters=["status != 'void'", "(amount > 15) = True"]))
        assert deprecations(resp) == [] and warned == []


# --------------------------------------------------------------------------- #
# A save stores the canonical twin
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("written", "stored"), [
    ("sum(iif(status == 'paid', amount, 0))", "sum(iif(status = 'paid', amount, 0))"),
    ("sum( iif(status=='paid' , amount , 0) )", "sum( iif(status='paid' , amount , 0) )"),
    ("sum(iif(region is not None, amount, None))", "sum(iif(region IS NOT NULL, amount, NULL))"),
    ("sum(iif(region in ['EU', 'US'], amount, 0))", "sum(iif(region in ('EU', 'US'), amount, 0))"),
    ("customers.count(*)", "count(customers.*)"),
])
async def test_a_save_stores_the_canonical_twin(storage: StorageBackend, written: str, stored: str) -> None:
    model = orders_model().model_copy(update={"measures": [ModelMeasure(name="paid", formula=written)]})
    with recorded_warnings() as caught:
        await storage.save_model(model)
    loaded = await storage.get_model("orders", data_source="test")
    assert loaded is not None
    assert [x.formula for x in loaded.measures] == [stored]
    assert future_warnings(caught, SLAYER_3)


async def test_a_saved_query_stores_the_canonical_twin(storage: StorageBackend) -> None:
    await storage.save_model(orders_model())
    saved = SlayerModel.model_validate({"name": "paid_orders", "data_source": "test", "source_queries": [
        {"source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}],
         "filters": ["status == 'paid' and region is not None"]}]})
    await storage.save_model(saved)
    loaded = await storage.get_model("paid_orders", data_source="test")
    assert loaded is not None and loaded.source_queries
    assert loaded.source_queries[0].filters == ["status = 'paid' and region IS NOT NULL"]


async def test_a_canonical_save_is_silent_and_verbatim(storage: StorageBackend) -> None:
    formula = "sum(iif(status = 'paid' AND region IS NOT NULL, amount, NULL))"
    model = orders_model().model_copy(update={"measures": [ModelMeasure(name="paid", formula=formula)]})
    with recorded_warnings() as caught:
        await storage.save_model(model)
    loaded = await storage.get_model("orders", data_source="test")
    assert loaded is not None and [x.formula for x in loaded.measures] == [formula]
    assert future_warnings(caught, SLAYER_3) == []


# --------------------------------------------------------------------------- #
# SLayer never self-warns
# --------------------------------------------------------------------------- #
class TestNeverSelfWarns:
    def test_case_lowering_is_canonical(self) -> None:
        parsed = parse_expr("CASE status WHEN 'a' THEN 1 WHEN 'b' THEN 2 END")
        assert parsed == parse_expr("iif(status = 'a', 1, iif(status = 'b', 2, NULL))")
        assert canonical_measure_text(parsed) == "iif(status = 'a', 1, iif(status = 'b', 2, NULL))"

    @pytest.mark.parametrize(("text", "canonical"), [
        ("a == 1", "a = 1"),
        ("region is None", "region IS NULL"),
        ("region is not None", "region IS NOT NULL"),
        ("iif(a = 1, b, None)", "iif(a = 1, b, NULL)"),
        ("region in ['EU', 'US']", "region in ('EU', 'US')"),
    ])
    def test_canonical_text_uses_the_canonical_twins(self, text: str, canonical: str) -> None:
        rendered = canonical_measure_text(parse_expr(text))
        assert rendered == canonical
        assert parse_expr(rendered) == parse_expr(text)

    async def test_case_query(self, engine: SlayerQueryEngine) -> None:
        resp, warned = await run(engine, q(measures=[m("sum(CASE status WHEN 'paid' THEN 1 END)")],
                                           filters=["CASE WHEN region IS NULL THEN 0 ELSE 1 END = 1"]))
        assert num(single(resp)) == 4.0
        assert deprecations(resp) == [] and warned == []

    async def test_facade_query(self, engine: SlayerQueryEngine) -> None:
        catalog = build_catalog(models_by_datasource={"test": dev2215_models()})
        result = translate("SELECT status, COUNT(*) FROM orders WHERE region IS NULL OR status IN ('a', 'b') "
                           "OR status <> 'x' GROUP BY status", catalog)
        assert isinstance(result, QueryResult)
        resp, warned = await run(engine, result.query)
        assert deprecations(resp) == [] and warned == []

    async def test_engine_remedies_are_canonical(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ValueError) as err:
            await engine.execute(q(measures=[m("weighted_avg(amount, wieght = qty)")]))
        assert "==" not in str(err.value)
