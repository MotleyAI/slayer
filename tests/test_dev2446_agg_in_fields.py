"""AGG is refused in a field expression (spec: models/aggregation-names)."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.errors import SlayerError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev1954_fixtures import execution_spy
from tests._engine_helpers import seeded_exec_engine
from tests._stored_upgrade_fixtures import DS, seed_shop

#: The wording the Ossie-document rule uses for the same refusal.
FIELD_PHRASE = "calls AGG in a field expression"
ONLY_IN_METRICS = ("allows AGG only in metrics, as AGG(dataset.metric) in a model-scoped metric and "
                   "AGG(source.measure) in a dataset-scoped metric")
#: ``amount`` per ``order_id`` of the shop seed.
AMOUNTS = {1: 10.0, 2: 20.0, 3: 30.0, 4: 40.0, 5: 50.0, 6: 60.0}


def orders(*extra: dict[str, Any]) -> SlayerDataset:
    return SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": DS,
        "fields": [{"name": "order_id", "datatype": "Integer", "primary_key": True},
                   {"name": "amount", "datatype": "Float"}, *extra],
    })


def agg_field(name: str, expression: Any) -> dict[str, Any]:
    return {"name": name, "expression": expression, "datatype": "Float"}


@pytest.fixture
async def shop() -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect="sqlite", seed=lambda p: seed_shop(p, dialect="sqlite"),
                                  datasets=[orders()], datasource=DS, validate=True) as (engine, _db):
        yield engine


def assert_agg_refusal(exc: BaseException, *, field: str) -> None:
    msg = str(exc)
    assert isinstance(exc, SlayerError), (type(exc), msg)
    assert FIELD_PHRASE in msg, msg
    assert ONLY_IN_METRICS in msg, msg
    assert re.search(rf"'(orders\.)?{field}'", msg), msg


async def stored_field_names(engine: SlayerQueryEngine) -> set[str]:
    stored = await engine.storage.get_dataset("orders", data_source=DS)
    assert stored is not None
    return {f.name for f in stored.fields}


@pytest.mark.parametrize(("name", "expression"), [
    ("a", "AGG(amount)"),
    ("b", "agg(amount) + 1"),
    ("c", "Agg(amount) * 2"),
    ("d", {"sql": "AGG(amount)"}),
])
async def test_a_field_calling_agg_is_refused_when_saved(shop: SlayerQueryEngine, name: str, expression: Any) -> None:
    dataset = orders(agg_field(name, expression))
    with pytest.raises(SlayerError) as info:
        await shop.storage.save_dataset(dataset)
    assert_agg_refusal(info.value, field=name)
    assert name not in await stored_field_names(shop)


@pytest.mark.parametrize("query", [
    {"dimensions": ["orders.order_id", "orders.a"]},
    {"metrics": [{"metric": "sum(orders.a)", "alias": "t"}]},
    {"dimensions": ["orders.order_id"], "filter": ["orders.a > 15"]},
], ids=["dimension", "metric", "filter"])
async def test_a_field_saved_without_the_check_fails_every_query(shop: SlayerQueryEngine,
                                                                 monkeypatch: pytest.MonkeyPatch,
                                                                 query: dict[str, Any]) -> None:
    await shop.storage.save_dataset(orders(agg_field("a", "AGG(amount)")), _validate=False)
    sent = execution_spy(monkeypatch)
    q = SlayerQuery.model_validate({"population": "orders", **query})
    with pytest.raises(SlayerError) as dry:
        await shop.execute(q, dry_run=True)
    assert_agg_refusal(dry.value, field="a")
    with pytest.raises(SlayerError) as run:
        await shop.execute(q)
    assert_agg_refusal(run.value, field="a")
    assert [s for s in sent if "slayer_rk_" not in s] == []


async def test_save_and_query_refuse_with_one_error_class(shop: SlayerQueryEngine) -> None:
    dataset = orders(agg_field("a", "AGG(amount)"))
    with pytest.raises(SlayerError) as saved:
        await shop.storage.save_dataset(dataset)
    await shop.storage.save_dataset(dataset, _validate=False)
    query = SlayerQuery.model_validate({"population": "orders", "dimensions": ["orders.a"]})
    with pytest.raises(SlayerError) as queried:
        await shop.execute(query)
    assert type(queried.value) is type(saved.value)


async def test_a_field_filter_calling_agg_is_not_judged(shop: SlayerQueryEngine) -> None:
    await shop.storage.save_dataset(orders({"name": "c", "expression": "amount", "filter": "AGG(amount) > 1",
                                            "datatype": "Float"}))
    assert "c" in await stored_field_names(shop)
    resp = await shop.execute(SlayerQuery.model_validate({"population": "orders",
                                                          "dimensions": ["orders.order_id", "orders.c"]}),
                              dry_run=True)
    assert re.search(r"AGG\((orders\.)?amount\) > 1", resp.sql or ""), resp.sql


async def test_a_dialect_entry_calling_agg_is_not_judged(shop: SlayerQueryEngine) -> None:
    await shop.storage.save_dataset(orders(agg_field("e", {
        "sql": "amount", "dialects": [{"dialect": "SQLITE", "expression": "AGG(amount)"}]})))
    assert "e" in await stored_field_names(shop)
    resp = await shop.execute(SlayerQuery.model_validate({"population": "orders",
                                                          "dimensions": ["orders.order_id", "orders.e"]}),
                              dry_run=True)
    assert re.search(r"AGG\((orders\.)?amount\)", resp.sql or ""), resp.sql


async def test_a_plain_field_still_reads_its_column(shop: SlayerQueryEngine) -> None:
    await shop.storage.save_dataset(orders(agg_field("f", "amount + 1")))
    resp = await shop.execute(SlayerQuery.model_validate({"population": "orders",
                                                          "dimensions": ["orders.order_id", "orders.f"]}))
    assert {int(r["orders.order_id"]): float(r["orders.f"]) for r in resp.data} == {
        k: v + 1 for k, v in AMOUNTS.items()}
