"""Stored AGG calls in field expressions are unwrapped (spec: models/stored-upgrade)."""

from __future__ import annotations

import logging
import re
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig, ExpressionObject, SlayerDataset
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._stored_upgrade_fixtures import BACKENDS, DS, field_of, raw_store, run, seed_shop

#: The SlayerModel version before this change, and the one before the ``agg`` upgrade.
PREVIOUS = 29
BEFORE_AGG_UPGRADE = 19
#: ``amount`` per ``order_id`` of the shop seed.
AMOUNTS = {1: 10.0, 2: 20.0, 3: 30.0, 4: 40.0, 5: 50.0, 6: 60.0}
_FIELD_A = re.compile(r"['\"]a['\"]")


def orders_doc(version: int, *extra_fields: dict[str, Any], **extra: Any) -> dict[str, Any]:
    """A stored ``orders`` dataset in the shape its ``version`` wrote."""
    if version < 26:
        base = [{"name": "order_id", "type": "INT", "primary_key": True}, {"name": "amount", "type": "DOUBLE"}]
        return {"version": version, "name": "orders", "sql_table": "orders", "data_source": DS,
                "columns": [*base, *extra_fields], **extra}
    base = [{"name": "order_id", "datatype": "Integer", "primary_key": True}, {"name": "amount", "datatype": "Float"}]
    return {"version": version, "name": "orders", "sql_table": "orders", "data_source": DS,
            "fields": [*base, *extra_fields], **extra}


def field_a(version: int, expression: Any, **extra: Any) -> dict[str, Any]:
    return {"name": "a", ("type" if version < 26 else "datatype"): ("DOUBLE" if version < 26 else "Float"),
            "expression": expression, **extra}


def expression_text(expression: Any) -> Any:
    return expression.sql if isinstance(expression, ExpressionObject) else expression


@pytest.fixture
def shop_ds(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    ds = DatasourceConfig.model_validate({"name": DS, "type": "sqlite", "database": db})
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


async def store(tmp_path: Path, ds: DatasourceConfig, *, backend: str, doc: dict[str, Any]) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / f"store-{backend}"), datasource=ds, models=[doc])


def agg_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    texts = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    return [t for t in texts if "orders" in t and _FIELD_A.search(t)]


def rows(resp: Any, *, column: str) -> dict[int, float]:
    return {int(r["orders.order_id"]): float(r[column]) for r in resp.data}


def test_the_dataset_version_is_bumped() -> None:
    assert CURRENT_VERSIONS["SlayerModel"] == 30


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_stored_agg_field_is_unwrapped_with_one_warning(tmp_path: Path, shop_ds: DatasourceConfig,
                                                               backend: str, caplog: pytest.LogCaptureFixture) -> None:
    storage = await store(tmp_path, shop_ds, backend=backend, doc=orders_doc(PREVIOUS, field_a(PREVIOUS, "AGG(amount) + 1")))
    with caplog.at_level(logging.WARNING):
        model = await storage.get_dataset("orders", data_source=DS)
    assert model is not None
    assert expression_text(field_of(model, "a").expression) == "amount + 1"
    assert len(agg_warnings(caplog)) == 1, [r.getMessage() for r in caplog.records]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_stored_agg_field_keeps_its_value(tmp_path: Path, shop_ds: DatasourceConfig, backend: str) -> None:
    storage = await store(tmp_path, shop_ds, backend=backend, doc=orders_doc(PREVIOUS, field_a(PREVIOUS, "AGG(amount) + 1")))
    resp = await run(storage, {"population": "orders", "dimensions": ["orders.order_id", "orders.a"]})
    assert rows(resp, column="orders.a") == {k: v + 1 for k, v in AMOUNTS.items()}


@pytest.mark.parametrize(("version", "stored", "upgraded"), [
    (25, "AGG(amount) + 1", "amount + 1"),
    (27, "agg(amount) * 2", "amount * 2"),
    (28, "AGG(amount) - 1", "amount - 1"),
    (PREVIOUS, "Agg(amount)", "amount"),
    (PREVIOUS, "AGG(amount) + LENGTH('AGG(x)')", "amount + LENGTH('AGG(x)')"),
    (PREVIOUS, {"sql": "AGG(amount) + 1"}, "amount + 1"),
    (PREVIOUS, "AGG /* ( */ (amount) + 1", "amount + 1"),
    (PREVIOUS, '"AGG"(amount) + 1', "amount + 1"),
], ids=["v25", "v27-lower", "v28-chain", "v29-mixed-case", "v29-quoted", "v29-object", "v29-comment", "v29-quoted-callee"])
def test_a_field_agg_from_v20_to_v29_is_unwrapped(version: int, stored: Any, upgraded: str) -> None:
    model = SlayerDataset.model_validate(orders_doc(version, field_a(version, stored)))
    assert expression_text(field_of(model, "a").expression) == upgraded


def test_a_portable_dialect_entry_is_unwrapped() -> None:
    stored = {"dialects": [{"dialect": "OSSIE_SQL_2026", "expression": "AGG(amount) + 1"},
                           {"dialect": "SQLITE", "expression": "AGG(amount)"}]}
    entry = field_of(SlayerDataset.model_validate(orders_doc(PREVIOUS, field_a(PREVIOUS, stored))), "a").expression
    assert isinstance(entry, ExpressionObject)
    assert [d.expression for d in entry.dialects or []] == ["amount + 1", "AGG(amount)"]


def test_a_metric_agg_from_before_the_agg_upgrade_stays_renamed() -> None:
    model = SlayerDataset.model_validate(orders_doc(BEFORE_AGG_UPGRADE,
                                                    measures=[{"name": "m", "formula": "agg(amount)"}]))
    assert [m.expression for m in model.metrics] == ["agg_aggregation(amount)"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_custom_agg_aggregation_from_before_the_agg_upgrade(tmp_path: Path, shop_ds: DatasourceConfig,
                                                                    backend: str) -> None:
    doc = orders_doc(BEFORE_AGG_UPGRADE, aggregations=[{"name": "agg", "formula": "SUM({value}) * 2"}],
                     measures=[{"name": "d", "formula": "agg(amount)"}])
    storage = await store(tmp_path, shop_ds, backend=backend, doc=doc)
    model = await storage.get_dataset("orders", data_source=DS)
    assert model is not None
    assert [a.name for a in model.aggregations] == ["agg_aggregation"]
    assert [m.expression for m in model.metrics] == ["agg_aggregation(amount)"]
    resp = await run(storage, {"population": "orders", "metrics": [{"metric": "orders.d", "alias": "t"}]})
    assert float(resp.data[0]["t"]) == 420.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_custom_agg_aggregation_with_an_agg_field_from_v25(tmp_path: Path, shop_ds: DatasourceConfig,
                                                                   backend: str) -> None:
    doc = orders_doc(25, field_a(25, "AGG(amount) + 1"),
                     aggregations=[{"name": "agg_aggregation", "formula": "SUM({value}) * 2"}],
                     measures=[{"name": "d", "formula": "agg_aggregation(amount)"}])
    storage = await store(tmp_path, shop_ds, backend=backend, doc=doc)
    model = await storage.get_dataset("orders", data_source=DS)
    assert model is not None
    assert expression_text(field_of(model, "a").expression) == "amount + 1"
    assert [a.name for a in model.aggregations] == ["agg_aggregation"]
    assert [m.expression for m in model.metrics] == ["agg_aggregation(amount)"]
    resp = await run(storage, {"population": "orders", "metrics": [{"metric": "orders.d", "alias": "t"}]})
    assert float(resp.data[0]["t"]) == 420.0


def test_mode_a_text_is_left_unchanged(caplog: pytest.LogCaptureFixture) -> None:
    doc = orders_doc(
        PREVIOUS,
        {"name": "f", "datatype": "Float", "expression": "amount", "filter": "AGG(amount) > 1"},
        {"name": "e", "datatype": "Float",
         "expression": {"sql": "amount", "dialects": [{"dialect": "SQLITE", "expression": "AGG(amount)"}]}},
        filters=["AGG(amount) > 0"],
    )
    with caplog.at_level(logging.WARNING):
        model = SlayerDataset.model_validate(doc)
    assert field_of(model, "f").filter == "AGG(amount) > 1"
    entry = field_of(model, "e").expression
    assert isinstance(entry, ExpressionObject)
    assert [d.expression for d in entry.dialects or []] == ["AGG(amount)"]
    assert model.filters == ["AGG(amount) > 0"]
    assert [r.getMessage() for r in caplog.records if "AGG" in r.getMessage()] == []


def test_a_fresh_payload_is_not_unwrapped() -> None:
    doc = orders_doc(PREVIOUS, field_a(PREVIOUS, "AGG(amount) + 1"))
    del doc["version"]
    model = SlayerDataset.model_validate(doc)
    assert expression_text(field_of(model, "a").expression) == "AGG(amount) + 1"
