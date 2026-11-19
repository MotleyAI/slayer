"""Stored field ``sql``, stored filters and stored ``source.`` paths keep their meaning on load; the field ``sql``
deprecation and the dumped vocabulary.

Specs: models/stored-upgrade, compatibility/removal-tripwire, compatibility/ossie-vocabulary.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import DatasetField, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests._dev2175_fixtures import (
    DS,
    PREVIOUS,
    by_id,
    datasource,
    dumped_field,
    entry,
    metric_value,
    run_statements,
    seed,
)
from tests._engine_helpers import owned_query_engine
from tests._stored_upgrade_fixtures import BACKENDS, raw_store, stored_bytes

STORES = pytest.mark.parametrize("backend", BACKENDS)
ITEMS = ('CREATE TABLE items (id INTEGER, amount INTEGER, "Label" VARCHAR, qty INTEGER)',
         "INSERT INTO items VALUES (1, 10, 'a', 4), (2, 20, 'b', 8), (3, 5, 'c', 2)")
LEADS = ("CREATE TABLE leads (id INTEGER, amount INTEGER, name VARCHAR)",
         "INSERT INTO leads VALUES (101, 7, 'L1'), (102, 9, 'L2')")


def items_v(version: int = PREVIOUS) -> dict[str, Any]:
    """``items`` as written before this change: a bare, a quoted, a derived and a sibling-reading field ``sql``."""
    return {"version": version, "name": "items", "sql_table": "items", "data_source": DS, "fields": [
        {"name": "id", "datatype": "Integer", "primary_key": True},
        {"name": "amount", "sql": "amount", "datatype": "Integer"},
        {"name": "label", "sql": '"Label"'},
        {"name": "qty", "datatype": "Integer"},
        {"name": "doubled", "sql": "amount * 2", "datatype": "Integer"},
        {"name": "ratio", "sql": "doubled / qty", "datatype": "Float"},
    ]}


@asynccontextmanager
async def store_over(
    *, backend: str, models: list[dict[str, Any]], with_datasource: bool = True, extra: tuple[str, ...] = ITEMS,
) -> AsyncGenerator[tuple[StorageBackend, str, str]]:
    """A raw ``backend`` store holding ``models`` verbatim over a seeded DuckDB file; yields (store, base, db path)."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "shop.duckdb")
        seed(db_path, dialect="duckdb")
        run_statements(db_path, dialect="duckdb", statements=extra)
        base = os.path.join(tmp, "store")
        ds = datasource(dialect="duckdb", db_path=db_path) if with_datasource else None
        yield await raw_store(backend=backend, base=base, datasource=ds, models=models), base, db_path


@asynccontextmanager
async def engine_on(storage: StorageBackend, db_path: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with owned_query_engine(storage=storage, datasources=[datasource(dialect="duckdb", db_path=db_path)]) as eng:
        yield eng


# ── stored field sql ──────────────────────────────────────────────────────

class TestStoredFieldSql:
    @STORES
    async def test_a_stored_dataset_of_every_shape_loads(self, backend: str) -> None:
        async with store_over(backend=backend, models=[items_v()]) as (storage, base, db_path):
            with recorded() as caught:
                loaded = await storage.get_dataset("items")
            assert loaded is not None
            assert dumped_field(loaded, "amount")["expression"] == "amount"
            assert dumped_field(loaded, "label")["expression"] == '"Label"'
            assert dumped_field(loaded, "doubled")["expression"] == {"dialects": [entry("DUCKDB", "amount * 2")]}
            assert dumped_field(loaded, "ratio")["expression"] == {"dialects": [entry("DUCKDB", "items.doubled / qty")]}
            assert not [w for w in caught if issubclass(w.category, FutureWarning)]
            assert f"{CURRENT_VERSIONS['SlayerModel']}" in stored_bytes(backend=backend, base=base, name="items")
            assert "items.doubled" in stored_bytes(backend=backend, base=base, name="items")
            async with engine_on(storage, db_path) as engine:
                assert await by_id(engine, field="amount", ds="items") == [10, 20, 5]
                assert await by_id(engine, field="label", ds="items") == ["a", "b", "c"]
                assert await by_id(engine, field="doubled", ds="items") == [20, 40, 10]
                assert await by_id(engine, field="ratio", ds="items") == [5.0, 5.0, 5.0]

    @STORES
    async def test_a_stored_dataset_without_its_datasource_config(self, backend: str) -> None:
        async with store_over(backend=backend, models=[items_v()], with_datasource=False) as (storage, base, db_path):
            before = stored_bytes(backend=backend, base=base, name="items")
            loaded = await storage.get_dataset("items")
            assert loaded is not None
            assert dumped_field(loaded, "amount")["expression"] == "amount"
            assert dumped_field(loaded, "label")["expression"] == '"Label"'
            assert dumped_field(loaded, "doubled").get("sql") == "amount * 2"
            assert dumped_field(loaded, "ratio").get("sql") == "doubled / qty"
            assert stored_bytes(backend=backend, base=base, name="items") == before
            await storage.save_datasource(datasource(dialect="duckdb", db_path=db_path))
            reloaded = await storage.get_dataset("items")
            assert reloaded is not None
            assert dumped_field(reloaded, "ratio")["expression"] == {
                "dialects": [entry("DUCKDB", "items.doubled / qty")]}
            assert "items.doubled" in stored_bytes(backend=backend, base=base, name="items")

    def test_direct_validation_of_a_stored_document(self) -> None:
        doc = items_v()
        doc["fields"].append({"name": "alias", "sql": "doubled"})
        loaded = SlayerDataset.model_validate(doc)
        assert dumped_field(loaded, "amount")["expression"] == "amount"
        assert dumped_field(loaded, "label")["expression"] == '"Label"'
        assert dumped_field(loaded, "alias")["expression"] == "items.doubled"
        assert dumped_field(loaded, "doubled").get("sql") == "amount * 2"
        assert "expression" not in dumped_field(loaded, "doubled")


# ── stored filters ────────────────────────────────────────────────────────

def orders_v(*, version: int = PREVIOUS, **extra: Any) -> dict[str, Any]:
    return {"version": version, "name": "orders", "sql_table": "orders", "data_source": DS, **extra}


@STORES
async def test_stored_filters_keep_their_sibling_references(backend: str) -> None:
    doc = orders_v(fields=[{"name": "id", "datatype": "Integer", "primary_key": True},
                           {"name": "status", "sql": "state"},
                           {"name": "paid_amount", "sql": "amount", "filter": "status = 'paid'", "datatype": "Integer"}],
                   filters=["status <> 'x'"])
    async with store_over(backend=backend, models=[doc]) as (storage, _, db_path):
        loaded = await storage.get_dataset("orders")
        assert loaded is not None
        assert dumped_field(loaded, "paid_amount")["filter"] == "orders.status = 'paid'"
        assert loaded.filters == ["orders.status <> 'x'"]
        async with engine_on(storage, db_path) as engine:
            assert await by_id(engine, field="paid_amount") == [None, 20]


# ── stored source-qualified paths ─────────────────────────────────────────

def _orders_to_leads(*relationships: str, version: int, **extra: Any) -> dict[str, Any]:
    return orders_v(version=version, fields=[{"name": "id", "datatype": "Integer", "primary_key": True},
                                             {"name": "customer_id", "datatype": "Integer"},
                                             {"name": "amount", "datatype": "Integer"}],
                    relationships=[{"name": r, "to": "leads", "from_columns": ["customer_id"], "to_columns": ["id"],
                                    "cardinality": "many_to_one"} for r in relationships], **extra)


def _leads(version: int) -> dict[str, Any]:
    return {"version": version, "name": "leads", "sql_table": "leads", "data_source": DS, "fields": [
        {"name": "id", "datatype": "Integer", "primary_key": True}, {"name": "amount", "datatype": "Integer"},
        {"name": "name"}]}


class TestStoredSourcePaths:
    @STORES
    async def test_a_relationship_named_source(self, backend: str) -> None:
        orders = _orders_to_leads("source", version=PREVIOUS,
                                  metrics=[{"name": "lead_total", "expression": "sum(source.amount)"}])
        async with store_over(backend=backend, models=[_leads(PREVIOUS), orders], extra=ITEMS + LEADS) as (
                storage, _, db_path):
            loaded = await storage.get_dataset("orders")
            assert loaded is not None
            (metric,) = loaded.metrics
            assert metric.expression == "sum(leads.amount)"
            async with engine_on(storage, db_path) as engine:
                assert await metric_value(engine, metric="lead_total") == 16

    @STORES
    async def test_no_unique_spelling(self, backend: str) -> None:
        orders = _orders_to_leads("source", "referrer", version=PREVIOUS)
        orders["fields"].append({"name": "lead_name", "sql": "source.name"})
        async with store_over(backend=backend, models=[_leads(PREVIOUS), orders], extra=ITEMS + LEADS) as (
                storage, base, _):
            before = stored_bytes(backend=backend, base=base, name="orders")
            loaded, unloaded = await storage.load_datasets(data_source=DS)
            assert stored_bytes(backend=backend, base=base, name="orders") == before
        assert [d.name for d in loaded] == ["leads"]
        (failure,) = unloaded
        assert "orders" in str(failure) and "lead_name" in str(failure)

    @STORES
    async def test_a_current_document_is_not_rewritten(self, backend: str) -> None:
        current = CURRENT_VERSIONS["SlayerModel"]
        orders = _orders_to_leads("source", version=current,
                                  metrics=[{"name": "own_total", "expression": "sum(source.amount)"}])
        async with store_over(backend=backend, models=[_leads(current), orders], extra=ITEMS + LEADS) as (
                storage, _, db_path):
            async with engine_on(storage, db_path) as engine:
                assert await metric_value(engine, metric="own_total") == 35


# ── the deprecation marker and the dumped vocabulary ──────────────────────

def test_field_sql_is_a_marked_deprecated_input() -> None:
    with recorded() as caught:
        DatasetField(name="doubled", sql="amount * 2")
    (warning,) = deprecations(caught)
    payload = payload_of(warning)
    assert isinstance(warning.message, DeprecatedInputFutureWarning)
    assert "sql" in payload.old and "expression" in payload.new and payload.removal == "3.0"
    assert warning.filename == __file__


def test_a_dataset_is_dumped_without_a_field_level_sql() -> None:
    dumped = SlayerDataset.model_validate({
        "name": "orders", "sql_table": "orders", "data_source": DS,
        "fields": [{"name": "id", "primary_key": True}, {"name": "double", "expression": "amount * 2"}],
        "metrics": [{"name": "total", "expression": "sum(amount)"}]}).model_dump(mode="json", exclude_none=True)
    assert [f.get("expression") for f in dumped["fields"]] == [None, "amount * 2"]
    assert not [f for f in dumped["fields"] if "sql" in f]
    assert dumped["metrics"][0]["expression"] == "sum(amount)"
