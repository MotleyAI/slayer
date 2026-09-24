"""SlayerModel v10 → v11: stored physical spellings of renamed join keys are
canonicalised to ``Column.name`` on load (both sides, before the exact-inverse
dedup) and written back; YAML and SQLite backends."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
import yaml

from slayer.core.join_walker import edges_between
from slayer.core.models import DatasourceConfig
from slayer.storage import migrations as mig
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

Seeder = Callable[[list[dict]], Awaitable[StorageBackend]]


def test_v11_is_current_and_registered() -> None:
    assert mig.CURRENT_VERSIONS["SlayerModel"] >= 11
    assert ("SlayerModel", 10) in mig._REGISTRY


def _customers(*, pk_sql: str | None = "customer_pk", joins: list[dict] | None = None) -> dict:
    pk = {"name": "id", "type": "INT", "primary_key": True}
    if pk_sql is not None:
        pk["sql"] = pk_sql
    return {
        "version": 10, "name": "customers", "sql_table": "customers",
        "data_source": "ds",
        "columns": [pk, {"name": "name", "type": "TEXT"}],
        "joins": joins or [],
    }


def _orders(*, fk_sql: str | None = "cust_fk", joins: list[dict] | None = None) -> dict:
    fk = {"name": "customer_id", "type": "INT"}
    if fk_sql is not None:
        fk["sql"] = fk_sql
    return {
        "version": 10, "name": "orders", "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "id", "type": "INT", "primary_key": True}, fk],
        "joins": joins or [],
    }


def _join(target: str, pairs: list[list[str]], **extra) -> dict:
    return {"target_model": target, "join_pairs": pairs, **extra}


@pytest.fixture(params=["yaml", "sqlite"])
async def seed(request) -> AsyncIterator[Seeder]:
    with tempfile.TemporaryDirectory() as tmp:
        async def _seed(payloads: list[dict]) -> StorageBackend:
            if request.param == "yaml":
                storage: StorageBackend = YAMLStorage(base_dir=tmp)
                await storage.save_datasource(
                    DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
                for p in payloads:
                    path = os.path.join(tmp, "models", "ds", f"{p['name']}.yaml")
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "w") as f:  # NOSONAR(S7493) — test seed
                        yaml.dump(p, f, sort_keys=False)
                return storage
            db_path = os.path.join(tmp, "store.db")
            storage = SQLiteStorage(db_path=db_path)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
            with transaction(db_path) as conn:
                for p in payloads:
                    conn.execute(
                        "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                        ("ds", p["name"], json.dumps(p)))
            return storage
        yield _seed


async def _stored(storage: StorageBackend, name: str) -> dict:
    raw = await storage._load_raw_model_dict(name=name, data_source="ds")
    assert raw is not None
    return raw


class TestCanonicalisedOnLoad:
    async def test_source_side_physical_spelling_rewritten(self, seed: Seeder):
        storage = await seed([
            _orders(joins=[_join("customers", [["cust_fk", "id"]])]),
            _customers()])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]
        stored = await _stored(storage, "orders")
        assert stored["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert stored["joins"][0]["join_pairs"] == [["customer_id", "id"]]

    async def test_target_side_physical_spelling_rewritten(self, seed: Seeder):
        storage = await seed([
            _orders(joins=[_join("customers", [["customer_id", "customer_pk"]])]),
            _customers()])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]

    async def test_both_sides_rewritten_together(self, seed: Seeder):
        storage = await seed([
            _orders(joins=[_join("customers", [["cust_fk", "customer_pk"]])]),
            _customers()])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]

    async def test_quoted_rename_rewritten(self, seed: Seeder):
        storage = await seed([
            _orders(fk_sql='"CustFK"', joins=[_join("customers", [["CustFK", "id"]])]),
            _customers()])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]

    @pytest.mark.parametrize("first", ["orders", "customers"])
    async def test_mirror_pair_spelled_two_ways_collapses(self, seed: Seeder, first: str):
        storage = await seed([
            _orders(fk_sql=None, joins=[_join(
                "customers", [["customer_id", "customer_pk"]],
                cardinality="many_to_one", join_type="inner")]),
            _customers(joins=[_join(
                "orders", [["id", "customer_id"]],
                cardinality="one_to_many", join_type="inner")])])
        await storage.get_model(first, data_source="ds")
        orders = await storage.get_model("orders", data_source="ds")
        customers = await storage.get_model("customers", data_source="ds")
        assert orders is not None
        assert customers is not None
        assert len(edges_between(source=orders, target=customers)) == 1
        assert orders.joins[0].join_pairs == [["customer_id", "id"]]
        assert customers.joins == []


class TestLeftAlone:
    @pytest.mark.parametrize("spelled", [True, False])
    async def test_name_equal_sql_store_untouched_but_version(self, seed: Seeder, spelled: bool):
        joins = [_join("customers", [["customer_id", "id"]])]
        storage = await seed([
            _orders(fk_sql="customer_id" if spelled else None, joins=joins),
            _customers(pk_sql="id" if spelled else None)])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        stored = await _stored(storage, "orders")
        assert stored["version"] == mig.CURRENT_VERSIONS["SlayerModel"]
        assert stored["joins"][0]["join_pairs"] == [["customer_id", "id"]]

    async def test_declared_name_wins_over_a_rename(self, seed: Seeder):
        # ``cust_fk`` is both a declared name and another column's rename.
        doc = _orders(joins=[_join("customers", [["cust_fk", "id"]])])
        doc["columns"].append({"name": "cust_fk", "type": "INT", "sql": "legacy_fk"})
        storage = await seed([doc, _customers()])
        orders = await storage.get_model("orders", data_source="ds")
        assert orders is not None
        assert orders.joins[0].join_pairs == [["cust_fk", "id"]]

    async def test_ambiguous_rename_left_for_validation(self, seed: Seeder):
        doc = _orders(joins=[_join("customers", [["cust_fk", "id"]])])
        doc["columns"].append({"name": "customer_ref", "type": "INT", "sql": "cust_fk"})
        storage = await seed([doc, _customers()])
        with pytest.raises(ValueError, match="key 'cust_fk'"):
            await storage.get_model("orders", data_source="ds")

    async def test_unmatched_entry_fails_while_siblings_load(self, seed: Seeder):
        storage = await seed([
            _orders(joins=[_join("customers", [["nope", "id"]])]),
            _customers()])
        with pytest.raises(ValueError) as exc:
            await storage.get_model("orders", data_source="ds")
        assert "key 'nope'" in str(exc.value)
        assert "declare" in str(exc.value).lower()
        assert await storage.get_model("customers", data_source="ds") is not None
