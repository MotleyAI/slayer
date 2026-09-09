"""DEV-1853 — the SlayerModel v9 → v10 exact-inverse dedup migration.

The registered v9→v10 converter is a per-doc no-op; the cross-document dedup
runs in the load path against raw peer documents. Keep rule: the to-one side,
else the cardinality-carrying side, else lexicographic ``(model, target)``;
order-independent, idempotent, persisted; non-exact pairs are kept whole.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile

import pytest
import yaml

from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.join_walker import edges_between, resolve_hop
from slayer.core.models import DatasourceConfig
from slayer.storage import migrations as mig
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def test_v10_is_current_and_registered() -> None:
    assert mig.CURRENT_VERSIONS["SlayerModel"] >= 10
    assert ("SlayerModel", 9) in mig._REGISTRY


def test_v9_to_v10_step_is_a_no_op_forward() -> None:
    step = mig._REGISTRY[("SlayerModel", 9)]
    payload = {
        "version": 9, "name": "orders", "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "id", "type": "INT", "primary_key": True}],
        "joins": [{"target_model": "customers",
                   "join_pairs": [["customer_id", "id"]]}],
    }
    out = step(dict(payload))
    assert out["joins"] == payload["joins"]


def _customers_v9(joins: list[dict] | None = None) -> dict:
    return {
        "version": 9, "name": "customers", "sql_table": "customers",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "name", "type": "TEXT"},
        ],
        "joins": joins or [],
    }


def _orders_v9(joins: list[dict] | None = None) -> dict:
    return {
        "version": 9, "name": "orders", "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "customer_id", "type": "INT"},
        ],
        "joins": joins or [],
    }


def _fwd(*, cardinality: str | None = "many_to_one",
         join_type: str = "inner") -> dict:
    join = {"target_model": "customers",
            "join_pairs": [["customer_id", "id"]], "join_type": join_type}
    if cardinality:
        join["cardinality"] = cardinality
    return join


def _rev(*, cardinality: str | None = "one_to_many",
         join_type: str = "inner",
         pairs: list[list[str]] | None = None) -> dict:
    join = {"target_model": "orders",
            "join_pairs": pairs or [["id", "customer_id"]],
            "join_type": join_type}
    if cardinality:
        join["cardinality"] = cardinality
    return join


async def _seed_yaml(tmpdir: str, payloads: list[dict]) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    for p in payloads:
        path = os.path.join(tmpdir, "models", "ds", f"{p['name']}.yaml")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:  # NOSONAR(S7493) — test seed
            yaml.dump(p, f, sort_keys=False)
    return storage


async def _seed_sqlite(db_path: str, payloads: list[dict]) -> SQLiteStorage:
    storage = SQLiteStorage(db_path=db_path)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    with sqlite3.connect(db_path) as conn:
        for p in payloads:
            conn.execute(
                "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                ("ds", p["name"], json.dumps(p)),
            )
    return storage


async def _joins_after_load(storage) -> tuple[list, list]:
    orders = await storage.get_model("orders", data_source="ds")
    customers = await storage.get_model("customers", data_source="ds")
    assert orders is not None
    assert customers is not None
    return orders.joins, customers.joins


class TestMirroredPairCollapses:
    async def test_yaml_keeps_the_to_one_side(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(
                d, [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert orders_joins[0].target_model == "customers"
            assert customers_joins == []

    async def test_survivor_traverses_both_ways(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(
                d, [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            orders = await storage.get_model("orders", data_source="ds")
            customers = await storage.get_model("customers", data_source="ds")
            assert orders is not None
            assert customers is not None
            assert len(edges_between(source=orders, target=customers)) == 1
            assert len(edges_between(source=customers, target=orders)) == 1

    async def test_sqlite_keeps_the_to_one_side(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_sqlite(
                f"{d}/s.db", [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert customers_joins == []

    async def test_load_order_does_not_change_the_outcome(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(
                d, [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            customers = await storage.get_model("customers", data_source="ds")
            assert customers is not None
            assert customers.joins == []
            orders = await storage.get_model("orders", data_source="ds")
            assert orders is not None
            assert len(orders.joins) == 1

    async def test_dedup_is_persisted_and_version_bumped(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(
                d, [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            await _joins_after_load(storage)
            on_disk = yaml.safe_load(
                open(os.path.join(d, "models", "ds", "customers.yaml")).read())
            assert not on_disk.get("joins")
            assert on_disk["version"] == mig.CURRENT_VERSIONS["SlayerModel"]

    async def test_repeated_loads_change_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(
                d, [_orders_v9([_fwd()]), _customers_v9([_rev()])])
            first = await _joins_after_load(storage)
            second = await _joins_after_load(storage)
            assert [len(j) for j in first] == [len(j) for j in second] == [1, 0]


class TestKeepRuleTiebreaks:
    async def test_cardinality_carrying_side_wins(self) -> None:
        # Neither side is to-one; only customers carries a cardinality.
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd(cardinality=None)]),
                _customers_v9([_rev(cardinality="one_to_many")]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert orders_joins == []
            assert len(customers_joins) == 1

    async def test_lexicographic_tiebreak_when_neither_carries(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd(cardinality=None)]),
                _customers_v9([_rev(cardinality=None)]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            # ("customers", "orders") < ("orders", "customers")
            assert len(customers_joins) == 1
            assert orders_joins == []


class TestNonExactPairsAreKept:
    async def test_drifted_pairs_both_survive_and_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd()]),
                _customers_v9([_rev(pairs=[["name", "customer_id"]],
                                    cardinality=None)]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1
            orders = await storage.get_model("orders", data_source="ds")
            customers = await storage.get_model("customers", data_source="ds")
            assert orders is not None
            assert customers is not None
            models = {"orders": orders, "customers": customers}
            with pytest.raises(AmbiguousJoinPathError):
                resolve_hop(current=customers, token="orders",
                            models_by_name=models)

    async def test_different_join_types_both_survive(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd(join_type="inner")]),
                _customers_v9([_rev(join_type="left")]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1

    async def test_inversion_inconsistent_cardinalities_both_survive(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd(cardinality="many_to_one")]),
                _customers_v9([_rev(cardinality="many_to_one")]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1


class TestMissingPeer:
    async def test_absent_counterpart_leaves_the_document_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [_orders_v9([_fwd()])])
            orders = await storage.get_model("orders", data_source="ds")
            assert orders is not None
            assert len(orders.joins) == 1
            assert orders.version == mig.CURRENT_VERSIONS["SlayerModel"]


class TestNamedMirrorsSurvive:
    async def test_one_sided_name_keeps_both_halves(self) -> None:
        # The discarded half's name would be an unresolvable token — no dedup.
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd()]),
                _customers_v9([{**_rev(), "name": "back"}]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1
            assert customers_joins[0].name == "back"

    async def test_one_sided_name_both_load_orders(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd()]),
                _customers_v9([{**_rev(), "name": "back"}]),
            ])
            customers = await storage.get_model("customers", data_source="ds")
            orders = await storage.get_model("orders", data_source="ds")
            assert customers is not None and len(customers.joins) == 1
            assert orders is not None and len(orders.joins) == 1

    async def test_one_sided_name_repeated_loads_change_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([_fwd()]),
                _customers_v9([{**_rev(), "name": "back"}]),
            ])
            await _joins_after_load(storage)
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1

    async def test_differing_names_keep_both_halves(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([{**_fwd(), "name": "fwd"}]),
                _customers_v9([{**_rev(), "name": "back"}]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert len(customers_joins) == 1

    async def test_equal_names_still_dedup_and_keep_the_token(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _seed_yaml(d, [
                _orders_v9([{**_fwd(), "name": "link"}]),
                _customers_v9([{**_rev(), "name": "link"}]),
            ])
            orders_joins, customers_joins = await _joins_after_load(storage)
            assert len(orders_joins) == 1
            assert orders_joins[0].name == "link"
            assert customers_joins == []
