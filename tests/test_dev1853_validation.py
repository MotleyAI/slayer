"""DEV-1853 — ``ModelJoin.name`` rules and save-time join validation.

Save-time: edge names must not collide with datasource model names or with
another edge name incident to either endpoint; unnamed parallel edges warn;
declaring the exact inverse of an existing edge is rejected (reverse
traversal is automatic).
"""

from __future__ import annotations

import tempfile
import warnings

import pytest

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage


def _model(name: str, cols: list[str], joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="ds", sql_table=name,
        columns=[Column(name=c) for c in cols],
        joins=joins or [],
    )


class TestModelJoinNameField:
    def test_valid_name_accepted(self) -> None:
        join = ModelJoin(target_model="customers",
                         join_pairs=[["customer_id", "id"]],
                         name="billing_customer")
        assert join.name == "billing_customer"

    def test_default_is_unnamed(self) -> None:
        join = ModelJoin(target_model="customers",
                         join_pairs=[["customer_id", "id"]])
        assert join.name is None

    @pytest.mark.parametrize("bad", ["a.b", "a:b", "__slayer_x"])
    def test_model_name_identifier_rules_apply(self, bad: str) -> None:
        with pytest.raises(ValueError):
            ModelJoin(target_model="customers",
                      join_pairs=[["customer_id", "id"]], name=bad)


async def _storage(tmpdir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    return storage


class TestSaveTimeEdgeNames:
    async def test_name_colliding_with_model_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("customers", ["id"]))
            await storage.save_model(_model("regions", ["id"]))
            orders = _model("orders", ["id", "customer_id"], [
                ModelJoin(target_model="customers",
                          join_pairs=[["customer_id", "id"]], name="regions"),
            ])
            with pytest.raises(ValueError, match="regions"):
                await storage.save_model(orders)

    async def test_duplicate_name_on_one_model_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("customers", ["id"]))
            orders = _model("orders", ["id", "a_id", "b_id"], [
                ModelJoin(target_model="customers",
                          join_pairs=[["a_id", "id"]], name="bill"),
                ModelJoin(target_model="customers",
                          join_pairs=[["b_id", "id"]], name="bill"),
            ])
            with pytest.raises(ValueError, match="bill"):
                await storage.save_model(orders)

    async def test_duplicate_name_incident_to_shared_endpoint_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("regions", ["id"]))
            await storage.save_model(_model("customers", ["id", "region_id"], [
                ModelJoin(target_model="regions",
                          join_pairs=[["region_id", "id"]], name="hop"),
            ]))
            # Both edges are incident to customers — the shared name is ambiguous there.
            orders = _model("orders", ["id", "customer_id"], [
                ModelJoin(target_model="customers",
                          join_pairs=[["customer_id", "id"]], name="hop"),
            ])
            with pytest.raises(ValueError, match="hop"):
                await storage.save_model(orders)

    async def test_unnamed_parallel_edges_warn_but_save(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("customers", ["id"]))
            orders = _model("orders", ["id", "a_id", "b_id"], [
                ModelJoin(target_model="customers", join_pairs=[["a_id", "id"]]),
                ModelJoin(target_model="customers", join_pairs=[["b_id", "id"]]),
            ])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await storage.save_model(orders)
            texts = [str(w.message) for w in caught]
            assert any("customers" in t and "disambiguat" in t.lower()
                       or "customers" in t and "ambiguous" in t.lower()
                       for t in texts), texts
            assert await storage.get_model("orders", data_source="ds") is not None

    async def test_named_parallel_edges_do_not_warn(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("customers", ["id"]))
            orders = _model("orders", ["id", "a_id", "b_id"], [
                ModelJoin(target_model="customers",
                          join_pairs=[["a_id", "id"]], name="bill"),
                ModelJoin(target_model="customers",
                          join_pairs=[["b_id", "id"]], name="ship"),
            ])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await storage.save_model(orders)
            assert not [w for w in caught
                        if "disambiguat" in str(w.message).lower()
                        or "ambiguous" in str(w.message).lower()]


class TestModelNameVsEdgeNameCollision:
    async def test_model_named_like_an_existing_edge_rejected(self) -> None:
        # The symmetric direction of the edge-name/model-name namespace rule:
        # once an edge is named "billing", a MODEL named "billing" would make
        # every "billing" path token resolve to the edge, silently shadowing.
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_model(_model("customers", ["id"]))
            await storage.save_model(_model("orders", ["id", "customer_id"], [
                ModelJoin(target_model="customers",
                          join_pairs=[["customer_id", "id"]], name="billing"),
            ]))
            with pytest.raises(ValueError, match="billing"):
                await storage.save_model(_model("billing", ["id"]))


class TestSaveTimeExactInverse:
    async def _seed_forward(self, tmpdir: str) -> YAMLStorage:
        storage = await _storage(tmpdir)
        await storage.save_model(_model("customers", ["id"]))
        await storage.save_model(_model("orders", ["id", "customer_id"], [
            ModelJoin(target_model="customers",
                      join_pairs=[["customer_id", "id"]],
                      join_type=JoinType.INNER,
                      cardinality=JoinCardinality.MANY_TO_ONE),
        ]))
        return storage

    async def test_exact_inverse_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            customers = _model("customers", ["id"], [
                ModelJoin(target_model="orders",
                          join_pairs=[["id", "customer_id"]],
                          join_type=JoinType.INNER,
                          cardinality=JoinCardinality.ONE_TO_MANY),
            ])
            with pytest.raises(ValueError, match="automatic"):
                await storage.save_model(customers)

    async def test_inverse_with_unset_cardinality_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            customers = _model("customers", ["id"], [
                ModelJoin(target_model="orders",
                          join_pairs=[["id", "customer_id"]],
                          join_type=JoinType.INNER),
            ])
            with pytest.raises(ValueError, match="automatic"):
                await storage.save_model(customers)

    async def test_non_inverse_pair_saves(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            # Different pair set — a deliberate second edge, not a mirror.
            customers = _model("customers", ["id", "rep_order_id"], [
                ModelJoin(target_model="orders",
                          join_pairs=[["rep_order_id", "id"]],
                          join_type=JoinType.INNER),
            ])
            await storage.save_model(customers)
            loaded = await storage.get_model("customers", data_source="ds")
            assert loaded is not None and len(loaded.joins) == 1
