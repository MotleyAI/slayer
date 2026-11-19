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

from slayer.core.enums import RelationshipCardinality, JoinType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetRelationship, SlayerDataset
from slayer.storage.yaml_storage import YAMLStorage


def _dataset(name: str, cols: list[str], joins: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="ds", sql_table=name,
        fields=[DatasetField(name=c) for c in cols],
        relationships=joins or [],
    )


class TestModelJoinNameField:
    def test_valid_name_accepted(self) -> None:
        join = DatasetRelationship(to="customers",
                         from_columns=["customer_id"], to_columns=["id"],
                         name="billing_customer")
        assert join.name == "billing_customer"

    def test_default_is_unnamed(self) -> None:
        join = DatasetRelationship(to="customers",
                         from_columns=["customer_id"], to_columns=["id"])
        assert join.name is None

    @pytest.mark.parametrize("bad", ["a.b", "a:b", "__slayer_x"])
    def test_model_name_identifier_rules_apply(self, bad: str) -> None:
        with pytest.raises(ValueError):
            DatasetRelationship(to="customers",
                      from_columns=["customer_id"], to_columns=["id"], name=bad)


async def _storage(tmpdir: str) -> YAMLStorage:
    storage = YAMLStorage(base_dir=tmpdir)
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    return storage


class TestSaveTimeEdgeNames:
    async def test_name_colliding_with_model_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("customers", ["id"]))
            await storage.save_dataset(_dataset("regions", ["id"]))
            orders = _dataset("orders", ["id", "customer_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["customer_id"], to_columns=["id"], name="regions"),
            ])
            with pytest.raises(ValueError, match="regions"):
                await storage.save_dataset(orders)

    async def test_duplicate_name_on_one_model_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("customers", ["id"]))
            orders = _dataset("orders", ["id", "a_id", "b_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["a_id"], to_columns=["id"], name="bill"),
                DatasetRelationship(to="customers",
                          from_columns=["b_id"], to_columns=["id"], name="bill"),
            ])
            with pytest.raises(ValueError, match="bill"):
                await storage.save_dataset(orders)

    async def test_duplicate_name_incident_to_shared_endpoint_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("regions", ["id"]))
            await storage.save_dataset(_dataset("customers", ["id", "region_id"], [
                DatasetRelationship(to="regions",
                          from_columns=["region_id"], to_columns=["id"], name="hop"),
            ]))
            # Both edges are incident to customers — the shared name is ambiguous there.
            orders = _dataset("orders", ["id", "customer_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["customer_id"], to_columns=["id"], name="hop"),
            ])
            with pytest.raises(ValueError, match="hop"):
                await storage.save_dataset(orders)

    async def test_unnamed_parallel_edges_warn_but_save(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("customers", ["id"]))
            orders = _dataset("orders", ["id", "a_id", "b_id"], [
                DatasetRelationship(to="customers", from_columns=["a_id"], to_columns=["id"]),
                DatasetRelationship(to="customers", from_columns=["b_id"], to_columns=["id"]),
            ])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await storage.save_dataset(orders)
            texts = [str(w.message) for w in caught]
            assert any("customers" in t and "disambiguat" in t.lower()
                       or "customers" in t and "ambiguous" in t.lower()
                       for t in texts), texts
            assert await storage.get_dataset("orders", data_source="ds") is not None

    async def test_half_named_parallel_pair_warns(self) -> None:
        # One named + one unnamed edge: the unnamed one is unaddressable
        # (bare token ambiguous, no name) — the save must say so.
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("customers", ["id"]))
            orders = _dataset("orders", ["id", "a_id", "b_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["a_id"], to_columns=["id"], name="bill"),
                DatasetRelationship(to="customers", from_columns=["b_id"], to_columns=["id"]),
            ])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await storage.save_dataset(orders)
            texts = [str(w.message) for w in caught]
            assert any("customers" in t and "ambiguous" in t.lower()
                       for t in texts), texts

    async def test_named_parallel_edges_do_not_warn(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await _storage(d)
            await storage.save_dataset(_dataset("customers", ["id"]))
            orders = _dataset("orders", ["id", "a_id", "b_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["a_id"], to_columns=["id"], name="bill"),
                DatasetRelationship(to="customers",
                          from_columns=["b_id"], to_columns=["id"], name="ship"),
            ])
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                await storage.save_dataset(orders)
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
            await storage.save_dataset(_dataset("customers", ["id"]))
            await storage.save_dataset(_dataset("orders", ["id", "customer_id"], [
                DatasetRelationship(to="customers",
                          from_columns=["customer_id"], to_columns=["id"], name="billing"),
            ]))
            clashing = _dataset("billing", ["id"])
            with pytest.raises(ValueError, match="billing"):
                await storage.save_dataset(clashing)


class TestSaveTimeExactInverse:
    async def _seed_forward(self, tmpdir: str) -> YAMLStorage:
        storage = await _storage(tmpdir)
        await storage.save_dataset(_dataset("customers", ["id"]))
        await storage.save_dataset(_dataset("orders", ["id", "customer_id"], [
            DatasetRelationship(to="customers",
                      from_columns=["customer_id"], to_columns=["id"],
                      join_type=JoinType.INNER,
                      cardinality=RelationshipCardinality.MANY_TO_ONE),
        ]))
        return storage

    async def test_exact_inverse_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            customers = _dataset("customers", ["id"], [
                DatasetRelationship(to="orders",
                          from_columns=["id"], to_columns=["customer_id"],
                          join_type=JoinType.INNER,
                          cardinality=RelationshipCardinality.ONE_TO_MANY),
            ])
            with pytest.raises(ValueError, match="automatic"):
                await storage.save_dataset(customers)

    async def test_inverse_with_unset_cardinality_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            customers = _dataset("customers", ["id"], [
                DatasetRelationship(to="orders",
                          from_columns=["id"], to_columns=["customer_id"],
                          join_type=JoinType.INNER),
            ])
            with pytest.raises(ValueError, match="automatic"):
                await storage.save_dataset(customers)

    async def test_named_inverse_saves_as_disambiguator(self) -> None:
        # A NAMED reverse is not an exact inverse: its token must stay
        # resolvable, so the declaration is a legal disambiguator.
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            customers = _dataset("customers", ["id"], [
                DatasetRelationship(to="orders",
                          from_columns=["id"], to_columns=["customer_id"],
                          join_type=JoinType.INNER,
                          cardinality=RelationshipCardinality.ONE_TO_MANY,
                          name="back"),
            ])
            await storage.save_dataset(customers)
            loaded = await storage.get_dataset("customers", data_source="ds")
            assert loaded is not None
            assert len(loaded.relationships) == 1
            assert loaded.relationships[0].name == "back"

    async def test_non_inverse_pair_saves(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = await self._seed_forward(d)
            # Different pair set — a deliberate second edge, not a mirror.
            customers = _dataset("customers", ["id", "rep_order_id"], [
                DatasetRelationship(to="orders",
                          from_columns=["rep_order_id"], to_columns=["id"],
                          join_type=JoinType.INNER),
            ])
            await storage.save_dataset(customers)
            loaded = await storage.get_dataset("customers", data_source="ds")
            assert loaded is not None
            assert len(loaded.relationships) == 1
