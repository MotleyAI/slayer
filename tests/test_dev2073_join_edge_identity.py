"""Edge identity: the declaration reference, its string face, and the FK relationship signature."""

from __future__ import annotations

import json

import pytest

from slayer.core.relationship_edges import RelationshipEdgeRef, edge_reference, relationship_signature, resolve_relationship_ref
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev2073_fixtures import (
    BILLING_PAIRS,
    DS,
    SHIPPING_PAIRS,
    addresses_dataset,
    join_to,
    dataset_store,
    named_pair,
    orders_with,
    stored_joins,
    unnamed_pair,
)

CUSTOMER_PAIRS = [["customer_id", "id"]]


class TestEdgeReference:
    def test_sole_join_is_referenced_by_target(self) -> None:
        orders = orders_with(join_to(CUSTOMER_PAIRS, target="customers"))
        assert edge_reference(dataset=orders, relationship=orders.relationships[0]) == "customers"

    def test_parallel_join_is_referenced_by_name(self) -> None:
        orders = orders_with(*named_pair())
        assert [edge_reference(dataset=orders, relationship=j) for j in orders.relationships] == [
            "billing_address", "shipping_address",
        ]


class TestResolveJoinRef:
    def test_exact_reference_picks_one_of_two_unnamed_joins(self) -> None:
        orders = orders_with(*unnamed_pair())
        ref = RelationshipEdgeRef(to="addresses", from_columns=[p[0] for p in SHIPPING_PAIRS], to_columns=[p[1] for p in SHIPPING_PAIRS])
        assert resolve_relationship_ref(dataset=orders, ref=ref) == orders.relationships[1]

    def test_named_reference_requires_the_name(self) -> None:
        orders = orders_with(*unnamed_pair())
        ref = RelationshipEdgeRef(to="addresses", name="shipping_address", from_columns=[p[0] for p in SHIPPING_PAIRS], to_columns=[p[1] for p in SHIPPING_PAIRS])
        with pytest.raises(ValueError):
            resolve_relationship_ref(dataset=orders, ref=ref)

    def test_string_reference_by_name_and_by_unique_target(self) -> None:
        orders = orders_with(*named_pair(), join_to(CUSTOMER_PAIRS, target="customers"))
        assert resolve_relationship_ref(dataset=orders, ref="shipping_address") == orders.relationships[1]
        assert resolve_relationship_ref(dataset=orders, ref="customers") == orders.relationships[2]

    def test_ambiguous_target_reference_lists_both_candidates(self) -> None:
        orders = orders_with(*unnamed_pair())
        with pytest.raises(ValueError) as exc:
            resolve_relationship_ref(dataset=orders, ref="addresses")
        assert "billing_address_id" in str(exc.value)
        assert "shipping_address_id" in str(exc.value)

    def test_exact_reference_matching_two_declarations_fails(self) -> None:
        orders = orders_with(join_to(BILLING_PAIRS), join_to(BILLING_PAIRS))
        ref = RelationshipEdgeRef(to="addresses", from_columns=[p[0] for p in BILLING_PAIRS], to_columns=[p[1] for p in BILLING_PAIRS])
        with pytest.raises(ValueError, match="billing_address_id"):
            resolve_relationship_ref(dataset=orders, ref=ref)

    def test_ref_round_trips_through_json(self) -> None:
        orders = orders_with(*named_pair())
        ref = RelationshipEdgeRef.of(orders.relationships[0])
        assert RelationshipEdgeRef.model_validate(json.loads(ref.model_dump_json())) == ref
        assert resolve_relationship_ref(dataset=orders, ref=ref) == orders.relationships[0]


class TestRelationshipSignature:
    def test_equal_across_orientation_and_name(self) -> None:
        forward = relationship_signature(dataset_name="orders", relationship=join_to(CUSTOMER_PAIRS, target="customers"))
        reverse = relationship_signature(
            dataset_name="customers", relationship=join_to([["id", "customer_id"]], "placed", target="orders"),
        )
        assert forward == reverse

    def test_differs_on_key_pairs(self) -> None:
        billing = relationship_signature(dataset_name="orders", relationship=join_to(BILLING_PAIRS))
        shipping = relationship_signature(dataset_name="orders", relationship=join_to(SHIPPING_PAIRS))
        assert billing != shipping


class TestEngineRemovalByExactReference:
    async def test_removes_exactly_the_referenced_join(self) -> None:
        async with dataset_store(addresses_dataset(), orders_with(*unnamed_pair())) as storage:
            engine = SlayerQueryEngine(storage=storage)
            try:
                ref = RelationshipEdgeRef(to="addresses", from_columns=[p[0] for p in SHIPPING_PAIRS], to_columns=[p[1] for p in SHIPPING_PAIRS])
                await engine.edit_dataset_remove(dataset_name="orders", data_source=DS, remove_relationship_edges=[ref])
            finally:
                await engine.aclose()
            assert await stored_joins(storage) == [(None, BILLING_PAIRS)]
