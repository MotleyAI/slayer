"""The reverse hop is a reverse PATH (``_route_via_common_prefix`` with an empty
host path): each hop's reverse token is the edge name when declared else the
source model; a one-to-one reverse chain becomes attributable, an ambiguous
reverse hop fails closed, single-hop stays byte-identical. Spec:
queries/cross-model-aggregates — "Producer filter routing"."""

from __future__ import annotations

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError
from slayer.core.keys import FieldKey
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.relationship_safety import (
    _route_via_common_prefix,
    attributable_from_root,
    broadcast_reason,
    reroot_from_root,
)


def _m(name, cols, joins=None):
    return SlayerDataset(name=name, data_source="ds", sql_table=name,
                       fields=[DatasetField(name=c) for c in cols], relationships=joins or [])


def _unnamed_two_hop():
    """orders → customers → regions, both hops unnamed (many-to-one)."""
    orders = _m("orders", ["id", "customer_id", "status"], [
        DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (orders, customers, regions)}


def _named_two_hop():
    """orders → customers (unnamed) → regions (named ``home_region``)."""
    orders = _m("orders", ["id", "customer_id"], [
        DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE, name="home_region")])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (orders, customers, regions)}


def _one_to_one_chain():
    """orders → carts → sessions, both hops one-to-one (attributable reverse)."""
    orders = _m("orders", ["id", "cart_id"], [
        DatasetRelationship(to="carts", from_columns=["cart_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.ONE_TO_ONE)])
    carts = _m("carts", ["id", "session_id"], [
        DatasetRelationship(to="sessions", from_columns=["session_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.ONE_TO_ONE)])
    sessions = _m("sessions", ["id", "channel"])
    return {m.name: m for m in (orders, carts, sessions)}


def _ambiguous_reverse():
    """events → customers on TWO parallel unnamed edges (the reverse hop from a
    customers-side home is ambiguous), customers → regions single."""
    events = _m("events", ["id", "bill_cid", "ship_cid", "kind"], [
        DatasetRelationship(to="customers", from_columns=["bill_cid"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE),
        DatasetRelationship(to="customers", from_columns=["ship_cid"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (events, customers, regions)}


class TestBackPath:
    def test_single_hop_is_byte_identical(self):
        M = _unnamed_two_hop()
        assert _route_via_common_prefix(host_name="orders", host_path=(),
                          target_path=("customers",), datasets_by_name=M) == ("orders",)

    def test_unnamed_two_hop_reverses_both_source_models(self):
        M = _unnamed_two_hop()
        assert _route_via_common_prefix(host_name="orders", host_path=(),
                          target_path=("customers", "regions"),
                          datasets_by_name=M) == ("customers", "orders")

    def test_named_hop_uses_the_edge_name_reversed(self):
        M = _named_two_hop()
        assert _route_via_common_prefix(host_name="orders", host_path=(),
                          target_path=("customers", "regions"),
                          datasets_by_name=M) == ("home_region", "orders")

    def test_ambiguous_forward_walk_falls_back_to_the_host(self):
        """When the forward walk finds no path (not ambiguity), fall back to the
        host name — today's single-token behaviour."""
        M = _unnamed_two_hop()
        assert _route_via_common_prefix(host_name="orders", host_path=(),
                          target_path=("nonexistent",), datasets_by_name=M) == ("orders",)


class TestAttributabilityAcrossReversePath:
    def test_one_to_one_reverse_chain_is_attributable(self):
        """A chain of provably one-to-one reverse hops attributes a host-level
        reference to a home two hops away (False today: the single back token
        cannot express the two-hop reverse)."""
        M = _one_to_one_chain()
        assert attributable_from_root(
            host_path=(), target_path=("carts", "sessions"),
            root_dataset=M["sessions"], datasets_by_name=M, host_name="orders") is True

    def test_fanning_reverse_chain_stays_unattributable(self):
        M = _unnamed_two_hop()
        assert attributable_from_root(
            host_path=(), target_path=("customers", "regions"),
            root_dataset=M["regions"], datasets_by_name=M, host_name="orders") is False


class TestRerootAcrossReversePath:
    def test_two_hop_reroots_through_the_full_reverse_path(self):
        """An orders-level key reroots into a regions-rooted producer through the
        two-hop reverse path (today it wrongly reroots to the single ('orders',)
        hop)."""
        M = _unnamed_two_hop()
        rerooted = reroot_from_root(
            FieldKey(path=(), leaf="status"), target_path=("customers", "regions"),
            root_dataset=M["regions"], datasets_by_name=M, host_name="orders")
        assert rerooted == FieldKey(path=("customers", "orders"), leaf="status")

    def test_single_hop_reroot_is_unchanged(self):
        M = _unnamed_two_hop()
        rerooted = reroot_from_root(
            FieldKey(path=(), leaf="status"), target_path=("customers",),
            root_dataset=M["customers"], datasets_by_name=M, host_name="orders")
        assert rerooted == FieldKey(path=("orders",), leaf="status")


class TestBroadcastReasonAcrossReversePath:
    def test_two_hop_reason_names_the_fanning_hop(self):
        """The reverse-path broadcast reason names the fanning hop, not
        'unreachable' (today's two-hop degradation)."""
        M = _unnamed_two_hop()
        reason = broadcast_reason(
            host_path=(), target_path=("customers", "regions"),
            root_dataset=M["regions"], datasets_by_name=M, host_name="orders")
        assert "fanning" in reason or "unproven" in reason
        assert "unreachable" not in reason


class TestAmbiguousReverseHopFailsClosed:
    def test_reroot_over_ambiguous_reverse_hop_raises(self):
        """An ambiguous reverse hop (parallel unnamed edges, no resolving name)
        raises the ambiguous-hop error rather than silently guessing (today it is
        swallowed and a wrong single-token reroot is returned)."""
        M = _ambiguous_reverse()
        key = FieldKey(path=(), leaf="kind")
        with pytest.raises(AmbiguousRelationshipPathError):
            reroot_from_root(
                key, target_path=("customers", "regions"),
                root_dataset=M["regions"], datasets_by_name=M, host_name="events")
