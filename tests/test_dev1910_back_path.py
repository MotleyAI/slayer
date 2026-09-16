"""DEV-1910 task 2.1 — the reverse hop generalizes to a reverse PATH. ``_back_token``
becomes ``_back_path`` (a tuple), so a home several hops from the population root
reroots population-side references through every reversed hop: the edge name when
declared (direction-agnostic), else the hop's source model name. A chain of
provably one-to-one reverse hops becomes attributable; an ambiguous reverse hop
fails closed. Single-hop results stay byte-identical to today.

Spec: queries/cross-model-aggregates — "Producer filter routing" (reverse path).
"""

from __future__ import annotations

import pytest

from slayer.core.enums import JoinCardinality
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.keys import ColumnKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_safety import (
    _back_path,
    attributable_from_root,
    broadcast_reason,
    reroot_from_root,
)


def _m(name, cols, joins=None):
    return SlayerModel(name=name, data_source="ds", sql_table=name,
                       columns=[Column(name=c) for c in cols], joins=joins or [])


def _unnamed_two_hop():
    """orders → customers → regions, both hops unnamed (many-to-one)."""
    orders = _m("orders", ["id", "customer_id", "status"], [
        ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (orders, customers, regions)}


def _named_two_hop():
    """orders → customers (unnamed) → regions (named ``home_region``)."""
    orders = _m("orders", ["id", "customer_id"], [
        ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE, name="home_region")])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (orders, customers, regions)}


def _one_to_one_chain():
    """orders → carts → sessions, both hops one-to-one (attributable reverse)."""
    orders = _m("orders", ["id", "cart_id"], [
        ModelJoin(target_model="carts", join_pairs=[["cart_id", "id"]],
                  cardinality=JoinCardinality.ONE_TO_ONE)])
    carts = _m("carts", ["id", "session_id"], [
        ModelJoin(target_model="sessions", join_pairs=[["session_id", "id"]],
                  cardinality=JoinCardinality.ONE_TO_ONE)])
    sessions = _m("sessions", ["id", "channel"])
    return {m.name: m for m in (orders, carts, sessions)}


def _ambiguous_reverse():
    """events → customers on TWO parallel unnamed edges (the reverse hop from a
    customers-side home is ambiguous), customers → regions single."""
    events = _m("events", ["id", "bill_cid", "ship_cid", "kind"], [
        ModelJoin(target_model="customers", join_pairs=[["bill_cid", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE),
        ModelJoin(target_model="customers", join_pairs=[["ship_cid", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    regions = _m("regions", ["id", "pop"])
    return {m.name: m for m in (events, customers, regions)}


class TestBackPath:
    def test_single_hop_is_byte_identical(self):
        M = _unnamed_two_hop()
        assert _back_path(root_model=M["customers"], host_name="orders",
                          target_path=("customers",), models_by_name=M) == ("orders",)

    def test_unnamed_two_hop_reverses_both_source_models(self):
        M = _unnamed_two_hop()
        assert _back_path(root_model=M["regions"], host_name="orders",
                          target_path=("customers", "regions"),
                          models_by_name=M) == ("customers", "orders")

    def test_named_hop_uses_the_edge_name_reversed(self):
        M = _named_two_hop()
        assert _back_path(root_model=M["regions"], host_name="orders",
                          target_path=("customers", "regions"),
                          models_by_name=M) == ("home_region", "orders")

    def test_ambiguous_forward_walk_falls_back_to_the_host(self):
        """When the forward walk finds no path (not ambiguity), fall back to the
        host name — today's single-token behaviour."""
        M = _unnamed_two_hop()
        assert _back_path(root_model=M["regions"], host_name="orders",
                          target_path=("nonexistent",), models_by_name=M) == ("orders",)


class TestAttributabilityAcrossReversePath:
    def test_one_to_one_reverse_chain_is_attributable(self):
        """A chain of provably one-to-one reverse hops attributes a host-level
        reference to a home two hops away (False today: the single back token
        cannot express the two-hop reverse)."""
        M = _one_to_one_chain()
        assert attributable_from_root(
            host_path=(), target_path=("carts", "sessions"),
            root_model=M["sessions"], models_by_name=M, host_name="orders") is True

    def test_fanning_reverse_chain_stays_unattributable(self):
        M = _unnamed_two_hop()
        assert attributable_from_root(
            host_path=(), target_path=("customers", "regions"),
            root_model=M["regions"], models_by_name=M, host_name="orders") is False


class TestRerootAcrossReversePath:
    def test_two_hop_reroots_through_the_full_reverse_path(self):
        """An orders-level key reroots into a regions-rooted producer through the
        two-hop reverse path (today it wrongly reroots to the single ('orders',)
        hop)."""
        M = _unnamed_two_hop()
        rerooted = reroot_from_root(
            ColumnKey(path=(), leaf="status"), target_path=("customers", "regions"),
            root_model=M["regions"], models_by_name=M, host_name="orders")
        assert rerooted == ColumnKey(path=("customers", "orders"), leaf="status")

    def test_single_hop_reroot_is_unchanged(self):
        M = _unnamed_two_hop()
        rerooted = reroot_from_root(
            ColumnKey(path=(), leaf="status"), target_path=("customers",),
            root_model=M["customers"], models_by_name=M, host_name="orders")
        assert rerooted == ColumnKey(path=("orders",), leaf="status")


class TestBroadcastReasonAcrossReversePath:
    def test_two_hop_reason_names_the_fanning_hop(self):
        """The reverse-path broadcast reason names the fanning hop, not
        'unreachable' (today's two-hop degradation)."""
        M = _unnamed_two_hop()
        reason = broadcast_reason(
            host_path=(), target_path=("customers", "regions"),
            root_model=M["regions"], models_by_name=M, host_name="orders")
        assert "fanning" in reason or "unproven" in reason
        assert "unreachable" not in reason


class TestAmbiguousReverseHopFailsClosed:
    def test_reroot_over_ambiguous_reverse_hop_raises(self):
        """An ambiguous reverse hop (parallel unnamed edges, no resolving name)
        raises the ambiguous-hop error rather than silently guessing (today it is
        swallowed and a wrong single-token reroot is returned)."""
        M = _ambiguous_reverse()
        with pytest.raises(AmbiguousJoinPathError):
            reroot_from_root(
                ColumnKey(path=(), leaf="kind"),
                target_path=("customers", "regions"),
                root_model=M["regions"], models_by_name=M, host_name="events")
