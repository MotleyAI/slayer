"""DEV-1908 — the cancelling walk in ``core/join_walker.py`` (D1, D2, D4).

``walk_cancelling`` resolves a definition default's qualifier ``tokens`` in the
owner's frame (the owner is ``root`` walked along ``owner_path``). A token equal
to the name of a dataset already on ``root + owner_path`` truncates the absolute
path back to that dataset — keeping ``owner_path``'s own spelling of it — then
resolution continues forward from there; an incident edge-name token always hops
(precedence edge name → path model → hop) and never cancels. Returns the absolute
token path (from ``root``) or ``None`` on a miss/revisit; ambiguity raises. Plain
``walk`` is untouched and still refuses revisits.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError, CircularRelationshipPathError
from slayer.core.relationship_walker import walk, walk_cancelling
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset


def _m(name: str, cols: list[str], joins: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(name=name, data_source="ds", sql_table=name,
                       fields=[DatasetField(name=c) for c in cols], relationships=joins or [])


def _named_chain() -> dict[str, SlayerDataset]:
    """orders →(billing_customer)→ customers → regions, customers → plans."""
    orders = _m("orders", ["id", "bc_id"], [
        DatasetRelationship(to="customers", from_columns=["bc_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE, name="billing_customer")])
    customers = _m("customers", ["id", "region_id", "plan_id"], [
        DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE),
        DatasetRelationship(to="plans", from_columns=["plan_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    return {m.name: m for m in (orders, customers, _m("regions", ["id", "pop"]),
                                _m("plans", ["id", "fee"]))}


def _edge_name_collision() -> dict[str, SlayerDataset]:
    """regions carries an edge NAMED ``customers`` (→ plans) and an edge ``loop``
    (→ orders) — an unvalidated bundle save-time rules would forbid."""
    M = _named_chain()
    M["regions"] = _m("regions", ["id", "pop", "plan_id", "cust_id"], [
        DatasetRelationship(to="plans", from_columns=["plan_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE, name="customers"),
        DatasetRelationship(to="orders", from_columns=["cust_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE, name="loop")])
    return M


def _parallel_pair() -> dict[str, SlayerDataset]:
    """orders → customers on two parallel UNNAMED edges (ambiguous)."""
    orders = _m("orders", ["id", "bill_id", "ship_id"], [
        DatasetRelationship(to="customers", from_columns=["bill_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE),
        DatasetRelationship(to="customers", from_columns=["ship_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE)])
    return {m.name: m for m in (orders, _m("customers", ["id"]))}


# Owner is regions, reached from orders as orders →(billing_customer)→ customers → regions.
OWNER_PATH = ("billing_customer", "regions")


class TestCancels:
    def test_cancel_at_root(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("orders",), datasets_by_name=M) == ()

    def test_cancel_at_middle_keeps_the_edge_name_spelling(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("customers",), datasets_by_name=M) \
            == ("billing_customer",)

    def test_cancel_to_self(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("regions",), datasets_by_name=M) \
            == ("billing_customer", "regions")

    def test_cancel_then_walk_forward(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("customers", "plans"), datasets_by_name=M) \
            == ("billing_customer", "plans")

    def test_empty_tokens_stays_at_the_owner(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=(), datasets_by_name=M) == OWNER_PATH


class TestEdgeNames:
    def test_edge_name_token_never_cancels_hop_onto_visited_is_none(self):
        # `loop` is an edge name (regions → orders); orders is visited → None.
        M = _edge_name_collision()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("loop",), datasets_by_name=M) is None

    def test_precedence_edge_name_wins_over_the_path_model(self):
        # `customers` is BOTH an edge name (regions → plans) and a visited model.
        # The edge name wins: it hops to plans, it does not cancel to ("billing_customer",).
        M = _edge_name_collision()
        result = walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                                 tokens=("customers",), datasets_by_name=M)
        assert result == ("billing_customer", "regions", "customers")
        assert result != ("billing_customer",)


class TestMissAndAmbiguity:
    def test_miss_returns_none(self):
        M = _named_chain()
        assert walk_cancelling(root=M["orders"], owner_path=OWNER_PATH,
                               tokens=("nowhere",), datasets_by_name=M) is None

    def test_ambiguous_forward_hop_raises(self):
        M = _parallel_pair()
        with pytest.raises(AmbiguousRelationshipPathError):
            walk_cancelling(root=M["orders"], owner_path=(),
                            tokens=("customers",), datasets_by_name=M)


class TestPlainWalkUnchanged:
    def test_plain_walk_still_refuses_revisits(self):
        # DEV-1952: the plain walk refuses a revisit with a typed raise; only
        # ``walk_cancelling`` cancels.
        M = _named_chain()
        with pytest.raises(CircularRelationshipPathError):
            walk(root=M["regions"], path=("customers", "regions"), datasets_by_name=M)
