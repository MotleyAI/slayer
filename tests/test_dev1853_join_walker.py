"""The shared bidirectional walker (``slayer/core/join_walker.py``).

One traversal substrate: any declared relationship is a symmetric edge, traversed in
either direction with oriented pairs and cardinality; ambiguity fails closed;
edge names resolve before dataset names.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.enums import RelationshipCardinality, JoinType
from slayer.core.errors import AmbiguousHopError, AmbiguousRelationshipPathError, CircularRelationshipPathError
from slayer.core.relationship_walker import (
    OrientedRelationship,
    edges_between,
    neighbors,
    resolve_hop,
    walk,
)
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.sql.field_expansion import resolve_ref_target


def _dataset(name: str, cols: list[str], relationships: list[DatasetRelationship] | None = None) -> SlayerDataset:
    return SlayerDataset(
        name=name, data_source="ds", sql_table=name,
        fields=[DatasetField(name=c) for c in cols],
        relationships=relationships or [],
    )


def _forward_pair(
    *,
    cardinality: RelationshipCardinality | None = RelationshipCardinality.MANY_TO_ONE,
    join_type: JoinType = JoinType.LEFT,
) -> dict[str, SlayerDataset]:
    orders = _dataset("orders", ["id", "customer_id", "status"], [
        DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                  join_type=join_type, cardinality=cardinality),
    ])
    customers = _dataset("customers", ["id", "name"])
    return {"orders": orders, "customers": customers}


def _parallel_pair(*, named: bool) -> dict[str, SlayerDataset]:
    orders = _dataset("orders", ["id", "billing_customer_id", "shipping_customer_id"], [
        DatasetRelationship(to="customers",
                  from_columns=["billing_customer_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE,
                  name="billing_customer" if named else None),
        DatasetRelationship(to="customers",
                  from_columns=["shipping_customer_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE,
                  name="shipping_customer" if named else None),
    ])
    customers = _dataset("customers", ["id", "name"])
    return {"orders": orders, "customers": customers}


class TestOrientation:
    def test_forward_edge_keeps_declared_shape(self) -> None:
        models = _forward_pair()
        edges = edges_between(source=models["orders"], target=models["customers"])
        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_dataset == "orders"
        assert edge.to == "customers"
        assert edge.column_pairs == [("customer_id", "id")]
        assert edge.join_type == JoinType.LEFT
        assert edge.cardinality == RelationshipCardinality.MANY_TO_ONE
        assert edge.declaring_dataset == "orders"

    def test_inverted_edge_swaps_pairs_and_flips_cardinality(self) -> None:
        models = _forward_pair()
        edges = edges_between(source=models["customers"], target=models["orders"])
        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_dataset == "customers"
        assert edge.to == "orders"
        assert edge.column_pairs == [("id", "customer_id")]
        assert edge.cardinality == RelationshipCardinality.ONE_TO_MANY
        assert edge.declaring_dataset == "orders"

    @pytest.mark.parametrize("declared", [
        RelationshipCardinality.ONE_TO_ONE, RelationshipCardinality.MANY_TO_MANY, None,
    ])
    def test_self_inverse_cardinalities(self, declared) -> None:
        models = _forward_pair(cardinality=declared)
        edge = edges_between(source=models["customers"], target=models["orders"])[0]
        assert edge.cardinality == declared

    def test_inverted_edge_keeps_join_type(self) -> None:
        models = _forward_pair(join_type=JoinType.INNER)
        edge = edges_between(source=models["customers"], target=models["orders"])[0]
        assert edge.join_type == JoinType.INNER


class TestSymmetry:
    def test_declaring_side_does_not_affect_traversal_answers(self) -> None:
        fwd = _forward_pair()
        rev_customers = _dataset("customers", ["id", "name"], [
            DatasetRelationship(to="orders", from_columns=["id"], to_columns=["customer_id"],
                      cardinality=RelationshipCardinality.ONE_TO_MANY),
        ])
        rev = {"orders": _dataset("orders", ["id", "customer_id", "status"]),
               "customers": rev_customers}
        for source, target in (("orders", "customers"), ("customers", "orders")):
            edge_f = edges_between(source=fwd[source], target=fwd[target])[0]
            edge_r = edges_between(source=rev[source], target=rev[target])[0]
            assert (edge_f.column_pairs, edge_f.cardinality, edge_f.join_type) == \
                (edge_r.column_pairs, edge_r.cardinality, edge_r.join_type)


class TestNeighbors:
    def test_incoming_edge_listed_with_inverted_orientation(self) -> None:
        models = _forward_pair()
        hops = neighbors(dataset=models["customers"], datasets_by_name=models)
        assert [(h.to, h.cardinality) for h in hops] == \
            [("orders", RelationshipCardinality.ONE_TO_MANY)]

    def test_outgoing_edge_listed_as_declared(self) -> None:
        models = _forward_pair()
        hops = neighbors(dataset=models["orders"], datasets_by_name=models)
        assert [(h.to, h.cardinality) for h in hops] == \
            [("customers", RelationshipCardinality.MANY_TO_ONE)]


class TestResolveHop:
    def test_model_token_resolves_forward(self) -> None:
        models = _forward_pair()
        edge = resolve_hop(current=models["orders"], token="customers",
                           datasets_by_name=models)
        assert edge is not None
        assert edge.to == "customers"

    def test_model_token_resolves_reverse(self) -> None:
        models = _forward_pair()
        edge = resolve_hop(current=models["customers"], token="orders",
                           datasets_by_name=models)
        assert edge is not None
        assert edge.to == "orders"
        assert edge.cardinality == RelationshipCardinality.ONE_TO_MANY

    def test_unknown_token_returns_none(self) -> None:
        models = _forward_pair()
        assert resolve_hop(current=models["orders"], token="nope",
                           datasets_by_name=models) is None
        assert resolve_hop(current=models["orders"], token="status",
                           datasets_by_name=models) is None

    def test_parallel_edges_raise_in_both_directions(self) -> None:
        models = _parallel_pair(named=False)
        for current, token in (("orders", "customers"), ("customers", "orders")):
            with pytest.raises(AmbiguousRelationshipPathError) as ei:
                resolve_hop(current=models[current], token=token,
                            datasets_by_name=models)
            assert len(ei.value.candidates) == 2
            msg = str(ei.value)
            assert "billing_customer_id" in msg
            assert "shipping_customer_id" in msg

    def test_edge_name_resolves_from_either_endpoint(self) -> None:
        models = _parallel_pair(named=True)
        from_orders = resolve_hop(current=models["orders"],
                                  token="billing_customer",
                                  datasets_by_name=models)
        assert from_orders is not None
        assert from_orders.to == "customers"
        assert from_orders.column_pairs == [("billing_customer_id", "id")]
        from_customers = resolve_hop(current=models["customers"],
                                     token="billing_customer",
                                     datasets_by_name=models)
        assert from_customers is not None
        assert from_customers.to == "orders"
        assert from_customers.column_pairs == [("id", "billing_customer_id")]

    def test_an_edge_name_that_is_another_edges_target_is_ambiguous(self) -> None:
        a = _dataset("a", ["id", "b_id", "c_id"], [
            DatasetRelationship(to="b", from_columns=["b_id"], to_columns=["id"], name="c"),
            DatasetRelationship(to="c", from_columns=["c_id"], to_columns=["id"]),
        ])
        models = {"a": a, "b": _dataset("b", ["id"]), "c": _dataset("c", ["id"])}
        with pytest.raises(AmbiguousHopError):
            resolve_hop(current=models["a"], token="c", datasets_by_name=models)


class TestWalk:
    def _chain(self) -> dict[str, SlayerDataset]:
        orders = _dataset("orders", ["id", "customer_id"], [
            DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                      cardinality=RelationshipCardinality.MANY_TO_ONE),
        ])
        customers = _dataset("customers", ["id", "region_id"], [
            DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                      cardinality=RelationshipCardinality.MANY_TO_ONE),
        ])
        return {"orders": orders, "customers": customers,
                "regions": _dataset("regions", ["id", "name"])}

    def test_single_reverse_hop(self) -> None:
        models = self._chain()
        chain = walk(root=models["customers"], path=("orders",),
                     datasets_by_name=models)
        assert chain is not None
        assert len(chain) == 1
        assert chain[0].to == "orders"

    def test_two_hop_reverse_walk(self) -> None:
        models = self._chain()
        chain = walk(root=models["regions"], path=("customers", "orders"),
                     datasets_by_name=models)
        assert chain is not None
        assert [e.to for e in chain] == ["customers", "orders"]
        assert [e.cardinality for e in chain] == \
            [RelationshipCardinality.ONE_TO_MANY, RelationshipCardinality.ONE_TO_MANY]

    def test_unknown_hop_returns_none(self) -> None:
        models = self._chain()
        assert walk(root=models["orders"], path=("nope",),
                    datasets_by_name=models) is None

    def test_revisit_is_guarded(self) -> None:
        # A revisit is a typed circular refusal, not a silent ``None``.
        models = self._chain()
        with pytest.raises(CircularRelationshipPathError):
            walk(root=models["regions"], path=("customers", "regions"),
                 datasets_by_name=models)

    def test_ambiguous_hop_raises(self) -> None:
        models = _parallel_pair(named=False)
        with pytest.raises(AmbiguousRelationshipPathError):
            walk(root=models["customers"], path=("orders",),
                 datasets_by_name=models)


class TestModeAResolverIsBidirectional:
    def test_reverse_hop_resolves(self) -> None:
        models = _forward_pair()
        target = resolve_ref_target(
            qualifiers=("orders",), source_dataset=models["customers"],
            datasets_by_name=models)
        assert target is not None
        assert target.name == "orders"

    def test_ambiguous_hop_skips_best_effort(self) -> None:
        models = _forward_pair()
        models["orders"].relationships = [
            *models["orders"].relationships,
            DatasetRelationship(to="customers",
                      from_columns=["customer_id"], to_columns=["id"],
                      join_type=JoinType.INNER),
        ]
        assert resolve_ref_target(
            qualifiers=("orders",), source_dataset=models["customers"],
            datasets_by_name=models) is None


class TestOrientedJoinIsFrozen:
    def test_assignment_rejected(self) -> None:
        models = _forward_pair()
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        with pytest.raises(ValidationError):
            edge.source_dataset = "x"
        assert isinstance(edge, OrientedRelationship)
