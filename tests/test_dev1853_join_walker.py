"""DEV-1853 — the shared bidirectional walker (``slayer/core/join_walker.py``).

One traversal substrate: any declared join is a symmetric edge, traversed in
either direction with oriented pairs and cardinality; ambiguity fails closed;
edge names resolve before model names.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.join_walker import (
    OrientedJoin,
    edges_between,
    neighbors,
    resolve_hop,
    walk,
)
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.column_expansion import resolve_ref_target


def _model(name: str, cols: list[str], joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name, data_source="ds", sql_table=name,
        columns=[Column(name=c) for c in cols],
        joins=joins or [],
    )


def _forward_pair(
    *,
    cardinality: JoinCardinality | None = JoinCardinality.MANY_TO_ONE,
    join_type: JoinType = JoinType.LEFT,
) -> dict[str, SlayerModel]:
    orders = _model("orders", ["id", "customer_id", "status"], [
        ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                  join_type=join_type, cardinality=cardinality),
    ])
    customers = _model("customers", ["id", "name"])
    return {"orders": orders, "customers": customers}


def _parallel_pair(*, named: bool) -> dict[str, SlayerModel]:
    orders = _model("orders", ["id", "billing_customer_id", "shipping_customer_id"], [
        ModelJoin(target_model="customers",
                  join_pairs=[["billing_customer_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE,
                  name="billing_customer" if named else None),
        ModelJoin(target_model="customers",
                  join_pairs=[["shipping_customer_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE,
                  name="shipping_customer" if named else None),
    ])
    customers = _model("customers", ["id", "name"])
    return {"orders": orders, "customers": customers}


class TestOrientation:
    def test_forward_edge_keeps_declared_shape(self) -> None:
        models = _forward_pair()
        edges = edges_between(source=models["orders"], target=models["customers"])
        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_model == "orders"
        assert edge.target_model == "customers"
        assert edge.join_pairs == [["customer_id", "id"]]
        assert edge.join_type == JoinType.LEFT
        assert edge.cardinality == JoinCardinality.MANY_TO_ONE
        assert edge.declaring_model == "orders"

    def test_inverted_edge_swaps_pairs_and_flips_cardinality(self) -> None:
        models = _forward_pair()
        edges = edges_between(source=models["customers"], target=models["orders"])
        assert len(edges) == 1
        edge = edges[0]
        assert edge.source_model == "customers"
        assert edge.target_model == "orders"
        assert edge.join_pairs == [["id", "customer_id"]]
        assert edge.cardinality == JoinCardinality.ONE_TO_MANY
        assert edge.declaring_model == "orders"

    @pytest.mark.parametrize("declared", [
        JoinCardinality.ONE_TO_ONE, JoinCardinality.MANY_TO_MANY, None,
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
        rev_customers = _model("customers", ["id", "name"], [
            ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                      cardinality=JoinCardinality.ONE_TO_MANY),
        ])
        rev = {"orders": _model("orders", ["id", "customer_id", "status"]),
               "customers": rev_customers}
        for source, target in (("orders", "customers"), ("customers", "orders")):
            edge_f = edges_between(source=fwd[source], target=fwd[target])[0]
            edge_r = edges_between(source=rev[source], target=rev[target])[0]
            assert (edge_f.join_pairs, edge_f.cardinality, edge_f.join_type) == \
                (edge_r.join_pairs, edge_r.cardinality, edge_r.join_type)


class TestNeighbors:
    def test_incoming_edge_listed_with_inverted_orientation(self) -> None:
        models = _forward_pair()
        hops = neighbors(model=models["customers"], models_by_name=models)
        assert [(h.target_model, h.cardinality) for h in hops] == \
            [("orders", JoinCardinality.ONE_TO_MANY)]

    def test_outgoing_edge_listed_as_declared(self) -> None:
        models = _forward_pair()
        hops = neighbors(model=models["orders"], models_by_name=models)
        assert [(h.target_model, h.cardinality) for h in hops] == \
            [("customers", JoinCardinality.MANY_TO_ONE)]


class TestResolveHop:
    def test_model_token_resolves_forward(self) -> None:
        models = _forward_pair()
        edge = resolve_hop(current=models["orders"], token="customers",
                           models_by_name=models)
        assert edge is not None
        assert edge.target_model == "customers"

    def test_model_token_resolves_reverse(self) -> None:
        models = _forward_pair()
        edge = resolve_hop(current=models["customers"], token="orders",
                           models_by_name=models)
        assert edge is not None
        assert edge.target_model == "orders"
        assert edge.cardinality == JoinCardinality.ONE_TO_MANY

    def test_unknown_token_returns_none(self) -> None:
        models = _forward_pair()
        assert resolve_hop(current=models["orders"], token="nope",
                           models_by_name=models) is None
        assert resolve_hop(current=models["orders"], token="status",
                           models_by_name=models) is None

    def test_parallel_edges_raise_in_both_directions(self) -> None:
        models = _parallel_pair(named=False)
        for current, token in (("orders", "customers"), ("customers", "orders")):
            with pytest.raises(AmbiguousJoinPathError) as ei:
                resolve_hop(current=models[current], token=token,
                            models_by_name=models)
            assert len(ei.value.candidates) == 2
            msg = str(ei.value)
            assert "billing_customer_id" in msg
            assert "shipping_customer_id" in msg

    def test_edge_name_resolves_from_either_endpoint(self) -> None:
        models = _parallel_pair(named=True)
        from_orders = resolve_hop(current=models["orders"],
                                  token="billing_customer",
                                  models_by_name=models)
        assert from_orders is not None
        assert from_orders.target_model == "customers"
        assert from_orders.join_pairs == [["billing_customer_id", "id"]]
        from_customers = resolve_hop(current=models["customers"],
                                     token="billing_customer",
                                     models_by_name=models)
        assert from_customers is not None
        assert from_customers.target_model == "orders"
        assert from_customers.join_pairs == [["id", "billing_customer_id"]]

    def test_edge_name_resolves_before_model_name(self) -> None:
        a = _model("a", ["id", "b_id", "c_id"], [
            ModelJoin(target_model="b", join_pairs=[["b_id", "id"]], name="c"),
            ModelJoin(target_model="c", join_pairs=[["c_id", "id"]]),
        ])
        models = {"a": a, "b": _model("b", ["id"]), "c": _model("c", ["id"])}
        edge = resolve_hop(current=models["a"], token="c", models_by_name=models)
        assert edge is not None
        assert edge.target_model == "b"


class TestWalk:
    def _chain(self) -> dict[str, SlayerModel]:
        orders = _model("orders", ["id", "customer_id"], [
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                      cardinality=JoinCardinality.MANY_TO_ONE),
        ])
        customers = _model("customers", ["id", "region_id"], [
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                      cardinality=JoinCardinality.MANY_TO_ONE),
        ])
        return {"orders": orders, "customers": customers,
                "regions": _model("regions", ["id", "name"])}

    def test_single_reverse_hop(self) -> None:
        models = self._chain()
        chain = walk(root=models["customers"], path=("orders",),
                     models_by_name=models)
        assert chain is not None
        assert len(chain) == 1
        assert chain[0].target_model == "orders"

    def test_two_hop_reverse_walk(self) -> None:
        models = self._chain()
        chain = walk(root=models["regions"], path=("customers", "orders"),
                     models_by_name=models)
        assert chain is not None
        assert [e.target_model for e in chain] == ["customers", "orders"]
        assert [e.cardinality for e in chain] == \
            [JoinCardinality.ONE_TO_MANY, JoinCardinality.ONE_TO_MANY]

    def test_unknown_hop_returns_none(self) -> None:
        models = self._chain()
        assert walk(root=models["orders"], path=("nope",),
                    models_by_name=models) is None

    def test_revisit_is_guarded(self) -> None:
        models = self._chain()
        assert walk(root=models["regions"], path=("customers", "regions"),
                    models_by_name=models) is None

    def test_ambiguous_hop_raises(self) -> None:
        models = _parallel_pair(named=False)
        with pytest.raises(AmbiguousJoinPathError):
            walk(root=models["customers"], path=("orders",),
                 models_by_name=models)


class TestModeAResolverIsBidirectional:
    def test_reverse_hop_resolves(self) -> None:
        models = _forward_pair()
        target = resolve_ref_target(
            qualifiers=("orders",), source_model=models["customers"],
            models_by_name=models)
        assert target is not None
        assert target.name == "orders"

    def test_ambiguous_hop_skips_best_effort(self) -> None:
        models = _forward_pair()
        models["orders"].joins = [
            *models["orders"].joins,
            ModelJoin(target_model="customers",
                      join_pairs=[["customer_id", "id"]],
                      join_type=JoinType.INNER),
        ]
        assert resolve_ref_target(
            qualifiers=("orders",), source_model=models["customers"],
            models_by_name=models) is None


class TestOrientedJoinIsFrozen:
    def test_assignment_rejected(self) -> None:
        models = _forward_pair()
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        with pytest.raises(ValidationError):
            edge.source_model = "x"
        assert isinstance(edge, OrientedJoin)
