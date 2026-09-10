"""DEV-1853 — provability is per orientation of the symmetric edge.

``provably_to_one`` takes the oriented edge: the declared prong uses the
oriented cardinality (an inverted declared one_to_many proves many_to_one —
DEV-1840's "inversion never classifies safe" rule is repealed), the structural
prong checks traversal-target-side columns against that model's unique sets.
``safe_reachable`` walks any declared edge in either orientation.
"""

from __future__ import annotations

from slayer.core.enums import JoinCardinality, invert_cardinality
from slayer.core.join_walker import edges_between
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_safety import provably_to_one, safe_reachable


def _model(name, cols, joins=None):
    return SlayerModel(name=name, data_source="ds", sql_table=name,
                       columns=cols, joins=joins or [])


def _pair(*, declare, cardinality, customers_id_pk):
    """orders↔customers with one edge; cardinality read orders→customers."""
    customer_cols = [Column(name="id", primary_key=customers_id_pk),
                     Column(name="name")]
    order_cols = [Column(name="id", primary_key=True),
                  Column(name="customer_id")]
    if declare == "forward":
        orders = _model("orders", order_cols, [
            ModelJoin(target_model="customers",
                      join_pairs=[["customer_id", "id"]],
                      cardinality=cardinality),
        ])
        customers = _model("customers", customer_cols)
    else:
        orders = _model("orders", order_cols)
        customers = _model("customers", customer_cols, [
            ModelJoin(target_model="orders",
                      join_pairs=[["id", "customer_id"]],
                      cardinality=invert_cardinality(cardinality)),
        ])
    return {"orders": orders, "customers": customers}


class TestProvablyToOneOriented:
    def test_declared_forward_many_to_one_proves(self) -> None:
        models = _pair(declare="forward",
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       customers_id_pk=False)
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        assert provably_to_one(edge=edge, target_model=models["customers"]) is True

    def test_inverted_declared_one_to_many_proves_many_to_one(self) -> None:
        # Only stored edge: customers → orders (one_to_many); traversing
        # orders → customers is the inverted orientation. No structural help.
        models = _pair(declare="reverse",
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       customers_id_pk=False)
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        assert edge.cardinality == JoinCardinality.MANY_TO_ONE
        assert provably_to_one(edge=edge, target_model=models["customers"]) is True

    def test_inverting_a_to_one_hop_is_fan_out(self) -> None:
        models = _pair(declare="forward",
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       customers_id_pk=False)
        edge = edges_between(source=models["customers"], target=models["orders"])[0]
        assert edge.cardinality == JoinCardinality.ONE_TO_MANY
        assert provably_to_one(edge=edge, target_model=models["orders"]) is False

    def test_structural_proof_from_covered_primary_key(self) -> None:
        models = _pair(declare="forward", cardinality=None, customers_id_pk=True)
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        assert provably_to_one(edge=edge, target_model=models["customers"]) is True

    def test_structural_proof_applies_on_the_inverted_orientation(self) -> None:
        # Stored customers → orders (no cardinality); traversing orders →
        # customers targets customers.id, which is a PK — structurally proven.
        models = _pair(declare="reverse", cardinality=None, customers_id_pk=True)
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        assert provably_to_one(edge=edge, target_model=models["customers"]) is True

    def test_unknown_arity_fails_closed(self) -> None:
        models = _pair(declare="forward", cardinality=None, customers_id_pk=False)
        edge = edges_between(source=models["orders"], target=models["customers"])[0]
        assert provably_to_one(edge=edge, target_model=models["customers"]) is False


class TestSafeReachableBidirectional:
    def test_reverse_hop_with_inverted_proof_is_safe(self) -> None:
        models = _pair(declare="reverse",
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       customers_id_pk=False)
        assert safe_reachable(root=models["orders"], path=("customers",),
                              models_by_name=models) is True

    def test_reverse_fan_out_hop_is_unsafe(self) -> None:
        models = _pair(declare="forward",
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       customers_id_pk=False)
        assert safe_reachable(root=models["customers"], path=("orders",),
                              models_by_name=models) is False

    def test_two_hop_path_mixing_orientations(self) -> None:
        orders = _model("orders", [Column(name="id", primary_key=True),
                                   Column(name="customer_id")])
        customers = _model(
            "customers",
            [Column(name="id", primary_key=True), Column(name="region_id")],
            [ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]],
                       cardinality=JoinCardinality.ONE_TO_MANY),
             ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        )
        regions = _model("regions", [Column(name="id", primary_key=True)])
        models = {"orders": orders, "customers": customers, "regions": regions}
        # orders → customers is the inverted (proven) hop, then forward to regions.
        assert safe_reachable(root=models["orders"],
                              path=("customers", "regions"),
                              models_by_name=models) is True

    def test_named_edge_token_is_walkable(self) -> None:
        orders = _model(
            "orders",
            [Column(name="id", primary_key=True), Column(name="bill_id")],
            [ModelJoin(target_model="customers", join_pairs=[["bill_id", "id"]],
                       cardinality=JoinCardinality.MANY_TO_ONE,
                       name="billing_customer")],
        )
        customers = _model("customers", [Column(name="id", primary_key=True)])
        models = {"orders": orders, "customers": customers}
        assert safe_reachable(root=models["orders"], path=("billing_customer",),
                              models_by_name=models) is True
