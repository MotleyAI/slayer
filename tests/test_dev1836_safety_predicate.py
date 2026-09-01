"""DEV-1836 task 1.1 — the join-arity safety predicate (design D1).

Spec: openspec …/specs/models/join-cardinality — "Provable many-to-one arity",
"Composite keys prove arity only when fully covered". Pure unit tests over
declared model metadata; before implementation the module is absent and this
file fails on import (the right reason).
"""

from __future__ import annotations

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_safety import provably_to_one, safe_reachable

from tests._dev1836_fixtures import (
    customers_model,
    dev1836_models,
    inventory_model,
    orders_model,
    regions_model,
    segments_model,
)


def _models_by_name() -> dict[str, SlayerModel]:
    return {m.name: m for m in dev1836_models()}


def _join(target: str, pairs: list[list[str]], **kw) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=pairs, **kw)


class TestStructuralProof:
    def test_covered_solo_primary_key_proves(self) -> None:
        # orders → customers on customers.id (PK): no declaration needed.
        join = orders_model().joins[0]
        assert provably_to_one(join=join, target_model=customers_model())

    def test_covered_solo_unique_column_proves(self) -> None:
        target = SlayerModel(
            name="codes", data_source="test", sql_table="codes",
            columns=[
                Column(name="code", type=DataType.TEXT, unique=True),
                Column(name="label", type=DataType.TEXT),
            ],
        )
        join = _join("codes", [["c", "code"]])
        assert provably_to_one(join=join, target_model=target)

    def test_uncovered_target_is_unproven(self) -> None:
        # customers → segments: segments.code carries no PK/unique claim.
        join = next(
            j for j in customers_model().joins if j.target_model == "segments"
        )
        assert not provably_to_one(join=join, target_model=segments_model())

    def test_non_key_target_column_is_unproven(self) -> None:
        # Joining on a non-key column of a model that HAS a PK elsewhere.
        join = _join("regions", [["r", "name"]])
        assert not provably_to_one(join=join, target_model=regions_model())


class TestCompositeKeys:
    """F6 — composite uniqueness proves only under complete coverage."""

    def test_full_composite_coverage_proves(self) -> None:
        join = _join("inventory", [["wh", "wh"], ["sku", "sku"]])
        assert provably_to_one(join=join, target_model=inventory_model())

    def test_partial_composite_coverage_proves_nothing(self) -> None:
        join = _join("inventory", [["wh", "wh"]])
        assert not provably_to_one(join=join, target_model=inventory_model())

    def test_superset_of_composite_key_proves(self) -> None:
        join = _join(
            "inventory", [["wh", "wh"], ["sku", "sku"], ["q", "qty"]],
        )
        assert provably_to_one(join=join, target_model=inventory_model())


class TestDeclaredCardinality:
    def test_declared_many_to_one_is_trusted(self) -> None:
        # No structural proof on segments.code — the declaration alone proves.
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=JoinCardinality.MANY_TO_ONE,
        )
        assert provably_to_one(join=join, target_model=segments_model())

    def test_declared_one_to_one_is_trusted(self) -> None:
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=JoinCardinality.ONE_TO_ONE,
        )
        assert provably_to_one(join=join, target_model=segments_model())

    def test_declared_one_to_many_is_unsafe(self) -> None:
        # The fixture's declared reverse edge customers → orders.
        join = next(
            j for j in customers_model().joins if j.target_model == "orders"
        )
        assert not provably_to_one(join=join, target_model=orders_model())

    def test_declared_many_to_many_is_unsafe(self) -> None:
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=JoinCardinality.MANY_TO_MANY,
        )
        assert not provably_to_one(join=join, target_model=segments_model())


class TestMirroredInnerEdges:
    """A mirrored INNER edge carries the inverted forward cardinality
    (``join_sync``); the predicate reads the stored edge as-is."""

    def test_inverted_many_to_one_reverse_edge_is_unsafe(self) -> None:
        # Forward m:1 mirrors to 1:N — exactly the fixture's reverse edge shape.
        reverse = _join(
            "orders", [["id", "customer_id"]],
            cardinality=JoinCardinality.ONE_TO_MANY,
        )
        assert not provably_to_one(join=reverse, target_model=orders_model())

    def test_inverted_one_to_one_reverse_edge_is_safe(self) -> None:
        reverse = _join(
            "segments", [["segment_code", "code"]],
            cardinality=JoinCardinality.ONE_TO_ONE,
        )
        assert provably_to_one(join=reverse, target_model=segments_model())


class TestSafeReachable:
    def test_empty_path_is_safe(self) -> None:
        assert safe_reachable(
            root=orders_model(), path=(), models_by_name=_models_by_name(),
        )

    def test_all_proven_hops_are_safe(self) -> None:
        models = _models_by_name()
        assert safe_reachable(
            root=orders_model(), path=("customers",), models_by_name=models,
        )
        assert safe_reachable(
            root=orders_model(), path=("customers", "regions"),
            models_by_name=models,
        )

    def test_unproven_terminal_hop_is_unsafe(self) -> None:
        assert not safe_reachable(
            root=orders_model(), path=("customers", "segments"),
            models_by_name=_models_by_name(),
        )

    def test_declared_one_to_many_hop_is_unsafe(self) -> None:
        assert not safe_reachable(
            root=customers_model(), path=("orders",),
            models_by_name=_models_by_name(),
        )

    def test_unsafe_first_hop_poisons_the_whole_path(self) -> None:
        # customers → orders (1:N) → customers: a proven later hop cannot
        # recover an unsafe prefix.
        assert not safe_reachable(
            root=customers_model(), path=("orders", "customers"),
            models_by_name=_models_by_name(),
        )


class TestNoSynthesizedTraversal:
    """F1 — safety is evaluated over EXISTING stored edges only; proving a
    forward join never makes the absent reverse hop traversable."""

    def test_absent_reverse_edge_is_not_reachable(self) -> None:
        # customers → regions is stored and proven; regions stores NO join
        # back to customers.
        assert not safe_reachable(
            root=regions_model(), path=("customers",),
            models_by_name=_models_by_name(),
        )

    def test_unknown_edge_is_not_reachable(self) -> None:
        assert not safe_reachable(
            root=orders_model(), path=("regions",),
            models_by_name=_models_by_name(),
        )
