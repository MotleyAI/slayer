"""The join-arity safety predicate.

Spec: openspec …/specs/models/join-cardinality — "Provable many-to-one arity",
"Composite keys prove arity only when fully covered". Pure unit tests over
declared model metadata; before implementation the module is absent and this
file fails on import (the right reason).
"""

from __future__ import annotations

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.relationship_walker import provably_to_one
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.cardinality.relationship_safety import may_inline_crossing_inputs, safe_reachable
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query

from tests._dev1836_fixtures import (
    customers_dataset,
    dev1836_datasets,
    inventory_dataset,
    orders_dataset,
    q,
    regions_dataset,
    segments_dataset,
)
from slayer.engine.cardinality import relationship_safety
import pytest


def _datasets_by_name() -> dict[str, SlayerDataset]:
    return {m.name: m for m in dev1836_datasets()}


def _join(target: str, pairs: list[list[str]], **kw) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[p[0] for p in pairs], to_columns=[p[1] for p in pairs], **kw)


class TestStructuralProof:
    def test_covered_solo_primary_key_proves(self) -> None:
        # orders → customers on customers.id (PK): no declaration needed.
        join = orders_dataset().relationships[0]
        assert provably_to_one(edge=join, target_dataset=customers_dataset())

    def test_covered_solo_unique_column_proves(self) -> None:
        target = SlayerDataset(
            name="codes", data_source="test", sql_table="codes",
            fields=[
                DatasetField(name="code", datatype=DataType.STRING, unique=True),
                DatasetField(name="label", datatype=DataType.STRING),
            ],
        )
        join = _join("codes", [["c", "code"]])
        assert provably_to_one(edge=join, target_dataset=target)

    def test_logical_spelling_of_a_renamed_pk_proves(self) -> None:
        # A renamed PK proves the hop when the join names it by column name.
        target = SlayerDataset(
            name="loss_payment", data_source="test", sql_table="Loss_Payment",
            fields=[
                DatasetField(name="id", sql="Claim_Amount_Identifier",
                       datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
        )
        join = _join("loss_payment", [["id", "id"]])
        assert provably_to_one(edge=join, target_dataset=target)

    def test_a_derived_pk_sql_does_not_prove_the_raw_spelling(self) -> None:
        # A NON-bare ``sql`` is an expression, not a rename — no uniqueness
        # transfer to any raw column it mentions.
        target = SlayerDataset(
            name="derived", data_source="test", sql_table="derived",
            fields=[
                DatasetField(name="id", sql="UPPER(code)", datatype=DataType.STRING,
                       primary_key=True),
                DatasetField(name="code", datatype=DataType.STRING),
            ],
        )
        join = _join("derived", [["c", "code"]])
        assert not provably_to_one(edge=join, target_dataset=target)

    def test_uncovered_target_is_unproven(self) -> None:
        # customers → segments: segments.code carries no PK/unique claim.
        join = next(
            j for j in customers_dataset().relationships if j.to == "segments"
        )
        assert not provably_to_one(edge=join, target_dataset=segments_dataset())

    def test_non_key_target_column_is_unproven(self) -> None:
        # Joining on a non-key column of a model that HAS a PK elsewhere.
        join = _join("regions", [["r", "name"]])
        assert not provably_to_one(edge=join, target_dataset=regions_dataset())


class TestCompositeKeys:
    """F6 — composite uniqueness proves only under complete coverage."""

    def test_full_composite_coverage_proves(self) -> None:
        join = _join("inventory", [["wh", "wh"], ["sku", "sku"]])
        assert provably_to_one(edge=join, target_dataset=inventory_dataset())

    def test_partial_composite_coverage_proves_nothing(self) -> None:
        join = _join("inventory", [["wh", "wh"]])
        assert not provably_to_one(edge=join, target_dataset=inventory_dataset())

    def test_superset_of_composite_key_proves(self) -> None:
        join = _join(
            "inventory", [["wh", "wh"], ["sku", "sku"], ["q", "qty"]],
        )
        assert provably_to_one(edge=join, target_dataset=inventory_dataset())


class TestDeclaredCardinality:
    def test_declared_many_to_one_is_trusted(self) -> None:
        # No structural proof on segments.code — the declaration alone proves.
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=RelationshipCardinality.MANY_TO_ONE,
        )
        assert provably_to_one(edge=join, target_dataset=segments_dataset())

    def test_declared_one_to_one_is_trusted(self) -> None:
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=RelationshipCardinality.ONE_TO_ONE,
        )
        assert provably_to_one(edge=join, target_dataset=segments_dataset())

    def test_declared_one_to_many_is_unsafe(self) -> None:
        # The fixture declares no reverse edge — same shape inline.
        join = _join(
            "orders", [["id", "customer_id"]],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )
        assert not provably_to_one(edge=join, target_dataset=orders_dataset())

    def test_declared_many_to_many_is_unsafe(self) -> None:
        join = _join(
            "segments", [["segment_code", "code"]],
            cardinality=RelationshipCardinality.MANY_TO_MANY,
        )
        assert not provably_to_one(edge=join, target_dataset=segments_dataset())


class TestInvertedCardinalityEdges:
    """A declared join read in its inverted-cardinality shape; the predicate reads it as-is."""

    def test_inverted_many_to_one_reverse_edge_is_unsafe(self) -> None:
        # Forward m:1 inverts to 1:N — the reverse-orientation shape.
        reverse = _join(
            "orders", [["id", "customer_id"]],
            cardinality=RelationshipCardinality.ONE_TO_MANY,
        )
        assert not provably_to_one(edge=reverse, target_dataset=orders_dataset())

    def test_inverted_one_to_one_reverse_edge_is_safe(self) -> None:
        reverse = _join(
            "segments", [["segment_code", "code"]],
            cardinality=RelationshipCardinality.ONE_TO_ONE,
        )
        assert provably_to_one(edge=reverse, target_dataset=segments_dataset())


class TestSafeReachable:
    def test_empty_path_is_safe(self) -> None:
        assert safe_reachable(
            root=orders_dataset(), path=(), datasets_by_name=_datasets_by_name(),
        )

    def test_all_proven_hops_are_safe(self) -> None:
        models = _datasets_by_name()
        assert safe_reachable(
            root=orders_dataset(), path=("customers",), datasets_by_name=models,
        )
        assert safe_reachable(
            root=orders_dataset(), path=("customers", "regions"),
            datasets_by_name=models,
        )

    def test_unproven_terminal_hop_is_unsafe(self) -> None:
        assert not safe_reachable(
            root=orders_dataset(), path=("customers", "segments"),
            datasets_by_name=_datasets_by_name(),
        )

    def test_declared_one_to_many_hop_is_unsafe(self) -> None:
        assert not safe_reachable(
            root=customers_dataset(), path=("orders",),
            datasets_by_name=_datasets_by_name(),
        )

    def test_unsafe_first_hop_poisons_the_whole_path(self) -> None:
        # customers → orders (1:N) → customers: a proven later hop cannot
        # recover an unsafe prefix.
        assert not safe_reachable(
            root=customers_dataset(), path=("orders", "customers"),
            datasets_by_name=_datasets_by_name(),
        )


class TestInvertedOrientationsStayProven:
    """Every declared
    edge traverses both ways, but proof is per orientation — an inverted
    to-one hop is a fan-out orientation and stays unsafe."""

    def test_inverted_fan_out_orientation_is_unsafe(self) -> None:
        # customers → regions is stored and proven; the inverted hop
        # regions → customers now WALKS but its 1:N orientation is unproven.
        assert not safe_reachable(
            root=regions_dataset(), path=("customers",),
            datasets_by_name=_datasets_by_name(),
        )

    def test_unconnected_hop_is_not_reachable(self) -> None:
        # orders and regions share no edge in either direction (2 hops apart).
        assert not safe_reachable(
            root=orders_dataset(), path=("regions",),
            datasets_by_name=_datasets_by_name(),
        )


class TestMayInlineSeam:
    """The crossing-input inlining seam. ``ScopeFrame.may_inline`` guards individual values at the
    projection boundary and is pinned separately (``test_scope``); this seam
    guards whole aggregates at plan time."""

    def test_inlining_a_crossing_input_is_refused(self) -> None:
        """Hardcoded ``False``: a crossing input desugars onto a producer,
        always. Inlining one is only safe when the crossed join is provably
        1:N-free for the aggregate."""
        assert may_inline_crossing_inputs([("customers",)]) is False
        assert may_inline_crossing_inputs([]) is False

    def test_the_seam_is_load_bearing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Flipping the seam must change the verdict — otherwise it is
        decorative. With it
        returning ``True`` a crossing-input local aggregate stays inline
        instead of desugaring onto a host-rooted producer."""
        host = orders_dataset()
        host.fields.append(DatasetField(
            name="gold_amount", datatype=DataType.FLOAT, sql="amount",
            filter="customers.tier = 'gold'",
        ))
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=host,
            referenced_datasets=[host, customers_dataset(), regions_dataset(),
                               segments_dataset()],
        )
        query = q(
            dimensions=["orders.status"],
            metrics=[{"metric": "sum(orders.gold_amount)", "alias": "g"}],
        )
        planned = plan_query(query=query, bundle=bundle)
        assert planned.regroup_attach_plans, (
            "fixture rot: the crossing-input aggregate no longer desugars"
        )
        monkeypatch.setattr(
            relationship_safety, "may_inline_crossing_inputs", lambda paths: True,
        )
        planned = plan_query(query=query, bundle=bundle)
        assert not planned.regroup_attach_plans, (
            "flipping may_inline_crossing_inputs did not change the verdict — "
            "the seam is not consulted"
        )
