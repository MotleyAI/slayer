"""Emitted stage schemas: the pass-through origin of each output and the edges the stage inherits.

Spec: openspec …/specs/queries/multi-stage — "Stages inherit edges on pass-through keys",
"Inherited edges re-derive their cardinality from the stage grain".
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageSchema
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev2219_fixtures import base_datasets, m


def _schema(**body: Any) -> StageSchema:
    datasets = base_datasets()
    population = next(d for d in datasets if d.name == body.get("population", "orders"))
    bundle = ResolvedSourceBundle(population=population, referenced_datasets=datasets, dialect="duckdb")
    query = SlayerQuery.model_validate({"population": "orders", **body})
    schema = plan_query(query=query, bundle=bundle).stage_schema
    assert schema is not None
    return schema


def _passes(schema: StageSchema) -> dict[str, Any]:
    return {c.name: c.pass_through for c in schema.fields}


class TestPassThroughOrigin:
    def test_plain_dimension_passes_its_field(self) -> None:
        origin = _passes(_schema(dimensions=["orders.customer_id"], metrics=[m("count(orders.*)", "n")]))["customer_id"]
        assert origin is not None
        assert origin.path == ()
        assert origin.field == "customer_id"

    def test_alias_keeps_the_origin(self) -> None:
        origin = _passes(_schema(dimensions=[{"field": "orders.customer_id", "alias": "cid"}]))["cid"]
        assert origin is not None
        assert origin.field == "customer_id"

    def test_dotted_dimension_records_its_hop(self) -> None:
        origin = _passes(_schema(dimensions=["customers.region"]))["customers__region"]
        assert origin is not None
        assert origin.field == "region"
        (hop,) = origin.path
        assert (hop.source_dataset, hop.to) == ("orders", "customers")

    @pytest.mark.parametrize("expr", ["CAST(orders.customer_id AS TEXT)", "orders.customer_id + 0"])
    def test_cast_or_computed_is_not_a_pass_through(self, expr: str) -> None:
        assert _passes(_schema(dimensions=[{"field": expr, "alias": "cid"}]))["cid"] is None

    def test_time_bucket_is_not_a_pass_through(self) -> None:
        schema = _schema(time_dimensions=[{"field": "orders.ordered_at", "granularity": "month"}], metrics=[m("count(orders.*)", "n")])
        assert _passes(schema)["ordered_at"] is None

    def test_metric_is_not_a_pass_through(self) -> None:
        assert _passes(_schema(dimensions=["orders.status"], metrics=[m("max(orders.customer_id)", "c")]))["c"] is None


class TestInheritedEdges:
    def test_grouped_key_inherits_a_one_to_one_edge(self) -> None:
        (edge,) = [e for e in _schema(dimensions=["orders.customer_id"], metrics=[m("count(orders.*)", "n")]).inherited_edges
                   if e.to == "customers"]
        assert edge.from_columns == ("customer_id",)
        assert edge.to_columns == ("id",)
        assert edge.cardinality is RelationshipCardinality.ONE_TO_ONE
        assert edge.join_type is JoinType.LEFT
        assert (edge.origin.dataset, edge.origin.relationship, edge.origin.forward) == ("orders", "customers", True)

    def test_wider_grain_keeps_the_stage_side_many(self) -> None:
        schema = _schema(dimensions=["orders.customer_id", "orders.status"], metrics=[m("count(orders.*)", "n")])
        (edge,) = [e for e in schema.inherited_edges if e.to == "customers"]
        assert edge.cardinality is RelationshipCardinality.MANY_TO_ONE

    def test_undeclared_cardinality_stays_undeclared(self) -> None:
        (edge,) = [e for e in _schema(dimensions=["orders.ordered_at"]).inherited_edges if e.to == "calendar"]
        assert edge.cardinality is None

    def test_reverse_edge_is_inherited(self) -> None:
        schema = _schema(population="customers", dimensions=["customers.id"], metrics=[m("count(customers.*)", "n")])
        targets = {(e.to, e.origin.dataset, e.origin.forward) for e in schema.inherited_edges}
        assert ("orders", "orders", False) in targets

    def test_target_key_proxy_inherits_the_hop(self) -> None:
        schema = _schema(dimensions=["customers.id"], metrics=[m("count(orders.*)", "n")])
        (edge,) = [e for e in schema.inherited_edges if e.to == "customers"]
        assert edge.from_columns == ("customers__id",)

    def test_no_pass_through_no_edge(self) -> None:
        assert _schema(dimensions=["orders.status"], metrics=[m("count(orders.*)", "n")]).inherited_edges == ()
