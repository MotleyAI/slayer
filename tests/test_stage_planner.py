"""Stage 7a.7 (DEV-1450) — stage planner tests.

The stage planner orchestrates multi-stage ``source_queries`` DAGs:

1. Topologically sorts a list of ``SlayerQuery`` stages.
2. For each stage in order, parses → binds → plans (using
   ProjectionPlanner + the cross-model planner Protocol).
3. Downstream stages bind against the upstream stage's
   ``StageSchema`` (flat namespace — DEV-1449).
4. Each stage's emitted ``StageSchema`` uses the user-supplied ``name``
   as the column alias (DEV-1448).

Dormant in 7a — no engine wiring. Stage 7b cuts ``engine.execute`` /
``engine.save_model`` over to this orchestrator.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import IllegalScopeReferenceError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query, plan_stages


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="amount", datatype=DataType.DOUBLE),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="region_id", datatype=DataType.INT),
        ],
        relationships=[
            DatasetRelationship(
                to="customers",
                join_pairs=[["customer_id", "id"]],
            ),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers",
        data_source="prod",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INT),
            DatasetField(name="revenue", datatype=DataType.DOUBLE),
        ],
        relationships=[
            DatasetRelationship(to="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions",
        data_source="prod",
        sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="name", datatype=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        dialect="postgres",
        population=_orders(),
        referenced_datasets=[_customers(), _regions()],
    )


# ---------------------------------------------------------------------------
# Single-stage smoke
# ---------------------------------------------------------------------------


class TestSingleStage:
    def test_simple_aggregation(self):
        q = SlayerQuery.model_validate({"population": "orders", "metrics": [{"expression": "sum(amount)"}]})
        planned = plan_query(query=q, bundle=_bundle())
        # One slot in projection.
        assert len(planned.projection) == 1
        slot_id = planned.projection[0]
        slots = planned.row_slots + planned.aggregate_slots
        agg_slots = [s for s in slots if s.id == slot_id]
        assert len(agg_slots) == 1

    def test_dimension_plus_measure(self):
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(amount)"}],
            "dimensions": ["status"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        # Two projection slots: status (row) + amount_sum (aggregate).
        assert len(planned.projection) == 2

    def test_filter_in_planned_query(self):
        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"expression": "sum(amount)"}],
            "filter": ["status == 'paid'"],
        })
        planned = plan_query(query=q, bundle=_bundle())
        # One typed mask per filter conjunct.
        assert len(planned.masks) == 1


# ---------------------------------------------------------------------------
# DEV-1448 acceptance: user `name` on join-traversed measure
# ---------------------------------------------------------------------------


class TestUserNameAlias:
    def test_named_measure_becomes_stage_schema_column(self):
        # Stage 1 declares `{"formula": "customers.revenue:sum", "name": "rev_sum"}`.
        # The stage's emitted StageSchema must include a column named
        # "rev_sum" — DEV-1448 contract.
        q = SlayerQuery(
            population="orders",
            metrics=[
                {"formula": "sum(customers.revenue)", "name": "rev_sum"},
            ],
            dimensions=["status"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        assert planned.stage_schema is not None
        col_names = [c.name for c in planned.stage_schema.fields]
        assert "rev_sum" in col_names


# ---------------------------------------------------------------------------
# DEV-1449 acceptance: downstream stage references upstream by flat name
# ---------------------------------------------------------------------------


class TestDownstreamStageFlatRefs:
    def test_multi_stage_dag_downstream_uses_flat_name(self):
        # Stage 1: orders → produces `status` and `amount_sum`.
        # Stage 2: source_model="stage1" → references upstream's
        # `amount_sum` by flat name.
        stage1 = SlayerQuery.model_validate({
            "name": "stage1",
            "population": "orders",
            "metrics": [{"expression": "sum(amount)"}],
            "dimensions": ["status"],
        })
        stage2 = SlayerQuery.model_validate({
            "population": "stage1",
            "metrics": [{"expression": "max(amount_sum)"}],
            "dimensions": ["status"],
        })
        planned_stages = plan_stages(
            queries=[stage1, stage2], bundle=_bundle(),
        )
        assert len(planned_stages) == 2
        # Stage 1 emitted a StageSchema with amount_sum.
        s1 = planned_stages[0]
        assert s1.stage_schema is not None
        s1_names = [c.name for c in s1.stage_schema.fields]
        assert "amount_sum" in s1_names

    def test_dotted_ref_in_downstream_stage_raises(self):
        # In stage 2 (StageSchema scope), dotted refs are illegal.
        stage1 = SlayerQuery(
            name="stage1",
            population="orders",
            metrics=[{"formula": "sum(customers.revenue)",
                       "name": "robot_details__modelseriesval"}],
            dimensions=["status"],
        )
        stage2 = SlayerQuery(
            population="stage1",
            metrics=[{"formula": "robot_details.modelseriesval"}],
            dimensions=["status"],
        )
        bundle = _bundle()
        with pytest.raises(IllegalScopeReferenceError):
            plan_stages(queries=[stage1, stage2], bundle=bundle)


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopoSort:
    def test_stages_in_dependency_order(self):
        # stage_b depends on stage_a; pass the two NAMED stages out of
        # dependency order (stage_b before stage_a) so plan_stages must
        # topo-sort them. The root (last entry) stays last by contract.
        stage_a = SlayerQuery.model_validate({
            "name": "stage_a",
            "population": "orders",
            "metrics": [{"expression": "sum(amount)"}],
            "dimensions": ["status"],
        })
        stage_b = SlayerQuery.model_validate({
            "name": "stage_b",
            "population": "stage_a",
            "metrics": [{"expression": "max(amount_sum)"}],
            "dimensions": ["status"],
        })
        root = SlayerQuery(population="stage_b", dimensions=["status"])
        planned = plan_stages(
            queries=[stage_b, stage_a, root], bundle=_bundle(),
        )
        assert len(planned) == 3
        # Reordered to dependency order: stage_a (FROM orders) before
        # stage_b (FROM stage_a) before the root (FROM stage_b).
        assert [p.source_relation for p in planned] == [
            "orders", "stage_a", "stage_b",
        ]

    def test_duplicate_stage_names_rejected(self):
        s1 = SlayerQuery.model_validate({
            "name": "dup",
            "population": "orders",
            "metrics": [{"expression": "sum(amount)"}],
            "dimensions": ["status"],
        })
        s2 = SlayerQuery.model_validate({
            "name": "dup",
            "population": "orders",
            "metrics": [{"expression": "max(amount)"}],
            "dimensions": ["status"],
        })
        root = SlayerQuery(
            population="dup",
            dimensions=["status"],
        )
        bundle = _bundle()
        with pytest.raises(ValueError, match="[Dd]uplicate stage"):
            plan_stages(queries=[s1, s2, root], bundle=bundle)

    def test_cycle_in_stages_rejected(self):
        # stage_a depends on stage_b, stage_b depends on stage_a — cycle.
        a = SlayerQuery(
            name="stage_a",
            population="stage_b",
            dimensions=["status"],
        )
        b = SlayerQuery(
            name="stage_b",
            population="stage_a",
            dimensions=["status"],
        )
        root = SlayerQuery(
            population="stage_a",
            dimensions=["status"],
        )
        bundle = _bundle()
        with pytest.raises(ValueError, match="[Cc]ycle"):
            plan_stages(queries=[a, b, root], bundle=bundle)


class TestStageSchemaMultiAlias:
    def test_multi_alias_emits_separate_columns(self):
        # Two declared measures with the same key but different names
        # share a slot. The StageSchema should expose BOTH aliases.
        q = SlayerQuery(
            population="orders",
            metrics=[
                {"formula": "sum(amount)", "name": "rev1"},
                {"formula": "sum(amount)", "name": "rev2"},
            ],
            dimensions=["status"],
        )
        planned = plan_query(query=q, bundle=_bundle())
        col_names = [c.name for c in planned.stage_schema.fields]
        # Both aliases present.
        assert "rev1" in col_names
        assert "rev2" in col_names
