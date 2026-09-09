"""DEV-1745 (W3) / DEV-1865 — the plan carries TYPED masks; the one lowering
(``_lower_positions``) derives the outer-WHERE routing deterministically from
them, and every render site consumes that one decision.

The decisive test is that the plan's mask list is AUTHORITATIVE: clear it, and
the outer WHERE disappears. A generator that re-walked the filters on its own
would keep emitting it and the test fails — exactly the coupling removed.

``frame_bound_columns`` and the windowed ``_src`` residuals
(``SrcFilterRewrite``) are already plan-side; the guards here pin that so the
migration does not quietly re-introduce a render-time derivation.
"""

from __future__ import annotations

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.planned import PlannedQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import SQLGenerator, _lower_positions

from tests._engine_helpers import _engine_generate, _outer_select


# --------------------------------------------------------------------------- #
# A filtered-local isolated aggregate: `eu_amount` carries a Column.filter that
# crosses into `customers`, so the measure is isolated into a _cm_ CTE with
# cte_root_model set, and the AGGREGATE-phase filter on it routes to the outer
# combined SELECT as a plain WHERE on the joined-back column.
# --------------------------------------------------------------------------- #
def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="tier", type=DataType.TEXT),
        ],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="eu_amount", sql="amount",
                   filter="customers.tier = 'eu'", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders(), referenced_models=[_customers()],
    )


def _outer_where_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[{"formula": "status", "name": "status"}],
        measures=[{"formula": "eu_amount:sum", "name": "eu"}],
        filters=["eu_amount:sum > 100"],
    )


# --------------------------------------------------------------------------- #
class TestPlanCarriesOuterWhereRouting:

    def test_plan_declares_the_masks_field_on_the_schema(self) -> None:
        """A DECLARED Pydantic field, not merely an attribute — ``model_copy``
        can graft an undeclared key onto an instance, so ``hasattr`` alone
        would not prove the schema owns it."""
        assert "masks" in PlannedQuery.model_fields, (
            "PlannedQuery must DECLARE the typed mask list the lowering "
            f"routes from; fields are {sorted(PlannedQuery.model_fields)}"
        )

    def test_lowering_routes_the_isolated_shape_outer(self) -> None:
        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        (mask,) = planned.masks
        assert _lower_positions(planned).outer_where_ids == [mask.slot_id], (
            "expected the isolated-aggregate mask routed to the outer WHERE"
        )

    def test_routing_is_empty_without_an_isolated_aggregate(self) -> None:
        plain = SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "amount:sum", "name": "a"}],
            filters=["amount:sum > 100"],
        )
        planned = plan_query(query=plain, bundle=_bundle())
        assert _lower_positions(planned).outer_where_ids == []

    def test_the_isolated_plan_is_the_trigger(self) -> None:
        """Sanity-pin the shape the routing keys off — DEV-1838 D5: a
        HOST-rooted regroup producer, its filter routed to the outer WHERE."""
        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        assert any(
            a.producer_root_model is None and a.attach_phase == "combined"
            for a in planned.regroup_attach_plans
        ), planned.regroup_attach_plans
        assert _lower_positions(planned).outer_where_ids, (
            "the filter over the isolated aggregate must route to the outer "
            "WHERE"
        )


@pytest.mark.asyncio
class TestGeneratorConsumesThePlanVerbatim:

    async def _sql(self, query: SlayerQuery) -> str:
        return await _engine_generate(
            query=query, model=_orders(), dialect="postgres",
            validate=False, extra_models=[_customers()],
        )

    @staticmethod
    def _has_outer_cm_where(sql: str) -> bool:
        """True iff the OUTERMOST SELECT filters the joined-back ``_cm_`` measure
        ``orders.eu`` with ``> 100`` — the routed outer WHERE. Matched by
        STRUCTURE, not by the generated ``_cm_`` alias spelling, so an
        alias-naming change cannot make the negative assertion pass vacuously."""
        where = _outer_select(sql).args.get("where")
        if where is None:
            return False
        for gt in where.this.find_all(exp.GT):
            col = gt.this
            if (
                isinstance(col, exp.Column)
                and col.name == "orders.eu"
                and col.table.startswith("_cm_")
                and gt.expression == exp.Literal.number(100)
            ):
                return True
        return False

    async def test_outer_where_is_emitted_for_the_isolated_shape(self) -> None:
        sql = await self._sql(_outer_where_query())
        assert self._has_outer_cm_where(sql), sql

    async def test_clearing_the_masks_removes_the_outer_where(self) -> None:
        """P-D: the plan's mask list is authoritative. A generator that
        re-walked the raw filters at render time would ignore the cleared
        masks and keep emitting the outer WHERE."""
        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        assert _lower_positions(planned).outer_where_ids, (
            "precondition: the routing must be POPULATED before clearing, "
            "otherwise clearing proves nothing"
        )
        cleared = planned.model_copy(update={"masks": []})
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate_from_planned(planned_query=cleared, bundle=_bundle())
        assert not self._has_outer_cm_where(sql), (
            "the generator re-derived the outer-WHERE routing instead of "
            f"consuming the plan:\n{sql}"
        )


class TestFrameBoundColumnsStayPlanSide:
    """Parity guards — already true today, pinned so the door migration does
    not re-introduce a render-time derivation."""

    def _windowed_query(self) -> SlayerQuery:
        return SlayerQuery(
            source_model="orders",
            time_dimensions=[{
                "dimension": "created_at",
                "granularity": TimeGranularity.MONTH,
                "date_range": ["2024-01-01", "2024-12-31"],
            }],
            measures=[{"formula": "amount:sum", "name": "a"}],
        )

    def test_plan_carries_frame_bound_columns(self) -> None:
        """A DECLARED field, checked the same way as masks.
        ``hasattr`` is always true for a field with a default_factory, so it
        could not fail regardless of planner behaviour."""
        assert "frame_bound_columns" in PlannedQuery.model_fields

    def test_frame_bound_columns_covers_the_time_dimension(self) -> None:
        """Names the expected column, not just "non-empty" — the query has one
        time dimension, so a plan carrying some OTHER column would satisfy a
        truthiness check while getting the frame-bound set wrong."""
        planned = plan_query(query=self._windowed_query(), bundle=_bundle())
        leaves = {getattr(k, "leaf", None) for k in planned.frame_bound_columns}
        assert "created_at" in leaves, (
            f"the time dimension's raw column must be carried on the plan so "
            f"both strip_frame_bounds call sites read the SAME set; got "
            f"{planned.frame_bound_columns!r}"
        )
