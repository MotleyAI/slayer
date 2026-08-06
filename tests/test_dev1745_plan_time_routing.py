"""DEV-1745 (W3) — filter classification is plan-time; the generator consumes
the plan verbatim (P-D: plan decides, render emits).

The outer combined-SELECT WHERE wrapper (DEV-1503) currently decides at RENDER
time: the generator re-walks ``planned_query.filters_by_phase`` with
``walk_value_keys`` looking for AGGREGATE-phase filters that reference a
filtered-local isolated aggregate, and builds its own id set. That is policy
decided during emission.

After this change the planner computes the routing and ``PlannedQuery`` carries
it; the generator reads the field and never re-derives it.

The decisive test is that the plan field is AUTHORITATIVE: clear it, and the
outer WHERE disappears. A generator that re-walks would keep emitting it and
the test fails — which is exactly the coupling being removed.

``frame_bound_columns`` and the windowed ``_src`` residuals
(``SrcFilterRewrite``) are already plan-side; the guards here pin that so the
migration does not quietly re-introduce a render-time derivation.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.planned import PlannedQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._engine_helpers import _engine_generate


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

    def test_plan_declares_the_field_on_the_schema(self) -> None:
        """A DECLARED Pydantic field, not merely an attribute — ``model_copy``
        can graft an undeclared key onto an instance, so ``hasattr`` alone
        would not prove the schema owns it."""
        from slayer.engine.planned import PlannedQuery

        assert "outer_where_filter_ids" in PlannedQuery.model_fields, (
            "PlannedQuery must DECLARE the outer-WHERE routing field decided "
            f"at plan time; fields are {sorted(PlannedQuery.model_fields)}"
        )

    def test_field_is_populated_for_the_isolated_shape(self) -> None:
        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        assert list(planned.outer_where_filter_ids) == ["f0"], (
            f"expected f0 routed to the outer WHERE, got "
            f"{getattr(planned, 'outer_where_filter_ids', None)!r}"
        )

    def test_field_is_empty_without_an_isolated_aggregate(self) -> None:
        plain = SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "amount:sum", "name": "a"}],
            filters=["amount:sum > 100"],
        )
        planned = plan_query(query=plain, bundle=_bundle())
        assert list(planned.outer_where_filter_ids) == []

    def test_the_isolated_plan_is_the_trigger(self) -> None:
        """Sanity-pin the shape the routing keys off: a cross-model plan whose
        cte_root_model is set."""
        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        roots = [
            p.cte_root_model for p in planned.cross_model_aggregate_plans
        ]
        assert any(r is not None for r in roots), roots


@pytest.mark.asyncio
class TestGeneratorConsumesThePlanVerbatim:

    async def _sql(self, query: SlayerQuery) -> str:
        return await _engine_generate(
            query=query, model=_orders(), dialect="postgres",
            validate=False, extra_models=[_customers()],
        )

    # The predicate applied to the JOINED-BACK ``_cm_`` column on the outer,
    # non-aggregating SELECT — the shape this routing exists to produce, and
    # one nothing else in the query emits.
    OUTER_WHERE = 'WHERE _cm_orders__eu_amount_sum."orders.eu" > 100'

    async def test_outer_where_is_emitted_for_the_isolated_shape(self) -> None:
        sql = await self._sql(_outer_where_query())
        assert self.OUTER_WHERE in sql, sql

    async def test_clearing_the_plan_field_removes_the_outer_where(self) -> None:
        """P-D: the plan is authoritative. A generator that re-walks the
        filters at render time would ignore the cleared field and keep
        emitting the outer WHERE.

        Asserted on the outer WHERE specifically rather than on the predicate
        text appearing anywhere: clearing the routing does not delete the
        user's filter, it returns it to the default HAVING placement. Demanding
        that ``> 100`` vanish from the whole query would be demanding that a
        filter be silently dropped.
        """
        from slayer.sql.generator import SQLGenerator

        planned = plan_query(query=_outer_where_query(), bundle=_bundle())
        assert list(planned.outer_where_filter_ids) == ["f0"], (
            "precondition: the plan must be POPULATED before clearing, "
            "otherwise clearing proves nothing"
        )
        cleared = planned.model_copy(update={"outer_where_filter_ids": []})
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate_from_planned(planned_query=cleared, bundle=_bundle())
        assert self.OUTER_WHERE not in sql, (
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
        """A DECLARED field, checked the same way as outer_where_filter_ids.
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
