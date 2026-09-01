"""DEV-1746 §5.12 — the empty-base grain becomes a typed planner node.

When a query asks only for isolated aggregates — no host row slots and no
host-local aggregates — the host base has nothing to project, so the generator
synthesises a one-row spine for the cross-model CROSS JOIN to hang off::

    _base AS (SELECT 1 AS _placeholder)                       -- unfiltered
    _base AS (SELECT 1 AS _placeholder FROM orders AS orders_x
              WHERE orders_x.status = 'paid' LIMIT 1)         -- host-filtered

Three decisions are made at RENDER time today: whether the shape applies at all,
which ROW-phase filters are host-local rather than routed into a CTE, and the
``LIMIT 1`` collapse. P-D says the plan decides and the renderer emits, so they
move to a typed node. **The emitted SQL does not change** — that is the whole
point, and it is asserted verbatim below.

Why ``LIMIT 1`` is load-bearing rather than an optimisation: the filtered form
keeps the host FROM so the WHERE can gate the result, but a host FROM yields N
rows, and CROSS JOINing N rows to a 1-row scalar aggregate would repeat the
answer N times. ``LIMIT 1`` collapses the spine to one row while an empty match
still yields zero rows overall. The unfiltered form drops the FROM entirely for
the same reason.

Per Codex D7 the node stays minimal — its PRESENCE is the discriminator, and it
carries the host filter ids. No ``grain_slot_ids`` field is added: in this shape
the grain is empty by definition, and a field that is always ``[]`` documents
nothing. The invariant it would have encoded is asserted directly instead —
whenever the node is present, every cross-model plan's join-back pairs are
empty, which is exactly why the join degenerates to a CROSS JOIN.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._cross_model_chain import (
    _countries,
    _customers_v2,
    _gen,
    _orders_x,
    _regions,
)
from tests._dev1746_fixtures import (
    make_sqlite_engine,
    seed_dev1746_sqlite,
)
from tests._engine_helpers import _extract_cte_body, _norm

#: The emitted ``_base`` bodies, pinned verbatim (whitespace-normalised).
UNFILTERED_BASE = _norm("SELECT 1 AS _placeholder")
FILTERED_BASE = _norm(
    """
    SELECT 1 AS _placeholder
    FROM orders AS orders_x
    WHERE orders_x.status = 'paid'
    LIMIT 1
    """
)


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_x(),
        referenced_models=[_customers_v2(), _regions(), _countries()],
    )


def _unfiltered_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        measures=[ModelMeasure(
            formula="customers_v2.lifetime_value:sum", name="ltv",
        )],
    )


def _filtered_query() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders_x",
        measures=[ModelMeasure(
            formula="customers_v2.lifetime_value:sum", name="ltv",
        )],
        filters=["status == 'paid'"],
    )


def _non_empty_base_query() -> SlayerQuery:
    """A host dimension means the base is NOT empty — the node must be absent."""
    return SlayerQuery(
        source_model="orders_x",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(
            formula="customers_v2.lifetime_value:sum", name="ltv",
        )],
    )


@pytest.fixture
async def exec_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "dev1746.db")
        seed_dev1746_sqlite(db_path)
        yield await make_sqlite_engine(os.path.join(d, "store"), db_path)


# =========================================================================== #
# The typed node.
# =========================================================================== #
class TestEmptyBaseGrainPlanNode:

    def test_node_is_present_for_the_unfiltered_shape(self) -> None:
        """NEW (§5.12): the decision is on the plan, not re-derived at render."""
        planned = plan_query(query=_unfiltered_query(), bundle=_bundle())
        assert planned.empty_base_plan is not None, (
            "the empty-base shape is still decided at render time — the plan "
            "carries no node for it."
        )
        assert list(planned.empty_base_plan.host_filter_ids) == [], (
            f"unfiltered shape recorded host filters: "
            f"{planned.empty_base_plan.host_filter_ids}"
        )

    def test_node_records_the_host_local_filters(self) -> None:
        """The filtered form's WHERE is exactly the ROW-phase filters that were
        NOT routed into a cross-model CTE."""
        planned = plan_query(query=_filtered_query(), bundle=_bundle())
        assert planned.empty_base_plan is not None
        ids = list(planned.empty_base_plan.host_filter_ids)
        assert ids, (
            "the host-local filter was not recorded on the node, so the "
            "renderer would have to re-walk `filters_by_phase` to find it."
        )
        known = {f.id for f in planned.filters_by_phase}
        assert set(ids) <= known, (
            f"node references unknown filter ids: {set(ids) - known}"
        )

    def test_node_is_absent_when_the_base_is_not_empty(self) -> None:
        """A host dimension gives the base something to project; the placeholder
        spine must not appear."""
        planned = plan_query(query=_non_empty_base_query(), bundle=_bundle())
        assert planned.empty_base_plan is None, (
            "the empty-base node was set for a query that HAS host row slots"
        )

    def test_node_presence_implies_the_projection_is_isolated_only(self) -> None:
        """Codex D7: the grain semantics the node would have carried as a field,
        asserted as the invariant it actually is.

        The node is present exactly when every projected value is an isolated
        aggregate — which is what leaves ``_base`` with no grain columns, and so
        why the combined SELECT CROSS JOINs rather than joining on a predicate.

        Deliberately NOT asserted against the producer's shared-grain slots:
        that set can name a HIDDEN row slot a filter created (``status`` in the
        filtered shape), which never becomes a projected grain column. The
        emitted CROSS JOIN is asserted directly in
        ``TestEmittedSqlIsUnchanged.test_combined_select_cross_joins_the_scalar_cte``,
        which is the operative guarantee.
        """
        for factory in (_unfiltered_query, _filtered_query):
            planned = plan_query(query=factory(), bundle=_bundle())
            assert planned.empty_base_plan is not None
            # DEV-1836: a cross-model aggregate is now an isolated regroup-attach
            # placeholder (its producer is a target-rooted _cm_ CTE), so its host
            # projection slot carries the substitution placeholder key.
            attach_placeholders = {
                sub.placeholder
                for a in planned.regroup_attach_plans
                for sub in a.substitutions
            }
            slots_by_id = {s.id: s for s in planned.row_slots + planned.aggregate_slots}
            isolated = {
                sid for sid in planned.projection
                if slots_by_id.get(sid) is not None
                and slots_by_id[sid].key in attach_placeholders
            }
            assert planned.projection, "expected a non-empty projection"
            assert all(sid in isolated for sid in planned.projection), (
                f"empty-base plan present but the projection {planned.projection} "
                f"contains a slot that is not an isolated aggregate "
                f"(isolated: {sorted(isolated)}) — such a slot would have to be "
                f"materialised in _base, which then is not a placeholder spine."
            )

    def test_generator_consumes_the_node_rather_than_re_deriving(self) -> None:
        """P-D: clearing the plan field must change the emitted SQL. If it does
        not, the generator re-derived the decision and the node is decorative.
        """
        from slayer.sql.generator import SQLGenerator

        planned = plan_query(query=_filtered_query(), bundle=_bundle())
        assert planned.empty_base_plan is not None, (
            "precondition: the plan must be POPULATED before clearing, "
            "otherwise clearing proves nothing"
        )
        cleared = planned.model_copy(update={"empty_base_plan": None})
        gen = SQLGenerator(dialect="postgres")
        sql = gen.generate_from_planned(planned_query=cleared, bundle=_bundle())
        assert "_placeholder" not in sql, (
            "the generator re-derived the empty-base shape instead of consuming "
            f"the plan:\n{sql}"
        )


# =========================================================================== #
# Byte-parity — §5.12 changes where the decision lives, not the SQL.
# =========================================================================== #
class TestEmittedSqlIsUnchanged:

    async def test_unfiltered_base_body_is_unchanged(self) -> None:
        sql = await _gen(_unfiltered_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_base"))
        assert body == UNFILTERED_BASE, (
            f"unfiltered placeholder spine changed:\n  actual:   {body}\n"
            f"  expected: {UNFILTERED_BASE}"
        )

    async def test_filtered_base_body_is_unchanged(self) -> None:
        sql = await _gen(_filtered_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_base"))
        assert body == FILTERED_BASE, (
            f"filtered placeholder spine changed:\n  actual:   {body}\n"
            f"  expected: {FILTERED_BASE}"
        )

    async def test_unfiltered_shape_has_no_from_clause(self) -> None:
        """Explicit: the unfiltered spine must NOT read the host table. With a
        FROM it would be N rows, and the CROSS JOIN would repeat the scalar
        aggregate N times."""
        sql = await _gen(_unfiltered_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_base"))
        assert "FROM" not in body.upper(), (
            f"the unfiltered placeholder acquired a FROM clause:\n{body}"
        )

    async def test_filtered_shape_keeps_the_limit_one_collapse(self) -> None:
        """The LIMIT 1 rule, stated as its own assertion so a refactor cannot
        drop it silently."""
        sql = await _gen(_filtered_query(), dialect="postgres")
        body = _norm(_extract_cte_body(sql, r"_base"))
        assert body.upper().endswith("LIMIT 1"), (
            f"the filtered placeholder lost its LIMIT 1 collapse:\n{body}"
        )

    async def test_combined_select_cross_joins_the_scalar_cte(self) -> None:
        for factory in (_unfiltered_query, _filtered_query):
            sql = await _gen(factory(), dialect="postgres")
            assert "CROSS JOIN _cm_" in _norm(sql), sql


# =========================================================================== #
# Execution — the semantics the shape exists to preserve.
# =========================================================================== #
class TestEmptyBaseExecution:

    async def test_scalar_aggregate_is_not_multiplied(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """The reason for the LIMIT-1 collapse: one row, counted once."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.spend:sum", name="total")],
        )
        resp = await exec_engine.execute(query)
        assert len(resp.data) == 1, (
            f"the scalar aggregate was multiplied by the host rowset:\n"
            f"{resp.data}"
        )
        assert resp.data[0]["orders.total"] == pytest.approx(1325.0), resp.data

    async def test_host_filter_gates_the_whole_result(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """A host-local filter that MATCHES still yields the full scalar
        aggregate (the filter gates the spine, it does not restrict the
        isolated CTE)."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.spend:sum", name="total")],
            filters=["status == 'paid'"],
        )
        resp = await exec_engine.execute(query)
        assert len(resp.data) == 1, resp.data
        assert resp.data[0]["orders.total"] == pytest.approx(1325.0), resp.data

    async def test_host_filter_matching_nothing_yields_no_rows(
        self, exec_engine: SlayerQueryEngine,
    ) -> None:
        """The other half of the gate: no host row matches -> empty spine ->
        zero rows, NOT a row with the scalar aggregate in it."""
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.spend:sum", name="total")],
            filters=["status == 'nonexistent'"],
        )
        resp = await exec_engine.execute(query)
        assert resp.data == [], (
            f"a non-matching host filter still returned rows:\n{resp.data}"
        )
