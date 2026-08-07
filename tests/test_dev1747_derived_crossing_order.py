"""DEV-1747 / DEV-1735 — ORDER BY on a LOCAL DERIVED column whose SQL crosses.

``orders.cust_region`` is a local derived column (``path == ()``) whose
``Column.sql`` is ``customers__regions.name`` — it reaches THROUGH two joins.

DEV-1735 recorded this as "rejected in BOTH grouped and ungrouped queries".
That is **stale for the grouped half**: DEV-1709 widened the Law-3 host-rooted
isolation trigger to any crossing INPUT, and a derived source whose SQL crosses
is such an input, so a grouped query already routes it to a host-rooted CTE
today. Group 1 pins that shape so the rerooting/ORDER BY consolidation cannot
regress it — it is the exact reference shape the bare joined column adopts in
``test_dev1747_grouped_joined_order.py``.

The UNGROUPED half is still rejected, and that is the real inconsistency
DEV-1735 names: ``ORDER BY customers.regions.name`` resolves ungrouped (Law 1
pulls the join, DEV-1703 Phase 1) but ``ORDER BY cust_region`` — the same
column reached through a derived definition — raises. Group 2 closes it.

The fix is plan-side: the crossed paths are already structural (§5.3), so the
planner registers them and Law 1 pulls the join into the base FROM, exactly as
it does for the bare joined ref. That is also what lets the render-time
throwaway-``ScopeFrame`` probe in ``_apply_order_limit_from_planned`` go — it
exists solely to DETECT this crossing at render time.

Refs: DEV-1747 (D9), DEV-1735 ("Also in scope"), DEV-1709, DEV-1703 Phase 1.
"""
from __future__ import annotations

import os
import tempfile

from slayer.core.errors import UnresolvableOrderColumnError
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery
from tests._dev1747_fixtures import (
    GROUP_A_AMOUNT,
    GROUP_B_AMOUNT,
    GROUP_NULL_AMOUNT,
    base_from_join_aliases,
    dev1747_bundle,
    dev1747_models,
    make_sqlite_engine,
    order_by_text,
    outermost_select,
    response_column_values,
    seed_dev1747_sqlite,
)
from tests._engine_helpers import _engine_generate


def _squash(sql: str) -> str:
    """Collapse whitespace — sqlglot pretty-prints a long expression across
    lines in one context and inline in another; that is not the subject."""
    return " ".join(sql.split())

_MEASURE = [{"formula": "amount:sum", "name": "rev"}]


async def _sql(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    models = dev1747_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:], dialect=dialect,
    )


async def _execute(query: SlayerQuery):
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "dev1747.db")
        seed_dev1747_sqlite(db)
        engine = await make_sqlite_engine(d, db)
        return await engine.execute(query)


def _grouped(direction: str, column: str = "cust_region") -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=_MEASURE,
        order=[OrderItem(column=ColumnRef(name=column), direction=direction)],
    )


def _ungrouped(direction: str, column: str = "cust_region") -> SlayerQuery:
    """Raw rows — ``distinct_dimension_values=False`` means no GROUP BY, so the
    row IS the grain and a bare reference to the sort key is legal."""
    return SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        distinct_dimension_values=False,
        order=[OrderItem(column=ColumnRef(name=column), direction=direction)],
    )


# ---------------------------------------------------------------------------
# Group 1 — grouped: already works; pin the shape and the D10 direction
# ---------------------------------------------------------------------------
class TestGroupedDerivedCrossing:
    async def test_grouped_derived_crossing_resolves(self) -> None:
        sql = await _sql(_grouped("asc"))
        assert order_by_text(sql), f"no ORDER BY emitted:\n{sql}"

    async def test_crossed_join_lives_in_the_isolated_cte_not_the_base(self) -> None:
        sql = await _sql(_grouped("asc"))
        assert "customers__regions" not in base_from_join_aliases(sql), (
            f"the derived column's join leaked into the host base:\n{sql}"
        )
        assert "customers__regions" in sql

    async def test_ascending_orders_by_each_group_minimum(self) -> None:
        """D10 changes this from today's MAX. Under MAX the ASC order is
        [B, A, N] (Bravo < Zulu); under MIN it is [A, B, N] (Alpha < Bravo).
        The corpus makes the two orderings disagree on purpose."""
        response = await _execute(_grouped("asc"))
        assert response_column_values(response.data, "orders.status") == ["A", "B", "N"]

    async def test_descending_orders_by_each_group_maximum(self) -> None:
        response = await _execute(_grouped("desc"))
        assert response_column_values(response.data, "orders.status") == ["A", "B", "N"]

    async def test_sibling_measure_untouched(self) -> None:
        response = await _execute(_grouped("asc"))
        by_status = {r["orders.status"]: r["orders.rev"] for r in response.data}
        assert by_status == {
            "A": GROUP_A_AMOUNT, "B": GROUP_B_AMOUNT, "N": GROUP_NULL_AMOUNT,
        }

    async def test_derived_sort_key_is_not_projected(self) -> None:
        response = await _execute(_grouped("asc"))
        for row in response.data:
            assert set(row) == {"orders.status", "orders.rev"}


# ---------------------------------------------------------------------------
# Group 2 — ungrouped: the DEV-1735 remainder
# ---------------------------------------------------------------------------
class TestUngroupedDerivedCrossing:
    async def test_ungrouped_derived_crossing_no_longer_rejects(self) -> None:
        try:
            sql = await _sql(_ungrouped("asc"))
        except UnresolvableOrderColumnError as exc:  # pragma: no cover
            raise AssertionError(
                f"ungrouped derived crossing still rejects: {exc}"
            ) from exc
        assert order_by_text(sql), f"no ORDER BY emitted:\n{sql}"

    async def test_ungrouped_pulls_the_join_into_the_base_from(self) -> None:
        """Ungrouped is the Law-1 case, NOT the isolation case: the row is the
        grain, so the join belongs in the base FROM exactly as it does for the
        bare joined ref that already works."""
        sql = await _sql(_ungrouped("asc"))
        assert "customers__regions" in base_from_join_aliases(sql), (
            f"Law 1 did not pull the derived column's join:\n{sql}"
        )

    async def test_ungrouped_emits_no_aggregate_wrap(self) -> None:
        """No GROUP BY means no wrap — an aggregate here would both be wrong
        and force grouping the query never asked for."""
        sql = await _sql(_ungrouped("asc"))
        upper = sql.upper()
        assert "MIN(" not in upper, (
            f"ungrouped sort key was wrapped in MIN:\n{sql}"
        )
        assert "MAX(" not in upper, (
            f"ungrouped sort key was wrapped in MAX:\n{sql}"
        )

    async def test_ungrouped_derived_matches_the_bare_joined_shape(self) -> None:
        """The consistency DEV-1735 asks for: ``ORDER BY cust_region`` and
        ``ORDER BY customers.regions.name`` denote the same column, so their
        emitted sort terms must agree."""
        derived_sql = await _sql(_ungrouped("asc"))
        bare_sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            distinct_dimension_values=False,
            order=[OrderItem(
                column=ColumnRef(name="name", model="customers.regions"),
                direction="asc",
            )],
        ))
        assert order_by_text(derived_sql) == order_by_text(bare_sql)

    async def test_ungrouped_executes_in_the_right_order(self) -> None:
        response = await _execute(_ungrouped("asc"))
        statuses = response_column_values(response.data, "orders.status")
        # Alpha(A) < Bravo(B) < Zulu(A), NULL last → A, B, A, N.
        assert statuses == ["A", "B", "A", "N"]

    async def test_non_crossing_derived_column_still_works_ungrouped(self) -> None:
        """The control: ``amount_x2`` is derived but LOCAL. It resolved before
        this change and must keep resolving — the fix must not be "stop
        checking whether the SQL crosses"."""
        sql = await _sql(_ungrouped("asc", column="amount_x2"))
        assert order_by_text(sql), f"non-crossing derived sort key broke:\n{sql}"
        assert "customers__regions" not in base_from_join_aliases(sql)


# ---------------------------------------------------------------------------
# Group 3 — the render-time probe is gone
# ---------------------------------------------------------------------------
class TestRenderTimeProbeRemoved:
    async def test_the_probe_host_method_is_never_called(
        self, monkeypatch,
    ) -> None:
        """The probe builds a throwaway ``ScopeFrame`` inside
        ``_apply_order_limit_from_planned`` purely to DETECT this crossing at
        render time, then raises when it finds one. §5.10 makes the decision at
        plan time, so the method must leave the production path entirely.

        A sentinel on the method rather than on ``ScopeFrame.__init__``:
        legitimate scopes are constructed constantly, so counting them cannot
        isolate the probe, whereas "this method is not called" is exactly the
        claim.
        """
        from slayer.sql.generator import SQLGenerator

        assert hasattr(SQLGenerator, "_apply_order_limit_from_planned"), (
            "the method was deleted; P-J defers deletion to PR 6"
        )

        def _boom(*_a, **_kw):
            raise AssertionError(
                "the render-time crossing probe is still on the production "
                "path — §5.10 requires the decision to be planned"
            )

        monkeypatch.setattr(
            SQLGenerator, "_apply_order_limit_from_planned", _boom,
        )
        sql = await _sql(_ungrouped("desc"))
        assert order_by_text(sql)

    async def test_plan_carries_the_crossing_decision(self) -> None:
        """Plan-level: the order entry's scope is decided before rendering
        (P-D). A HOST_BASE_HIDDEN scope on the ungrouped derived entry is what
        tells the renderer to split-emit rather than probe."""
        from slayer.engine.planned import OrderScope
        from slayer.engine.stage_planner import plan_query

        plan = plan_query(query=_ungrouped("asc"), bundle=dev1747_bundle())
        assert plan.order, "plan carries no order entries"
        assert plan.order[0].scope in (
            OrderScope.HOST_BASE, OrderScope.HOST_BASE_HIDDEN,
        )


# ---------------------------------------------------------------------------
# Group 4 — a derived column defined over ANOTHER derived column
# ---------------------------------------------------------------------------
class TestDerivedOfDerivedSortKey:
    """``amount_x4`` is ``amount_x2 * 2``, and ``amount_x2`` is ``amount * 2``.

    Only the EXPANDING resolver inlines the sibling. The projection path always
    used it; the hidden sort-key path resolved the raw ``Column.sql`` instead
    and emitted the sibling's NAME — and ``amount_x2`` is not a column in the
    database, so the statement failed there rather than here (CodeRabbit).

    The two paths now share one expansion, which is the only thing that keeps
    them from drifting again.
    """

    @staticmethod
    def _sort_term(sql: str) -> str:
        terms = order_by_text(sql)
        assert terms, f"no ORDER BY emitted:\n{sql}"
        return terms

    async def test_hidden_derived_of_derived_expands_its_sibling(self) -> None:
        sql = await _sql(_ungrouped("asc", column="amount_x4"))
        term = self._sort_term(sql)
        assert "amount_x2" not in term, (
            f"the sort term names the DERIVED sibling, which is not a database "
            f"column — the statement would fail at the DB:\n{sql}"
        )
        assert "orders.amount" in term, (
            f"the sort term does not reach the real underlying column:\n{sql}"
        )

    async def test_it_matches_what_the_projection_would_emit(self) -> None:
        """P-G over the two paths: the same derived column must render the same
        expression whether it is projected or only sorted on. Compared to the
        PROJECTED expansion rather than to a literal, so the two cannot drift
        apart again without this failing."""
        hidden_sql = await _sql(_ungrouped("asc", column="amount_x4"))
        projected_sql = await _sql(SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status"), ColumnRef(name="amount_x4")],
            distinct_dimension_values=False,
            order=[OrderItem(column=ColumnRef(name="amount_x4"), direction="asc")],
        ))
        # The projected query sorts by the ALIAS, so compare the sort term
        # against the projected SELECT expression for the same column.
        projected_expr = next(
            (
                s.this.sql(dialect="postgres")
                for s in outermost_select(projected_sql).expressions
                if s.alias_or_name == "orders.amount_x4"
            ),
            None,
        )
        assert projected_expr, f"amount_x4 was not projected:\n{projected_sql}"
        hidden_term = self._sort_term(hidden_sql).replace(" ASC", "").strip()
        assert _squash(hidden_term) == _squash(projected_expr), (
            f"hidden sort key renders {hidden_term!r}, projection renders "
            f"{projected_expr!r}"
        )

    async def test_it_executes(self) -> None:
        """The end of the argument: the statement the DB actually runs. Under
        the old form SQLite raises ``no such column: amount_x2``."""
        response = await _execute(_ungrouped("asc", column="amount_x4"))
        # 4 raw rows, ordered by amount * 4 ascending: 11, 13, 17, 19.
        assert response_column_values(response.data, "orders.status") == [
            "A", "A", "B", "N",
        ]
