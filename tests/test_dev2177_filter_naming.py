"""FILTER result keys come from the formula text; a filtered aggregate never shares its twin's name or value.

Spec: aggregations/functional-form › An aggregation accepts a FILTER clause. Data: the idiom dataset; ``first`` /
``last`` rank by ``order_date``. Earliest order 1 (10), earliest open 2 (20); latest 11 (5), latest open 9 (25).
"""

from __future__ import annotations

from typing import get_args

import pytest

from slayer.core.keys import AggregateKey
from slayer.core.scope import ModelScope
from slayer.core.syntax import parse_expr
from slayer.engine.binding import bind_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.naming import AggAliasProfile, canonical_aggregate_alias
from tests._dev2177_fixtures import m, num, q
from tests._mcp_idiom_fixtures import idiom_models, make_idiom_engine

PAID_COUNT = "count(*) FILTER (WHERE status = 'paid')"
OPEN_FIRST = "first(amount) FILTER (WHERE status = 'open')"


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_idiom_engine(dialect=request.param):
        yield e


def _row(resp) -> dict:
    assert len(resp.data) == 1, resp.data
    return {k: num(v) for k, v in resp.data[0].items()}


class TestResultKeys:
    async def test_constant_condition_named_from_its_text(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[PAID_COUNT, "sum(amount) FILTER (WHERE amount > 10)"]))
        assert _row(resp) == {"orders.count_filter_where_status_paid": 7, "orders.amount_sum_filter_where_amount_10": 475}

    async def test_variable_and_constant_conditions_follow_one_rule(self, engine: SlayerQueryEngine) -> None:
        variable = await engine.execute(q(measures=["sum(amount) FILTER (WHERE amount > {n})"], variables={"n": 10}))
        constant = await engine.execute(q(measures=["sum(amount) FILTER (WHERE amount > 10)"]))
        assert _row(variable) == {"orders.amount_sum_filter_where_amount_n": 475}
        assert _row(constant) == {"orders.amount_sum_filter_where_amount_10": 475}

    async def test_spellings_of_one_formula_share_a_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=["count(*)   filter (where status = 'paid')"]))
        assert _row(resp) == {"orders.count_filter_where_status_paid": 7}

    async def test_ordering_by_the_filtered_formula(self, engine: SlayerQueryEngine) -> None:
        for direction, expected in (("desc", ["paid", "open"]), ("asc", ["open", "paid"])):
            resp = await engine.execute(q(dimensions=["status"], measures=[PAID_COUNT],
                                          order=[{"column": PAID_COUNT, "direction": direction}]))
            assert resp.columns == ["orders.status", "orders.count_filter_where_status_paid"], resp.columns
            assert [r["orders.status"] for r in resp.data] == expected, resp.sql


class TestFilteredTwinsStayDistinct:
    async def test_unnamed_filtered_first_beside_first(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[OPEN_FIRST, "first(amount)"]))
        assert _row(resp) == {"orders.amount_first_filter_where_status_open": 20, "orders.amount_first": 10}

    async def test_named_filtered_first_beside_first(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m(OPEN_FIRST, "fo"), m("first(amount)", "f")]))
        assert _row(resp) == {"orders.fo": 20, "orders.f": 10}

    @pytest.mark.parametrize(("agg", "expected"), [("first", 20 - 10), ("last", 25 - 5)])
    async def test_filtered_and_unfiltered_inside_one_formula(self, engine: SlayerQueryEngine, agg: str,
                                                              expected: int) -> None:
        resp = await engine.execute(q(measures=[
            m(f"{agg}(amount) FILTER (WHERE status = 'open') - {agg}(amount)", "d"),
        ]))
        assert _row(resp) == {"orders.d": expected}, resp.sql

    async def test_conditions_whose_names_coincide(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[
            m(OPEN_FIRST, "a"), m("first(amount) FILTER (WHERE status <> 'open')", "b"),
        ]))
        assert _row(resp) == {"orders.a": 20, "orders.b": 10}, resp.sql

    async def test_star_count_over_a_joined_condition_beside_count(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=["count(*) FILTER (WHERE customers.name = 'Alice')", "count(*)"]))
        row = _row(resp)
        assert len(row) == 2, row
        assert row.pop("orders._count") == 11
        assert list(row.values()) == [5]


class TestCanonicalAggregateAlias:
    """The internal alias carries the filter in every profile."""

    @staticmethod
    def _bound(formula: str) -> AggregateKey:
        orders, *rest = idiom_models()
        bundle = ResolvedSourceBundle(dialect="postgres", source_model=orders, referenced_models=rest)
        key = bind_expr(parse_expr(formula), scope=ModelScope(source_model=orders), bundle=bundle,
                        allow_measures=True).value_key
        assert isinstance(key, AggregateKey), key
        assert key.filter is not None, key
        return key

    @pytest.mark.parametrize("formula", [OPEN_FIRST, "count(*) FILTER (WHERE customers.name = 'Alice')"])
    @pytest.mark.parametrize("profile", get_args(AggAliasProfile))
    def test_filter_changes_the_alias(self, formula: str, profile: AggAliasProfile) -> None:
        filtered = self._bound(formula)
        twin = filtered.model_copy(update={"filter": None})
        relation = {"source_relation": "orders"} if profile == "cross_model_cte" else {}
        got = canonical_aggregate_alias(filtered, profile=profile, **relation)
        assert got is not None, got
        assert "_filter_" in got, got
        assert got != canonical_aggregate_alias(twin, profile=profile, **relation)
