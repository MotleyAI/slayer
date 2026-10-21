"""Unknown-function error, ``count(distinct x)`` and the method-call spelling (spec: aggregations/functional-form)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnknownFunctionError
from slayer.core.models import Column
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import AggCall, Cmp, Literal, Ref, parse_expr
from tests._engine_helpers import _engine_generate
from tests._query_heal_fixtures import (
    BACKENDS,
    assert_equivalent,
    customers_model,
    heal_engine,
    order_items_model,
    orders_model,
    products_model,
)

DISTINCT_ONLY = "count(distinct x)"


@pytest.fixture(params=BACKENDS)
async def engine(request) -> AsyncIterator[SlayerQueryEngine]:
    async with heal_engine(request.param) as eng:
        yield eng


def by_region(*measures: str) -> dict[str, Any]:
    return {"source_model": "orders", "dimensions": ["region"], "measures": list(measures)}


class TestUnknownFunction:
    @pytest.mark.parametrize("formula", ["bogus(amount)", "bogus(*)", "bogus(amount * 2)"])
    async def test_names_function_and_categories(self, engine: SlayerQueryEngine, formula: str) -> None:
        query = by_region(formula)
        with pytest.raises(UnknownFunctionError) as info:
            await engine.execute(query)
        message = str(info.value)
        assert "Unknown function 'bogus'" in message
        assert "Unknown aggregation" not in message
        for category in ("aggregation", "scalar", "transform", "granularit"):
            assert category in message.lower()

    async def test_close_match_suggested(self, engine: SlayerQueryEngine) -> None:
        query = by_region("summ(amount)")
        with pytest.raises(UnknownFunctionError, match="Did you mean 'sum'"):
            await engine.execute(query)

    def test_construction_time_custom_filter_deferred(self) -> None:
        query = SlayerQuery.model_validate(
            {"source_model": "orders", "measures": ["count(*)"], "filters": ["my_agg(amount) > 0"]})
        assert query.filters == ["my_agg(amount) > 0"]


class TestCountDistinctParse:
    @pytest.mark.parametrize("spelling", [
        "count(distinct customer_id)", "COUNT(DISTINCT customer_id)", "Count(Distinct customer_id)",
        "count( distinct  customer_id )", "count(\n  distinct\n  customer_id\n)",
    ])
    def test_equals_count_distinct(self, spelling: str) -> None:
        assert parse_expr(spelling) == parse_expr("count_distinct(customer_id)")

    @pytest.mark.parametrize(("healed", "canonical_text"), [
        ("count(distinct coalesce(customer_id, 0), partition_by=region)",
         "count_distinct(coalesce(customer_id, 0), partition_by=region)"),
        ("count(distinct customer_id) / count(*)", "count_distinct(customer_id) / count(*)"),
        ("count(distinct customers.name)", "count_distinct(customers.name)"),
        ("count(distinct distinct_id)", "count_distinct(distinct_id)"),
        ("round(count(distinct customer_id), 0)", "round(count_distinct(customer_id), 0)"),
    ])
    def test_composed(self, healed: str, canonical_text: str) -> None:
        assert parse_expr(healed) == parse_expr(canonical_text)

    def test_filter_position(self) -> None:
        assert parse_expr("count(distinct customer_id) > 1") == parse_expr(
            "count_distinct(customer_id) > 1")

    def test_identifier_starting_with_distinct_untouched(self) -> None:
        assert parse_expr("count(distinct_id)") == AggCall(source=Ref(name="distinct_id"), agg="count")

    def test_quoted_distinct_unchanged(self) -> None:
        assert parse_expr("status == 'count(distinct x)'") == Cmp(
            op="==", left=Ref(name="status"), right=Literal(value="count(distinct x)"))

    @pytest.mark.parametrize("expr", [
        "sum(distinct amount)", "count(distinct *)", "count(distinct a, b)", "AVG(DISTINCT amount)",
        "coalesce(distinct amount, 0)", "count(distinct)", "sum(amount, distinct tax)",
        "coalesce(a, distinct b)", "count(a, distinct b)", "count(distinct a, distinct b)",
        "count(a, partition_by=distinct b)",
    ])
    def test_other_distinct_rejected(self, expr: str) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr(expr)
        message = str(info.value)
        assert DISTINCT_ONLY in message
        assert "count_distinct(x)" in message


class TestCountDistinctExecuted:
    @pytest.mark.parametrize("spelling", ["count(distinct customer_id)", "COUNT(DISTINCT customer_id)"])
    async def test_equals_count_distinct(self, engine: SlayerQueryEngine, spelling: str) -> None:
        await assert_equivalent(engine, by_region(spelling), by_region("count_distinct(customer_id)"))

    async def test_expression_with_partition(self, engine: SlayerQueryEngine) -> None:
        def query(formula: str) -> dict[str, Any]:
            return {"source_model": "orders", "dimensions": ["region", "status"], "measures": [formula]}

        await assert_equivalent(
            engine, query("count(distinct coalesce(customer_id, 0), partition_by=region)"),
            query("count_distinct(coalesce(customer_id, 0), partition_by=region)"),
        )

    async def test_mode_a_distinct_unchanged(self) -> None:
        orders = orders_model()
        orders.columns.append(Column(name="n_customers", sql="COUNT(DISTINCT customer_id)", type=DataType.INT))
        sql = await _engine_generate(
            query=SlayerQuery.model_validate({"source_model": "orders", "measures": ["max(n_customers)"]}),
            model=orders, extra_models=[customers_model(), order_items_model(), products_model()],
        )
        assert "COUNT(DISTINCT" in sql.upper()


METHOD_CALLS = [
    ("order_items.count(*)", "count(order_items.*)"),
    ("order_items.sum(price * qty)", "sum(order_items.price * order_items.qty)"),
    ("order_items.sum(coalesce(price, 0))", "sum(coalesce(order_items.price, 0))"),
    ("order_items.percentile(price, 0.9)", "percentile(order_items.price, 0.9)"),
    ("order_items.percentile(price, p=0.9)", "percentile(order_items.price, p=0.9)"),
    ("order_items.month(created_at)", "month(order_items.created_at)"),
    ("order_items.first(price)", "first(order_items.price)"),
    ("order_items.COUNTD(id)", "COUNTD(order_items.id)"),
    ("order_items.products.max(list_price)", "max(order_items.products.list_price)"),
    ("order_items.my_agg(price)", "my_agg(order_items.price)"),
    ("order_items.sum(price) / order_items.count(*)", "sum(order_items.price) / count(order_items.*)"),
]


class TestMethodCallParse:
    @pytest.mark.parametrize(("healed", "canonical_text"), METHOD_CALLS)
    def test_equals_functional_twin(self, healed: str, canonical_text: str) -> None:
        assert parse_expr(healed) == parse_expr(canonical_text)

    def test_filter_position(self) -> None:
        assert parse_expr("order_items.count(*) > 1") == parse_expr("count(order_items.*) > 1")

    @pytest.mark.parametrize(("expr", "functional"), [
        ("order_items.weighted_avg(price, qty)", "weighted_avg(order_items."),
        ("order_items.sum(products.price)", "sum(order_items."),
        ("order_items.sum(price, partition_by=region)", "sum(order_items.price"),
        ("order_items.round(price)", "round(order_items.price)"),
        ("order_items.cumsum(price)", "cumsum("),
    ])
    def test_ambiguous_shapes_show_functional_spelling(self, expr: str, functional: str) -> None:
        with pytest.raises(ValueError) as info:
            parse_expr(expr)
        assert functional in str(info.value)


class TestMethodCallExecuted:
    @pytest.mark.parametrize(("healed", "canonical_text"), [
        pair for pair in METHOD_CALLS if pair[0] not in {"order_items.month(created_at)", "order_items.my_agg(price)"}
    ])
    async def test_measure_equals_functional_twin(
        self, engine: SlayerQueryEngine, healed: str, canonical_text: str,
    ) -> None:
        await assert_equivalent(engine, by_region(healed), by_region(canonical_text))

    async def test_granularity_dimension_equals_functional_twin(self, engine: SlayerQueryEngine) -> None:
        def query(dimension: str) -> dict[str, Any]:
            return {"source_model": "orders", "dimensions": [dimension], "measures": ["count(order_items.*)"]}

        await assert_equivalent(engine, query("order_items.month(created_at)"), query("month(order_items.created_at)"))
