"""``OrderItem.nulls`` — explicit NULL placement (spec: queries/positions)."""

from __future__ import annotations

import pytest
import sqlglot
from pydantic import ValidationError
from sqlglot import exp

from slayer.core.query import OrderItem, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import (
    CITY_ORDER,
    DISCOUNT_ORDER,
    EXEC_BACKENDS,
    TIER1,
    check_null_ordering,
    discount_rows_query,
    emit,
    hidden_max_query,
    m,
    make_exec_engine,
    ordered_ids,
    q,
)


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestOrderItemField:
    @pytest.mark.parametrize("nulls", ["first", "last"])
    def test_accepted_and_serialised(self, nulls: str) -> None:
        item = OrderItem.model_validate({"column": "discount", "direction": "desc", "nulls": nulls})
        assert item.nulls == nulls
        assert OrderItem.model_validate(item.model_dump()).nulls == nulls

    def test_default_is_unset(self) -> None:
        assert OrderItem.model_validate({"column": "discount"}).nulls is None

    def test_invalid_policy_names_the_allowed_values(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerQuery.model_validate({"source_model": "orders", "dimensions": ["id"],
                                        "order": [{"column": "discount", "nulls": "middle"}]})
        msg = str(info.value)
        assert "first" in msg, msg
        assert "last" in msg, msg

    def test_survives_query_round_trip(self) -> None:
        query = discount_rows_query(direction="desc", nulls="first")
        again = SlayerQuery.model_validate(query.model_dump(mode="json"))
        assert again.order is not None
        assert again.order[0].nulls == "first"

    def test_expression_order_item_keeps_the_policy(self) -> None:
        item = OrderItem.model_validate({"column": "sum(amount) * 2", "nulls": "last"})
        assert item.nulls == "last"
        assert item.raw_formula == "sum(amount) * 2"


class TestExecutedPlacement:
    async def test_all_cases(self, engine: SlayerQueryEngine) -> None:
        await check_null_ordering(engine)

    @pytest.mark.parametrize(("direction", "nulls"), sorted(DISCOUNT_ORDER))
    async def test_projected_measure(self, engine: SlayerQueryEngine, direction: str, nulls: str) -> None:
        resp = await engine.execute(q(dimensions=["city"], measures=[m("max(discount)")], order=[
            {"column": "v", "direction": direction, "nulls": nulls}, {"column": "city"}]))
        assert [r["orders.city"] for r in resp.data] == CITY_ORDER[(direction, nulls)]

    @pytest.mark.parametrize(("direction", "nulls"), sorted(DISCOUNT_ORDER))
    async def test_expression_target(self, engine: SlayerQueryEngine, direction: str, nulls: str) -> None:
        resp = await engine.execute(q(dimensions=["id"], distinct_dimension_values=False, order=[
            {"column": "discount * 2", "direction": direction, "nulls": nulls}, {"column": "id"}]))
        assert ordered_ids(resp) == DISCOUNT_ORDER[(direction, nulls)]

    async def test_hidden_value_is_not_returned(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(hidden_max_query(direction="asc", nulls="last"))
        assert resp.columns == ["orders.city"]

    async def test_stage_rewrite_keeps_the_policy(self, engine: SlayerQueryEngine) -> None:
        inner = SlayerQuery.model_validate({"name": "by_city", "source_model": "orders", "dimensions": ["city"],
                                            "measures": [{"formula": "max(discount)", "name": "md"}]})
        outer = SlayerQuery.model_validate({"source_model": "by_city", "dimensions": ["city"],
                                            "measures": [{"formula": "max(md)", "name": "v"}],
                                            "order": [{"column": "v", "direction": "asc", "nulls": "first"},
                                                      {"column": "city"}]})
        resp = await engine.execute([inner, outer])
        assert [r["by_city.city"] for r in resp.data] == CITY_ORDER[("asc", "first")]

    async def test_refinement_keeps_the_policy(self, engine: SlayerQueryEngine) -> None:
        await engine.create_model_from_query(query=discount_rows_query(direction="desc", nulls="first"),
                                             name="discount_rows")
        resp = await engine.execute("discount_rows", refine={"filters": ["id <> 2"]})
        id_key = next(c for c in resp.columns if c.endswith(".id"))
        assert [int(r[id_key]) for r in resp.data] == [1, 4, 5, 3]


def _null_policy_terms(sql: str, *, dialect: str) -> list[exp.Ordered]:
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Ordered))


class TestEmission:
    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize(("direction", "nulls"), sorted(DISCOUNT_ORDER))
    async def test_policy_reaches_every_dialect(self, dialect: str, direction: str, nulls: str) -> None:
        for query in (discount_rows_query(direction=direction, nulls=nulls),
                      hidden_max_query(direction=direction, nulls=nulls)):
            sql = await emit(query, dialect=dialect)
            terms = _null_policy_terms(sql, dialect=dialect)
            native = any(t.args.get("nulls_first") is (nulls == "first") and "discount" in t.this.sql().lower()
                         for t in terms)
            emulated = any(t.this.find(exp.Is) is not None for t in terms)
            assert native or emulated, sql

    async def test_tsql_orders_over_the_resolved_expression(self) -> None:
        sql = await emit(hidden_max_query(direction="asc", nulls="last"), dialect="tsql")
        null_keys = [t for t in _null_policy_terms(sql, dialect="tsql") if t.this.find(exp.Is) is not None]
        assert null_keys, sql
        assert all(t.this.find(exp.Max) is not None for t in null_keys), sql

    @pytest.mark.parametrize("dialect", ["postgres", "duckdb", "sqlite"])
    async def test_no_policy_emits_no_modifier(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["id"], distinct_dimension_values=False,
                           order=[{"column": "discount", "direction": "desc"}]), dialect=dialect)
        assert "NULLS" not in sql.upper(), sql
