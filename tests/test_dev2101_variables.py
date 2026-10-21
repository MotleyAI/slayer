"""``{var}`` placeholders bind as literal values only (spec: queries/variables)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.query import ComputedDimension
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import EXEC_BACKENDS, by_key, ids, m, make_exec_engine, num, q, single

MONTH = {"dimension": "created_at", "granularity": "month"}


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


async def _rejected(engine: SlayerQueryEngine, *needles: str, **kw: Any) -> str:
    with pytest.raises(ValueError) as info:
        await engine.execute(q(**kw))
    msg = str(info.value)
    for needle in needles:
        assert needle.lower() in msg.lower(), msg
    return msg


class TestLiteralSlots:
    async def test_numeric_string_in_a_bare_position(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["amount > {min}"], variables={"min": "10"}))
        assert ids(resp) == {2, 3, 4}

    async def test_number_in_a_bare_position(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["amount > {min}"], variables={"min": 25}))
        assert ids(resp) == {3, 4}

    async def test_boolean_in_a_bare_position(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(iif({on}, amount, 0))")], variables={"on": True}))
        assert num(single(resp)) == 100.0

    async def test_whole_string_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["status = '{s}'"], variables={"s": "Pending"}))
        assert ids(resp) == {3}

    async def test_partial_string_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["note LIKE '%{q}%'"], variables={"q": "cats"}))
        assert ids(resp) == {1}

    async def test_quoted_value_with_an_apostrophe(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["customer_name = '{n}'"], variables={"n": "O'Brien"}))
        assert ids(resp) == {1}

    async def test_hostile_string_stays_a_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["status = '{s}'"],
                                      variables={"s": "x' OR '1' = '1"}))
        assert ids(resp) == set()

    async def test_backslash_value_stays_literal(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["path = '{p}'"], variables={"p": r"C:\temp"}))
        assert ids(resp) == {1}

    async def test_list_in_the_in_slot(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["customer_name IN ({names})"],
                                      variables={"names": ["O'Brien", "Lee"]}))
        assert ids(resp) == {1, 5}

    async def test_doubled_braces_render_literal_braces(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[{"expression": "'{{' || status || '}}'", "name": "b"}],
                                      measures=[m("count(*)")], variables={"k": 1}))
        assert set(by_key(resp, "b")) == {"{paid}", "{Pending}", "{void}"}

    async def test_placeholder_in_a_case_branch_and_scalar_argument(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(
            measures=[m("sum(CASE WHEN status = '{s}' THEN round(amount / {d}, {p}) ELSE 0 END)")],
            variables={"s": "paid", "d": 3, "p": 0},
        ))
        assert num(single(resp)) == 3 + 7 + 13


class TestStructureIsNeverInjected:
    async def test_string_in_a_bare_position_is_rejected_with_the_quote_hint(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "literal values", "quote", measures=[m("sum({col})")], variables={"col": "amount"})

    async def test_non_numeric_string_compared_bare(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "literal values", dimensions=["id"], filters=["amount > {min}"],
                        variables={"min": "1 OR 1 = 1"})

    async def test_list_outside_the_in_slot(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "literal values", dimensions=["id"], filters=["amount > {xs}"],
                        variables={"xs": [1, 2]})

    @pytest.mark.parametrize("kw", [
        {"measures": ["{f}(amount)"], "variables": {"f": "sum"}},
        {"dimensions": ["customers.{c}"], "variables": {"c": "name"}},
        {"measures": ["sum(amount, partition_by={x})"], "dimensions": ["region"], "variables": {"x": "region"}},
        {"measures": ["{col}:sum"], "variables": {"col": "amount"}},
        {"measures": ["sum(amount, {k}=1)"], "variables": {"k": "partition_by"}},
    ], ids=["function", "path-segment", "reference-kwarg", "colon-source", "kwarg-name"])
    async def test_non_literal_positions_rejected(self, engine: SlayerQueryEngine, kw: dict) -> None:
        await _rejected(engine, "literal values", **kw)


class TestTemplateNames:
    @pytest.mark.parametrize("k", [2, 3])
    async def test_measure_name_comes_from_the_written_text(self, engine: SlayerQueryEngine, k: int) -> None:
        resp = await engine.execute(q(measures=["sum(amount) * {k}"], variables={"k": k}))
        assert resp.data == [{"orders.amount_sum_k": pytest.approx(100.0 * k)}]

    @pytest.mark.parametrize("k", [2, 3])
    @pytest.mark.parametrize("dimension", ["quantity * {k}", {"expression": "quantity * {k}"}])
    async def test_dimension_name_comes_from_the_written_text(self, engine: SlayerQueryEngine, dimension, k: int) -> None:
        resp = await engine.execute(q(dimensions=[dimension], measures=[m("count(*)")], variables={"k": k}))
        assert {float(v) for v in by_key(resp, "quantity_k")} == {float(x * k) for x in (1, 2, 3, 4)}

    def test_no_text_splice_on_computed_dimensions(self) -> None:
        assert not hasattr(ComputedDimension, "substituted")


class TestDateRangeBounds:
    async def test_both_bounds(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(amount)")], time_dimensions=[
            {**MONTH, "date_range": ["{start}", "{end}"]}], variables={"start": "2024-02-01", "end": "2024-02-29"}))
        assert [num(r["orders.v"]) for r in resp.data] == [70.0]

    async def test_one_sided_lower(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(amount)")], time_dimensions=[
            {**MONTH, "date_range": ["{start}", None]}], variables={"start": "2024-02-01"}))
        assert sorted(str(r["orders.created_at"])[:7] for r in resp.data) == ["2024-02", "2024-03"]

    async def test_one_sided_upper(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(amount)")], time_dimensions=[
            {**MONTH, "date_range": [None, "{end}"]}], variables={"end": "2024-01-31"}))
        assert [num(r["orders.v"]) for r in resp.data] == [30.0]

    async def test_single_period(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(amount)")], time_dimensions=[
            {**MONTH, "date_range": "{p}"}], variables={"p": "2024-02"}))
        assert [num(r["orders.v"]) for r in resp.data] == [70.0]

    async def test_missing_variable(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, "end", measures=[m("sum(amount)")],
                        time_dimensions=[{**MONTH, "date_range": ["{start}", "{end}"]}],
                        variables={"start": "2024-02-01"})

    async def test_non_string_year(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(amount)")], time_dimensions=[
            {**MONTH, "date_range": ["{start}", None]}], variables={"start": 2024}))
        assert len(resp.data) == 3

    async def test_bound_value_is_not_mode_b(self, engine: SlayerQueryEngine) -> None:
        await _rejected(engine, measures=[m("sum(amount)")],
                        time_dimensions=[{**MONTH, "date_range": ["{start}", None]}],
                        variables={"start": "2024-01-01' OR '1'='1"})
