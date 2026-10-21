"""SQL forms in every Mode-B position, executed on SQLite / DuckDB and emitted on every Tier-1 dialect.

Spec: queries/expression-grammar, queries/expression-keywords. Dataset: ``tests/_dev2101_fixtures.py``.
"""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot

from slayer.core.errors import UnknownReferenceError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import (
    ALL_IDS,
    DATE_VALUES,
    EXEC_BACKENDS,
    PRED_IDS,
    PREDICATES,
    TIER1,
    VALUE_IDS,
    VALUES,
    by_key,
    day,
    emit,
    ids,
    m,
    make_exec_engine,
    num,
    ordered_ids,
    q,
    single,
)

@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _norm(value: Any) -> Any:
    return value if value is None or isinstance(value, str) else float(value)


def _expected(values: dict[int, Any]) -> dict[int, Any]:
    return {k: _norm(v) for k, v in values.items()}


class TestPredicateInEveryPosition:
    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_filter(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[pred]))
        assert ids(resp) == expected

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_measure(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(measures=[m(f"sum(iif({pred}, 1, 0))")]))
        assert num(single(resp)) == len(expected)

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_dimension(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": pred, "name": "p"}]))
        assert {int(r["orders.id"]) for r in resp.data if r["orders.p"]} == expected

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_order_target(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(
            dimensions=["id"], distinct_dimension_values=False,
            order=[{"column": f"iif({pred}, 0, 1)"}, {"column": "id"}],
        ))
        assert ordered_ids(resp) == sorted(expected) + sorted(ALL_IDS - expected)


class TestValueInEveryPosition:
    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_dimension(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "x"}]))
        assert {int(k): _norm(v) for k, v in by_key(resp, "id", "x").items()} == _expected(values)

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_measure(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["id"], measures=[m(f"max({expr})")]))
        assert {int(k): _norm(v) for k, v in by_key(resp, "id").items()} == _expected(values)

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_filter(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[f"{expr} IS NOT NULL"]))
        assert ids(resp) == {k for k, v in values.items() if v is not None}

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_order_target(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(
            dimensions=["id"], distinct_dimension_values=False, filters=[f"{expr} IS NOT NULL"],
            order=[{"column": expr, "direction": "desc"}, {"column": "id"}],
        ))
        present = {k: _norm(v) for k, v in values.items() if v is not None}
        assert ordered_ids(resp) == sorted(sorted(present), key=present.__getitem__, reverse=True)

    @pytest.mark.parametrize(("expr", "values"), DATE_VALUES, ids=[v for v, _ in DATE_VALUES])
    async def test_interval_dimension(self, engine: SlayerQueryEngine, expr: str, values: dict[int, str]) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "x"}]))
        assert {int(k): day(v) for k, v in by_key(resp, "id", "x").items()} == values


class TestAggregateForms:
    async def test_count_distinct_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("count(distinct region)"), m("count_distinct(region)", "w")]))
        assert resp.data[0]["orders.v"] == resp.data[0]["orders.w"] == 2

    async def test_count_distinct_in_post_aggregate_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["status"], measures=[m("count(*)")],
                                      filters=["count(distinct region) > 1"]))
        assert set(by_key(resp, "status")) == {"paid"}

    async def test_sql_spelled_post_aggregate_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["region"], measures=[m("sum(amount)")],
                                      filters=["sum(amount) BETWEEN 25 AND 75", "region IS NOT NULL"]))
        assert {k: num(v) for k, v in by_key(resp, "region").items()} == {"EU": 30.0, "US": 70.0}

    async def test_sql_predicate_in_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[
            m("sum(iif(status = 'paid' AND amount IS NOT NULL, amount, 0))", "sql"),
            m("sum(iif(status == 'paid' and amount is not None, amount, 0))", "py"),
        ]))
        row = resp.data[0]
        assert num(row["orders.sql"]) == num(row["orders.py"]) == 70.0

    async def test_order_by_sql_spelled_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["status"], measures=[m("count(*)")],
                                      order=[{"column": "sum(iif(status <> 'paid', 1, 0))", "direction": "desc"},
                                             {"column": "status"}]))
        assert [r["orders.status"] for r in resp.data] == ["Pending", "void", "paid"]

    async def test_case_condition_in_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(CASE WHEN region = 'EU' AND amount IS NOT NULL THEN 1 ELSE 0 END)")]))
        assert num(single(resp)) == 2


class TestExpressionGrammarScenarios:
    async def test_same_between_text_in_filter_measure_and_order(self, engine: SlayerQueryEngine) -> None:
        pred = "amount BETWEEN 10 AND 20"
        filtered = await engine.execute(q(dimensions=["id"], filters=[pred]))
        measured = await engine.execute(q(measures=[m(f"sum(iif({pred}, 1, 0))")]))
        ordered = await engine.execute(q(dimensions=["id"], distinct_dimension_values=False,
                                         order=[{"column": f"iif({pred}, 0, 1)"}, {"column": "id"}]))
        assert ids(filtered) == {1, 2}
        assert num(single(measured)) == 2
        assert ordered_ids(ordered)[:2] == [1, 2]

    async def test_boolean_tests_partition_the_rows(self, engine: SlayerQueryEngine) -> None:
        kept = ids(await engine.execute(q(dimensions=["id"], filters=["(amount > 15) IS TRUE"])))
        rest = ids(await engine.execute(q(dimensions=["id"], filters=["(amount > 15) IS NOT TRUE"])))
        assert kept | rest == ALL_IDS and not kept & rest
        assert 5 in rest

    async def test_concatenation_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[{"expression": "status || '-' || region", "name": "tag"}],
                                      measures=[m("count(*)")]))
        assert by_key(resp, "tag") == {"paid-EU": 2, "Pending-US": 1, "paid-US": 1, None: 1}

    async def test_modulo_sign(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(measures=[m("min(-7 % 3)")])))) == -1

    @pytest.mark.parametrize(("formula", "expected"), [
        ("min(2 ** 3 ** 2)", 512), ("min(-2 ** 2)", -4), ("min((-2) ** 2)", 4), ("min(2 ** -2)", 0.25),
    ])
    async def test_power(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        assert num(single(await engine.execute(q(measures=[m(formula)])))) == pytest.approx(expected)

    async def test_power_over_an_aggregate(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(measures=[m("sum(amount) ** 2")])))) == pytest.approx(10000.0)

    async def test_not_binds_looser_than_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["NOT amount > 15 AND status = 'paid'"]))
        assert ids(resp) == {1}

    async def test_concatenation_binds_tighter_than_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["status || 'x' = 'paidx'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_like_over_nested_call(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["lower(trim(status)) LIKE 'pa%'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_ilike_case_insensitive(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["status ILIKE 'PA%'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_like_inside_a_string_is_text(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["note = 'we like cats'"]))
        assert ids(resp) == {1}

    async def test_substring_and_position_equal_their_calls(self, engine: SlayerQueryEngine) -> None:
        sql_form = await engine.execute(q(dimensions=["id", "SUBSTRING(status FROM 1 FOR 3)", "POSITION('a' IN status)"]))
        calls = await engine.execute(q(dimensions=["id", "substr(status, 1, 3)", "instr(status, 'a')"]))
        assert sql_form.columns == calls.columns
        assert sql_form.data == calls.data
        assert sql_form.sql == calls.sql

    async def test_quoted_column_name(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(measures=[m('sum("order total")')])))) == 1000.0

    async def test_quoted_segment_in_dotted_path(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=['customers."Region"'], measures=[m("count(*)")]))
        assert by_key(resp, "customers.Region") == {"North": 2, "South": 2, None: 1}

    async def test_double_quoted_string_hint(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(UnknownReferenceError) as info:
            await engine.execute(q(dimensions=["id"], filters=['status = "paid"']))
        msg = str(info.value)
        assert "paid" in msg and "single quote" in msg.lower(), msg

    async def test_doubled_quote(self, engine: SlayerQueryEngine) -> None:
        assert ids(await engine.execute(q(dimensions=["id"], filters=["customer_name = 'O''Brien'"]))) == {1}

    async def test_backslash_is_literal(self, engine: SlayerQueryEngine) -> None:
        assert ids(await engine.execute(q(dimensions=["id"], filters=[r"path = 'C:\temp'"]))) == {1}

    async def test_python_and_sql_spellings_render_identical_sql(self, engine: SlayerQueryEngine) -> None:
        py = await engine.execute(q(dimensions=["id"], filters=["status == 'paid' and region is not None"]), dry_run=True)
        sql = await engine.execute(q(dimensions=["id"], filters=["status = 'paid' AND region IS NOT NULL"]), dry_run=True)
        assert py.sql == sql.sql

    async def test_comparison_inside_scalar_call(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(measures=[m("sum(iif(status = 'paid', amount, 0))")])))) == 70.0


class TestEmission:
    """Every form renders SQL that parses on every Tier-1 dialect, in a filter and a dimension."""

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("pred", PRED_IDS)
    async def test_predicate_emits(self, dialect: str, pred: str) -> None:
        await emit(q(dimensions=["id", {"expression": pred, "name": "p"}], filters=[pred]), dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("expr", VALUE_IDS + [v for v, _ in DATE_VALUES])
    async def test_value_emits(self, dialect: str, expr: str) -> None:
        await emit(q(dimensions=["id", {"expression": expr, "name": "x"}], measures=[m(f"max({expr})")]),
                   dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_ilike_is_case_insensitive_everywhere(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["id"], filters=["status ILIKE 'PA%'"]), dialect=dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        native = tree.find(sqlglot.exp.ILike) is not None or (dialect == "sqlite" and " LIKE " in sql.upper())
        assert native or "LOWER(" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_null_safe_comparison_everywhere(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["id"], filters=["region IS NOT DISTINCT FROM NULL"]), dialect=dialect)
        upper = sql.upper()
        assert any(form in upper for form in ("IS NULL", "DISTINCT FROM", "DISTINCTFROM", "<=>", "EQUAL_NULL")), sql
