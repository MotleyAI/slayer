"""SQL forms in every Mode-B position, executed on SQLite / DuckDB and emitted on every Tier-1 dialect.

Spec: queries/expression-grammar, queries/expression-keywords. Dataset: ``tests/_dev2101_fixtures.py``.
"""

from __future__ import annotations

from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import UnknownReferenceError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.render.row_expr import render_null_safe
from slayer.storage.sqlite_conn import open_connection
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
        resp = await engine.execute(q(dimensions=["orders.id"], filter=[pred]))
        assert ids(resp) == expected

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_measure(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(metrics=[m(f"sum(iif({pred}, 1, 0))")]))
        assert num(single(resp)) == len(expected)

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_dimension(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(dimensions=["orders.id", {"field": pred, "alias": "p"}]))
        assert {int(r["orders.id"]) for r in resp.data if r["p"]} == expected

    @pytest.mark.parametrize(("pred", "expected"), PREDICATES, ids=PRED_IDS)
    async def test_order_target(self, engine: SlayerQueryEngine, pred: str, expected: frozenset[int]) -> None:
        resp = await engine.execute(q(
            dimensions=["orders.id"], distinct_dimension_values=False,
            order=[{"field": f"iif({pred}, 0, 1)"}, {"field": "orders.id"}],
        ))
        assert ordered_ids(resp) == sorted(expected) + sorted(ALL_IDS - expected)


class TestValueInEveryPosition:
    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_dimension(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]))
        assert {int(k): _norm(v) for k, v in by_key(resp, "id", "x").items()} == _expected(values)

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_measure(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], metrics=[m(f"max({expr})")]))
        assert {int(k): _norm(v) for k, v in by_key(resp, "id").items()} == _expected(values)

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_filter(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=[f"{expr} IS NOT NULL"]))
        assert ids(resp) == {k for k, v in values.items() if v is not None}

    @pytest.mark.parametrize(("expr", "values"), VALUES, ids=VALUE_IDS)
    async def test_order_target(self, engine: SlayerQueryEngine, expr: str, values: dict[int, Any]) -> None:
        resp = await engine.execute(q(
            dimensions=["orders.id"], distinct_dimension_values=False, filter=[f"{expr} IS NOT NULL"],
            order=[{"field": expr, "direction": "desc"}, {"field": "orders.id"}],
        ))
        present = {k: _norm(v) for k, v in values.items() if v is not None}
        assert ordered_ids(resp) == sorted(sorted(present), key=present.__getitem__, reverse=True)

    @pytest.mark.parametrize(("expr", "values"), DATE_VALUES, ids=[v for v, _ in DATE_VALUES])
    async def test_interval_dimension(self, engine: SlayerQueryEngine, expr: str, values: dict[int, str]) -> None:
        resp = await engine.execute(q(dimensions=["orders.id", {"field": expr, "alias": "x"}]))
        assert {int(k): day(v) for k, v in by_key(resp, "id", "x").items()} == values


class TestAggregateForms:
    async def test_count_distinct_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[m("count(distinct orders.region)"), m("count_distinct(orders.region)", "w")]))
        assert resp.data[0]["v"] == resp.data[0]["w"] == 2

    async def test_count_distinct_in_post_aggregate_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.status"], metrics=[m("count(orders.*)")],
                                      filter=["count(distinct orders.region) > 1"]))
        assert set(by_key(resp, "status")) == {"paid"}

    async def test_sql_spelled_post_aggregate_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.region"], metrics=[m("sum(orders.amount)")],
                                      filter=["sum(orders.amount) BETWEEN 25 AND 75", "orders.region IS NOT NULL"]))
        assert {k: num(v) for k, v in by_key(resp, "region").items()} == {"EU": 30.0, "US": 70.0}

    async def test_sql_predicate_in_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[
            m("sum(iif(orders.status = 'paid' AND orders.amount IS NOT NULL, orders.amount, 0))", "sql"),
            m("sum(iif(orders.status == 'paid' and orders.amount is not None, orders.amount, 0))", "py"),
        ]))
        row = resp.data[0]
        assert num(row["sql"]) == num(row["py"]) == 70.0

    async def test_order_by_sql_spelled_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.status"], metrics=[m("count(orders.*)")],
                                      order=[{"field": "sum(iif(orders.status <> 'paid', 1, 0))", "direction": "desc"},
                                             {"field": "orders.status"}]))
        assert [r["orders.status"] for r in resp.data] == ["Pending", "void", "paid"]

    async def test_case_condition_in_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(metrics=[m("sum(CASE WHEN orders.region = 'EU' AND orders.amount IS NOT NULL THEN 1 ELSE 0 END)")]))
        assert num(single(resp)) == 2


class TestExpressionGrammarScenarios:
    async def test_same_between_text_in_filter_measure_and_order(self, engine: SlayerQueryEngine) -> None:
        pred = "orders.amount BETWEEN 10 AND 20"
        filtered = await engine.execute(q(dimensions=["orders.id"], filter=[pred]))
        measured = await engine.execute(q(metrics=[m(f"sum(iif({pred}, 1, 0))")]))
        ordered = await engine.execute(q(dimensions=["orders.id"], distinct_dimension_values=False,
                                         order=[{"field": f"iif({pred}, 0, 1)"}, {"field": "orders.id"}]))
        assert ids(filtered) == {1, 2}
        assert num(single(measured)) == 2
        assert ordered_ids(ordered)[:2] == [1, 2]

    async def test_boolean_tests_partition_the_rows(self, engine: SlayerQueryEngine) -> None:
        kept = ids(await engine.execute(q(dimensions=["orders.id"], filter=["(orders.amount > 15) IS TRUE"])))
        rest = ids(await engine.execute(q(dimensions=["orders.id"], filter=["(orders.amount > 15) IS NOT TRUE"])))
        assert kept | rest == ALL_IDS
        assert not kept & rest
        assert 5 in rest

    async def test_concatenation_dimension(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[{"field": "orders.status || '-' || orders.region", "alias": "tag"}],
                                      metrics=[m("count(orders.*)")]))
        assert by_key(resp, "tag") == {"paid-EU": 2, "Pending-US": 1, "paid-US": 1, None: 1}

    async def test_modulo_sign(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(metrics=[m("min(-7 % 3)")])))) == -1

    @pytest.mark.parametrize(("formula", "expected"), [
        ("min(2 ** 3 ** 2)", 512), ("min(-2 ** 2)", -4), ("min((-2) ** 2)", 4), ("min(2 ** -2)", 0.25),
    ])
    async def test_power(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        assert num(single(await engine.execute(q(metrics=[m(formula)])))) == pytest.approx(expected)

    async def test_power_over_an_aggregate(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(metrics=[m("sum(orders.amount) ** 2")])))) == pytest.approx(10000.0)

    async def test_not_binds_looser_than_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=["NOT orders.amount > 15 AND orders.status = 'paid'"]))
        assert ids(resp) == {1}

    async def test_concatenation_binds_tighter_than_comparison(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=["orders.status || 'x' = 'paidx'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_like_over_nested_call(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=["lower(trim(orders.status)) LIKE 'pa%'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_ilike_case_insensitive(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=["orders.status ILIKE 'PA%'"]))
        assert ids(resp) == {1, 2, 4}

    async def test_like_inside_a_string_is_text(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["orders.id"], filter=["orders.note = 'we like cats'"]))
        assert ids(resp) == {1}

    async def test_substring_and_position_equal_their_calls(self, engine: SlayerQueryEngine) -> None:
        sql_form = await engine.execute(q(dimensions=["orders.id", "SUBSTRING(orders.status FROM 1 FOR 3)", "POSITION('a' IN orders.status)"],
                                          order=[{"field": "orders.id"}]))
        calls = await engine.execute(q(dimensions=["orders.id", "substr(orders.status, 1, 3)", "instr(orders.status, 'a')"],
                                       order=[{"field": "orders.id"}]))
        assert sql_form.columns == calls.columns
        assert sql_form.data == calls.data
        assert sql_form.sql == calls.sql

    async def test_quoted_column_name(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(metrics=[m('sum(orders."order total")')])))) == 1000.0

    async def test_quoted_segment_in_dotted_path(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=['customers."Region"'], metrics=[m("count(orders.*)")]))
        assert by_key(resp, "customers.Region") == {"North": 2, "South": 2, None: 1}

    async def test_double_quoted_string_hint(self, engine: SlayerQueryEngine) -> None:
        query = q(dimensions=["orders.id"], filter=['orders.status = orders."paid"'])
        with pytest.raises(UnknownReferenceError) as info:
            await engine.execute(query)
        msg = str(info.value)
        assert "paid" in msg, msg
        assert "single quote" in msg.lower(), msg

    async def test_doubled_quote(self, engine: SlayerQueryEngine) -> None:
        assert ids(await engine.execute(q(dimensions=["orders.id"], filter=["orders.customer_name = 'O''Brien'"]))) == {1}

    async def test_backslash_is_literal(self, engine: SlayerQueryEngine) -> None:
        assert ids(await engine.execute(q(dimensions=["orders.id"], filter=["orders.path = 'C:\\temp'"]))) == {1}

    async def test_python_and_sql_spellings_render_identical_sql(self, engine: SlayerQueryEngine) -> None:
        py = await engine.execute(q(dimensions=["orders.id"], filter=["orders.status == 'paid' and orders.region is not None"]), dry_run=True)
        sql = await engine.execute(q(dimensions=["orders.id"], filter=["orders.status = 'paid' AND orders.region IS NOT NULL"]), dry_run=True)
        assert py.sql == sql.sql

    async def test_comparison_inside_scalar_call(self, engine: SlayerQueryEngine) -> None:
        assert num(single(await engine.execute(q(metrics=[m("sum(iif(orders.status = 'paid', orders.amount, 0))")])))) == 70.0


class TestEmission:
    """Every form renders SQL that parses on every Tier-1 dialect, in a filter and a dimension."""

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("pred", PRED_IDS)
    async def test_predicate_emits(self, dialect: str, pred: str) -> None:
        await emit(q(dimensions=["orders.id", {"field": pred, "alias": "p"}], filter=[pred]), dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("expr", VALUE_IDS + [v for v, _ in DATE_VALUES])
    async def test_value_emits(self, dialect: str, expr: str) -> None:
        await emit(q(dimensions=["orders.id", {"field": expr, "alias": "x"}], metrics=[m(f"max({expr})")]),
                   dialect=dialect)

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_ilike_is_case_insensitive_everywhere(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["orders.id"], filter=["orders.status ILIKE 'PA%'"]), dialect=dialect)
        tree = sqlglot.parse_one(sql, dialect=dialect)
        native = tree.find(exp.ILike) is not None or (dialect == "sqlite" and " LIKE " in sql.upper())
        assert native or "LOWER(" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_null_safe_comparison_everywhere(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["orders.id"], filter=["orders.region IS NOT DISTINCT FROM NULL"]), dialect=dialect)
        upper = sql.upper()
        assert any(form in upper for form in ("IS NULL", "DISTINCT FROM", "DISTINCTFROM", "<=>", "EQUAL_NULL")), sql


_NULL_PAIRS = [(1, 1, False), (1, 2, True), (1, None, True), (None, 1, True), (None, None, False)]


@pytest.mark.parametrize("dialect", ["tsql", "oracle", "redshift"])
@pytest.mark.parametrize(("left", "right", "distinct"), _NULL_PAIRS)
def test_expanded_distinct_from_is_two_valued(dialect: str, left: Any, right: Any, distinct: bool) -> None:
    """The expanded null-safe form negates to ``IS DISTINCT FROM`` exactly, NULL against a value included."""
    predicate = render_null_safe(
        op="is distinct from", operands=[exp.column("a", table="t"), exp.column("b", table="t")],
        dialect=get_dialect(dialect),
    )
    # SQLite, not DuckDB: DuckDB rewrites the expanded pattern into a native null-safe test.
    sql = f"SELECT {predicate.sql(dialect='sqlite')} FROM (SELECT ? AS a, ? AS b) AS t"
    with open_connection(":memory:") as conn:
        assert conn.execute(sql, [left, right]).fetchone() == (int(distinct),)
