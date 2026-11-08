"""SQL comments in Mode-B text (spec: queries/expression-grammar › SQL comments)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column
from slayer.core.query import SlayerQuery, extract_placeholder_names
from slayer.core.syntax import (
    AggCall, BoundVariable, Cmp, Literal, Ref, bind_variables, parse_bound_expr, parse_expr, variable_names,
)
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import (
    EXEC_BACKENDS, customers_model, emit, ids, m, make_exec_engine, num, orders_model, q, single,
)


def same(a: str, b: str) -> None:
    assert parse_expr(a) == parse_expr(b), (a, b)


def _error(text: str) -> str:
    with pytest.raises(ValueError) as info:
        parse_expr(text)
    return str(info.value)


class TestParse:
    @pytest.mark.parametrize(("text", "means"), [
        ("a --b", "a"),
        ("a--b", "a"),
        ("a -- b\n+ c", "a + c"),
        ("a -- one\n-- two\n+ c", "a + c"),
        ("a /* c */ + b", "a + b"),
        ("a/**/+b", "a + b"),
        ("a /* x\ny */ + b", "a + b"),
        ("a /* x /* y */ z */ + b", "a + b"),
        ("/* lead */ a + b", "a + b"),
        ("a + b /* trail */", "a + b"),
        ("a + b -- trail", "a + b"),
        ("sum(amount) /* gross */ - sum(refund) -- net of refunds\n+ 0", "sum(amount) - sum(refund) + 0"),
        ("status = 'paid' /* old: /* status = 'open' */ */ AND amount > 0", "status = 'paid' AND amount > 0"),
        ("payload ->> 'a' -- x", "payload ->> 'a'"),
        ("a - /* c */ -b", "a - -b"),
    ])
    def test_comment_means_nothing(self, text: str, means: str) -> None:
        same(text, means)

    def test_markers_inside_a_string_literal(self) -> None:
        parsed = parse_expr("note = 'a -- b /* c */'")
        assert isinstance(parsed, Cmp)
        assert parsed.right == Literal(value="a -- b /* c */")

    def test_markers_inside_a_quoted_identifier(self) -> None:
        for name in ("x--y", "p/*q*/"):
            parsed = parse_expr(f'sum("{name}")')
            assert isinstance(parsed, AggCall)
            assert parsed.source == Ref(name=name), parsed

    def test_comment_end_inside_a_string_does_not_close_it(self) -> None:
        same("a /* ' */ + b", "a + b")

    @pytest.mark.parametrize("text", ["-- nothing", "/* nothing */", "  -- a\n  /* b */  "])
    def test_only_comments_is_empty(self, text: str) -> None:
        assert "empty" in _error(text).lower()

    def test_unterminated_block_reports_its_start(self) -> None:
        text = "status = 'paid' /* open"
        assert f"column {text.index('/*') + 1}" in _error(text)

    def test_unterminated_nested_block_reports_the_outer_start(self) -> None:
        text = "a + /* x /* y */ b"
        assert f"column {text.index('/*') + 1}" in _error(text)

    @pytest.mark.parametrize("text", ["amount /* c */ > )", "amount -- c\n > )", "/* é */ amount > )"])
    def test_syntax_error_column_counts_the_comment(self, text: str) -> None:
        assert f"column {text.index(')') + 1}" in _error(text)


class TestPlaceholders:
    @pytest.mark.parametrize("text", [
        "status = 'paid' -- region = {region}",
        "status = 'paid' /* region = {region} */",
        "status = 'paid' /* {{ and }} and { */",
    ])
    def test_placeholder_in_a_comment_is_neither_bound_nor_required(self, text: str) -> None:
        bound = bind_variables(text, {})
        same(bound, "status = 'paid'")

    @pytest.mark.parametrize(("text", "names"), [
        ("status = 'paid' -- region = {region}", []),
        ("status = 'paid' /* {a} */ AND x = {b}", ["b"]),
        ("x = {a} -- {b}\n AND y = '{c}'", ["a", "c"]),
    ])
    def test_placeholder_in_a_comment_is_not_discovered(self, text: str, names: list[str]) -> None:
        assert variable_names(text) == names

    def test_query_does_not_require_a_commented_placeholder(self) -> None:
        query = SlayerQuery.model_validate({"source_model": "orders", "filters": ["status = 'paid' -- r = {region}"]})
        assert extract_placeholder_names(query) == set()

    def test_placeholder_outside_the_comment_still_binds(self) -> None:
        same(bind_variables("amount > {n} -- {other}", {"n": 15}), "amount > 15")

    @pytest.mark.parametrize("value", [-5, "-5", -2.5])
    def test_negative_value_after_a_minus_is_no_comment(self, value) -> None:
        same(bind_variables("amount -{d} + 1", {"d": value}), f"amount - ({value}) + 1")

    def test_negative_values_in_an_in_list(self) -> None:
        variables = {"ds": [-5, 3]}
        assert bind_variables("amount in ({ds})", variables) == "amount in ({ds})"
        assert parse_bound_expr("amount in ({ds})", variables) == Cmp(
            op="in", left=Ref(name="amount"), right=BoundVariable(name="ds", values=(-5, 3), listed=True))


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestExecution:
    async def test_double_dash_renders_like_no_comment(self) -> None:
        with_comment = await emit(q(measures=[m("sum(amount) --discount")]), dialect="postgres")
        plain = await emit(q(measures=[m("sum(amount)")]), dialect="postgres")
        assert with_comment == plain

    async def test_multi_line_formula(self, engine: SlayerQueryEngine) -> None:
        formula = "sum(amount) /* gross */ - sum(discount) -- net of discounts\n+ 0"
        assert num(single(await engine.execute(q(measures=[m(formula)])))) == 92

    async def test_nested_block_in_a_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[
            "status = 'paid' /* old: /* status = 'open' */ */ AND amount > 15",
        ]))
        assert ids(resp) == {2, 4}, resp.sql

    async def test_markers_in_a_quoted_identifier_name_the_column(self) -> None:
        orders = orders_model()
        orders.columns.append(Column(name="x--y", sql="amount", type=DataType.DOUBLE))
        models = [orders, customers_model()]
        quoted = await emit(q(measures=[m('sum("x--y")')]), dialect="postgres", models=models)
        plain = await emit(q(measures=[m("sum(amount)")]), dialect="postgres", models=models)
        assert quoted.replace("x--y", "amount") == plain, quoted

    async def test_markers_in_a_literal_are_text(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=[
            "status || ' -- /* x */' = 'paid -- /* x */'",
        ]))
        assert ids(resp) == {1, 2, 4}, resp.sql

    async def test_placeholder_in_a_comment_runs_without_the_variable(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["status = 'paid' -- region = {region}"]))
        assert ids(resp) == {1, 2, 4}, resp.sql
