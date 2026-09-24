"""SqlTemplate: token-level placeholder discovery and structural substitution."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.sql.generator import SQLGenerator
from slayer.sql.sql_template import SqlTemplate, SqlTemplateError


def _col(name: str, table: str = "t") -> exp.Column:
    return exp.column(name, table=table)


def _expr(sql: str, dialect: str = "postgres") -> Expression:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    assert isinstance(tree, Expression)
    return tree


def _render(text: str, dialect: str = "postgres", **bindings: Expression) -> str:
    return SqlTemplate(text=text, dialect=dialect).render(bindings).sql(dialect=dialect)


class TestPrecedence:
    def test_operator_binding_under_multiplication_is_parenthesised(self) -> None:
        assert _render("{x} * c", x=_expr("a + b")) == "(a + b) * c"

    def test_operator_binding_as_right_operand_is_parenthesised(self) -> None:
        assert _render("a / {x}", x=_expr("b * c")) == "a / (b * c)"

    def test_operator_binding_under_unary_minus_is_parenthesised(self) -> None:
        assert _render("-{x}", x=_expr("a + b")) == "-(a + b)"

    def test_operator_binding_under_is_null_is_parenthesised(self) -> None:
        assert _render("{x} IS NULL", x=_expr("a + b")) == "(a + b) IS NULL"

    def test_function_argument_is_not_wrapped(self) -> None:
        assert _render("SUM({x})", x=_expr("a + b")) == "SUM(a + b)"

    def test_column_binding_under_operator_is_not_wrapped(self) -> None:
        assert _render("{x} * c", x=_col("a")) == "t.a * c"

    def test_function_binding_under_operator_is_not_wrapped(self) -> None:
        assert _render("{x} * c", x=_expr("COALESCE(a, 0)")) == "COALESCE(a, 0) * c"

    def test_case_operand_keeps_grouping(self) -> None:
        out = _render("MAX(CASE WHEN {x} > 0 THEN 1 END)", x=_expr("a - b"))
        assert out == "MAX(CASE WHEN (a - b) > 0 THEN 1 END)"


class TestInertTokens:
    def test_string_literal_is_inert(self) -> None:
        t = SqlTemplate(text="MAX(CASE WHEN {value} > 0 THEN '{value}' END)", dialect="postgres")
        out = t.render({"value": _col("amount")}).sql(dialect="postgres")
        assert out == "MAX(CASE WHEN t.amount > 0 THEN '{value}' END)"

    def test_escaped_quote_string_is_inert(self) -> None:
        t = SqlTemplate(text="SUM({value}) + LENGTH('it''s {x}')", dialect="postgres")
        out = t.render({"value": _col("a")}).sql(dialect="postgres")
        assert "'it''s {x}'" in out
        assert out.startswith("SUM(t.a)")

    def test_quoted_identifier_is_inert(self) -> None:
        t = SqlTemplate(text='SUM({value}) + MAX("{value}")', dialect="postgres")
        out = t.render({"value": _col("a")}).sql(dialect="postgres")
        assert out == 'SUM(t.a) + MAX("{value}")'

    def test_line_comment_is_inert(self) -> None:
        t = SqlTemplate(text="SUM({value}) -- scaled by {scale}\n", dialect="postgres")
        out = t.render({"value": _col("a")}).sql(dialect="postgres")
        assert out.startswith("SUM(t.a)")

    def test_block_comment_is_inert(self) -> None:
        t = SqlTemplate(text="SUM({value}) /* {scale} */", dialect="postgres")
        out = t.render({"value": _col("a")}).sql(dialect="postgres")
        assert out.startswith("SUM(t.a)")

    def test_duckdb_struct_literal_is_left_to_the_grammar(self) -> None:
        t = SqlTemplate(text="SUM({value}) + STRUCT_EXTRACT({'a': 1}, 'a')", dialect="duckdb")
        out = t.render({"value": _col("a")}).sql(dialect="duckdb")
        assert out == "SUM(t.a) + STRUCT_EXTRACT({'a': 1}, 'a')"


class TestPlaceholderShapes:
    def test_unicode_before_placeholder(self) -> None:
        out = _render("MAX(CASE WHEN 'héllo — ü' = 'x' THEN {value} END)", value=_col("a"))
        assert out == "MAX(CASE WHEN 'héllo — ü' = 'x' THEN t.a END)"

    def test_adjacent_placeholders(self) -> None:
        assert _render("{a}*{b}", a=_col("x"), b=_col("y")) == "t.x * t.y"

    def test_whitespace_inside_braces(self) -> None:
        spaced = _render("SUM({ value })", value=_col("a"))
        assert spaced == _render("SUM({value})", value=_col("a")) == "SUM(t.a)"

    def test_keyword_placeholder_name(self) -> None:
        t = SqlTemplate(text="SUM({value} * {order})", dialect="postgres")
        out = t.render({"value": _col("a"), "order": _col("b")}).sql(dialect="postgres")
        assert out == "SUM(t.a * t.b)"

    def test_natural_sentinel_name_collision(self) -> None:
        names = ["value", "_value", "__value__", "__value", "value_", "__slayer_value__",
                 "_slayer_ph_value", "__ph_0__", "__placeholder_0__", "_p0", "ph0"]
        text = "SUM({value} * (" + " + ".join(names) + "))"
        out = _render(text, value=_col("a"))
        assert out == "SUM(t.a * (" + " + ".join(names) + "))"


class TestIndependentCopies:
    def test_repeated_placeholder_renders_independent_copies(self) -> None:
        binding = _col("w")
        root = SqlTemplate(text="SUM({w}) + SUM({w})", dialect="postgres").render({"w": binding})
        cols = [c for c in root.find_all(exp.Column) if c.name == "w"]
        assert len(cols) == 2
        assert cols[0] is not cols[1]
        assert all(c is not binding for c in cols)
        assert binding.parent is None
        assert root.sql(dialect="postgres") == "SUM(t.w) + SUM(t.w)"

    def test_unused_bindings_are_ignored(self) -> None:
        assert _render("SUM({value})", value=_col("a"), extra=_col("b")) == "SUM(t.a)"


class TestErrors:
    def test_unbound_placeholder_raises_naming_it(self) -> None:
        t = SqlTemplate(text="SUM({value}) / {scale}", dialect="postgres")
        bindings = {"value": _col("a")}
        with pytest.raises(SqlTemplateError, match="scale"):
            t.render(bindings)

    @pytest.mark.parametrize("text", ["SUM({value}", "SUM({value)", "SUM({value}) +"])
    def test_unparseable_template_raises_at_construction(self, text: str) -> None:
        with pytest.raises(SqlTemplateError):
            SqlTemplate(text=text, dialect="postgres")

    @pytest.mark.parametrize("text", [
        "SUM({t}.amount)",
        "SUM(amount) AS {x}",
        "{fn}(amount)",
    ])
    def test_non_expression_position_raises_at_construction(self, text: str) -> None:
        with pytest.raises(SqlTemplateError):
            SqlTemplate(text=text, dialect="postgres")

    def test_error_is_a_value_error(self) -> None:
        assert issubclass(SqlTemplateError, ValueError)


class TestCachedRootIsPristine:
    def test_repeated_renders_do_not_leak(self) -> None:
        t = SqlTemplate(text="SUM({value} * {w})", dialect="postgres")
        first = t.render({"value": _col("a"), "w": _col("x")})
        summed = first.find(exp.Sum)
        assert summed is not None
        summed.set("this", exp.Literal.number(0))
        second = t.render({"value": _col("b"), "w": _col("y")})
        assert second.sql(dialect="postgres") == "SUM(t.b * t.y)"
        third = t.render({"value": _col("a"), "w": _col("x")})
        assert third.sql(dialect="postgres") == "SUM(t.a * t.x)"

    def test_concurrent_renders(self) -> None:
        t = SqlTemplate(text="SUM({value} * {w}) / NULLIF(SUM({w}), 0)", dialect="postgres")

        def one(i: int) -> str:
            return t.render({"value": _col(f"v{i}"), "w": _col(f"w{i}")}).sql(dialect="postgres")

        with ThreadPoolExecutor(max_workers=16) as pool:
            outs = list(pool.map(one, range(200)))
        assert outs == [
            f"SUM(t.v{i} * t.w{i}) / NULLIF(SUM(t.w{i}), 0)" for i in range(200)
        ]


class TestSharedParsePipeline:
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "mysql", "bigquery"])
    def test_matches_generator_parse(self, dialect: str) -> None:
        text = "LOG10({value}) + SUM(myCol)"
        rendered = SqlTemplate(text=text, dialect=dialect).render({"value": _col("a")})
        expected = SQLGenerator(dialect=dialect)._parse(
            "LOG10(t.a) + SUM(myCol)",
        )
        assert rendered.sql(dialect=dialect) == expected.sql(dialect=dialect)
