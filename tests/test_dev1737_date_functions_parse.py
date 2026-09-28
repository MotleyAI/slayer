"""Mode-B date functions at the parser: unit/part literals, clock calls, and the ``interval`` rewrite."""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.keys import SCALAR_FUNCTION_ARITY, SCALAR_FUNCTIONS, check_scalar_arity
from slayer.engine.binding import bind_expr, bind_filter
from slayer.engine.syntax import (
    AggCall,
    Cmp,
    Literal,
    Ref,
    ScalarCall,
    UnaryOp,
    parse_expr,
    parse_filter_expr,
)
from slayer.core.scope import ModelScope
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1737_fixtures import PARTS, UNITS, customers_model, orders_model

DATE_NAMES = ("date_part", "date_diff", "date_add", "current_date", "now", "interval")


def _bind(text: str):
    model = orders_model()
    bundle = ResolvedSourceBundle(dialect="postgres", source_model=model, referenced_models=[customers_model()])
    return bind_expr(parse_expr(text), scope=ModelScope(source_model=model), bundle=bundle, allow_measures=True).value_key


def _date_add(ts, n, unit: str) -> ScalarCall:
    return ScalarCall(name="date_add", args=(ts, n, Literal(value=unit)))


def _lit(n: int) -> Literal:
    return Literal(value=Decimal(n))


class TestAllowlist:
    @pytest.mark.parametrize("name", DATE_NAMES)
    def test_names_are_allowlisted(self, name: str) -> None:
        assert name in SCALAR_FUNCTIONS

    @pytest.mark.parametrize("name,bounds", [
        ("date_part", (2, 2)), ("date_diff", (3, 3)), ("date_add", (3, 3)),
        ("current_date", (0, 0)), ("now", (0, 0)), ("interval", (2, 2)),
    ])
    def test_arity(self, name: str, bounds) -> None:
        assert SCALAR_FUNCTION_ARITY[name] == bounds

    @pytest.mark.parametrize("name,argc", [
        ("date_add", 2), ("date_add", 4), ("date_part", 1), ("date_diff", 2),
        ("now", 1), ("current_date", 1), ("interval", 1),
    ])
    def test_wrong_arity_message_names_function(self, name: str, argc: int) -> None:
        msg = check_scalar_arity(name=name, argc=argc)
        assert msg is not None and name in msg


class TestUnitLiterals:
    @pytest.mark.parametrize("part", PARTS)
    def test_every_part_parses(self, part: str) -> None:
        assert parse_expr(f"date_part('{part}', created_at)") == ScalarCall(
            name="date_part", args=(Literal(value=part), Ref(name="created_at")),
        )

    @pytest.mark.parametrize("unit", UNITS)
    def test_every_diff_unit_parses(self, unit: str) -> None:
        assert parse_expr(f"date_diff('{unit}', created_at, shipped_at)") == ScalarCall(
            name="date_diff", args=(Literal(value=unit), Ref(name="created_at"), Ref(name="shipped_at")),
        )

    @pytest.mark.parametrize("unit", UNITS)
    def test_every_add_unit_parses(self, unit: str) -> None:
        assert parse_expr(f"date_add(created_at, 3, '{unit}')") == _date_add(Ref(name="created_at"), _lit(3), unit)

    @pytest.mark.parametrize("spelling", ["'MONTH'", "'Month'", "'month'"])
    def test_unit_case_folds(self, spelling: str) -> None:
        assert parse_expr(f"DATE_PART({spelling}, created_at)") == parse_expr("date_part('month', created_at)")

    def test_add_unit_case_folds(self) -> None:
        assert parse_expr("Date_Add(created_at, 1, 'WEEK_SUNDAY')") == _date_add(
            Ref(name="created_at"), _lit(1), "week_sunday")

    @pytest.mark.parametrize("expr,listed", [
        ("date_diff('fortnight', created_at, shipped_at) > 1", "week_sunday"),
        ("date_diff('day_of_week', created_at, shipped_at) > 1", "week_sunday"),
        ("date_diff('iso_year', created_at, shipped_at) > 1", "week_sunday"),
        ("date_add(created_at, 1, 'day_of_year') > created_at", "week_sunday"),
        ("date_add(created_at, 1, 'days') > created_at", "week_sunday"),
        ("date_part('week_sunday', created_at) = 1", "day_of_week"),
        ("date_part('dow', created_at) = 1", "day_of_week"),
        ("date_part('', created_at) = 1", "day_of_week"),
    ])
    def test_unknown_unit_rejected_listing_accepted(self, expr: str, listed: str) -> None:
        with pytest.raises(ValueError, match=listed):
            parse_filter_expr(expr)

    @pytest.mark.parametrize("expr", [
        "date_part(status, created_at) = 1",
        "date_part(1, created_at) = 1",
        "date_part(lower('month'), created_at) = 1",
        "date_diff(status, created_at, shipped_at) > 1",
        "date_add(created_at, 1, status) > created_at",
        "date_add(created_at, 1, None) > created_at",
    ])
    def test_non_literal_unit_rejected(self, expr: str) -> None:
        with pytest.raises(ValueError, match="string literal"):
            parse_filter_expr(expr)

    def test_bucket_form_is_not_a_date_function(self) -> None:
        assert not isinstance(parse_expr("month(created_at)"), ScalarCall)


class TestClockCalls:
    @pytest.mark.parametrize("text,name", [
        ("now()", "now"), ("NOW()", "now"), ("current_date()", "current_date"), ("Current_Date()", "current_date"),
    ])
    def test_zero_arg_calls(self, text: str, name: str) -> None:
        assert parse_expr(text) == ScalarCall(name=name, args=())

    def test_clock_in_filter(self) -> None:
        assert parse_filter_expr("created_at <= now()") == Cmp(
            op="<=", left=Ref(name="created_at"), right=ScalarCall(name="now", args=()))


class TestKeywordArgsRejected:
    @pytest.mark.parametrize("expr", [
        "date_add(created_at, n=1, unit='day')",
        "date_part(part='year', ts=created_at)",
        "date_diff('day', start=created_at, end=shipped_at)",
        "order_date + interval(n=1, unit='day')",
        "now(tz='UTC')",
        "current_date(tz='UTC')",
    ])
    def test_keywords(self, expr: str) -> None:
        with pytest.raises(ValueError, match="keyword"):
            parse_expr(expr)


class TestIntervalRewrite:
    def test_plus(self) -> None:
        assert parse_expr("created_at + interval(1, 'month')") == _date_add(Ref(name="created_at"), _lit(1), "month")

    def test_commuted_plus(self) -> None:
        assert parse_expr("interval(7, 'day') + order_date") == _date_add(Ref(name="order_date"), _lit(7), "day")

    def test_minus_folds_numeric_literal(self) -> None:
        assert parse_expr("shipped_at - interval(2, 'day')") == _date_add(Ref(name="shipped_at"), _lit(-2), "day")

    def test_minus_negates_expression(self) -> None:
        assert parse_expr("created_at - interval(sla_days, 'day')") == _date_add(
            Ref(name="created_at"), UnaryOp(op="-", operand=Ref(name="sla_days")), "day")

    def test_unit_case_folds(self) -> None:
        assert parse_expr("created_at + interval(1, 'MONTH')") == parse_expr("created_at + interval(1, 'month')")

    def test_chain_folds_left(self) -> None:
        inner = _date_add(Ref(name="created_at"), _lit(1), "month")
        assert parse_expr("created_at + interval(1, 'month') + interval(2, 'hour')") == _date_add(inner, _lit(2), "hour")

    def test_chain_with_minus(self) -> None:
        inner = _date_add(Ref(name="created_at"), _lit(1), "month")
        assert parse_expr("created_at + interval(1, 'month') - interval(2, 'hour')") == _date_add(inner, _lit(-2), "hour")

    def test_chained_filter_binds_like_nested_date_add(self) -> None:
        assert parse_filter_expr(
            "created_at + interval(1, 'month') + interval(2, 'hour') < now()"
        ) == parse_filter_expr("date_add(date_add(created_at, 1, 'month'), 2, 'hour') < now()")

    def test_commuted_filter(self) -> None:
        assert parse_filter_expr("interval(7, 'day') + order_date > current_date()") == parse_filter_expr(
            "date_add(order_date, 7, 'day') > current_date()")

    def test_rewrite_happens_inside_an_aggregation(self) -> None:
        parsed = parse_expr("max(shipped_at - interval(2, 'day'))")
        assert isinstance(parsed, AggCall)
        assert parsed.source == _date_add(Ref(name="shipped_at"), _lit(-2), "day")

    @pytest.mark.parametrize("pair", [
        ("max(shipped_at - interval(2, 'day'))", "max(date_add(shipped_at, -2, 'day'))"),
        ("created_at - interval(1, 'hour')", "date_add(created_at, -1, 'hour')"),
        ("interval(3, 'week') + order_date", "date_add(order_date, 3, 'week')"),
        ("created_at - interval(sla_days, 'day')", "date_add(created_at, -sla_days, 'day')"),
    ])
    def test_operator_spelling_interns_with_date_add(self, pair) -> None:
        assert _bind(pair[0]) == _bind(pair[1])

    def test_bound_filter_interns(self) -> None:
        model = orders_model()
        bundle = ResolvedSourceBundle(dialect="postgres", source_model=model, referenced_models=[customers_model()])
        scope = ModelScope(source_model=model)

        def bound(text: str):
            return bind_filter(parse_filter_expr(text), scope=scope, bundle=bundle).value_key

        assert bound("created_at + interval(1, 'month') < now()") == bound("date_add(created_at, 1, 'month') < now()")


class TestStrayIntervalRejected:
    @pytest.mark.parametrize("expr", [
        "interval(7, 'day')",
        "interval(7, 'day') - order_date > 0",
        "-interval(7, 'day') + order_date > order_date",
        "order_date + interval(1, 'day') * 2 > order_date",
        "order_date > interval(1, 'day')",
        "interval(1, 'day') == interval(1, 'day')",
        "order_date + interval(interval(1, 'day'), 'day') > order_date",
        "interval(1, 'day') + interval(2, 'day') + order_date > order_date",
        "order_date + (interval(1, 'day') + interval(2, 'day')) > order_date",
        "coalesce(interval(1, 'day'), order_date) > order_date",
        "date_add(order_date, interval(1, 'day'), 'day') > order_date",
        "max(interval(1, 'day')) > 0",
        "order_date * interval(1, 'day') > 0",
    ])
    def test_rejected_pointing_to_date_add(self, expr: str) -> None:
        with pytest.raises(ValueError, match="date_add"):
            parse_filter_expr(expr)

    def test_unknown_interval_unit_rejected(self) -> None:
        with pytest.raises(ValueError, match="week_sunday"):
            parse_filter_expr("order_date + interval(1, 'fortnight') > order_date")

    def test_non_literal_interval_unit_rejected(self) -> None:
        with pytest.raises(ValueError, match="string literal"):
            parse_filter_expr("order_date + interval(1, status) > order_date")
