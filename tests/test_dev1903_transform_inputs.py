"""One transform-input checker: per-node row-leaf rule over every op, the boolean
rule for ``change`` / ``change_pct``; messages byte-identical to the ledger."""

from __future__ import annotations

import re

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import FieldKey, Grain, TimeTruncKey, TransformKey
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.engine.elaborate_env import check_transform_inputs
from slayer.engine.plan import plan_query
from slayer.core.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.core.query import QueryMetric

from tests._dev1846_fixtures import SlayerQuery, dev1846_datasets, month_td
from tests._dev1871_raise_ledger import ROWS


def _bundle() -> ResolvedSourceBundle:
    models = dev1846_datasets()
    return ResolvedSourceBundle(dialect="postgres", population=models[0], referenced_datasets=models[1:])


def _key(expression: str):
    bundle = _bundle()
    assert bundle.population is not None
    return bind_expr(
        parse_expr(expression), scope=DatasetScope(population=bundle.population),
        bundle=bundle, allow_metrics=True,
    ).value_key


def _check(expression: str, *, projected=frozenset()) -> None:
    check_transform_inputs(roots=[_key(expression)], projected_grain_keys=projected)


def _plan(**kw) -> None:
    kw.setdefault("metrics", [])
    plan_query(
        query=SlayerQuery(population="sales", time_dimensions=month_td(), **kw),
        bundle=_bundle(),
    )


def _ledger_pattern(fragment: str) -> re.Pattern:
    """The ``check_transform_inputs`` ledger row whose message contains ``fragment``,
    as a full-match regex (``…`` = any interpolation)."""
    (row,) = [
        r for r in ROWS
        if r.function == "check_transform_inputs" and fragment in r.message
    ]
    return re.compile(".+?".join(re.escape(p) for p in row.message.split("…")))


ROW_LEAF = "cannot consume the row-level"
BOOLEAN = "cannot consume a boolean-shaped predicate"


def _row_leaf_op(msg: str) -> str:
    assert _ledger_pattern(ROW_LEAF).fullmatch(msg.removeprefix("TransformInputError: ")), msg
    m = re.search(r"\n  at transform '(\w+)'", msg)
    assert m is not None, msg
    return m.group(1)


class TestMessagesMatchTheLedger:
    def test_row_leaf_message(self):
        with pytest.raises(ValueError) as ei:
            _check("cumsum(weight)")
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    @pytest.mark.parametrize("op", ["change", "change_pct"])
    def test_boolean_message(self, op):
        with pytest.raises(ValueError) as ei:
            _check(f"{op}(sum(revenue) > 100)")
        msg = str(ei.value)
        assert _ledger_pattern(BOOLEAN).fullmatch(msg.removeprefix("TransformInputError: ")), msg
        assert f"\n  at transform '{op}'\n" in msg

    @pytest.mark.parametrize("formula", [
        "time_shift(sum(revenue) > 100, -1)", "cumsum(sum(revenue) > 100)",
        "rank(sum(revenue) > 100, direction='desc')",
    ])
    def test_boolean_input_accepted_elsewhere(self, formula):
        _check(formula)

    def test_planner_raises_the_same_message(self):
        measures = [QueryMetric(metric="cumsum(weight)", alias="m")]
        with pytest.raises(ValueError) as ei:
            _plan(metrics=measures)
        assert _row_leaf_op(str(ei.value)) == "cumsum"


class TestInnermostTransformNamed:
    @pytest.mark.parametrize("formula", [
        "first(cumsum(weight))", "last(cumsum(weight))", "rank(cumsum(weight), direction='desc')",
        "time_shift(cumsum(weight), -1)", "change(cumsum(weight))",
    ])
    def test_consuming_transform_is_named(self, formula):
        measures = [QueryMetric(metric=formula, alias="m")]
        with pytest.raises(ValueError) as ei:
            _plan(metrics=measures)
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_first_over_row_leaf_transform_is_not_the_expression_error(self):
        measures = [QueryMetric(metric="first(cumsum(weight))", alias="m")]
        with pytest.raises(ValueError) as ei:
            _plan(metrics=measures)
        assert "not supported over an expression" not in str(ei.value)


class TestTraversal:
    @pytest.mark.parametrize("formula", [
        pytest.param("sum(cumsum(weight))", id="aggregate-source"),
        pytest.param("weighted_avg(revenue, cumsum(weight))", id="aggregate-positional-arg"),
        pytest.param("weighted_avg(revenue, weight=cumsum(weight))", id="aggregate-kwarg"),
        pytest.param("sum(qty * cumsum(weight))", id="expression-source"),
        pytest.param("count(cumsum(weight) > 0)", id="predicate"),
        pytest.param("first(cumsum(weight))", id="under-first"),
        pytest.param("last(cumsum(weight))", id="under-last"),
        pytest.param("cumsum(sum(revenue)) + cumsum(weight)", id="composite"),
    ])
    def test_row_leaf_transform_found(self, formula):
        with pytest.raises(ValueError) as ei:
            _check(formula)
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_filter_position(self):
        measures = [QueryMetric(metric="sum(revenue)", alias="r")]
        with pytest.raises(ValueError) as ei:
            _plan(metrics=measures, filter=["cumsum(weight) > 0"])
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_projected_leaf_exempt(self):
        _check("cumsum(weight)", projected=frozenset({FieldKey(path=(), leaf="weight")}))

    def test_partition_and_time_keys_are_not_leaves(self):
        key = _key("cumsum(sum(revenue))")
        assert isinstance(key, TransformKey)
        keyed = key.model_copy(update={
            "partition_keys": Grain.of([FieldKey(path=(), leaf="store")]),
            "time_key": TimeTruncKey(
                source=FieldKey(path=(), leaf="ordered_at"), granularity=TimeGranularity.MONTH),
        })
        check_transform_inputs(roots=[keyed], projected_grain_keys=frozenset())

    @pytest.mark.parametrize("formula", [
        "first(sum(revenue))", "last(sum(revenue) + 1)", "avg(cumsum(sum(revenue)))",
    ])
    def test_attached_inputs_pass(self, formula):
        _check(formula)
