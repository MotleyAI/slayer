"""One transform-input checker: per-node row-leaf rule over every op, the boolean
rule for ``change`` / ``change_pct``; messages byte-identical to the ledger."""

from __future__ import annotations

import re

import pytest

from slayer.core.keys import ColumnKey, Grain, TimeTruncKey, TransformKey
from slayer.core.models import ModelMeasure
from slayer.core.scope import ModelScope
from slayer.engine.binding import bind_expr
from slayer.engine.elaborate_env import check_transform_inputs
from slayer.engine.plan import plan_query
from slayer.engine.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import SlayerQuery, dev1846_models, month_td
from tests._dev1871_raise_ledger import ROWS


def _bundle() -> ResolvedSourceBundle:
    models = dev1846_models()
    return ResolvedSourceBundle(dialect="postgres", source_model=models[0], referenced_models=models[1:])


def _key(formula: str):
    bundle = _bundle()
    assert bundle.source_model is not None
    return bind_expr(
        parse_expr(formula), scope=ModelScope(source_model=bundle.source_model),
        bundle=bundle, allow_measures=True,
    ).value_key


def _check(formula: str, *, projected=frozenset()) -> None:
    check_transform_inputs(roots=[_key(formula)], projected_grain_keys=projected)


def _plan(**kw) -> None:
    kw.setdefault("measures", [])
    plan_query(
        query=SlayerQuery(source_model="sales", time_dimensions=month_td(), **kw),
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
            _check(f"{op}(revenue:sum > 100)")
        msg = str(ei.value)
        assert _ledger_pattern(BOOLEAN).fullmatch(msg.removeprefix("TransformInputError: ")), msg
        assert f"\n  at transform '{op}'\n" in msg

    @pytest.mark.parametrize("formula", [
        "time_shift(revenue:sum > 100, -1)", "cumsum(revenue:sum > 100)",
        "rank(revenue:sum > 100)",
    ])
    def test_boolean_input_accepted_elsewhere(self, formula):
        _check(formula)

    def test_planner_raises_the_same_message(self):
        measures = [ModelMeasure(formula="cumsum(weight)", name="m")]
        with pytest.raises(ValueError) as ei:
            _plan(measures=measures)
        assert _row_leaf_op(str(ei.value)) == "cumsum"


class TestInnermostTransformNamed:
    @pytest.mark.parametrize("formula", [
        "first(cumsum(weight))", "last(cumsum(weight))", "rank(cumsum(weight))",
        "time_shift(cumsum(weight), -1)", "change(cumsum(weight))",
    ])
    def test_consuming_transform_is_named(self, formula):
        measures = [ModelMeasure(formula=formula, name="m")]
        with pytest.raises(ValueError) as ei:
            _plan(measures=measures)
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_first_over_row_leaf_transform_is_not_the_expression_error(self):
        measures = [ModelMeasure(formula="first(cumsum(weight))", name="m")]
        with pytest.raises(ValueError) as ei:
            _plan(measures=measures)
        assert "not supported over an expression" not in str(ei.value)


class TestTraversal:
    @pytest.mark.parametrize("formula", [
        pytest.param("sum(cumsum(weight))", id="aggregate-source"),
        pytest.param("weighted_avg(revenue, cumsum(weight))", id="aggregate-positional-arg"),
        pytest.param("weighted_avg(revenue, weight=cumsum(weight))", id="aggregate-kwarg"),
        pytest.param("revenue:weighted_avg(weight=cumsum(weight))", id="colon-kwarg"),
        pytest.param("sum(qty * cumsum(weight))", id="expression-source"),
        pytest.param("count(cumsum(weight) > 0)", id="predicate"),
        pytest.param("first(cumsum(weight))", id="under-first"),
        pytest.param("last(cumsum(weight))", id="under-last"),
        pytest.param("cumsum(revenue:sum) + cumsum(weight)", id="composite"),
    ])
    def test_row_leaf_transform_found(self, formula):
        with pytest.raises(ValueError) as ei:
            _check(formula)
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_filter_position(self):
        measures = [ModelMeasure(formula="revenue:sum", name="r")]
        with pytest.raises(ValueError) as ei:
            _plan(measures=measures, filters=["cumsum(weight) > 0"])
        assert _row_leaf_op(str(ei.value)) == "cumsum"

    def test_projected_leaf_exempt(self):
        _check("cumsum(weight)", projected=frozenset({ColumnKey(path=(), leaf="weight")}))

    def test_partition_and_time_keys_are_not_leaves(self):
        key = _key("cumsum(revenue:sum)")
        assert isinstance(key, TransformKey)
        keyed = key.model_copy(update={
            "partition_keys": Grain.of([ColumnKey(path=(), leaf="store")]),
            "time_key": TimeTruncKey(
                column=ColumnKey(path=(), leaf="ordered_at"), granularity="month"),
        })
        check_transform_inputs(roots=[keyed], projected_grain_keys=frozenset())

    @pytest.mark.parametrize("formula", [
        "first(revenue:sum)", "last(sum(revenue) + 1)", "avg(cumsum(revenue:sum))",
    ])
    def test_attached_inputs_pass(self, formula):
        _check(formula)
