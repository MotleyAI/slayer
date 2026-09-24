"""``first`` / ``last`` parse to one ``AggCall`` and dispatch at bind by the bound
operand's type: attached operand → the series transform, row-grain operand → the
ranked aggregation.

Spec: openspec …/specs/aggregations/functional-form — "Ambiguous first and last
names dispatch by argument shape".
"""

from __future__ import annotations

import pytest

from slayer.core.keys import AggregateKey, TransformKey, walk_value_keys
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine import syntax
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.engine.syntax import AggCall, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    month_td,
    regions_model,
    sales_model,
)
from tests._engine_helpers import seeded_exec_engine

SAVED = [
    ModelMeasure(formula="revenue:sum", name="rev"),
    ModelMeasure(formula="rev * 2", name="rev_twice"),
    ModelMeasure(formula="cumsum(revenue:sum)", name="cum_rev"),
    ModelMeasure(formula="revenue:sum * 2", name="rev2"),
]


def _models():
    sales = sales_model()
    sales.measures = list(SAVED)
    regions = regions_model()
    regions.measures = [ModelMeasure(formula="factor:sum", name="tf")]
    return [sales, regions]


def _bundle() -> ResolvedSourceBundle:
    sales, regions = _models()
    return ResolvedSourceBundle(source_model=sales, referenced_models=[regions])


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "sales")
    kw.setdefault("time_dimensions", month_td())
    return SlayerQuery(**kw)


def _measure_key(formula: str, **kw):
    env = elaborate_query(
        query=_q(measures=[ModelMeasure(formula=formula, name="m")], **kw),
        bundle=_bundle(),
    )
    assert env.prebound is not None
    return env.prebound.declared_measures[-1].bound.value_key


def _node(k, *, fn: str) -> str:
    if isinstance(k, TransformKey):
        return f"TransformKey:{'<op>' if k.op == fn else k.op}"
    if isinstance(k, AggregateKey):
        return f"AggregateKey:{k.agg}"
    return type(k).__name__


def _root_nodes(root, *, fn: str) -> list:
    """Pre-order nodes of ``root``; only the first ``fn`` transform becomes ``<op>``."""
    out, pending = [], fn
    for k in walk_value_keys(root):
        out.append(_node(k, fn=pending))
        if isinstance(k, TransformKey) and k.op == pending:
            pending = ""
    return out


def _outcome(query: SlayerQuery, *, fn: str):
    """``("ok", every bound node, "")`` over measure / filter / order roots with
    ``fn`` normalised to ``<op>``, or ``("err", type, message)``."""
    try:
        env = elaborate_query(query=query, bundle=_bundle())
        plan_query(query=query, bundle=_bundle())
    except Exception as e:  # noqa: BLE001 — the error itself is the observation
        return ("err", type(e).__name__, str(e).replace(f"'{fn}'", "'<op>'"))
    assert env.prebound is not None
    roots = [
        *(dm.bound.value_key for dm in env.prebound.declared_measures),
        *(bf.value_key for bf in env.prebound.bound_filters),
        *(sp.bound.value_key for sp in env.prebound.order_specs),
    ]
    return ("ok", ",".join(n for r in roots for n in _root_nodes(r, fn=fn)), "")


class TestParseIsOneNode:
    @pytest.mark.parametrize("fn", ["first", "last"])
    @pytest.mark.parametrize("arg", [
        "revenue", "1", "revenue:sum", "sum(revenue)", "sum(revenue) + 1",
        "revenue:sum > 100", "cumsum(weight)", "rev", "rev * 2",
        "qty * avg(revenue, partition_by=store)",
    ])
    def test_every_shape_parses_to_aggcall(self, fn, arg):
        parsed = parse_expr(f"{fn}({arg})")
        assert isinstance(parsed, AggCall), parsed
        assert parsed.agg == fn

    @pytest.mark.parametrize("colon, functional", [
        ("revenue:first", "first(revenue)"),
        ("revenue:last(ordered_at)", "last(revenue, ordered_at)"),
    ])
    def test_both_spellings_collapse(self, colon, functional):
        assert parse_expr(colon) == parse_expr(functional)

    def test_aggregated_operand_is_not_decided_in_the_parser(self):
        """engine P2: the parser makes no grain decision."""
        parsed = parse_expr("first(sum(revenue))")
        assert isinstance(parsed, AggCall)
        assert isinstance(parsed.source, AggCall)

    def test_mixed_source_classifier_deleted(self):
        assert not hasattr(syntax, "_is_mixed_agg_source")
        assert not hasattr(syntax, "_FIRST_LAST")


class TestBindDispatch:
    @pytest.mark.parametrize("formula", [
        "first(revenue:sum)", "last(sum(revenue))", "first(sum(revenue) + 1)",
        "first(revenue:sum > 100)",
    ])
    def test_aggregated_operand_binds_to_transform(self, formula):
        key = _measure_key(formula)
        assert isinstance(key, TransformKey), key
        assert key.op == formula.split("(")[0]

    @pytest.mark.parametrize("formula, ranking", [
        ("last(revenue)", None), ("last(revenue, ordered_at)", "ordered_at"),
    ])
    def test_row_operand_binds_to_ranked_aggregate(self, formula, ranking):
        key = _measure_key(formula)
        assert isinstance(key, AggregateKey), key
        assert key.agg == "last"
        assert key == _measure_key(
            "revenue:last" if ranking is None else f"revenue:last({ranking})",
        )

    @pytest.mark.parametrize("formula, kw", [
        ("first(qty * avg(revenue, partition_by=store))", {"dimensions": ["store"]}),
        ("first(1)", {}),
    ])
    def test_row_grain_expression_keeps_expression_error(self, formula, kw):
        with pytest.raises(ValueError, match="not supported over an expression"):
            _measure_key(formula, **kw)

    def test_transform_kwarg_error(self):
        with pytest.raises(
            ValueError,
            match="Transform 'first' does not accept keyword argument 'partition_by'",
        ):
            _measure_key("first(sum(revenue), partition_by=store)")

    def test_transform_operand_reaches_the_checker(self):
        """Dispatch does not pre-empt transform-input validation."""
        with pytest.raises(ValueError) as ei:
            _measure_key("first(cumsum(weight))")
        msg = str(ei.value)
        assert "Transform 'cumsum' cannot consume the row-level" in msg, msg
        assert "not supported over an expression" not in msg


class TestSavedMeasureOperand:
    @pytest.mark.parametrize("saved, inline", [
        ("first(rev)", "first(revenue:sum)"),
        ("first(rev * 2)", "first(revenue:sum * 2)"),
        ("last(rev)", "last(revenue:sum)"),
    ])
    def test_binds_to_the_inline_transform(self, saved, inline):
        key = _measure_key(saved)
        assert isinstance(key, TransformKey), key
        assert key == _measure_key(inline)

    @pytest.mark.parametrize("saved, inline", [
        ("first(rev)", "first(revenue:sum)"),
        ("first(rev * 2)", "first(revenue:sum * 2)"),
    ])
    def test_plans_identically(self, saved, inline):
        def plan(formula):
            return plan_query(
                query=_q(measures=[ModelMeasure(formula=formula, name="m")]),
                bundle=_bundle())
        assert plan(saved) == plan(inline)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if request.param == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=request.param, seed=seed, models=_models(),
    ) as (engine, _):
        yield engine


class TestSavedMeasureExecution:
    @pytest.mark.parametrize("saved, inline", [
        ("first(rev)", "first(revenue:sum)"),
        ("first(rev * 2)", "first(revenue:sum * 2)"),
    ])
    async def test_values_match_inline(self, exec_engine, saved, inline):
        got = await exec_engine.execute(
            _q(measures=[ModelMeasure(formula=saved, name="m")]))
        want = await exec_engine.execute(
            _q(measures=[ModelMeasure(formula=inline, name="m")]))
        assert got.data, "expected monthly rows"
        assert (got.columns, got.data, got.sql, got.warnings) == (
            want.columns, want.data, want.sql, want.warnings)


#: operand shape id -> query kwargs for ``fn(X)``.
PARITY = {
    "local_saved": lambda fn: {"measures": [ModelMeasure(formula=f"{fn}(rev)", name="m")]},
    "dotted_cross_model_saved": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(regions.tf)", name="m")]},
    "saved_transform": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(cum_rev)", name="m")]},
    "saved_composite": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(rev2)", name="m")]},
    "recursive_saved": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(rev_twice)", name="m")]},
    "alias_in_filter": lambda fn: {
        "measures": [ModelMeasure(formula="revenue:sum", name="r")],
        "filters": [f"{fn}(r) > 0"]},
    "alias_in_order": lambda fn: {
        "measures": [ModelMeasure(formula="revenue:sum", name="r")],
        "order": [{"column": f"{fn}(r)", "direction": "desc"}]},
    "unselected_saved_in_filter": lambda fn: {
        "measures": [ModelMeasure(formula="qty:sum", name="q")],
        "filters": [f"{fn}(rev) > 0"]},
    "unselected_saved_in_order": lambda fn: {
        "measures": [ModelMeasure(formula="qty:sum", name="q")],
        "order": [{"column": f"{fn}(rev)", "direction": "desc"}]},
}


class TestCumsumParity:
    """``first(X)`` binds to the transform exactly when ``cumsum(X)`` binds, and
    raises the same error when it raises."""

    @pytest.mark.parametrize("shape", list(PARITY))
    def test_first_matches_cumsum(self, shape):
        first = _outcome(_q(**PARITY[shape]("first")), fn="first")
        cumsum = _outcome(_q(**PARITY[shape]("cumsum")), fn="cumsum")
        assert first == cumsum
