"""``first`` / ``last`` parse to one ``AggCall`` and dispatch at bind by the bound
operand's type: attached operand → the series transform, row-grain operand → the
ranked aggregation.

Spec: openspec …/specs/aggregations/functional-form — "Ambiguous first and last
names dispatch by argument shape".
"""

from __future__ import annotations

import re

import pytest
from pydantic import BaseModel

from slayer.core.keys import AggregateKey, ColumnKey, TransformKey
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


_OP_TOKEN = re.compile(r"(?<=Transform ')(first|last|cumsum)(?=')")


def _aligned(a, b, *, fn: str) -> bool:
    """``a == b`` except a ``fn`` transform may stand where ``b`` has ``cumsum``."""
    if isinstance(a, BaseModel):
        return type(a) is type(b) and all(
            (isinstance(a, TransformKey) and f == "op" and (a.op, b.op) == (fn, "cumsum"))
            or _aligned(getattr(a, f), getattr(b, f), fn=fn)
            for f in type(a).model_fields
        )
    if isinstance(a, tuple):
        return type(a) is type(b) and len(a) == len(b) and all(
            _aligned(x, y, fn=fn) for x, y in zip(a, b))
    return type(a) is type(b) and a == b


def _msg_aligned(a: str, b: str, *, fn: str) -> bool:
    """``a == b`` except ``fn`` may stand in a ``Transform '…'`` label where ``b`` names ``cumsum``."""
    sa, sb = _OP_TOKEN.split(a), _OP_TOKEN.split(b)
    return len(sa) == len(sb) and all(
        x == y or (i % 2 == 1 and (x, y) == (fn, "cumsum"))
        for i, (x, y) in enumerate(zip(sa, sb)))


def _outcome(query: SlayerQuery):
    """``("ok", bound measure / filter / order roots)`` or ``("err", exception)``."""
    try:
        env = elaborate_query(query=query, bundle=_bundle())
        plan_query(query=query, bundle=_bundle())
    except Exception as e:  # noqa: BLE001 — the error itself is the observation
        return ("err", e)
    assert env.prebound is not None
    return ("ok", (
        *(dm.bound.value_key for dm in env.prebound.declared_measures),
        *(bf.value_key for bf in env.prebound.bound_filters),
        *(sp.bound.value_key for sp in env.prebound.order_specs),
    ))


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
        assert "The transform cannot consume the row-level" in msg, msg
        assert "\n  at transform 'cumsum'\n" in msg, msg
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
    "sibling_wrappers": lambda fn: {
        "measures": [ModelMeasure(formula="revenue:sum", name="r"),
                     ModelMeasure(formula="qty:sum", name="s")],
        "filters": [f"{fn}(r) + {fn}(s) > 0"]},
    "wrapper_of_wrapper": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}({fn}(revenue:sum))", name="m")]},
    "wrapper_beside_cumsum": lambda fn: {
        "measures": [ModelMeasure(
            formula=f"{fn}(revenue:sum) * 2 + {fn}(revenue:sum) + cumsum(qty:sum)",
            name="m")]},
    "partitioned_operand": lambda fn: {
        "dimensions": ["store"],
        "measures": [ModelMeasure(formula=f"{fn}(sum(revenue, partition_by=store))", name="m")]},
    "cross_model_composite": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(regions.tf * 2 + rev)", name="m")]},
    "inner_cumsum_error": lambda fn: {
        "measures": [ModelMeasure(formula=f"{fn}(cumsum(weight))", name="m")]},
}


class TestCumsumParity:
    """``first(X)`` binds to the transform exactly when ``cumsum(X)`` binds, and
    raises the same error when it raises."""

    @pytest.mark.parametrize("fn", ["first", "last"])
    @pytest.mark.parametrize("shape", list(PARITY))
    def test_matches_cumsum(self, shape, fn):
        (kind, got), (want_kind, want) = (
            _outcome(_q(**PARITY[shape](op))) for op in (fn, "cumsum"))
        assert kind == want_kind, (got, want)
        if kind == "err":
            assert type(got) is type(want), (got, want)
            assert _msg_aligned(str(got), str(want), fn=fn), (got, want)
        else:
            assert _aligned(got, want, fn=fn), (got, want)

    def test_oracle_renames_only_aligned_wrappers(self):
        x = AggregateKey(agg="sum", source=ColumnKey(leaf="revenue"))
        cum = TransformKey(op="cumsum", input=x)
        assert _aligned(TransformKey(op="first", input=cum),
                        TransformKey(op="cumsum", input=cum), fn="first")
        assert not _aligned(TransformKey(op="first", input=cum),
                            TransformKey(op="first", input=cum.model_copy(update={"op": "first"})),
                            fn="first")
        assert not _aligned(TransformKey(op="last", input=x), cum, fn="first")
        assert _msg_aligned("Transform 'first' vs 'cumsum'", "Transform 'cumsum' vs 'cumsum'", fn="first")
        assert not _msg_aligned("in 'first(r)'", "in 'cumsum(r)'", fn="first")
        assert not _msg_aligned("Transform 'cumsum'", "Transform 'first'", fn="first")
