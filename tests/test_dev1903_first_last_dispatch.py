"""``first`` / ``last`` parse to one ``AggCall`` and always bind as ranked aggregations:
an attached operand ranks the operand's cells, a row-grain operand its home's rows.

Spec: openspec …/specs/aggregations/functional-form — "Ambiguous first and last
names dispatch by argument shape".
"""

from __future__ import annotations

import pytest

from slayer.core.errors import TimeAxisError
from slayer.core.keys import AggregateKey, FieldKey, walk_value_keys
from slayer.core.models import DatasetMetric
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.core.syntax import AggCall, parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1846_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    month_td,
    regions_dataset,
    sales_dataset,
)
from tests._engine_helpers import seeded_exec_engine
from slayer.core import syntax
from slayer.engine.query_engine import SlayerQueryEngine

SAVED = [
    DatasetMetric(expression="sum(revenue)", name="rev"),
    DatasetMetric(expression="rev * 2", name="rev_twice"),
    DatasetMetric(expression="cumsum(sum(revenue))", name="cum_rev"),
    DatasetMetric(expression="sum(revenue) * 2", name="rev2"),
]


def _datasets():
    sales = sales_dataset()
    sales.metrics = list(SAVED)
    regions = regions_dataset()
    regions.metrics = [DatasetMetric(expression="sum(factor)", name="tf")]
    return [sales, regions]


def _bundle() -> ResolvedSourceBundle:
    sales, regions = _datasets()
    return ResolvedSourceBundle(dialect="postgres", population=sales, referenced_datasets=[regions])


def _q(**kw) -> SlayerQuery:
    kw.setdefault("population", "sales")
    kw.setdefault("time_dimensions", month_td())
    return SlayerQuery(**kw)


def _metric_key(expression: str, **kw):
    env = elaborate_query(
        query=_q(metrics=[QueryMetric(metric=expression, alias="m")], **kw),
        bundle=_bundle(),
    )
    assert env.prebound is not None
    return env.prebound.declared_metrics[-1].bound.value_key


def _ranked(roots, *, fn: str) -> list[AggregateKey]:
    """Every ranked ``fn`` aggregate under ``roots``."""
    return [k for root in roots for k in walk_value_keys(root)
            if isinstance(k, AggregateKey) and k.agg == fn]


def _outcome(query: SlayerQuery):
    """``("ok", bound metric / filter / order roots)`` or ``("err", exception)``."""
    try:
        env = elaborate_query(query=query, bundle=_bundle())
        plan_query(query=query, bundle=_bundle())
    except Exception as e:  # noqa: BLE001 — the error itself is the observation
        return ("err", e)
    assert env.prebound is not None
    return ("ok", (
        *(dm.bound.value_key for dm in env.prebound.declared_metrics),
        *(bf.value_key for bf in env.prebound.bound_filters),
        *(sp.bound.value_key for sp in env.prebound.order_specs),
    ))


class TestParseIsOneNode:
    @pytest.mark.parametrize("fn", ["first", "last"])
    @pytest.mark.parametrize("arg", [
        "revenue", "1", "sum(sales.revenue)", "sum(revenue) + 1",
        "sum(revenue) > 100", "cumsum(weight)", "rev", "rev * 2",
        "qty * avg(revenue, partition_by=store)",
    ])
    def test_every_shape_parses_to_aggcall(self, fn, arg):
        parsed = parse_expr(f"{fn}({arg})")
        assert isinstance(parsed, AggCall), parsed
        assert parsed.agg == fn

    def test_aggregated_operand_is_not_decided_in_the_parser(self):
        """engine P2: the parser makes no grain decision."""
        parsed = parse_expr("first(sum(sales.revenue))")
        assert isinstance(parsed, AggCall)
        assert isinstance(parsed.source, AggCall)

    def test_mixed_source_classifier_deleted(self):
        assert not hasattr(syntax, "_is_mixed_agg_source")
        assert not hasattr(syntax, "_FIRST_LAST")


class TestBindDispatch:
    @pytest.mark.parametrize("formula", [
        "first(sum(sales.revenue))", "last(sum(sales.revenue))", "first(sum(sales.revenue) + 1)",
        "first(sum(sales.revenue) > 100)",
    ])
    def test_aggregated_operand_binds_to_second_order_ranked_aggregate(self, formula):
        key = _metric_key(formula)
        assert isinstance(key, AggregateKey), key
        assert key.agg == formula.split("(")[0]

    @pytest.mark.parametrize("formula, ranking", [
        ("last(sales.revenue)", None), ("last(sales.revenue, sales.ordered_at)", "sales.ordered_at"),
    ])
    def test_row_operand_binds_to_ranked_aggregate(self, formula, ranking):
        key = _metric_key(formula)
        assert isinstance(key, AggregateKey), key
        assert key.agg == "last"
        assert key == _metric_key(
            "last(sales.revenue)" if ranking is None else f"last(sales.revenue, {ranking})",
        )

    @pytest.mark.parametrize("formula, kw", [
        ("first(sales.qty * avg(sales.revenue, partition_by=sales.store))", {"dimensions": ["sales.store"]}),
        ("first(1)", {}),
    ])
    def test_row_grain_expression_binds_to_ranked_aggregate(self, formula, kw):
        key = _metric_key(formula, **kw)
        assert isinstance(key, AggregateKey), key
        assert key.agg == "first"

    def test_partition_by_on_an_aggregate_operand_binds(self):
        key = _metric_key("first(sum(sales.revenue), partition_by=sales.store)",
                          dimensions=["sales.store"])
        assert isinstance(key, AggregateKey), key
        assert key.agg == "first"
        assert key.partition_keys is not None
        assert {k.leaf for k in key.partition_keys.keys if isinstance(k, FieldKey)} == {"store"}
        assert len(key.partition_keys.keys) == 1

    def test_transform_operand_reaches_the_checker(self):
        """Dispatch does not pre-empt transform-input validation."""
        with pytest.raises(ValueError) as ei:
            _metric_key("first(cumsum(sales.weight))")
        msg = str(ei.value)
        assert "The transform cannot consume the row-level" in msg, msg
        assert "\n  at transform 'cumsum'\n" in msg, msg
        assert "not supported over an expression" not in msg


class TestSavedMeasureOperand:
    @pytest.mark.parametrize("saved, inline", [
        ("first(sales.rev)", "first(sum(sales.revenue))"),
        ("first(sales.rev * 2)", "first(sum(sales.revenue) * 2)"),
        ("last(sales.rev)", "last(sum(sales.revenue))"),
    ])
    def test_binds_to_the_inline_aggregation(self, saved, inline):
        key = _metric_key(saved)
        assert isinstance(key, AggregateKey), key
        assert key == _metric_key(inline)

    @pytest.mark.parametrize("saved, inline", [
        ("first(sales.rev)", "first(sum(sales.revenue))"),
        ("first(sales.rev * 2)", "first(sum(sales.revenue) * 2)"),
    ])
    def test_plans_identically(self, saved, inline):
        def plan(formula):
            return plan_query(
                query=_q(metrics=[QueryMetric(metric=formula, alias="m")]),
                bundle=_bundle())
        assert plan(saved) == plan(inline)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if request.param == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=request.param, seed=seed, datasets=_datasets(),
    ) as (engine, _):
        yield engine


class TestSavedMeasureExecution:
    @pytest.mark.parametrize("saved, inline", [
        ("first(sales.rev)", "first(sum(sales.revenue))"),
        ("first(sales.rev * 2)", "first(sum(sales.revenue) * 2)"),
    ])
    async def test_values_match_inline(self, exec_engine: SlayerQueryEngine, saved, inline):
        got = await exec_engine.execute(
            _q(metrics=[QueryMetric(metric=saved, alias="m")]))
        want = await exec_engine.execute(
            _q(metrics=[QueryMetric(metric=inline, alias="m")]))
        assert got.data, "expected monthly rows"
        assert (got.columns, got.data, got.sql, got.warnings) == (
            want.columns, want.data, want.sql, want.warnings)


#: operand shape id -> query kwargs for ``fn(X)``.
PARITY = {
    "local_saved": lambda fn: {"metrics": [QueryMetric(metric=f"{fn}(sales.rev)", alias="m")]},
    "dotted_cross_model_saved": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(regions.tf)", alias="m")]},
    "saved_transform": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(sales.cum_rev)", alias="m")]},
    "saved_composite": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(sales.rev2)", alias="m")]},
    "recursive_saved": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(sales.rev_twice)", alias="m")]},
    "alias_in_filter": lambda fn: {
        "metrics": [QueryMetric(metric="sum(sales.revenue)", alias="r")],
        "filter": [f"{fn}(r) > 0"]},
    "alias_in_order": lambda fn: {
        "metrics": [QueryMetric(metric="sum(sales.revenue)", alias="r")],
        "order": [{"field": f"{fn}(r)", "direction": "desc"}]},
    "unselected_saved_in_filter": lambda fn: {
        "metrics": [QueryMetric(metric="sum(sales.qty)", alias="q")],
        "filter": [f"{fn}(sales.rev) > 0"]},
    "unselected_saved_in_order": lambda fn: {
        "metrics": [QueryMetric(metric="sum(sales.qty)", alias="q")],
        "order": [{"field": f"{fn}(sales.rev)", "direction": "desc"}]},
    "sibling_wrappers": lambda fn: {
        "metrics": [QueryMetric(metric="sum(sales.revenue)", alias="r"),
                     QueryMetric(metric="sum(sales.qty)", alias="s")],
        "filter": [f"{fn}(r) + {fn}(s) > 0"]},
    "wrapper_beside_cumsum": lambda fn: {
        "metrics": [QueryMetric(
            metric=f"{fn}(sum(sales.revenue)) * 2 + {fn}(sum(sales.revenue)) + cumsum(sum(sales.qty))",
            alias="m")]},
    "partitioned_operand": lambda fn: {
        "dimensions": ["sales.store"],
        "metrics": [QueryMetric(metric=f"{fn}(sum(sales.revenue, partition_by=sales.store))", alias="m")]},
    "cross_model_composite": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(regions.tf * 2 + sales.rev)", alias="m")]},
    "inner_cumsum_error": lambda fn: {
        "metrics": [QueryMetric(metric=f"{fn}(cumsum(sales.weight))", alias="m")]},
}


class TestCumsumParity:
    """``first(X)`` binds exactly when ``cumsum(X)`` binds, to a ranked aggregation, and
    raises the same error type when it raises."""

    @pytest.mark.parametrize("fn", ["first", "last"])
    @pytest.mark.parametrize("shape", list(PARITY))
    def test_matches_cumsum(self, shape, fn):
        (kind, got), (want_kind, want) = (
            _outcome(_q(**PARITY[shape](op))) for op in (fn, "cumsum"))
        assert kind == want_kind, (got, want)
        if kind == "err":
            assert type(got) is type(want), (got, want)
        else:
            assert _ranked(got, fn=fn), got

    @pytest.mark.parametrize("fn", ["first", "last"])
    def test_wrapper_of_wrapper_lacks_the_time_bucket(self, fn):
        """The inner pick drops the month bucket, so the outer one has no default key."""
        kind, got = _outcome(_q(metrics=[QueryMetric(metric=f"{fn}({fn}(sum(sales.revenue)))", alias="m")]))
        assert kind == "err", got
        assert isinstance(got, TimeAxisError), got
