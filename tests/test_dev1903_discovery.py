"""The one discovery walk (design D1, D6): ``discover_roots``
yields one ``RootDisposition`` per consumer occurrence (root, phase, routing,
consumer names, declared type, shift ``series``)."""

from __future__ import annotations

import importlib
import inspect
from typing import Callable, List, Optional, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import PartitionKeyError, TimeAxisError
from slayer.core.keys import AggregateKey, Grain, TransformKey, ValueKey
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.compile.discovery import RootDisposition, discover_roots
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1832_fixtures import dev1832_datasets, make_exec_engine, monthly_q
from tests._dev1832_fixtures import month_td as monthly_month_td
from tests._dev1836_fixtures import SPEND_BAND, dev1836_datasets, month_td, q
from tests._dev1847_fixtures import (
    INNER_CR,
    INNER_UP_PRODUCT,
    MIXED_SUM,
    dev1847_datasets,
    sales_q,
)
from tests.test_filtered_local_isolation import _s5_bundle

LOCAL_BAND = {
    "field": "CASE WHEN sum(orders.amount, partition_by=orders.channel) > 30 THEN 1 ELSE 0 END",
    "alias": "band",
}
_X = "sum(amount, partition_by=[region, ordered_at])"
REAGG_STANDALONE = f"min({_X}, partition_by=region)"
HANDWRITTEN_MIN = f"sum(amount * {REAGG_STANDALONE})"
REAGG_BAND = {
    "field": f"CASE WHEN {REAGG_STANDALONE} > 5 THEN 'hi' ELSE 'lo' END",
    "alias": "rb",
}


def _bundle(datasets) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", population=datasets[0], referenced_datasets=datasets[1:])


def _monthly_datasets():
    return sorted(dev1832_datasets(), key=lambda m: m.name != "monthly")


def _discover(query: SlayerQuery, bundle: ResolvedSourceBundle) -> List[RootDisposition]:
    env = elaborate_query(query=query, bundle=bundle)
    return discover_roots(
        env.prebound, filter_typings=list(env.filter_typings),
        scope=env.scope, bundle=env.bundle,
    )


def _leaves(key: ValueKey) -> List[str]:
    out = []
    partition_keys: Optional[Grain] = getattr(key, "partition_keys", None)
    for pk in partition_keys or ():
        col = getattr(pk, "field", pk)
        out.append(getattr(col, "leaf", str(col)))
    return sorted(out)


def _agg(name: str, pks: Sequence[str] = ()) -> Callable[[ValueKey], bool]:
    def match(k: ValueKey) -> bool:
        return isinstance(k, AggregateKey) and k.agg == name and _leaves(k) == sorted(pks)
    return match


def _of(ds: List[RootDisposition], match) -> List[RootDisposition]:
    return [d for d in ds if match(d.root)]


def _pairs(ds: List[RootDisposition], match) -> set:
    return {(d.phase, d.routing) for d in _of(ds, match)}


class TestPhaseRoutingMatrix:
    def test_row_local_producer(self):
        ds = _discover(q(dimensions=["orders.status", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_combined_local_producer_partitioned(self):
        ds = _discover(q(dimensions=["orders.status", "orders.channel"],
                         metrics=[QueryMetric(metric="sum(orders.amount, partition_by=orders.channel)",
                                                alias="pt")]),
                       _bundle(dev1836_datasets()))
        [d] = _of(ds, _agg("sum", ["channel"]))
        assert (d.phase, d.routing) == ("combined", "local_producer")
        assert d.consumer_public_names == ("pt",)
        assert d.declared_type is None

    def test_combined_local_producer_ranked(self):
        ds = _discover(q(dimensions=["orders.status"],
                         metrics=[QueryMetric(metric="last(orders.amount, orders.ordered_at)", alias="l")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("last")) == {("combined", "local_producer")}

    def test_combined_local_producer_windowed(self):
        ds = _discover(q(time_dimensions=month_td(),
                         metrics=[QueryMetric(metric="sum(orders.amount, window='90d')", alias="w")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum")) == {("combined", "local_producer")}

    def test_combined_local_producer_crossing_input(self):
        ds = _discover(SlayerQuery(population="orders",
                                   metrics=[QueryMetric(metric="sum(orders.region_pay)", alias="m0")]),
                       _s5_bundle())
        assert _pairs(ds, _agg("sum")) == {("combined", "local_producer")}

    def test_row_target_rooted(self):
        ds = _discover(q(dimensions=[{"field": SPEND_BAND, "alias": "band"}],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["tier"])) == {("row", "target_rooted")}

    def test_combined_target_rooted_cross_model(self):
        ds = _discover(q(dimensions=["orders.status"],
                         metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm")]),
                       _bundle(dev1836_datasets()))
        [d] = _of(ds, _agg("sum"))
        assert (d.phase, d.routing) == ("combined", "target_rooted")
        assert d.consumer_public_names == ("cm",)

    def test_declared_type_carried(self):
        ds = _discover(q(dimensions=["orders.status"],
                         metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm",
                                                datatype=DataType.FLOAT)]),
                       _bundle(dev1836_datasets()))
        [d] = _of(ds, _agg("sum"))
        assert d.declared_type == DataType.FLOAT

    def test_combined_target_rooted_unattributable_grain(self):
        """A local aggregate grouped by an unattributable dimension routes target-rooted."""
        ds = _discover(q(dimensions=["customers.segments.label"],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum")) == {("combined", "target_rooted")}

    def test_combined_reaggregation(self):
        ds = _discover(sales_q(dimensions=["sales.region"],
                               metrics=[QueryMetric(metric=f"avg({INNER_CR})", alias="a")]),
                       _bundle(dev1847_datasets()))
        [d] = _of(ds, _agg("avg"))
        assert (d.phase, d.routing) == ("combined", "reaggregation")
        assert d.consumer_public_names == ("a",)
        assert not _of(ds, _agg("sum", ["city", "region"]))

    def test_row_reaggregation(self):
        ds = _discover(monthly_q(dimensions=["monthly.region", REAGG_BAND],
                                 time_dimensions=monthly_month_td(),
                                 metrics=[QueryMetric(metric="sum(monthly.amount)", alias="s")]),
                       _bundle(_monthly_datasets()))
        assert _pairs(ds, _agg("min", ["region"])) == {("row", "reaggregation")}

    def test_row_reaggregation_constituent(self):
        ds = _discover(monthly_q(dimensions=["monthly.region"], time_dimensions=monthly_month_td(),
                                 metrics=[QueryMetric(metric=HANDWRITTEN_MIN, alias="b")]),
                       _bundle(_monthly_datasets()))
        [d] = _of(ds, _agg("min", ["region"]))
        assert (d.phase, d.routing) == ("row", "reaggregation_constituent")
        assert d.consumer_public_names == ("b",)
        assert _pairs(ds, _agg("sum")) == {("combined", "inline")}


class TestRowAttachRoots:
    def test_inline_root_yields_its_inputs(self):
        ds = _discover(sales_q(dimensions=["sales.region"],
                               metrics=[QueryMetric(metric=MIXED_SUM, alias="m")]),
                       _bundle(dev1847_datasets()))
        [root] = _of(ds, _agg("sum"))
        assert (root.phase, root.routing) == ("combined", "inline")
        assert root.consumer_public_names == ("m",)
        assert _pairs(ds, _agg("avg", ["product"])) == {("row", "local_producer")}

    def test_producer_bound_root_omits_its_inputs(self):
        formula = f"sum(quantity * {INNER_UP_PRODUCT}, partition_by=region)"
        ds = _discover(sales_q(dimensions=["sales.region", "sales.city"],
                               metrics=[QueryMetric(metric=formula, alias="m")]),
                       _bundle(dev1847_datasets()))
        assert _pairs(ds, _agg("sum", ["region"])) == {("combined", "local_producer")}
        assert not _of(ds, _agg("avg", ["product"]))


class TestDualRole:
    def test_computed_dimension_and_measure(self):
        ds = _discover(q(dimensions=["orders.channel", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount, partition_by=orders.channel)",
                                                alias="x")]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {
            ("row", "local_producer"), ("combined", "local_producer"),
        }
        [combined] = [d for d in _of(ds, _agg("sum", ["channel"])) if d.phase == "combined"]
        assert combined.consumer_public_names == ("x",)

    def test_standalone_reaggregation_and_constituent_combined_row(self):
        ds = _discover(monthly_q(dimensions=["monthly.region"], time_dimensions=monthly_month_td(),
                                 metrics=[QueryMetric(metric=REAGG_STANDALONE, alias="a"),
                                           QueryMetric(metric=HANDWRITTEN_MIN, alias="b")]),
                       _bundle(_monthly_datasets()))
        by_pair = {(d.phase, d.routing): d for d in _of(ds, _agg("min", ["region"]))}
        assert set(by_pair) == {
            ("combined", "reaggregation"), ("row", "reaggregation_constituent"),
        }
        assert by_pair["combined", "reaggregation"].consumer_public_names == ("a",)
        assert by_pair["row", "reaggregation_constituent"].consumer_public_names == ("b",)

    def test_standalone_reaggregation_and_constituent_row_row(self):
        ds = _discover(monthly_q(dimensions=["monthly.region", REAGG_BAND],
                                 time_dimensions=monthly_month_td(),
                                 metrics=[QueryMetric(metric=HANDWRITTEN_MIN, alias="b")]),
                       _bundle(_monthly_datasets()))
        assert _pairs(ds, _agg("min", ["region"])) == {
            ("row", "reaggregation"), ("row", "reaggregation_constituent"),
        }


class TestConsumerContextExclusions:
    def test_order_by_dimension_name_is_row_scope(self):
        ds = _discover(q(dimensions=["orders.status", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
                         order=[{"field": "band", "direction": "asc"}]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_field_typed_filter_on_dimension_aggregate_is_row_scope(self):
        ds = _discover(q(dimensions=["orders.status", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
                         filter=["sum(orders.amount, partition_by=orders.channel) > 30"]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_measure_typed_filter_dimension_subtree_not_walked(self):
        ds = _discover(q(dimensions=["orders.status", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
                         filter=["sum(orders.amount) > band"]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_measure_typed_filter_keeps_dual_role_combined(self):
        ds = _discover(q(dimensions=["orders.channel", LOCAL_BAND],
                         metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
                         filter=["sum(orders.amount, partition_by=orders.channel) > sum(orders.amount)"]),
                       _bundle(dev1836_datasets()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {
            ("row", "local_producer"), ("combined", "local_producer"),
        }


class TestConsumerNames:
    def test_constituent_shared_by_two_measures_names_both(self):
        other = f"sum(amount * {REAGG_STANDALONE} + 1)"
        ds = _discover(monthly_q(dimensions=["monthly.region"], time_dimensions=monthly_month_td(),
                                 metrics=[QueryMetric(metric=HANDWRITTEN_MIN, alias="b"),
                                           QueryMetric(metric=other, alias="c")]),
                       _bundle(_monthly_datasets()))
        occ = _of(ds, _agg("min", ["region"]))
        assert {(d.phase, d.routing) for d in occ} == {("row", "reaggregation_constituent")}
        names = [n for d in occ for n in d.consumer_public_names]
        assert tuple(dict.fromkeys(names)) == ("b", "c")


class TestShiftCandidates:
    @pytest.mark.parametrize(("formula", "series"), [
        pytest.param("time_shift(sum(orders.amount), -1)", False, id="re-aggregating"),
        pytest.param("time_shift(cumsum(sum(orders.amount)), -1)", True, id="series"),
    ])
    def test_time_shift_carries_series(self, formula, series):
        ds = _discover(q(time_dimensions=month_td(),
                         metrics=[QueryMetric(metric=formula, alias="ts")]),
                       _bundle(dev1836_datasets()))
        [d] = [d for d in ds
               if isinstance(d.root, TransformKey) and d.root.op == "time_shift"]
        assert (d.phase, d.routing, d.series) == ("combined", "shifted", series)

    def test_non_shift_disposition_has_no_series(self):
        ds = _discover(q(dimensions=["orders.status"],
                         metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm")]),
                       _bundle(dev1836_datasets()))
        assert ds
        assert all(d.series is None for d in ds)


class TestOccurrenceOrder:
    def test_first_seen_order(self):
        ds = _discover(q(dimensions=["orders.status"],
                         metrics=[QueryMetric(metric="sum(customers.spend)", alias="cm"),
                                   QueryMetric(metric="last(orders.amount, orders.ordered_at)", alias="l")]),
                       _bundle(dev1836_datasets()))
        routings = [d.routing for d in ds if d.routing != "inline"]
        assert routings == ["target_rooted", "local_producer"]


class TestDeletedSymbols:
    @pytest.mark.parametrize(("module", "name"), [
        ("slayer.ir.bound", "combined_consumer_aggregates"),
        ("slayer.ir.bound", "dimension_regroup_roots"),
        ("slayer.ir.bound", "dimension_partitioned_aggregates"),
        ("slayer.ir.bound", "CombinedConsumers"),
        ("slayer.engine.compile.stages", "_bare_combined_roots"),
        ("slayer.engine.compile.stages", "_discover_roots"),
        ("slayer.engine.compile.stages", "_answers_need_nested_regroups"),
        ("slayer.engine.cardinality.relationship_safety", "local_crossing_input_paths"),
        ("slayer.ir.prebound", "position_typing_context"),
    ])
    def test_symbol_gone(self, module, name):
        assert not hasattr(importlib.import_module(module), name)

    def test_closure_has_one_mode(self):
        closure = importlib.import_module("slayer.engine.reference_closure")
        params = inspect.signature(closure.aggregate_input_closure).parameters
        assert "descend_aggregates" not in params


_R = "avg(sum(sales.amount, partition_by=[sales.city, sales.region]), partition_by=sales.region)"
_RLEVEL = {"field": f"CASE WHEN {_R} > 50 THEN 'hi' ELSE 'lo' END", "alias": "rlevel"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestReaggregationInComputedDimension:
    """Row-role leniency keeps re-aggregations; a combined consumer of the same key
    still needs query-dimension partition keys."""

    @pytest.mark.parametrize(("kw", "location"), [
        pytest.param({"metrics": [QueryMetric(metric=_R, alias="r")]},
                     "metric 'r'", id="dual-role-measure"),
        pytest.param({"metrics": [QueryMetric(metric="sum(sales.amount)", alias="s")],
                      "order": [{"field": _R, "direction": "asc"}]},
                     "order item", id="dual-role-order"),
    ])
    def test_partition_key_error(self, kw, location):
        query = sales_q(dimensions=["sales.product", _RLEVEL], **kw)
        bundle = _bundle(dev1847_datasets())
        with pytest.raises(PartitionKeyError) as ei:
            plan_query(query=query, bundle=bundle)
        assert "partition_by field 'region' is not a query dimension" in ei.value.summary
        assert ei.value.location is not None
        assert ei.value.location.startswith(location)

    @pytest.mark.parametrize(("filters", "expected"), [
        pytest.param([], {("P", "lo"): 40.0, ("P", "hi"): 140.0,
                          ("Q", "lo"): 70.0, ("Q", "hi"): 180.0}, id="dimension-only"),
        pytest.param([f"{_R} > 50"], {("P", "hi"): 140.0, ("Q", "hi"): 180.0},
                     id="filter-over-it"),
    ])
    async def test_dimension_only_consumption_executes(self, exec_engine, filters, expected):
        resp = await exec_engine.execute(sales_q(
            dimensions=["sales.product", _RLEVEL],
            metrics=[QueryMetric(metric="sum(sales.amount)", alias="s")], filter=filters))
        got = {(r["sales.product"], r["rlevel"]): r["s"] for r in resp.data}
        assert got == pytest.approx(expected)


class TestUnderReaggregationPartitionKeys:
    def test_computed_dimension_key_keeps_its_row_role(self):
        """A computed dimension named in a re-aggregation's ``partition_by`` yields no
        combined twin for its own partitioned aggregate."""
        band = {"field": "CASE WHEN sum(amount, partition_by=product) > 100 "
                              "THEN 'big' ELSE 'small' END", "alias": "pband"}
        formula = "avg(sum(sales.amount, partition_by=[sales.city, sales.region, pband]), partition_by=pband)"
        ds = _discover(sales_q(dimensions=[band],
                               metrics=[QueryMetric(metric=formula, alias="r")]),
                       _bundle(dev1847_datasets()))
        assert [(d.phase, d.routing) for d in ds] == [
            ("row", "local_producer"), ("combined", "reaggregation"),
        ]
        assert ds[1].consumer_public_names == ("r",)


class TestTransformOverReaggregationDimension:
    async def test_filter_on_it_is_a_time_axis_error(self, exec_engine):
        """The re-aggregation is opaque at grain ``region`` — no time axis for ``cumsum``,
        exactly as over ``sum(amount, partition_by=region)``."""
        query = monthly_q(
            dimensions=["monthly.region", {"field": f"cumsum({REAGG_STANDALONE})", "alias": "rr"}],
            time_dimensions=monthly_month_td(),
            metrics=[QueryMetric(metric="sum(amount)", alias="s")],
            filter=["rr > 10"])
        with pytest.raises(TimeAxisError, match="cumsum"):
            await exec_engine.execute(query)
