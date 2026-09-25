"""The one discovery walk (design D1, D6): ``discover_roots``
yields one ``RootDisposition`` per consumer occurrence (root, phase, routing,
consumer names, declared type, shift ``series``)."""

from __future__ import annotations

import importlib
import inspect
from typing import Callable, List, Sequence

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, TransformKey, ValueKey
from slayer.core.models import ModelMeasure
from slayer.core.query import SlayerQuery
from slayer.engine.compile.discovery import RootDisposition, discover_roots
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1832_fixtures import dev1832_models, make_exec_engine, month_key, monthly_q
from tests._dev1832_fixtures import month_td as monthly_month_td
from tests._dev1836_fixtures import SPEND_BAND, dev1836_models, month_td, q
from tests._dev1847_fixtures import (
    INNER_CR,
    INNER_UP_PRODUCT,
    MIXED_SUM,
    dev1847_models,
    sales_q,
)
from tests.test_filtered_local_isolation import _s5_bundle

LOCAL_BAND = {
    "expression": "CASE WHEN amount:sum(partition_by=channel) > 30 THEN 1 ELSE 0 END",
    "name": "band",
}
_X = "amount:sum(partition_by=[region, ordered_at])"
REAGG_STANDALONE = f"min({_X}, partition_by=region)"
HANDWRITTEN_MIN = f"sum(amount * {REAGG_STANDALONE})"
REAGG_BAND = {
    "expression": f"CASE WHEN {REAGG_STANDALONE} > 5 THEN 'hi' ELSE 'lo' END",
    "name": "rb",
}


def _bundle(models) -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", source_model=models[0], referenced_models=models[1:])


def _monthly_models():
    return sorted(dev1832_models(), key=lambda m: m.name != "monthly")


def _discover(query: SlayerQuery, bundle: ResolvedSourceBundle) -> List[RootDisposition]:
    env = elaborate_query(query=query, bundle=bundle)
    return discover_roots(
        env.prebound, filter_typings=list(env.filter_typings),
        scope=env.scope, bundle=env.bundle,
    )


def _leaves(key: ValueKey) -> List[str]:
    out = []
    for pk in getattr(key, "partition_keys", None) or ():
        col = getattr(pk, "column", pk)
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
        ds = _discover(q(dimensions=["status", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum", name="s")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_combined_local_producer_partitioned(self):
        ds = _discover(q(dimensions=["status", "channel"],
                         measures=[ModelMeasure(formula="amount:sum(partition_by=channel)",
                                                name="pt")]),
                       _bundle(dev1836_models()))
        [d] = _of(ds, _agg("sum", ["channel"]))
        assert (d.phase, d.routing) == ("combined", "local_producer")
        assert d.consumer_public_names == ("pt",)
        assert d.declared_type is None

    def test_combined_local_producer_ranked(self):
        ds = _discover(q(dimensions=["status"],
                         measures=[ModelMeasure(formula="amount:last(ordered_at)", name="l")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("last")) == {("combined", "local_producer")}

    def test_combined_local_producer_windowed(self):
        ds = _discover(q(time_dimensions=month_td(),
                         measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum")) == {("combined", "local_producer")}

    def test_combined_local_producer_crossing_input(self):
        ds = _discover(SlayerQuery(source_model="orders",
                                   measures=[ModelMeasure(formula="region_pay:sum", name="m0")]),
                       _s5_bundle())
        assert _pairs(ds, _agg("sum")) == {("combined", "local_producer")}

    def test_row_target_rooted(self):
        ds = _discover(q(dimensions=[{"expression": SPEND_BAND, "name": "band"}],
                         measures=[ModelMeasure(formula="amount:sum", name="s")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["tier"])) == {("row", "target_rooted")}

    def test_combined_target_rooted_cross_model(self):
        ds = _discover(q(dimensions=["status"],
                         measures=[ModelMeasure(formula="customers.spend:sum", name="cm")]),
                       _bundle(dev1836_models()))
        [d] = _of(ds, _agg("sum"))
        assert (d.phase, d.routing) == ("combined", "target_rooted")
        assert d.consumer_public_names == ("cm",)

    def test_declared_type_carried(self):
        ds = _discover(q(dimensions=["status"],
                         measures=[ModelMeasure(formula="customers.spend:sum", name="cm",
                                                type=DataType.DOUBLE)]),
                       _bundle(dev1836_models()))
        [d] = _of(ds, _agg("sum"))
        assert d.declared_type == DataType.DOUBLE

    def test_combined_target_rooted_unattributable_grain(self):
        """A local aggregate grouped by an unattributable dimension routes target-rooted."""
        ds = _discover(q(dimensions=["customers.segments.label"],
                         measures=[ModelMeasure(formula="amount:sum", name="s")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum")) == {("combined", "target_rooted")}

    def test_combined_reaggregation(self):
        ds = _discover(sales_q(dimensions=["region"],
                               measures=[ModelMeasure(formula=f"avg({INNER_CR})", name="a")]),
                       _bundle(dev1847_models()))
        [d] = _of(ds, _agg("avg"))
        assert (d.phase, d.routing) == ("combined", "reaggregation")
        assert d.consumer_public_names == ("a",)
        assert not _of(ds, _agg("sum", ["city", "region"]))

    def test_row_reaggregation(self):
        ds = _discover(monthly_q(dimensions=["region", REAGG_BAND],
                                 time_dimensions=monthly_month_td(),
                                 measures=[ModelMeasure(formula="amount:sum", name="s")]),
                       _bundle(_monthly_models()))
        assert _pairs(ds, _agg("min", ["region"])) == {("row", "reaggregation")}

    def test_row_reaggregation_constituent(self):
        ds = _discover(monthly_q(dimensions=["region"], time_dimensions=monthly_month_td(),
                                 measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="b")]),
                       _bundle(_monthly_models()))
        [d] = _of(ds, _agg("min", ["region"]))
        assert (d.phase, d.routing) == ("row", "reaggregation_constituent")
        assert d.consumer_public_names == ("b",)
        assert _pairs(ds, _agg("sum")) == {("combined", "inline")}


class TestRowAttachRoots:
    def test_inline_root_yields_its_inputs(self):
        ds = _discover(sales_q(dimensions=["region"],
                               measures=[ModelMeasure(formula=MIXED_SUM, name="m")]),
                       _bundle(dev1847_models()))
        [root] = _of(ds, _agg("sum"))
        assert (root.phase, root.routing) == ("combined", "inline")
        assert root.consumer_public_names == ("m",)
        assert _pairs(ds, _agg("avg", ["product"])) == {("row", "local_producer")}

    def test_producer_bound_root_omits_its_inputs(self):
        formula = f"sum(quantity * {INNER_UP_PRODUCT}, partition_by=region)"
        ds = _discover(sales_q(dimensions=["region", "city"],
                               measures=[ModelMeasure(formula=formula, name="m")]),
                       _bundle(dev1847_models()))
        assert _pairs(ds, _agg("sum", ["region"])) == {("combined", "local_producer")}
        assert not _of(ds, _agg("avg", ["product"]))


class TestDualRole:
    def test_computed_dimension_and_measure(self):
        ds = _discover(q(dimensions=["channel", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum(partition_by=channel)",
                                                name="x")]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {
            ("row", "local_producer"), ("combined", "local_producer"),
        }
        [combined] = [d for d in _of(ds, _agg("sum", ["channel"])) if d.phase == "combined"]
        assert combined.consumer_public_names == ("x",)

    def test_standalone_reaggregation_and_constituent_combined_row(self):
        ds = _discover(monthly_q(dimensions=["region"], time_dimensions=monthly_month_td(),
                                 measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                                           ModelMeasure(formula=HANDWRITTEN_MIN, name="b")]),
                       _bundle(_monthly_models()))
        by_pair = {(d.phase, d.routing): d for d in _of(ds, _agg("min", ["region"]))}
        assert set(by_pair) == {
            ("combined", "reaggregation"), ("row", "reaggregation_constituent"),
        }
        assert by_pair["combined", "reaggregation"].consumer_public_names == ("a",)
        assert by_pair["row", "reaggregation_constituent"].consumer_public_names == ("b",)

    def test_standalone_reaggregation_and_constituent_row_row(self):
        ds = _discover(monthly_q(dimensions=["region", REAGG_BAND],
                                 time_dimensions=monthly_month_td(),
                                 measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="b")]),
                       _bundle(_monthly_models()))
        assert _pairs(ds, _agg("min", ["region"])) == {
            ("row", "reaggregation"), ("row", "reaggregation_constituent"),
        }


class TestConsumerContextExclusions:
    def test_order_by_dimension_name_is_row_scope(self):
        ds = _discover(q(dimensions=["status", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum", name="s")],
                         order=[{"column": "band", "direction": "asc"}]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_field_typed_filter_on_dimension_aggregate_is_row_scope(self):
        ds = _discover(q(dimensions=["status", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum", name="s")],
                         filters=["amount:sum(partition_by=channel) > 30"]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_measure_typed_filter_dimension_subtree_not_walked(self):
        ds = _discover(q(dimensions=["status", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum", name="s")],
                         filters=["amount:sum > band"]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {("row", "local_producer")}

    def test_measure_typed_filter_keeps_dual_role_combined(self):
        ds = _discover(q(dimensions=["channel", LOCAL_BAND],
                         measures=[ModelMeasure(formula="amount:sum", name="s")],
                         filters=["amount:sum(partition_by=channel) > amount:sum"]),
                       _bundle(dev1836_models()))
        assert _pairs(ds, _agg("sum", ["channel"])) == {
            ("row", "local_producer"), ("combined", "local_producer"),
        }


class TestConsumerNames:
    def test_constituent_shared_by_two_measures_names_both(self):
        other = f"sum(amount * {REAGG_STANDALONE} + 1)"
        ds = _discover(monthly_q(dimensions=["region"], time_dimensions=monthly_month_td(),
                                 measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="b"),
                                           ModelMeasure(formula=other, name="c")]),
                       _bundle(_monthly_models()))
        occ = _of(ds, _agg("min", ["region"]))
        assert {(d.phase, d.routing) for d in occ} == {("row", "reaggregation_constituent")}
        names = [n for d in occ for n in d.consumer_public_names]
        assert tuple(dict.fromkeys(names)) == ("b", "c")


class TestShiftCandidates:
    @pytest.mark.parametrize(("formula", "series"), [
        pytest.param("time_shift(amount:sum, -1)", False, id="re-aggregating"),
        pytest.param("time_shift(cumsum(amount:sum), -1)", True, id="series"),
    ])
    def test_time_shift_carries_series(self, formula, series):
        ds = _discover(q(time_dimensions=month_td(),
                         measures=[ModelMeasure(formula=formula, name="ts")]),
                       _bundle(dev1836_models()))
        [d] = [d for d in ds
               if isinstance(d.root, TransformKey) and d.root.op == "time_shift"]
        assert (d.phase, d.routing, d.series) == ("combined", "shifted", series)

    def test_non_shift_disposition_has_no_series(self):
        ds = _discover(q(dimensions=["status"],
                         measures=[ModelMeasure(formula="customers.spend:sum", name="cm")]),
                       _bundle(dev1836_models()))
        assert ds
        assert all(d.series is None for d in ds)


class TestOccurrenceOrder:
    def test_first_seen_order(self):
        ds = _discover(q(dimensions=["status"],
                         measures=[ModelMeasure(formula="customers.spend:sum", name="cm"),
                                   ModelMeasure(formula="amount:last(ordered_at)", name="l")]),
                       _bundle(dev1836_models()))
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
        ("slayer.engine.join_safety", "local_crossing_input_paths"),
        ("slayer.ir.prebound", "position_typing_context"),
    ])
    def test_symbol_gone(self, module, name):
        assert not hasattr(importlib.import_module(module), name)

    def test_closure_has_one_mode(self):
        closure = importlib.import_module("slayer.engine.reference_closure")
        params = inspect.signature(closure.aggregate_input_closure).parameters
        assert "descend_aggregates" not in params


_R = "avg(sum(amount, partition_by=[city, region]), partition_by=region)"
_RLEVEL = {"expression": f"CASE WHEN {_R} > 50 THEN 'hi' ELSE 'lo' END", "name": "rlevel"}
_NOT_A_DIM = "The partition_by column 'region' is not a query dimension.\n  at aggregation 'avg'"
_REAGG_DECLARES = "declares partition_by=region, which is not a query dimension"


class TestReaggregationInComputedDimension:
    """Row-role leniency keeps re-aggregations; a combined consumer of the same key
    still needs query-dimension partition keys."""

    @pytest.mark.parametrize(("kw", "message"), [
        pytest.param({"measures": [ModelMeasure(formula="amount:sum", name="s")]},
                     _REAGG_DECLARES, id="dimension-only"),
        pytest.param({"measures": [ModelMeasure(formula=_R, name="r")]},
                     _NOT_A_DIM, id="dual-role-measure"),
        pytest.param({"measures": [ModelMeasure(formula="amount:sum", name="s")],
                      "order": [{"column": _R, "direction": "asc"}]},
                     _NOT_A_DIM, id="dual-role-order"),
        pytest.param({"measures": [ModelMeasure(formula="amount:sum", name="s")],
                      "filters": [f"{_R} > 50"]},
                     _REAGG_DECLARES, id="filter-over-it"),
    ])
    def test_partition_key_error(self, kw, message):
        query = sales_q(dimensions=["product", _RLEVEL], **kw)
        bundle = _bundle(dev1847_models())
        with pytest.raises(ValueError, match=message):
            plan_query(query=query, bundle=bundle)


class TestUnderReaggregationPartitionKeys:
    def test_computed_dimension_key_keeps_its_row_role(self):
        """A computed dimension named in a re-aggregation's ``partition_by`` yields no
        combined twin for its own partitioned aggregate."""
        band = {"expression": "CASE WHEN sum(amount, partition_by=product) > 100 "
                              "THEN 'big' ELSE 'small' END", "name": "pband"}
        formula = "avg(sum(amount, partition_by=[city, region, pband]), partition_by=pband)"
        ds = _discover(sales_q(dimensions=[band],
                               measures=[ModelMeasure(formula=formula, name="r")]),
                       _bundle(dev1847_models()))
        assert [(d.phase, d.routing) for d in ds] == [
            ("row", "local_producer"), ("combined", "reaggregation"),
        ]
        assert ds[1].consumer_public_names == ("r",)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestTransformOverReaggregationDimension:
    async def test_filter_on_it_executes(self, exec_engine):
        """Typing and discovery share one computed-dimension transform-root definition,
        so a filter on ``cumsum(<re-aggregation>)`` compiles and keeps its rows."""
        resp = await exec_engine.execute(monthly_q(
            dimensions=["region", {"expression": f"cumsum({REAGG_STANDALONE})", "name": "rr"}],
            time_dimensions=monthly_month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=["rr > 10"]))
        mcol = next(c for c in resp.columns if "ordered_at" in c)
        got = {(r["monthly.region"], month_key(r[mcol])): r["monthly.rr"] for r in resp.data}
        assert got == {("North", "2024-02"): 20.0, ("North", "2024-03"): 30.0}
