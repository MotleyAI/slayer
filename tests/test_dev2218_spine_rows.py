"""Spine rows follow the row supply of the other dimensions; spine bounds restrict homes like field filters
(spec: queries/time-spine). Scenarios unchanged by this change stay in ``test_dev2015_time_spine.py``."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnrelatedDatasetError, WindowDurationError
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.population.row_supply import stored_rows
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev2015_fixtures import (
    BACKENDS,
    CUSTOMERS,
    JAN_JUN,
    JAN_MAR,
    MONTHS,
    ORDERS_ROWS,
    TWO_FACTS,
    bucket_key,
    by_bucket,
    m,
    num,
    spine_datasets,
    spine_engine,
    spine_query,
    spine_tables,
    value,
)
from tests._dev2212_fixtures import DS, run, seed_datasets, seed_engine

FEB_MAR = ["2025-02-01", "2025-03-31"]
MAR_JUN = ["2025-03-01", "2025-06-30"]
PER_GROUP = [m("sum(orders.amount)", "o"), m("sum(returns.amount)", "r")]
#: The inferred per-group rows over Jan–Mar: region E has no fact in range.
PER_GROUP_ROWS = {
    ("N", "2025-01"): (100.0, None), ("N", "2025-02"): (70.0, 20.0), ("N", "2025-03"): (None, None),
    ("S", "2025-01"): (50.0, None), ("S", "2025-02"): (None, None), ("S", "2025-03"): (None, 10.0),
}
E_ROWS = {("E", k): (None, None) for k in MONTHS[:3]}


@pytest.fixture(params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncIterator[SlayerQueryEngine]:
    async with spine_engine(request.param) as eng:
        yield eng


def _per_group(*, metrics: list[Any] = PER_GROUP, date_range: Any = JAN_MAR, **extra: Any) -> SlayerQuery:
    return spine_query(metrics=metrics, date_range=date_range, dimensions=["customers.region"], **extra)


# ---------------------------------------------------------------------------
# A spine query's population is the spine times P
# ---------------------------------------------------------------------------

class TestPopulation:
    async def test_per_group_rows_come_from_the_row_supply(self, engine) -> None:
        resp = await engine.execute(_per_group())
        assert by_bucket(resp, ["o", "r"], by=["region"]) == PER_GROUP_ROWS
        assert resp.population == "time_spine × customers"
        assert resp.population_inferred is True
        assert resp.row_sources == ["orders", "returns"]

    async def test_a_named_p_keeps_the_grid(self, engine) -> None:
        resp = await engine.execute(_per_group(population="customers"))
        assert by_bucket(resp, ["o", "r"], by=["region"]) == {**PER_GROUP_ROWS, **E_ROWS}
        assert resp.population == "time_spine × customers"
        assert resp.population_inferred is False

    async def test_an_explicit_spine_equals_its_inferred_twin(self, engine) -> None:
        inferred = await engine.execute(spine_query(metrics=TWO_FACTS))
        named = await engine.execute(spine_query(metrics=TWO_FACTS, population="time_spine"))
        assert by_bucket(named, ["o", "r", "n"]) == by_bucket(inferred, ["o", "r", "n"])

    async def test_adding_a_measure_never_removes_a_row_or_changes_a_value(self, engine) -> None:
        more = await engine.execute(_per_group(metrics=[*PER_GROUP, m("avg(orders.amount)", "a")]))
        avg = {("N", "2025-01"): 100.0, ("N", "2025-02"): 70.0, ("S", "2025-01"): 50.0}
        assert by_bucket(more, ["o", "r", "a"], by=["region"]) == {
            k: (*v, avg.get(k)) for k, v in PER_GROUP_ROWS.items()}

    async def test_removing_a_measure_keeps_a_named_ps_rows(self, engine) -> None:
        both = await engine.execute(_per_group(population="customers"))
        one = await engine.execute(_per_group(population="customers", metrics=PER_GROUP[:1]))
        assert set(by_bucket(one, ["o"], by=["region"])) == set(by_bucket(both, ["o"], by=["region"]))

    async def test_a_sibling_grows_rows_where_the_measure_has_none(self, engine) -> None:
        alone = await engine.execute(_per_group(metrics=PER_GROUP[:1], date_range=MAR_JUN))
        both = await engine.execute(_per_group(date_range=MAR_JUN))
        assert alone.data == []
        got = by_bucket(both, ["o", "r"], by=["region"])
        assert set(got) == {(g, k) for g in "NS" for k in MONTHS[2:]}
        assert all(v[0] is None for v in got.values())
        assert got[("S", "2025-03")] == (None, 10.0)
        assert got[("N", "2025-05")] == (None, 5.0)

    async def test_tuples_come_from_rows_inside_the_frame(self, engine) -> None:
        inferred = await engine.execute(_per_group(metrics=PER_GROUP[:1], date_range=FEB_MAR))
        named = await engine.execute(_per_group(metrics=PER_GROUP[:1], date_range=FEB_MAR, population="customers"))
        assert by_bucket(inferred, ["o"], by=["region"]) == {("N", "2025-02"): (70.0,), ("N", "2025-03"): (None,)}
        assert set(by_bucket(named, ["o"], by=["region"])) == {(g, k) for g in "NSE" for k in MONTHS[1:3]}

    async def test_a_windowed_measure_supplies_the_groups_its_window_reaches(self, engine) -> None:
        resp = await engine.execute(_per_group(metrics=[m("sum(orders.amount, window='2m')", "w")],
                                               date_range=FEB_MAR))
        assert by_bucket(resp, ["w"], by=["region"]) == {
            ("N", "2025-02"): (170.0,), ("N", "2025-03"): (70.0,),
            ("S", "2025-02"): (50.0,), ("S", "2025-03"): (None,),
        }

    async def test_a_dimension_only_spine_query_over_an_axis_bearing_anchor(self, engine) -> None:
        inferred = await engine.execute(spine_query(metrics=[], date_range=FEB_MAR, dimensions=["orders.id"]))
        named = await engine.execute(spine_query(metrics=[], date_range=FEB_MAR, dimensions=["orders.id"],
                                                 population="orders"))
        cells = lambda resp: sorted((value(r, "id"), bucket_key(r["time_spine.timestamp"])) for r in resp.data)  # noqa: E731
        assert cells(inferred) == [(3, "2025-02"), (3, "2025-03")]
        assert cells(named) == [(i, k) for i in (1, 2, 3) for k in MONTHS[1:3]]

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_dimension_only_anchor_with_a_to_one_axis_is_bounded(self, backend) -> None:
        async with spine_engine(backend, datasets=spine_datasets(signed_up_at=True)) as eng:
            resp = await eng.execute(spine_query(metrics=[], date_range=JAN_MAR, dimensions=["customers.region"]))
        # signups: N 2024-11-15, S 2025-01-05, E 2025-04-20 — only S lies inside the frame
        assert sorted((value(r, "region"), bucket_key(r["time_spine.timestamp"])) for r in resp.data) == [
            ("S", k) for k in MONTHS[:3]]

    async def test_no_other_dimensions_keeps_every_bucket(self, engine) -> None:
        resp = await engine.execute(spine_query(metrics=PER_GROUP[:1], date_range=["2025-04-01", "2025-06-30"]))
        assert by_bucket(resp, ["o"]) == {k: (None,) for k in MONTHS[3:]}
        assert resp.row_sources == ["unit"]

    async def test_other_dimensions_with_no_in_frame_tuple_have_no_rows(self, engine) -> None:
        resp = await engine.execute(_per_group(metrics=PER_GROUP[:1], date_range=["2025-04-01", "2025-06-30"]))
        assert resp.data == []


class TestPopulationFilters:
    async def test_a_filter_on_a_finer_dataset_keeps_p(self, engine) -> None:
        resp = await engine.execute(_per_group(filter=["orders.amount > 60"]))
        assert by_bucket(resp, ["o", "r"], by=["region"]) == {
            ("N", "2025-01"): (100.0, None), ("N", "2025-02"): (70.0, 20.0), ("N", "2025-03"): (None, None),
        }
        assert resp.population == "time_spine × customers"

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_an_orphan_beside_the_spine_lands_in_the_null_tuple(self, backend) -> None:
        tables = spine_tables(orders_rows=[*ORDERS_ROWS, (4, 99, "2025-02-15", 30.0)])
        async with spine_engine(backend, tables=tables) as eng:
            resp = await eng.execute(_per_group())
        assert by_bucket(resp, ["o", "r"], by=["region"]) == {
            **PER_GROUP_ROWS,
            (None, "2025-01"): (None, None), (None, "2025-02"): (30.0, None), (None, "2025-03"): (None, None),
        }
        assert resp.population == "time_spine × customers"

    async def test_a_filter_on_p_restricts_each_contributing_measure(self, engine) -> None:
        resp = await engine.execute(_per_group(filter=["customers.region = 'N'"]))
        assert by_bucket(resp, ["o", "r"], by=["region"]) == {k: v for k, v in PER_GROUP_ROWS.items() if k[0] == "N"}


# ---------------------------------------------------------------------------
# Supply bounds vs evaluation bounds: the first visible bucket, one producer form at a time
# ---------------------------------------------------------------------------

FORMS = {
    "windowed": ("sum(orders.amount, window='2m')",
                 {("N", "2025-02"): 170.0, ("N", "2025-03"): 70.0, ("S", "2025-02"): 50.0, ("S", "2025-03"): None}),
    "shifted": ("time_shift(sum(orders.amount), -1)",
                {("N", "2025-02"): 100.0, ("N", "2025-03"): 70.0, ("S", "2025-02"): 50.0, ("S", "2025-03"): None}),
    "shifted_keyword": ("time_shift(sum(orders.amount), periods=-1)",
                        {("N", "2025-02"): 100.0, ("N", "2025-03"): 70.0, ("S", "2025-02"): 50.0,
                         ("S", "2025-03"): None}),
    # evaluation bounds reach January: the shifted running total starts before the frame
    "series_shift": ("time_shift(cumsum(sum(orders.amount)), -1)",
                     {("N", "2025-02"): 100.0, ("N", "2025-03"): 170.0, ("S", "2025-02"): 50.0, ("S", "2025-03"): 50.0}),
    "nested_shift": ("time_shift(time_shift(cumsum(sum(orders.amount)), -1), -1)",
                     {("N", "2025-02"): None, ("N", "2025-03"): 100.0, ("S", "2025-02"): None,
                      ("S", "2025-03"): 50.0}),
    "nested": ("lag(cumsum(sum(orders.amount)))", {("N", "2025-02"): None, ("N", "2025-03"): 70.0}),
    "ranked": ("rank(sum(orders.amount), direction='desc')", {("N", "2025-02"): 1.0, ("N", "2025-03"): None}),
    "first": ("first(sum(orders.amount))", {("N", "2025-02"): 70.0, ("N", "2025-03"): 70.0}),
    "last": ("last(sum(orders.amount))", {("N", "2025-02"): None, ("N", "2025-03"): None}),
    "second_order": ("max(sum(orders.amount, partition_by=[customers.region, orders.order_date]))", None),
}


@pytest.mark.parametrize("form", FORMS)
async def test_a_producer_form_alone_supplies_its_in_frame_groups(engine, form: str) -> None:
    expr, expected = FORMS[form]
    inferred = await engine.execute(_per_group(metrics=[m(expr, "v")], date_range=FEB_MAR))
    named = await engine.execute(_per_group(metrics=[m(expr, "v")], date_range=FEB_MAR, population="customers"))
    got = {k: v[0] for k, v in by_bucket(inferred, ["v"], by=["region"]).items()}
    twin = {k: v[0] for k, v in by_bucket(named, ["v"], by=["region"]).items()}
    assert {k: twin[k] for k in got} == got
    if expected is None:
        assert {k[0] for k in got} == {"N"}
    else:
        assert got == expected


@pytest.mark.parametrize(("form", "expr", "variables"), [
    ("shifted", "time_shift(sum(orders.amount), {p})", {"p": -1}),
    ("windowed", "sum(orders.amount, window='{w}')", {"w": "2m"}),
])
async def test_a_variable_bound_form_supplies_the_groups_of_its_literal_twin(
    engine, form: str, expr: str, variables: dict,
) -> None:
    resp = await engine.execute(_per_group(metrics=[m(expr, "v")], date_range=FEB_MAR, variables=variables))
    assert {k: v[0] for k, v in by_bucket(resp, ["v"], by=["region"]).items()} == FORMS[form][1]


@pytest.mark.parametrize("layer", ["stage", "root", "runtime"])
async def test_a_sibling_stage_witness_reads_every_variable_layer(engine, layer: str) -> None:
    stage = _per_group(metrics=[m("time_shift(sum(orders.amount), {p})", "o")], date_range=FEB_MAR, name="monthly",
                       **({"variables": {"p": -1}} if layer == "stage" else {}))
    over = SlayerQuery.model_validate({"population": "monthly", "dimensions": ["monthly.region"],
                                       "metrics": [m("count(monthly.*)", "c")],
                                       **({"variables": {"p": -1}} if layer == "root" else {})})
    resp = await engine.execute([stage, over], **({"variables": {"p": -1}} if layer == "runtime" else {}))
    assert sorted(value(r, "region") for r in resp.data) == ["N", "S"]


def _rolling_datasets() -> list[SlayerDataset]:
    """The spine seed with ``orders.rolling = sum(amount, window='{w}')``."""
    rolling = DatasetMetric(name="rolling", expression="sum(amount, window='{w}')")
    return [d.model_copy(update={"metrics": [*d.metrics, rolling]}) if d.name == "orders" else d for d in spine_datasets()]


async def test_a_variable_bound_saved_metric_supplies_its_windowed_groups() -> None:
    async with spine_engine("duckdb", datasets=_rolling_datasets()) as eng:
        resp = await eng.execute(spine_query(metrics=[m("orders.rolling", "v")], date_range=FEB_MAR,
                                             dimensions=["orders.customer_id"], variables={"w": "2m"}))
    # customer 2's only order is in January: February's two-month window still reads it
    assert {(value(r, "customer_id"), bucket_key(value(r, "timestamp"))): num(value(r, "v")) for r in resp.data} == {
        (1, "2025-02"): 170.0, (1, "2025-03"): 70.0, (2, "2025-02"): 50.0, (2, "2025-03"): None}


@pytest.mark.xfail(strict=True, raises=WindowDurationError,
                   reason="DEV-2396: a cross-dataset producer binds no {var} into a saved metric")
async def test_a_saved_metric_variable_binds_in_a_cross_dataset_producer() -> None:
    query = {"dimensions": ["customers.region"], "time_dimensions": [{"field": "orders.order_date", "granularity": "month"}],
             "metrics": [m("orders.rolling", "v")], "variables": {"w": "2m"}}
    async with spine_engine("duckdb", datasets=_rolling_datasets()) as eng:
        rootless = await eng.execute(SlayerQuery.model_validate(query))
        rooted = await eng.execute(SlayerQuery.model_validate({**query, "population": "customers"}))
    assert {(value(r, "region"), str(value(r, "order_date"))[:7]): num(value(r, "v")) for r in rooted.data
            if value(r, "region") != "E"} == {
        (value(r, "region"), str(value(r, "order_date"))[:7]): num(value(r, "v")) for r in rootless.data}


# ---------------------------------------------------------------------------
# Transforms and windows see the dense series of their own tuples
# ---------------------------------------------------------------------------

async def test_per_group_running_totals_ignore_a_siblings_tuples(engine) -> None:
    cs = m("cumsum(sum(orders.amount))", "cs")
    alone = await engine.execute(_per_group(metrics=[cs], date_range=FEB_MAR))
    both = await engine.execute(_per_group(metrics=[cs, PER_GROUP[1]], date_range=FEB_MAR))
    assert by_bucket(alone, ["cs"], by=["region"]) == {("N", "2025-02"): (70.0,), ("N", "2025-03"): (70.0,)}
    assert by_bucket(both, ["cs"], by=["region"]) == {
        ("N", "2025-02"): (70.0,), ("N", "2025-03"): (70.0,), ("S", "2025-02"): (None,), ("S", "2025-03"): (None,)}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_anchor_supplied_measure_keeps_its_tuples_beside_a_coarse_one(backend: str) -> None:
    # customers 2 (S) and 3 (E) sign up in frame; only S has orders
    def query(*metrics: dict) -> SlayerQuery:
        return spine_query(metrics=list(metrics), date_range=JAN_JUN, dimensions=["customers.id", "customers.region"])

    rank, coarse = m("rank(customers.id, direction='asc')", "rk"), m("sum(orders.amount, partition_by=customers.region)", "o")
    async with spine_engine(backend, datasets=spine_datasets(signed_up_at=True)) as eng:
        alone = await eng.execute(query(rank))
        both = await eng.execute(query(rank, coarse))
    tuples = {(r["customers.id"], r["customers.region"]) for r in alone.data}
    assert tuples == {(2, "S"), (3, "E")}
    assert {(r["customers.id"], r["customers.region"]) for r in both.data} == tuples


@pytest.mark.parametrize("grain", ["customers.region", "[]"])
@pytest.mark.parametrize(("march_order", "expected"), [(False, set()), (True, {(4, "N")})], ids=["out", "in"])
async def test_a_coarse_measure_restricts_the_anchor_to_its_in_frame_cells(
    march_order: bool, expected: set, grain: str,
) -> None:
    # customer 4 (N) signs up in February; N's other order is in January, outside the frame
    customers = CUSTOMERS.model_copy(update={"rows": [*CUSTOMERS.rows, (4, "N", "2025-02-10")]})
    orders = [(1, 1, "2025-01-10", 100.0), (2, 2, "2025-01-20", 50.0), *([(3, 1, "2025-03-05", 70.0)] if march_order else [])]
    tables = [customers if t.name == "customers" else t for t in spine_tables(orders_rows=orders)]
    async with spine_engine("duckdb", datasets=spine_datasets(signed_up_at=True), tables=tables) as eng:
        resp = await eng.execute(spine_query(metrics=[m(f"sum(orders.amount, partition_by={grain})", "o")],
                                             date_range=FEB_MAR, dimensions=["customers.id", "customers.region"]))
    assert {(r["customers.id"], r["customers.region"]) for r in resp.data} == expected


async def test_a_filtered_out_anchor_row_never_frames_a_coarse_measures_tuple() -> None:
    # customer 4 (N) signs up in frame but fails the filter; customer 1 (N) passes it but signed up before the frame
    customers = CUSTOMERS.model_copy(update={"rows": [*CUSTOMERS.rows, (4, "N", "2025-02-10")]})
    tables = [customers if t.name == "customers" else t for t in spine_tables()]
    async with spine_engine("duckdb", datasets=spine_datasets(signed_up_at=True), tables=tables) as eng:
        resp = await eng.execute(spine_query(metrics=[m("sum(orders.amount, partition_by=[])", "o")],
                                             date_range=FEB_MAR, dimensions=["customers.region"],
                                             filter=["customers.id = 1"]))
    assert resp.data == []


# ---------------------------------------------------------------------------
# Spine stages, saved spine queries and population_inferred
# ---------------------------------------------------------------------------

def _monthly_stage(**extra: Any) -> SlayerQuery:
    return spine_query(metrics=[m("sum(orders.amount)", "o")], date_range=JAN_MAR, name="monthly", **extra)


def _over(population: str, **extra: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "population": population, "metrics": [m(f"sum({population}.o)", "t"), m(f"count({population}.*)", "c")],
        **extra,
    })


def _totals(resp: SlayerResponse, *, by: str = "region") -> dict:
    return {value(r, by): (num(value(r, "t")), num(value(r, "c"))) for r in resp.data}


class TestSpineStages:
    @pytest.mark.parametrize("extra", [{}, {"population": "time_spine"}], ids=["rootless", "explicit-spine"])
    async def test_a_spine_stage_keeps_every_bucket(self, engine, extra) -> None:
        resp = await engine.execute([_monthly_stage(**extra), _over("monthly")])
        assert [(num(value(r, "t")), num(value(r, "c"))) for r in resp.data] == [(220.0, 3.0)]

    async def test_a_per_group_spine_stage(self, engine) -> None:
        resp = await engine.execute([_monthly_stage(dimensions=["customers.region"]),
                                     _over("monthly", dimensions=["monthly.region"])])
        assert _totals(resp) == {"N": (170.0, 3.0), "S": (50.0, 3.0)}

    async def test_a_stage_pinned_with_the_marker_keeps_inferred_rows(self, engine) -> None:
        stage = _monthly_stage(dimensions=["customers.region"], population="customers", population_inferred=True)
        resp = await engine.execute([stage, _over("monthly", dimensions=["monthly.region"])])
        assert _totals(resp) == {"N": (170.0, 3.0), "S": (50.0, 3.0)}

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_a_spine_pinned_with_the_marker_infers_its_anchor(self, backend: str) -> None:
        def query(**extra: Any) -> SlayerQuery:
            return spine_query(metrics=[m("sum(orders.amount, partition_by=[])", "o")], date_range=JAN_MAR,
                               dimensions=["customers.region"], **extra)

        async with spine_engine(backend, datasets=spine_datasets(signed_up_at=True)) as eng:
            unpinned = await eng.execute(query())
            pinned = await eng.execute(query(population="time_spine", population_inferred=True))
        assert by_bucket(pinned, ["o"], by=["region"]) == by_bucket(unpinned, ["o"], by=["region"])
        assert {k[0] for k in by_bucket(pinned, ["o"], by=["region"])} == {"S"}

    async def test_a_saved_spine_pinned_stage_reads_its_direct_rows(self, engine: SlayerQueryEngine) -> None:
        stage = _monthly_stage(dimensions=["customers.region"], population="time_spine", population_inferred=True)
        over = _over("monthly", dimensions=["monthly.region"])
        direct = await engine.execute([stage, over])
        await engine.create_dataset_from_query(query=stage, name="monthly")
        saved = await engine.execute(over)
        assert _totals(saved) == _totals(direct) == {"N": (170.0, 3.0), "S": (50.0, 3.0)}

    async def test_a_saved_spine_pin_refuses_a_product_split_as_direct_execution_does(self) -> None:
        weather = SlayerDataset(name="weather", sql_table="weather", data_source=DS,
                                fields=[DatasetField(name="city", datatype=DataType.STRING, primary_key=True)])
        datasets = seed_datasets(extra=(weather,))
        query = SlayerQuery.model_validate({
            "population": "time_spine", "population_inferred": True, "dimensions": ["customers.region", "weather.city"],
            "time_dimensions": [{"field": "time_spine.timestamp", "granularity": "month", "date_range": JAN_MAR}],
            "metrics": [m("sum(orders.amount)", "o")]})
        with pytest.raises(UnrelatedDatasetError):
            stored_rows(query, datasets_by_name={d.name: d for d in datasets})
        async with seed_engine("duckdb", datasets=datasets) as eng:
            with pytest.raises(UnrelatedDatasetError):
                await eng.execute(query, dry_run=True)

    async def test_a_stage_naming_p_keeps_the_grid(self, engine) -> None:
        stage = _monthly_stage(dimensions=["customers.region"], population="customers")
        resp = await engine.execute([stage, _over("monthly", dimensions=["monthly.region"])])
        assert _totals(resp) == {"N": (170.0, 3.0), "S": (50.0, 3.0), "E": (None, 3.0)}

    async def test_a_spine_query_over_a_saved_query_reports_it(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query=SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.customer_id"],
            "time_dimensions": [{"field": "orders.order_date", "granularity": "day"}],
            "metrics": [m("sum(orders.amount)", "rev")],
        }), name="ob")
        resp = await engine.execute(spine_query(metrics=[m("sum(ob.rev)", "rev")], dimensions=["ob.customer_id"],
                                                date_range=FEB_MAR))
        # customer 2's only order lies before the frame
        assert by_bucket(resp, ["rev"], by=["customer_id"]) == {(1, "2025-02"): (70.0,), (1, "2025-03"): (None,)}
        assert resp.row_sources == ["ob"]


class TestSavedSpineQueries:
    async def test_a_saved_per_group_spine_query_keeps_inferred_rows(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query=_monthly_stage(dimensions=["customers.region"]),
                                               name="saved_monthly")
        stored = await engine.storage.get_dataset("saved_monthly", data_source="test")
        assert stored is not None
        assert stored.source_queries is not None
        assert [(s.population, s.population_inferred) for s in stored.source_queries] == [("customers", True)]
        direct = await engine.execute("saved_monthly")
        assert by_bucket(direct, ["o"], by=["region"]) == {k: v[:1] for k, v in PER_GROUP_ROWS.items()}
        over = await engine.execute(_over("saved_monthly", dimensions=["saved_monthly.region"]))
        assert _totals(over) == {"N": (170.0, 3.0), "S": (50.0, 3.0)}

    async def test_a_refined_saved_spine_query_keeps_inferred_rows(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(query=_monthly_stage(dimensions=["customers.region"]),
                                               name="saved_monthly")
        refined = await engine.execute("saved_monthly", refine={"metrics": [m("sum(returns.amount)", "r")]})
        assert by_bucket(refined, ["o", "r"], by=["region"]) == PER_GROUP_ROWS

    async def test_a_legacy_saved_spine_query_keeps_the_grid(self, engine: SlayerQueryEngine) -> None:
        await engine.storage.save_dataset(SlayerDataset(
            name="legacy_monthly", data_source="test",
            source_queries=[_monthly_stage(dimensions=["customers.region"], population="customers")]))
        over = await engine.execute(_over("legacy_monthly", dimensions=["legacy_monthly.region"]))
        assert _totals(over) == {"N": (170.0, 3.0), "S": (50.0, 3.0), "E": (None, 3.0)}


# ---------------------------------------------------------------------------
# Reporting over the Appendix B seed
# ---------------------------------------------------------------------------

SPINE_MONTH = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]}
R = "customers.region"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_spine_query_over_a_product_anchor_reports_its_parts(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"], time_dimensions=[SPINE_MONTH])
    # customers reach the spine by association across orders: NORTH has no in-frame order; tickets is unrestricted
    assert {(r[R], r["tickets.priority"]) for r in resp.data} == {
        (g, p) for g in ("EAST", "WEST") for p in ("high", "low")}
    assert resp.row_count == 2 * 2 * 3
    assert resp.population == "time_spine × customers × tickets"
    assert resp.row_sources == ["customers", "tickets"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_unrelated_home_beside_a_fact_reports_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], time_dimensions=[SPINE_MONTH],
                         metrics=["sum(orders.amount)", "tickets.ticket_count"])
    assert {r[R] for r in resp.data} == {"EAST", "WEST"}
    assert resp.row_sources == ["customers", "orders"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_inferred_spine_query_reports_its_supplying_homes(backend: str) -> None:
    async with seed_engine(backend) as eng:
        inferred = await run(eng, dimensions=[R], time_dimensions=[SPINE_MONTH], metrics=["sum(orders.amount)"])
        named = await run(eng, population="customers", dimensions=[R], time_dimensions=[SPINE_MONTH],
                          metrics=["sum(orders.amount)"])
    assert (inferred.population, inferred.population_inferred, inferred.row_sources) == (
        "time_spine × customers", True, ["orders"])
    assert (named.population, named.population_inferred, named.row_sources) == (
        "time_spine × customers", False, ["customers"])
    assert _regions_b(inferred) == {"EAST", "WEST"}
    assert _regions_b(named) == {"EAST", "WEST", "NORTH"}


def _regions_b(resp: SlayerResponse) -> set:
    return {r[R] for r in resp.data}
