"""Spine rows follow the row supply of the other dimensions; spine bounds restrict homes like field filters
(spec: queries/time-spine). Scenarios unchanged by this change stay in ``test_dev2015_time_spine.py``."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev2015_fixtures import (
    BACKENDS,
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
from tests._dev2212_fixtures import run, seed_engine

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
    # evaluation bounds reach January: the shifted running total starts before the frame
    "series_shift": ("time_shift(cumsum(sum(orders.amount)), -1)",
                     {("N", "2025-02"): 100.0, ("N", "2025-03"): 170.0, ("S", "2025-02"): 50.0, ("S", "2025-03"): 50.0}),
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
        assert stored is not None and stored.source_queries is not None
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
