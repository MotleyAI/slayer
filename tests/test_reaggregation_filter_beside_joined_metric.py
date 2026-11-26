"""A re-aggregation in a filter beside a metric that reads across a relationship.

Oracle per case: rows == the ``count(orders.*)``-only control's rows; sibling values ==
the same query without the predicate under test (every other filter kept) == hand values
(``tests/_reagg_filter_fixtures.py``)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest

from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.ir.planned import MaskTyping

from tests._dev1739_fixtures import month_key, month_td
from tests._reagg_filter_fixtures import make_exec_engine, reagg_filter_bundle

INNER = "sum(orders.amount, partition_by=[orders.region, orders.city])"
REGION_TOTAL = "sum(orders.amount, partition_by=orders.region)"
SPEND = QueryMetric(metric="sum(customers.spend)", alias="m0")
COUNT = QueryMetric(metric="count(orders.*)", alias="m0")
REGION = ["orders.region"]
REGION_CITY = ["orders.region", "orders.city"]
MONTH = "orders.ordered_at"

Key = Tuple[Any, ...]


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _q(
    *, metrics: Sequence[QueryMetric], filters: Sequence[str], dims: Sequence[Any] = REGION,
    time_dims: bool = False, population: bool = True,
) -> SlayerQuery:
    kw: Dict[str, Any] = {"dimensions": list(dims), "metrics": list(metrics), "filters": list(filters)}
    if time_dims:
        kw["time_dimensions"] = month_td()
    if population:
        kw["population"] = "orders"
    return SlayerQuery(**kw)


def _cols(dims: Sequence[Any], time_dims: bool) -> List[str]:
    return [d if isinstance(d, str) else d["alias"] for d in dims] + ([MONTH] if time_dims else [])


def _by_key(resp, cols: Sequence[str]) -> Dict[Key, Dict[str, Any]]:
    return {
        tuple(month_key(r[c]) if c == MONTH else r[c] for c in cols): r
        for r in resp.data
    }


def _same(actual: Any, expected: Any) -> bool:
    if expected is None or actual is None:
        return actual is expected
    return float(actual) == pytest.approx(expected)


async def _assert_masked(
    engine, *, metrics: Sequence[QueryMetric], predicate: str, hand: Dict[Key, Dict[str, Optional[float]]],
    other: Sequence[str] = (), dims: Sequence[Any] = REGION, time_dims: bool = False,
    population: bool = True, control_predicate: Optional[str] = None,
) -> None:
    shape = {"dims": dims, "time_dims": time_dims, "population": population}
    cols = _cols(dims, time_dims)
    got = _by_key(await engine.execute(_q(metrics=metrics, filters=[predicate, *other], **shape)), cols)
    control = _by_key(await engine.execute(
        _q(metrics=[COUNT], filters=[control_predicate or predicate, *other], **shape)), cols)
    oracle = _by_key(await engine.execute(_q(metrics=metrics, filters=other, **shape)), cols)
    assert set(got) == set(control) == set(hand)
    for key, values in hand.items():
        for alias, expected in values.items():
            assert _same(got[key][alias], oracle[key][alias]), (key, alias)
            assert _same(got[key][alias], expected), (key, alias)


def _typings(query: SlayerQuery) -> List[Tuple[MaskTyping, int]]:
    env = elaborate_query(query=query, bundle=reagg_filter_bundle())
    return [(t.typing, t.stratum) for t in env.filter_typings]


class TestReaggregationFilterBesideJoinedMetric:
    async def test_target_side_sibling(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=f"max({INNER}) > 45",
            hand={("South",): {"m0": 50.0}, (None,): {"m0": 100.0}},
        )

    async def test_child_sibling(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[QueryMetric(metric="sum(items.qty)", alias="m0")],
            predicate=f"max({INNER}) > 45",
            hand={("South",): {"m0": 8.0}, (None,): {"m0": 9.0}},
        )

    async def test_degenerate_reaggregation_at_query_grain(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate="max(sum(orders.amount)) > 35", dims=REGION_CITY,
            hand={
                ("North", "CityB"): {"m0": 200.0},
                ("South", "CityC"): {"m0": 50.0},
                (None, "CityD"): {"m0": 100.0},
            },
        )

    async def test_filter_names_declared_reaggregation_metric(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine,
            metrics=[QueryMetric(metric=f"max({INNER})", alias="m0"),
                     QueryMetric(metric="sum(customers.spend)", alias="m1")],
            predicate="m0 > 45", control_predicate=f"max({INNER}) > 45",
            hand={("South",): {"m0": 50.0, "m1": 50.0}, (None,): {"m0": 60.0, "m1": 100.0}},
        )

    async def test_no_explicit_population(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=f"max({INNER}) > 45", population=False,
            hand={("South",): {"m0": 50.0}, (None,): {"m0": 100.0}},
        )

    async def test_partitioned_outer_reaggregation(self, exec_engine) -> None:
        predicate = f"max({INNER}, partition_by=orders.region) > 45"
        assert _typings(_q(metrics=[SPEND], filters=[predicate], dims=REGION_CITY)) == [(MaskTyping.METRIC, 1)]
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=predicate, dims=REGION_CITY,
            hand={("South", "CityC"): {"m0": 50.0}, (None, "CityD"): {"m0": 100.0}},
        )

    async def test_beside_attached_field_filter(self, exec_engine) -> None:
        dims = [*REGION_CITY, {"field": REGION_TOTAL, "alias": "rt"}]
        attached = f"{REGION_TOTAL} > 55"
        predicate = f"max({INNER}) > 35"
        typings = _typings(_q(metrics=[SPEND], filters=[attached, predicate], dims=dims))
        assert typings[0][0] == MaskTyping.FIELD and typings[0][1] > 0
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=predicate, other=[attached], dims=dims,
            hand={("North", "CityB", 100.0): {"m0": 200.0}, (None, "CityD", 60.0): {"m0": 100.0}},
        )

    async def test_reaggregation_of_target_side_measure(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[SPEND],
            predicate="max(sum(customers.spend, partition_by=[orders.region, orders.city])) > 150",
            hand={("North",): {"m0": 300.0}},
        )

    async def test_month_time_dimension(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=f"max({INNER}) > 25", dims=[], time_dims=True,
            hand={("2024-01",): {"m0": 150.0}, ("2024-02",): {"m0": 200.0}, ("2024-03",): {"m0": 150.0}},
        )

    async def test_month_time_dimension_beside_time_shift(self, exec_engine) -> None:
        await _assert_masked(
            exec_engine, metrics=[QueryMetric(metric="time_shift(sum(customers.spend), -1)", alias="m0")],
            predicate=f"max({INNER}) > 25", dims=[], time_dims=True,
            hand={("2024-01",): {"m0": None}, ("2024-02",): {"m0": 150.0}, ("2024-03",): {"m0": 200.0}},
        )

    async def test_row_filter_reaches_joined_sibling(self, exec_engine) -> None:
        predicate = f"max({INNER}) > 35"
        row_filter = "orders.customer_id <> 1"
        assert _typings(_q(metrics=[SPEND], filters=[predicate, row_filter])) == [
            (MaskTyping.METRIC, 1), (MaskTyping.FIELD, 0),
        ]
        unfiltered = _by_key(await exec_engine.execute(_q(metrics=[SPEND], filters=[])), REGION)
        assert float(unfiltered[("North",)]["m0"]) == pytest.approx(300.0)
        await _assert_masked(
            exec_engine, metrics=[SPEND], predicate=predicate, other=[row_filter],
            hand={("North",): {"m0": 200.0}, ("South",): {"m0": 50.0}},
        )


class TestCountOnlyControls:
    @pytest.mark.parametrize(("dims", "predicate", "expected"), [
        (REGION, f"max({INNER}) > 45", {("South",): 2, (None,): 1}),
        (REGION_CITY, "max(sum(orders.amount)) > 35",
         {("North", "CityB"): 1, ("South", "CityC"): 2, (None, "CityD"): 1}),
        (REGION, "max(sum(customers.spend, partition_by=[orders.region, orders.city])) > 150", {("North",): 4}),
    ])
    async def test_control_rows(self, exec_engine, dims, predicate, expected) -> None:
        resp = await exec_engine.execute(_q(metrics=[COUNT], filters=[predicate], dims=dims))
        assert {k: r["m0"] for k, r in _by_key(resp, dims).items()} == expected
