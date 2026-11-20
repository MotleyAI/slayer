"""Every aggregation associates: ranked and windowed association, the one mode gate for local first/last and windows,
and the fanning-time-axis precedence (spec: queries/attribution-modes › Ranked and windowed association, Local
first/last and windowed aggregates follow the mode axis; queries/cross-model-aggregates; queries/semantics).
Executed on SQLite and DuckDB."""

from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

from slayer.core.errors import PartitionKeyError, SlayerError, TimeAxisError, UnsafeRelationshipInputError
from tests._dev1892_fixtures import assert_ref_free
from tests._dev1914_fixtures import (
    FIRST_BY_STATUS,
    LAST_BY_STATUS,
    LOCAL_LAST_BROADCAST,
    LOCAL_LAST_BY_STATUS,
    LOCAL_WINDOW_OK_JAN_FAN_DEFECT,
    OK,
    ORDERED_MONTH,
    PARTITIONED_WINDOW_BY_TIER_MONTH,
    PARTITIONED_WINDOW_LAST_BY_TIER_MONTH,
    RANKED_FIRST_BY_KIND,
    RANKED_LAST_BY_KIND,
    REAGG_LAST_BY_STATUS,
    SAME_ROW_C8_BUCKET,
    SAME_ROW_SPLIT_DEFECT,
    SIGNUP_MONTH,
    VISIT_MONTH,
    VISITS_PLAIN_COUNT,
    VISITS_PLAIN_SUM,
    VISITS_WINDOW_COUNT,
    VISITS_WINDOW_SUM,
    WINDOW_BROADCAST_BY_SIGNUP,
    WINDOW_BY_ORDER_MONTH,
    WINDOW_BY_STATUS_SIGNUP,
    WINDOW_COUNT_BY_ORDER_MONTH,
    WINDOW_LAST_BY_ORDER_MONTH,
    WINDOW_OK_BY_ORDER_MONTH,
    associated_dimensions,
    associated_warnings,
    broadcast_warnings,
    by_month,
    by_status,
    by_status_month,
    dev1900_datasets,
    make_exec_engine,
    make_list_axis_engine,
    make_ranked_fanout_engine,
    make_same_row_engine,
    metric,
    num,
    query,
)

MODES = [pytest.param(None, id="default"), pytest.param("associate", id="associate")]
OUTSIDE = ["broadcast", "error"]
ALL_MODES = [None, "broadcast", "associate", "error"]

LAST = metric("last(customers.spend, customers.signup_at)", "l")
FIRST = metric("first(customers.spend, customers.signup_at)", "f")
AMT = metric("sum(orders.amount)", "amt")
WIN = metric("sum(customers.spend, window='1y')", "w")
WIN_COUNT = metric("count(customers.*, window='1y')", "n")
#: The window's time-axis spellings: population-rooted (customers) and cross-model (orders).
SPELLINGS = ["customers", "orders"]
ASSOCIATION_REMEDY = re.compile(r"to_many_handling|associat", re.IGNORECASE)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def same_row_engine(request):
    async for e in make_same_row_engine(request):
        yield e


@pytest.fixture(params=[("sqlite", True), ("duckdb", True), ("sqlite", False), ("duckdb", False)],
                ids=["sqlite-keyed", "duckdb-keyed", "sqlite-keyless", "duckdb-keyless"])
async def fanout(request):
    dialect, keyed = request.param
    async for e in make_ranked_fanout_engine(dialect=dialect, keyed=keyed):
        yield e


#: last(customers.spend) by signup per (status, order month) over the customers with such an order.
IMPLICIT_LAST_BY_STATUS_ORDER_MONTH = {
    ("ok", "2024-01"): 100.0, ("new", "2024-01"): 100.0, ("ok", "2024-02"): 150.0, ("new", "2024-02"): 150.0,
    ("ok", "2024-03"): 60.0, ("new", "2024-03"): 40.0, ("ok", "2024-04"): 30.0,
}


def signup_default_datasets():
    datasets = dev1900_datasets()
    for ds in datasets:
        if ds.name == "customers":
            ds.default_time_dimension = "signup_at"
    return datasets


@pytest.fixture(params=["sqlite", "duckdb"])
async def signup_default_engine(request):
    async for e in make_exec_engine(SimpleNamespace(param=request.param), datasets=signup_default_datasets()):
        yield e


def _ordered(population: str, mode=None, **kw):
    return query(mode, population=population, time_dimensions=ORDERED_MONTH, **kw)


def by_month_key(row) -> str | None:
    value = row["orders.ordered_at"]
    return None if value is None else str(value)[:7]


def by_tier_month(resp, value: str) -> dict:
    return {(r["customers.tier"], by_month_key(r)): num(r[value]) for r in resp.data}


# --------------------------------------------------------------------------- #
# Ranked association
# --------------------------------------------------------------------------- #
class TestRankedAssociation:
    @pytest.mark.parametrize("mode", MODES)
    async def test_cross_model_first_last_per_cell(self, engine, mode) -> None:
        resp = await engine.execute(query(mode, population="orders", dimensions=["orders.status"],
                                          metrics=[LAST, FIRST]))
        assert by_status(resp, "l") == LAST_BY_STATUS
        assert by_status(resp, "f") == FIRST_BY_STATUS
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_unequal_fanout_never_changes_the_pick(self, fanout, mode) -> None:
        resp = await fanout.execute(query(mode, population="events", dimensions=["events.kind"],
                                          metrics=[metric("last(accounts.score, accounts.ranked_at)", "l"),
                                                   metric("first(accounts.score, accounts.ranked_at)", "f")]))
        assert {r["events.kind"]: num(r["l"]) for r in resp.data} == RANKED_LAST_BY_KIND
        assert {r["events.kind"]: num(r["f"]) for r in resp.data} == RANKED_FIRST_BY_KIND

    @pytest.mark.parametrize("mode", MODES)
    async def test_explicit_partition_key_associates(self, engine, mode) -> None:
        part = metric("last(customers.spend, customers.signup_at, partition_by=orders.status)", "lp")
        resp = await engine.execute(query(mode, population="orders", dimensions=["orders.status"], metrics=[part]))
        assert by_status(resp, "lp") == LAST_BY_STATUS
        assert not associated_warnings(resp)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", OUTSIDE)
    async def test_explicit_partition_key_refused_outside_association(self, engine, mode) -> None:
        part = metric("last(customers.spend, customers.signup_at, partition_by=orders.status)", "lp")
        q = query(mode, population="orders", dimensions=["orders.status"], metrics=[part])
        with pytest.raises(PartitionKeyError):
            await engine.execute(q)

    @pytest.mark.parametrize("mode", MODES)
    async def test_ranked_reaggregation_inner_associates(self, engine, mode) -> None:
        nested = metric("sum(last(customers.spend, customers.signup_at))", "sl")
        resp = await engine.execute(query(mode, population="orders", dimensions=["orders.status"], metrics=[nested]))
        assert by_status(resp, "sl") == REAGG_LAST_BY_STATUS
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_implicit_key_never_ranks_by_an_association_dimension(self, signup_default_engine, mode) -> None:
        """The fanning order month associates; the implicit key is the home's default (signup), never that month."""
        resp = await signup_default_engine.execute(query(
            mode, population="orders", dimensions=["orders.status"], time_dimensions=ORDERED_MONTH,
            metrics=[metric("last(customers.spend)", "l")]))
        got = by_status_month(resp, "l", td="orders.ordered_at")
        assert {k: v for k, v in got.items() if k[1] is not None} == IMPLICIT_LAST_BY_STATUS_ORDER_MONTH

    async def test_ranked_reaggregation_inner_outside_association(self, engine) -> None:
        nested = metric("sum(last(customers.spend, customers.signup_at))", "sl")
        resp = await engine.execute(query("broadcast", population="orders", dimensions=["orders.status"],
                                          metrics=[nested]))
        assert len(set(by_status(resp, "sl").values())) == 1
        assert broadcast_warnings(resp)
        refused = query("error", population="orders", dimensions=["orders.status"], metrics=[nested])
        with pytest.raises(SlayerError):
            await engine.execute(refused)


# --------------------------------------------------------------------------- #
# Windowed association
# --------------------------------------------------------------------------- #
class TestWindowedAssociation:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("population", SPELLINGS)
    async def test_window_over_a_fanning_time_axis(self, engine, population, mode) -> None:
        resp = await engine.execute(_ordered(population, mode, metrics=[WIN]))
        expected: dict[str | None, float | None] = {**WINDOW_BY_ORDER_MONTH}
        if population == "customers":
            expected[None] = None  # c7, no orders
        assert by_month(resp, "w") == expected
        assert any("ordered_at" in d for d in associated_dimensions(resp))
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("population", SPELLINGS)
    async def test_windowed_count_counts_distinct_entities_in_the_frame(self, engine, population) -> None:
        resp = await engine.execute(_ordered(population, metrics=[WIN_COUNT]))
        expected: dict[str | None, float] = {**WINDOW_COUNT_BY_ORDER_MONTH}
        if population == "customers":
            expected[None] = 0.0
        assert by_month(resp, "n") == expected

    @pytest.mark.parametrize("population", SPELLINGS)
    async def test_axis_filter_binds_to_the_in_frame_related_row(self, same_row_engine, population) -> None:
        resp = await same_row_engine.execute(_ordered(population, metrics=[WIN], filter=[OK]))
        got = by_month(resp, "w")
        assert got == {**SAME_ROW_C8_BUCKET, **WINDOW_OK_BY_ORDER_MONTH}
        assert all(got[k] != v for k, v in SAME_ROW_SPLIT_DEFECT.items())

    @pytest.mark.parametrize("mode", MODES)
    async def test_window_over_an_attributable_axis_associates_the_other_dimensions(self, engine, mode) -> None:
        resp = await engine.execute(query(mode, population="customers", dimensions=["orders.status"],
                                          time_dimensions=SIGNUP_MONTH, metrics=[WIN]))
        cells = by_status_month(resp, "w")
        assert cells == WINDOW_BY_STATUS_SIGNUP
        assert cells[("ok", "2024-01")] != LOCAL_WINDOW_OK_JAN_FAN_DEFECT
        assert any("status" in d for d in associated_dimensions(resp))

    @pytest.mark.parametrize("mode", MODES)
    async def test_windowed_last_picks_among_the_in_frame_entities(self, engine, mode) -> None:
        resp = await engine.execute(_ordered("customers", mode, metrics=[
            metric("last(customers.spend, customers.signup_at, window='1y')", "lw")]))
        got = by_month(resp, "lw")
        assert {k: got[k] for k in WINDOW_LAST_BY_ORDER_MONTH} == WINDOW_LAST_BY_ORDER_MONTH

    @pytest.mark.parametrize("mode", MODES)
    async def test_partitioned_window_over_a_fanning_axis_associates(self, engine, mode) -> None:
        resp = await engine.execute(_ordered("customers", mode, dimensions=["customers.tier"], metrics=[
            metric("sum(customers.spend, window='1y', partition_by=customers.tier)", "w"),
            metric("last(customers.spend, customers.signup_at, window='1y', partition_by=customers.tier)", "lw")]))
        assert by_tier_month(resp, "w") == PARTITIONED_WINDOW_BY_TIER_MONTH
        last = by_tier_month(resp, "lw")
        assert {k: last[k] for k in PARTITIONED_WINDOW_LAST_BY_TIER_MONTH} == PARTITIONED_WINDOW_LAST_BY_TIER_MONTH

    @pytest.mark.parametrize("mode", OUTSIDE)
    async def test_partitioned_window_over_a_fanning_axis_refused_outside_association(self, engine, mode) -> None:
        q = _ordered("customers", mode, dimensions=["customers.tier"],
                     metrics=[metric("sum(customers.spend, window='1y', partition_by=customers.tier)", "w")])
        with pytest.raises(TimeAxisError, match=ASSOCIATION_REMEDY):
            await engine.execute(q)

    @pytest.mark.parametrize("mode", ALL_MODES)
    async def test_implicit_fanning_ranking_key_fails_closed(self, engine, mode) -> None:
        q = _ordered("customers", mode, metrics=[metric("last(customers.spend, window='1y')", "lw")])
        with pytest.raises(UnsafeRelationshipInputError, match="ordered_at"):
            await engine.execute(q)


# --------------------------------------------------------------------------- #
# Empty frames take each aggregation's empty value
# --------------------------------------------------------------------------- #
#: case → (window, FILTER clause, bucket): the NULL bucket, a month whose 7-day frame is empty (Feb), and a month
#: whose only in-frame entity (c1, gold) the aggregate's filter removes (Jan).
EMPTY_FRAMES = {
    "null-bucket": ("1y", "", None),
    "empty-month": ("7d", "", "2024-02"),
    "filter-removed": ("1y", " FILTER (WHERE customers.tier = 'silver')", "2024-01"),
}
EMPTY_VALUES = {"count(customers.*": 0.0, "count(customers.spend": 0.0, "sum(customers.spend": None,
                "wgood(customers.spend": None}


@pytest.fixture(params=["duckdb"])
async def duckdb_engine(request):
    async for e in make_exec_engine(request):
        yield e


class TestEmptyFrames:
    @pytest.mark.parametrize("case", EMPTY_FRAMES)
    async def test_each_aggregation_takes_its_empty_value(self, engine, case) -> None:
        window, clause, bucket = EMPTY_FRAMES[case]
        metrics = [metric(f"{head}, window='{window}'){clause}", f"m{i}") for i, head in enumerate(EMPTY_VALUES)]
        resp = await engine.execute(_ordered("customers", metrics=metrics))
        row = next(r for r in resp.data if by_month_key(r) == bucket)
        assert [num(row[f"m{i}"]) for i in range(len(EMPTY_VALUES))] == list(EMPTY_VALUES.values())

    @pytest.mark.parametrize("case", EMPTY_FRAMES)
    async def test_collect_takes_the_empty_list(self, duckdb_engine, case) -> None:
        window, clause, bucket = EMPTY_FRAMES[case]
        resp = await duckdb_engine.execute(_ordered("customers", metrics=[
            metric(f"collect(customers.tier, window='{window}'){clause}", "c")]))
        row = next(r for r in resp.data if by_month_key(r) == bucket)
        assert list(row["c"]) == []


# --------------------------------------------------------------------------- #
# A list child as the time axis: each home row once per bucket, however many of its elements fall in it
# --------------------------------------------------------------------------- #
@pytest.fixture(params=[True, False], ids=["keyed", "keyless"])
async def list_axis(request):
    async for e in make_list_axis_engine(keyed=request.param):
        yield e


class TestListChildTimeAxis:
    @pytest.mark.parametrize("mode", MODES)
    async def test_each_member_counts_once_per_bucket(self, list_axis, mode) -> None:
        resp = await list_axis.execute(query(mode, population="members", time_dimensions=VISIT_MONTH, metrics=[
            metric("count(members.*)", "n"), metric("sum(members.score)", "s"),
            metric("count(members.*, window='1y')", "wn"), metric("sum(members.score, window='1y')", "ws")]))
        td = "members.visits.value"
        assert by_month(resp, "n", td=td) == VISITS_PLAIN_COUNT
        assert by_month(resp, "s", td=td) == VISITS_PLAIN_SUM
        assert by_month(resp, "wn", td=td) == VISITS_WINDOW_COUNT
        assert by_month(resp, "ws", td=td) == VISITS_WINDOW_SUM


# --------------------------------------------------------------------------- #
# A fanning time axis outside association
# --------------------------------------------------------------------------- #
class TestFanningAxisPrecedence:
    @pytest.mark.parametrize("filtered", [False, True], ids=["unfiltered", "filtered"])
    @pytest.mark.parametrize("mode", OUTSIDE)
    @pytest.mark.parametrize("population", SPELLINGS)
    async def test_time_axis_error_names_association(self, engine, population, mode, filtered) -> None:
        q = _ordered(population, mode, metrics=[WIN], **({"filter": [OK]} if filtered else {}))
        with pytest.raises(TimeAxisError) as ei:
            await engine.execute(q)
        assert "ordered_at" in str(ei.value)
        assert ei.value.location == "metric 'w'"
        assert ASSOCIATION_REMEDY.search(ei.value.suggestion or "")
        assert_ref_free(str(ei.value))


# --------------------------------------------------------------------------- #
# Local first/last and windowed aggregates follow the mode axis
# --------------------------------------------------------------------------- #
class TestLocalModeAxis:
    def _last(self, mode):
        return query(mode, population="customers", dimensions=["orders.status"], metrics=[LAST])

    @pytest.mark.parametrize("mode", MODES)
    async def test_local_last_associates(self, engine, mode) -> None:
        resp = await engine.execute(self._last(mode))
        assert by_status(resp, "l") == LOCAL_LAST_BY_STATUS
        assert associated_warnings(resp)

    async def test_local_last_broadcasts(self, engine) -> None:
        resp = await engine.execute(self._last("broadcast"))
        assert by_status(resp, "l") == dict.fromkeys(LOCAL_LAST_BY_STATUS, LOCAL_LAST_BROADCAST)
        assert broadcast_warnings(resp)

    async def test_local_last_refused_in_error_mode(self, engine) -> None:
        q = self._last("error")
        with pytest.raises(SlayerError) as ei:
            await engine.execute(q)
        assert "status" in str(ei.value)
        assert "'l'" in str(ei.value) or "spend" in str(ei.value)

    def _window(self, mode):
        return query(mode, population="customers", dimensions=["orders.status"], time_dimensions=SIGNUP_MONTH,
                     metrics=[WIN])

    async def test_local_window_broadcasts_the_monthly_total(self, engine) -> None:
        resp = await engine.execute(self._window("broadcast"))
        cells = by_status_month(resp, "w")
        assert set(cells) == set(WINDOW_BY_STATUS_SIGNUP)
        assert all(v == WINDOW_BROADCAST_BY_SIGNUP[month] for (_, month), v in cells.items())
        assert broadcast_warnings(resp)

    async def test_local_window_refused_in_error_mode(self, engine) -> None:
        q = self._window("error")
        with pytest.raises(SlayerError):
            await engine.execute(q)


# --------------------------------------------------------------------------- #
# Every position reads the associated value; dice–slice
# --------------------------------------------------------------------------- #
class TestRankedPositions:
    async def test_measure_filter(self, engine) -> None:
        base = await engine.execute(query(population="orders", dimensions=["orders.status"], metrics=[AMT]))
        resp = await engine.execute(query(population="orders", dimensions=["orders.status"], metrics=[AMT, LAST],
                                          filter=["last(customers.spend, customers.signup_at) > 35"]))
        assert by_status(resp, "l") == {"new": LAST_BY_STATUS["new"]}
        assert by_status(resp, "amt") == {"new": by_status(base, "amt")["new"]}

    async def test_order_key(self, engine) -> None:
        """Ascending by the associated last (ok 30, new 40) — the reverse of the status order."""
        resp = await engine.execute(query(population="orders", dimensions=["orders.status"], metrics=[AMT],
                                          order=[{"field": "last(customers.spend, customers.signup_at)",
                                                  "direction": "asc"}]))
        assert [r["orders.status"] for r in resp.data] == ["ok", "new"]

    async def test_cardinality_neutral(self, engine) -> None:
        base = await engine.execute(query(population="orders", dimensions=["orders.status"], metrics=[AMT]))
        resp = await engine.execute(query(population="orders", dimensions=["orders.status"], metrics=[AMT, LAST]))
        assert len(resp.data) == len(base.data)
        assert by_status(resp, "amt") == by_status(base, "amt")

    @pytest.mark.parametrize("mode", MODES)
    async def test_dice_slice(self, engine, mode) -> None:
        sliced = await engine.execute(query(mode, population="orders", metrics=[LAST], filter=[OK]))
        grouped = await engine.execute(query(mode, population="orders", dimensions=["orders.status"],
                                             metrics=[LAST]))
        assert num(sliced.data[0]["l"]) == by_status(grouped, "l")["ok"] == LAST_BY_STATUS["ok"]


#: Orders-rooted window by status × signup month: the associated cells (the orphan order's NULL signup aside).
ORDERS_WINDOW_BY_STATUS_SIGNUP = {k: v for k, v in WINDOW_BY_STATUS_SIGNUP.items() if k[0] is not None}


class TestWindowedPositions:
    def _by_signup(self, *metrics, **kw):
        return query(population="orders", dimensions=["orders.status"], time_dimensions=SIGNUP_MONTH,
                     metrics=list(metrics), **kw)

    @staticmethod
    def _known(cells: dict) -> dict:
        return {k: v for k, v in cells.items() if k[1] is not None}

    async def test_measure(self, engine) -> None:
        resp = await engine.execute(self._by_signup(WIN))
        assert self._known(by_status_month(resp, "w")) == ORDERS_WINDOW_BY_STATUS_SIGNUP

    @pytest.mark.parametrize("mode", MODES)
    async def test_explicit_partition_key(self, engine, mode) -> None:
        part = metric("sum(customers.spend, window='1y', partition_by=orders.status)", "wp")
        resp = await engine.execute(self._by_signup(part, to_many_handling=mode))
        assert self._known(by_status_month(resp, "wp")) == ORDERS_WINDOW_BY_STATUS_SIGNUP
        assert not associated_warnings(resp)

    async def test_measure_filter(self, engine) -> None:
        resp = await engine.execute(self._by_signup(WIN, filter=["sum(customers.spend, window='1y') > 260"]))
        expected = {k: v for k, v in ORDERS_WINDOW_BY_STATUS_SIGNUP.items() if v > 260}
        assert by_status_month(resp, "w") == expected

    async def test_order_key(self, engine) -> None:
        """Ascending by the associated windowed last (Jan 100, Feb 150, Mar 40, Apr 30) — not month order."""
        resp = await engine.execute(_ordered("orders", metrics=[AMT], order=[
            {"field": "last(customers.spend, customers.signup_at, window='1y')", "direction": "asc"}]))
        assert [by_month_key(r) for r in resp.data] == ["2024-04", "2024-03", "2024-01", "2024-02"]

    async def test_cardinality_neutral(self, engine) -> None:
        base = await engine.execute(_ordered("orders", metrics=[AMT]))
        resp = await engine.execute(_ordered("orders", metrics=[AMT, WIN]))
        assert by_month(resp, "amt") == by_month(base, "amt")

    @pytest.mark.parametrize("mode", MODES)
    async def test_dice_slice(self, engine, mode) -> None:
        sliced = await engine.execute(_ordered("orders", mode, metrics=[WIN], filter=[OK]))
        grouped = await engine.execute(_ordered("orders", mode, dimensions=["orders.status"], metrics=[WIN]))
        ok_cells = {month: v for (status, month), v in by_status_month(grouped, "w", td="orders.ordered_at").items()
                    if status == "ok"}
        assert by_month(sliced, "w") == ok_cells == WINDOW_OK_BY_ORDER_MONTH
