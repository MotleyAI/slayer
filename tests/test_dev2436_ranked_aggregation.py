"""``first`` / ``last`` are always aggregations: over an aggregate they rank the operand's cells, and an
unwindowed ranked aggregation's grain drops its ranking key.

Spec: queries/semantics › first and last over an aggregate rank their operand's cells; › A ranked
aggregation's grain drops its ranking key; aggregations/functional-form. Snapshot fixture:
``tests/_dev2006_fixtures.py``; ``sum(balance)`` by (customer, month) is (100, Jan) 370, (100, Feb) 160,
(200, Jan) 70, (200, Feb) NULL.
"""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import AggregationFilterError, ReaggregationError, TimeAxisError, UnsafeRelationshipInputError
from slayer.core.models import Aggregation, DatasetMetric, SlayerDataset
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev2006_fixtures import (
    FEB,
    JAN,
    by_customer,
    by_customer_month,
    customers_dataset,
    m,
    make_exec_engine,
    month_td,
    num,
    seed_duckdb,
    seed_sqlite,
    snap_q,
    snapshots_dataset,
    warnings_of,
)
from tests._engine_helpers import seeded_exec_engine

CUST = "account_snapshots.customer_id"
DATE = "account_snapshots.snapshot_date"
BAL = "sum(account_snapshots.balance)"
LAST_BAL = f"last({BAL})"
#: The per-customer value every position reads.
CLOSE = {100: 160.0, 200: None}
#: ``last(balance)`` per (customer, month): each month's closing balance.
CLOSE_BY_MONTH = {(100, JAN): 70.0, (100, FEB): 160.0, (200, JAN): 40.0, (200, FEB): None}
MONTHS = (JAN, FEB)


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _monthly(*metrics: QueryMetric, **kw: Any) -> SlayerQuery:
    return snap_q(dimensions=[CUST], time_dimensions=month_td(), metrics=list(metrics), **kw)


def _per_customer(cells: dict) -> dict:
    """``{customer: value}`` from ``{(customer, month): value}``, asserting one value per customer."""
    out: dict = {}
    for (cust, _), v in cells.items():
        assert out.get(cust, v) == v, (cust, cells)
        out[cust] = v
    return out


def _broadcast(per_customer: dict) -> dict:
    """The per-customer value on every (customer, month) cell the fixture has."""
    return {(c, mo): per_customer[c] for c in per_customer for mo in MONTHS}


def _approx(got: dict, want: dict) -> None:
    assert set(got) == set(want), (sorted(got, key=repr), sorted(want, key=repr))
    for k, v in want.items():
        assert got[k] == (None if v is None else pytest.approx(v)), (k, got[k])


def _no_grain_warnings(resp: SlayerResponse) -> None:
    assert not warnings_of(resp, "broadcast"), resp.warnings
    assert not warnings_of(resp, "associated"), resp.warnings


def _by_customer_date(resp: SlayerResponse, name: str = "v") -> dict:
    """``{(customer, YYYY-MM-DD): value}`` over a raw-date dimension."""
    out = {(r[CUST], str(r[DATE])[:10]): num(r[name]) for r in resp.data}
    assert len(out) == len(resp.data)
    return out


# --------------------------------------------------------------------------- #
# first and last over an aggregate rank their operand's cells
# --------------------------------------------------------------------------- #
class TestEveryPosition:
    async def test_measure(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(LAST_BAL)))
        _approx(by_customer_month(resp), _broadcast(CLOSE))

    async def test_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(BAL), filter=[f"{LAST_BAL} > 100"]))
        _approx(by_customer_month(resp), {(100, JAN): 370.0, (100, FEB): 160.0})

    async def test_order_key(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(BAL), order=[OrderItem(field=LAST_BAL, direction="desc")]))
        assert [r[CUST] for r in resp.data] == [100, 100, 200, 200]

    async def test_inside_rank(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(f"rank({LAST_BAL}, direction='desc')")))
        _approx(by_customer_month(resp), _broadcast({100: 1.0, 200: None}))

    async def test_aggregation_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(f"max({LAST_BAL})")))
        _approx(by_customer_month(resp), _broadcast(CLOSE))

    async def test_aggregation_parameter(self, engine: SlayerQueryEngine) -> None:
        # weight 160 on every customer-100 row: the cell's plain average; customer 200's NULL weight: NULL
        resp = await engine.execute(_monthly(m(f"weighted_avg(account_snapshots.balance, weight={LAST_BAL})")))
        _approx(by_customer_month(resp), {(100, JAN): 92.5, (100, FEB): 160.0, (200, JAN): None, (200, FEB): None})

    async def test_mixed_source(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(f"sum(account_snapshots.balance * {LAST_BAL})")))
        _approx(by_customer_month(resp), {(100, JAN): 370.0 * 160, (100, FEB): 160.0 * 160,
                                          (200, JAN): None, (200, FEB): None})

    async def test_downstream_stage(self, engine: SlayerQueryEngine) -> None:
        inner = {"name": "s1", "population": "account_snapshots", "dimensions": [CUST],
                 "time_dimensions": [{"field": DATE, "granularity": "month"}],
                 "metrics": [{"metric": BAL, "alias": "b"}]}
        outer = SlayerQuery.model_validate({
            "population": "s1", "dimensions": ["s1.customer_id"],
            "time_dimensions": [{"field": "s1.snapshot_date", "granularity": "month"}],
            "metrics": [{"metric": "last(sum(s1.b))", "alias": "v"}]})
        resp = await engine.execute([inner, outer])
        got = {(r["s1.customer_id"], str(r["s1.snapshot_date"])[:7]): num(r["v"]) for r in resp.data}
        _approx(got, _broadcast(CLOSE))


class TestDefaultRankingKeyOverAnAggregate:
    async def test_the_active_bucket_ranks_and_a_null_value_is_picked(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(LAST_BAL)))
        assert _per_customer(by_customer_month(resp)) == CLOSE
        _no_grain_warnings(resp)

    async def test_first_picks_the_earliest_bucket(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(_monthly(m(f"first({BAL})")))
        assert _per_customer(by_customer_month(resp)) == {100: 370.0, 200: 70.0}

    async def test_an_operand_without_the_bucket_fails(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(TimeAxisError):
            await engine.execute(_monthly(m(f"last(sum(account_snapshots.balance, partition_by={CUST}))")))

    async def test_no_time_dimension_fails(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(TimeAxisError):
            await engine.execute(snap_q(dimensions=[CUST], metrics=[m(LAST_BAL)]))


async def test_a_mixed_grain_operand_ranks_its_union_grain_cells(engine: SlayerQueryEngine) -> None:
    # (customer, month) minus month totals (Jan 440, Feb 160): 100 → -70, 0; 200 → -370, NULL
    expr = (f"last(sum(account_snapshots.balance, partition_by=[{CUST}, {DATE}]) - "
            f"sum(account_snapshots.balance, partition_by={DATE}))")
    resp = await engine.execute(_monthly(m(expr)))
    _approx(by_customer_month(resp), _broadcast({100: 0.0, 200: None}))


class TestReaggregationRefusals:
    async def test_window_is_refused(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(ReaggregationError):
            await engine.execute(_monthly(m(f"last({BAL}, window='60d')")))

    async def test_filter_is_refused(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(AggregationFilterError):
            await engine.execute(_monthly(m(f"{LAST_BAL} FILTER (WHERE account_snapshots.balance > 0)")))


# --------------------------------------------------------------------------- #
# A ranked aggregation's grain drops its ranking key
# --------------------------------------------------------------------------- #
class TestGrainDropsTheRankingKey:
    @pytest.mark.parametrize("mode", [None, "error"])
    async def test_a_field_key_that_is_a_query_dimension_is_dropped(self, engine, mode) -> None:
        resp = await engine.execute(snap_q(
            dimensions=[CUST, DATE], metrics=[m(f"last(account_snapshots.balance, {DATE})")],
            to_many_handling=mode))
        got = _by_customer_date(resp)
        assert len(got) == 8
        assert got == {k: CLOSE[k[0]] for k in got}
        assert not resp.warnings, resp.warnings

    async def test_a_raw_key_never_drops_its_month_bucket(self, engine) -> None:
        resp = await engine.execute(_monthly(m(f"last(account_snapshots.balance, {DATE})")))
        _approx(by_customer_month(resp), CLOSE_BY_MONTH)

    async def test_a_field_partition_naming_the_key_drops_it(self, engine) -> None:
        resp = await engine.execute(snap_q(dimensions=[CUST, DATE], metrics=[
            m(f"last(account_snapshots.balance, {DATE}, partition_by=[{CUST}, {DATE}])"),
            m(f"last(account_snapshots.balance, {DATE}, partition_by={CUST})", "w")]))
        got, want = _by_customer_date(resp), _by_customer_date(resp, "w")
        assert got == want == {k: CLOSE[k[0]] for k in got}
        assert not resp.warnings, resp.warnings

    async def test_an_aggregate_partition_naming_the_bucket_drops_it(self, engine) -> None:
        resp = await engine.execute(_monthly(m(f"last({BAL}, partition_by=[{CUST}, {DATE}])"),
                                             m(f"last({BAL}, partition_by={CUST})", "w")))
        _approx(by_customer_month(resp), _broadcast(CLOSE))
        _approx(by_customer_month(resp, "w"), _broadcast(CLOSE))
        _no_grain_warnings(resp)

    @pytest.mark.parametrize("mode", [None, "error"])
    async def test_the_bucket_over_an_aggregate_is_dropped_without_a_warning(self, engine, mode) -> None:
        resp = await engine.execute(_monthly(m(LAST_BAL), to_many_handling=mode))
        _approx(by_customer_month(resp), _broadcast(CLOSE))
        _no_grain_warnings(resp)

    async def test_a_windowed_ranked_aggregation_keeps_the_bucket(self, engine) -> None:
        resp = await engine.execute(_monthly(
            m(f"last(account_snapshots.balance, date_trunc('month', {DATE}), window='60d')")))
        got = by_customer_month(resp)
        # January's rows tie on the bucket key; February has one latest-bucket row per customer.
        assert got[(100, FEB)] == 160.0 and got[(100, JAN)] in {100.0, 150.0, 50.0, 70.0}
        assert got[(200, FEB)] is None and got[(200, JAN)] in {30.0, 40.0}


# --------------------------------------------------------------------------- #
# Ranking keys over an aggregate operand
# --------------------------------------------------------------------------- #
GRAINED = f"sum(account_snapshots.balance, partition_by=[{CUST}, {DATE}])"


class TestOperandRankingKeys:
    async def test_a_grain_member_ranks_the_cells(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(snap_q(time_dimensions=month_td(), metrics=[m(f"first({GRAINED}, {CUST})")]))
        got = {str(r[DATE])[:7]: num(r["v"]) for r in resp.data}
        assert got == {JAN: 370.0, FEB: 160.0}

    @pytest.mark.parametrize("expr", [
        f"last({GRAINED}, account_snapshots.account_id)",
        f"last(sum(account_snapshots.balance, partition_by={CUST}), "
        f"sum(account_snapshots.balance, partition_by=account_snapshots.account_id))",
    ])
    async def test_a_key_the_operand_does_not_determine_fails_closed(self, engine, expr) -> None:
        with pytest.raises(UnsafeRelationshipInputError, match="account_id"):
            await engine.execute(snap_q(time_dimensions=month_td(), metrics=[m(expr)]))

    async def test_a_literal_key_over_an_aggregate_fails(self, engine) -> None:
        with pytest.raises(ValueError, match="ranks by a field"):
            await engine.execute(_monthly(m(f"last({BAL}, 1)")))


# --------------------------------------------------------------------------- #
# Stored metrics keep their meaning (on-disk YAML storage)
# --------------------------------------------------------------------------- #
STORED = [
    DatasetMetric(name="bal", expression="sum(balance)"),
    DatasetMetric(name="close", expression="last(bal)"),
    DatasetMetric(name="close_raw", expression="last(balance)"),
    DatasetMetric(name="close_rank", expression="rank(last(bal), direction='desc')"),
]


def _stored_snapshots(**extra: Any) -> SlayerDataset:
    ds = snapshots_dataset()
    return ds.model_copy(update={"metrics": list(STORED), **extra})


@pytest.fixture(params=["sqlite", "duckdb"])
async def stored(request):
    seed = seed_duckdb if request.param == "duckdb" else seed_sqlite
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect=request.param, seed=seed,
                                  datasets=[_stored_snapshots(), customers_dataset()]) as (eng, _):
        yield eng


class TestStoredMetrics:
    @pytest.mark.parametrize(("name", "want"), [
        ("close", _broadcast(CLOSE)),
        ("close_raw", CLOSE_BY_MONTH),
        ("close_rank", _broadcast({100: 1.0, 200: None})),
    ])
    async def test_measure(self, stored: SlayerQueryEngine, name: str, want: dict) -> None:
        resp = await stored.execute(_monthly(QueryMetric(metric=f"account_snapshots.{name}", alias="v")))
        _approx(by_customer_month(resp), want)

    @pytest.mark.parametrize(("name", "predicate", "kept"), [
        ("close", "s > 100", {(100, JAN), (100, FEB)}),
        ("close_raw", "s > 60", {(100, JAN), (100, FEB)}),
        ("close_raw", "s < 60", {(200, JAN)}),
        ("close_rank", "s = 1", {(100, JAN), (100, FEB)}),
    ])
    async def test_filter(self, stored: SlayerQueryEngine, name: str, predicate: str, kept: set) -> None:
        resp = await stored.execute(_monthly(m(BAL), m(f"account_snapshots.{name}", "s"), filter=[predicate]))
        assert set(by_customer_month(resp)) == kept

    @pytest.mark.parametrize("name", ["close", "close_raw"])
    async def test_order_key(self, stored: SlayerQueryEngine, name: str) -> None:
        resp = await stored.execute(_monthly(m(BAL), m(f"account_snapshots.{name}", "s"),
                                             order=[OrderItem(field="s", direction="desc")]))
        assert [r[CUST] for r in resp.data][:2] == [100, 100]


# --------------------------------------------------------------------------- #
# A dataset-defined custom first / last shadows the built-in
# --------------------------------------------------------------------------- #
@pytest.fixture(params=["sqlite", "duckdb"])
async def shadowed(request):
    seed = seed_duckdb if request.param == "duckdb" else seed_sqlite
    if request.param == "duckdb":
        pytest.importorskip("duckdb")
    ds = snapshots_dataset().model_copy(update={"aggregations": [Aggregation(name="last", expression="MAX({value})")]})
    async with seeded_exec_engine(dialect=request.param, seed=seed, datasets=[ds, customers_dataset()]) as (eng, _):
        yield eng


class TestCustomShadow:
    async def test_over_rows(self, shadowed: SlayerQueryEngine) -> None:
        resp = await shadowed.execute(snap_q(dimensions=[CUST], metrics=[m("last(account_snapshots.balance)")]))
        assert by_customer(resp) == {100: 160.0, 200: 40.0}

    async def test_over_an_aggregate(self, shadowed: SlayerQueryEngine) -> None:
        # MAX over the per-date totals, never the built-in pick (which would need a time dimension)
        resp = await shadowed.execute(snap_q(dimensions=[CUST], metrics=[
            m(f"last(sum(account_snapshots.balance, partition_by=[{CUST}, {DATE}]))")]))
        assert by_customer(resp) == {100: 160.0, 200: 40.0}


# --------------------------------------------------------------------------- #
# The `first` / `last` field form on query dimensions (DATE-typed raw key)
# --------------------------------------------------------------------------- #
async def test_first_over_a_raw_date_dimension_is_one_value_per_customer(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(snap_q(dimensions=[CUST, DATE], metrics=[m(f"first(account_snapshots.balance, {DATE})")]))
    # earliest snapshot: customer 100 → 100 (2025-01-05), customer 200 → 30 (2025-01-15)
    got = _by_customer_date(resp)
    assert len(got) == 8
    assert got == {k: {100: 100.0, 200: 30.0}[k[0]] for k in got}
