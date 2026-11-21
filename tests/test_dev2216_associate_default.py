"""Omitting ``to_many_handling`` associates every aggregation, first/last and windowed included; ungrained
re-aggregation inners resolve per the mode (spec: queries/attribution-modes, queries/semantics)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError, UnsafeRelationshipInputError
from slayer.core.warnings import DICE_SLICE_HINT
from slayer.core.query import QueryMetric

from tests._dev1914_fixtures import (
    LAST_BY_STATUS,
    LOCAL_LAST_BROADCAST,
    LOCAL_LAST_BY_STATUS,
    WINDOW_90D_BY_STATUS_SIGNUP,
    known_status_signup,
)
from tests._dev1841_fixtures import (
    ASSOC_COUNT_BY_STATUS,
    ASSOC_LOCAL_SPEND_BY_STATUS,
    ASSOC_SPEND_BY_STATUS,
    BCAST_SPEND_CROSS,
    LOCAL_OK_FAN_DEFECT,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    associated_warnings,
    broadcast_warnings,
    crossing_input_datasets,
    make_exec_engine,
    status_key,
)
from tests._dev1900_fixtures import (
    AMOUNT_SUM,
    ASSOC_AMOUNT_BY_BAD_POP,
    ASSOC_SPEND_BY_BAD_POP,
    BAD_POP,
    SPEND_SUM,
    bad_pop_vals,
)
from tests._dev1900_fixtures import make_exec_engine as dev1900_engine
from tests._dev1910_fixtures import make_null_status_engine

MODES = [pytest.param(None, id="default"), pytest.param("associate", id="associate")]
CM = QueryMetric(metric="sum(customers.spend)", alias="cm")
COUNT = QueryMetric(metric="count(customers.*)", alias="n")
LAST = QueryMetric(metric="last(customers.spend, customers.signup_at)", alias="ls")
WINDOWED = QueryMetric(metric="sum(customers.spend, window='90d')", alias="win")
STAGE_ROWS = QueryMetric(metric="count(s.*)", alias="k")
SIGNUP_MONTH = [TimeDimension(field="customers.signup_at", granularity=TimeGranularity.MONTH)]
#: shape → (query keywords, measure, reader, associated cells).
RANKED_WINDOWED = {
    "first_last": ({"dimensions": ["orders.status"]}, LAST,
                   lambda resp: _status(resp, "ls"), LAST_BY_STATUS),
    "window": ({"dimensions": ["orders.status"], "time_dimensions": SIGNUP_MONTH}, WINDOWED,
               lambda resp: known_status_signup(resp, "win"), WINDOW_90D_BY_STATUS_SIGNUP),
}


def _q(mode: str | None, **kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    if mode is not None:
        kw["to_many_handling"] = mode
    return SlayerQuery(**kw)


def _same_rows(a, b) -> bool:
    """Equal result rows, ignoring row order."""
    def canon(resp):
        return sorted((sorted(row.items(), key=str) for row in resp.data), key=str)
    return canon(a) == canon(b)


def _status(resp, metric: str) -> dict:
    return {k[0]: None if v[metric] is None else float(v[metric]) for k, v in status_key(resp).items()}


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def crossing_engine(request):
    async for engine in make_exec_engine(request, datasets=crossing_input_datasets()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def fanning_engine(request):
    async for engine in dev1900_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def null_status_engine(request):
    async for engine in make_null_status_engine(request):
        yield engine


class TestTheDefaultAssociates:
    async def test_cross_model_metric(self, engine) -> None:
        default = await engine.execute(_q(None, dimensions=["orders.status"], metrics=[CM]))
        assoc = await engine.execute(_q("associate", dimensions=["orders.status"], metrics=[CM]))
        assert _status(default, "cm") == ASSOC_SPEND_BY_STATUS
        assert _same_rows(default, assoc)
        assert associated_warnings(default)
        assert not broadcast_warnings(default)

    async def test_explicit_broadcast_keeps_the_previous_behaviour(self, engine) -> None:
        resp = await engine.execute(_q("broadcast", dimensions=["orders.status"], metrics=[CM]))
        assert _status(resp, "cm") == dict.fromkeys(ASSOC_SPEND_BY_STATUS, BCAST_SPEND_CROSS)
        assert len(broadcast_warnings(resp)) == 1
        assert not associated_warnings(resp)

    async def test_local_metric_never_double_counts(self, engine) -> None:
        kw = {"population": "customers", "dimensions": ["orders.status"],
              "metrics": [QueryMetric(metric="sum(customers.spend)", alias="sp")]}
        default = await engine.execute(_q(None, **kw))
        assoc = await engine.execute(_q("associate", **kw))
        cells = _status(default, "sp")
        assert _same_rows(default, assoc)
        assert {k: cells[k] for k in ASSOC_LOCAL_SPEND_BY_STATUS} == ASSOC_LOCAL_SPEND_BY_STATUS
        assert cells["ok"] != LOCAL_OK_FAN_DEFECT

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_dimension(self, fanning_engine, mode) -> None:
        resp = await fanning_engine.execute(_q(mode, dimensions=[BAD_POP], metrics=[AMOUNT_SUM, SPEND_SUM]))
        assert bad_pop_vals(resp, "amt") == pytest.approx(ASSOC_AMOUNT_BY_BAD_POP)
        assert bad_pop_vals(resp, "csp") == pytest.approx(ASSOC_SPEND_BY_BAD_POP)
        assert associated_warnings(resp)

    async def test_host_safe_partition_key_associates(self, engine) -> None:
        part = QueryMetric(metric="sum(customers.spend, partition_by=orders.status)", alias="cm")
        default = await engine.execute(_q(None, dimensions=["orders.status"], metrics=[part]))
        assoc = await engine.execute(_q("associate", dimensions=["orders.status"], metrics=[part]))
        assert _same_rows(default, assoc)
        assert _status(default, "cm") == ASSOC_SPEND_BY_STATUS


class TestModeInvariance:
    async def test_fully_attributable_query(self, engine) -> None:
        kw = {"dimensions": ["orders.status", "customers.tier"],
              "metrics": [QueryMetric(metric="sum(orders.amount)", alias="amt")]}
        runs = [await engine.execute(_q(mode, **kw)) for mode in (None, "broadcast", "associate", "error")]
        assert all(_same_rows(r, runs[0]) for r in runs)
        for r in runs:
            assert not broadcast_warnings(r)
            assert not associated_warnings(r)
        total = await engine.execute(_q(None, metrics=[QueryMetric(metric="sum(orders.amount)", alias="amt")]))
        assert sum(float(r["amt"]) for r in runs[0].data) == pytest.approx(float(total.data[0]["amt"]))

    async def test_an_associated_measure_is_cardinality_neutral(self, engine) -> None:
        amt = QueryMetric(metric="sum(orders.amount)", alias="amt")
        without = await engine.execute(_q(None, dimensions=["orders.status"], metrics=[amt]))
        with_cm = await engine.execute(_q(None, dimensions=["orders.status"], metrics=[amt, CM]))
        assert len(with_cm.data) == len(without.data)
        assert _status(with_cm, "amt") == _status(without, "amt")


class TestRowSupply:
    async def test_the_population_keeps_only_its_own_tuples(self, engine) -> None:
        resp = await engine.execute(_q(None, dimensions=["orders.status"], metrics=[COUNT]))
        assert _status(resp, "n") == ASSOC_COUNT_BY_STATUS

    async def test_the_orderless_entity_reaches_the_null_cell(self, null_status_engine) -> None:
        resp = await null_status_engine.execute(_q(None, dimensions=["orders.status"], metrics=[COUNT]))
        assert _status(resp, "n")[None] == 2.0


class TestRankedAndWindowedResolveByMode:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("shape", RANKED_WINDOWED)
    async def test_default_and_associate_associate(self, engine, shape, mode) -> None:
        kw, measure, read, expected = RANKED_WINDOWED[shape]
        resp = await engine.execute(_q(mode, metrics=[measure], **kw))
        assert read(resp) == expected
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("shape", RANKED_WINDOWED)
    async def test_explicit_broadcast_warns_ordinarily(self, engine, shape) -> None:
        kw, measure, _, _ = RANKED_WINDOWED[shape]
        resp = await engine.execute(_q("broadcast", metrics=[measure], **kw))
        (w,) = broadcast_warnings(resp)
        assert w.hint == DICE_SLICE_HINT

    @pytest.mark.parametrize("shape", RANKED_WINDOWED)
    async def test_error_mode_refuses(self, engine, shape) -> None:
        kw, measure, _, _ = RANKED_WINDOWED[shape]
        query = _q("error", metrics=[measure], **kw)
        with pytest.raises(SlayerError):
            await engine.execute(query)


class TestModeResolution:

    async def test_a_stages_error_mode_refuses_under_a_default_root(self, engine) -> None:
        stages = [_q("error", name="s", dimensions=["orders.status"], metrics=[CM]),
                  _q(None, population="s", metrics=[STAGE_ROWS])]
        with pytest.raises(SlayerError):
            await engine.execute(query=stages)

    async def test_a_root_error_mode_spares_a_default_stages_association(self, engine) -> None:
        stage = _q(None, name="s", dimensions=["orders.status"], metrics=[LAST])
        resp = await engine.execute(query=[stage, _q("error", population="s", metrics=[STAGE_ROWS])])
        assert resp.data[0]["k"] == len(ASSOC_SPEND_BY_STATUS)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_requested_grain_is_never_broadcast(self, engine, mode) -> None:
        part = QueryMetric(metric="last(customers.spend, customers.signup_at, partition_by=orders.status)", alias="lp")
        resp = await engine.execute(_q(mode, dimensions=["orders.status"], metrics=[part]))
        assert _status(resp, "lp") == LAST_BY_STATUS
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", [None, "broadcast", "associate", "error"])
    async def test_input_safety_is_not_a_fallback(self, crossing_engine, mode) -> None:
        query = _q(mode, dimensions=["orders.status"], metrics=[QueryMetric(metric="sum(customers.vip_spend)", alias="vip")])
        with pytest.raises(UnsafeRelationshipInputError, match="unproven relationship hop"):
            await crossing_engine.execute(query)

    async def test_partition_key_fanning_from_its_host_associates_by_default(self, fanning_engine) -> None:
        resp = await fanning_engine.execute(_q(
            None, population="regions", dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.bad_pop)", alias="w")]))
        assert {float(r["regions.bad_pop"]): float(r["w"]) for r in resp.data} == {150.0: 100.0, 230.0: 200.0}
        assert not associated_warnings(resp)
        assert not broadcast_warnings(resp)

    async def test_local_ranked_aggregate_follows_the_mode(self, engine) -> None:
        kw = {"population": "customers", "dimensions": ["orders.status"],
              "metrics": [QueryMetric(metric="last(customers.spend, customers.signup_at)", alias="l")]}
        default = await engine.execute(_q(None, **kw))
        assoc = await engine.execute(_q("associate", **kw))
        bcast = await engine.execute(_q("broadcast", **kw))
        assert _status(default, "l") == LOCAL_LAST_BY_STATUS
        assert _same_rows(default, assoc)
        assert _status(bcast, "l") == dict.fromkeys(LOCAL_LAST_BY_STATUS, LOCAL_LAST_BROADCAST)
        refused = _q("error", **kw)
        with pytest.raises(SlayerError):
            await engine.execute(refused)


class TestUngrainedReaggregationInner:
    @pytest.mark.parametrize("mode", MODES)
    async def test_plain_inner_associates(self, engine, mode) -> None:
        nested = QueryMetric(metric="sum(sum(customers.spend))", alias="ss")
        resp = await engine.execute(_q(mode, dimensions=["orders.status"], metrics=[nested]))
        assert _status(resp, "ss") == ASSOC_SPEND_BY_STATUS
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    async def test_plain_inner_under_broadcast_and_error(self, engine) -> None:
        nested = QueryMetric(metric="sum(sum(customers.spend))", alias="ss")
        resp = await engine.execute(_q("broadcast", dimensions=["orders.status"], metrics=[nested]))
        assert _status(resp, "ss") == dict.fromkeys(ASSOC_SPEND_BY_STATUS, BCAST_SPEND_CROSS)
        query = _q("error", dimensions=["orders.status"], metrics=[nested])
        with pytest.raises(SlayerError):
            await engine.execute(query)

    @pytest.mark.parametrize("mode", MODES)
    async def test_ranked_inner_associates(self, engine, mode) -> None:
        nested = QueryMetric(metric="sum(last(customers.spend, customers.signup_at))", alias="sl")
        resp = await engine.execute(_q(mode, dimensions=["orders.status"], metrics=[nested]))
        assert _status(resp, "sl") == LAST_BY_STATUS
        assert not broadcast_warnings(resp)
