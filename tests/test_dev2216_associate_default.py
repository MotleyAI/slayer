"""Omitting ``to_many_handling`` associates; association-ineligible aggregates fall back to
broadcast with their reason; ungrained re-aggregation inners resolve per the mode (spec:
queries/attribution-modes, queries/semantics)."""

from __future__ import annotations

import re

import pytest

from slayer.core.errors import AssociationError, PartitionKeyError, SlayerError, UnsafeJoinInputError
from slayer.core.warnings import DICE_SLICE_HINT, BroadcastGrainWarningPayload
from tests._dev1840_fixtures import month_key
from tests._dev1841_fixtures import (
    ASSOC_COUNT_BY_STATUS,
    ASSOC_LOCAL_SPEND_BY_STATUS,
    ASSOC_SPEND_BY_STATUS,
    BCAST_SPEND_CROSS,
    LOCAL_OK_FAN_DEFECT,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    associated_warnings,
    broadcast_warnings,
    crossing_input_models,
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
CM = ModelMeasure(formula="sum(customers.spend)", name="cm")
COUNT = ModelMeasure(formula="count(customers.*)", name="n")
LAST = ModelMeasure(formula="last(customers.spend, customers.signup_at)", name="ls")
WINDOWED = ModelMeasure(formula="sum(customers.spend, window='90d')", name="win")
MIXED = ModelMeasure(
    formula="last(customers.spend, customers.signup_at) + sum(customers.spend, window='90d')", name="mix")
STAGE_ROWS = ModelMeasure(formula="count(*)", name="k")
SIGNUP_MONTH = [TimeDimension(dimension="customers.signup_at", granularity=TimeGranularity.MONTH)]
FIRST_LAST = re.compile(r"first|last", re.IGNORECASE)
WINDOW = re.compile(r"window", re.IGNORECASE)
REMEDY = re.compile(r"attribut", re.IGNORECASE)
INELIGIBLE = {
    "first_last": ({"dimensions": ["status"]}, LAST),
    "window": ({"dimensions": ["status"], "time_dimensions": SIGNUP_MONTH}, WINDOWED),
}


def _q(mode: str | None, **kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    if mode is not None:
        kw["to_many_handling"] = mode
    return SlayerQuery(**kw)


def _same_rows(a, b) -> bool:
    """Equal result rows, ignoring row order."""
    def canon(resp):
        return sorted((sorted(row.items(), key=str) for row in resp.data), key=str)
    return canon(a) == canon(b)


def _status(resp, measure: str, root: str = "orders") -> dict:
    return {k[0]: float(v[f"{root}.{measure}"]) for k, v in status_key(resp, root=root).items()}


def _fallbacks(resp) -> list[BroadcastGrainWarningPayload]:
    return [w for w in broadcast_warnings(resp) if getattr(w, "association_unsupported", None)]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for engine in make_exec_engine(request):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def crossing_engine(request):
    async for engine in make_exec_engine(request, models=crossing_input_models()):
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
        default = await engine.execute(_q(None, dimensions=["status"], measures=[CM]))
        assoc = await engine.execute(_q("associate", dimensions=["status"], measures=[CM]))
        assert _status(default, "cm") == ASSOC_SPEND_BY_STATUS
        assert _same_rows(default, assoc)
        assert associated_warnings(default)
        assert not broadcast_warnings(default)

    async def test_explicit_broadcast_keeps_the_previous_behaviour(self, engine) -> None:
        resp = await engine.execute(_q("broadcast", dimensions=["status"], measures=[CM]))
        assert _status(resp, "cm") == dict.fromkeys(ASSOC_SPEND_BY_STATUS, BCAST_SPEND_CROSS)
        (w,) = broadcast_warnings(resp)
        assert w.association_unsupported is None
        assert not associated_warnings(resp)

    async def test_local_metric_never_double_counts(self, engine) -> None:
        kw = {"source_model": "customers", "dimensions": ["orders.status"],
              "measures": [ModelMeasure(formula="sum(spend)", name="sp")]}
        default = await engine.execute(_q(None, **kw))
        assoc = await engine.execute(_q("associate", **kw))
        cells = _status(default, "sp", root="customers")
        assert _same_rows(default, assoc)
        assert {k: cells[k] for k in ASSOC_LOCAL_SPEND_BY_STATUS} == ASSOC_LOCAL_SPEND_BY_STATUS
        assert cells["ok"] != LOCAL_OK_FAN_DEFECT

    @pytest.mark.parametrize("mode", MODES)
    async def test_derived_fanning_dimension(self, fanning_engine, mode) -> None:
        resp = await fanning_engine.execute(_q(mode, dimensions=[BAD_POP], measures=[AMOUNT_SUM, SPEND_SUM]))
        assert bad_pop_vals(resp, "orders.amt") == pytest.approx(ASSOC_AMOUNT_BY_BAD_POP)
        assert bad_pop_vals(resp, "orders.csp") == pytest.approx(ASSOC_SPEND_BY_BAD_POP)
        assert associated_warnings(resp)

    async def test_host_safe_partition_key_associates(self, engine) -> None:
        part = ModelMeasure(formula="sum(customers.spend, partition_by=status)", name="cm")
        default = await engine.execute(_q(None, dimensions=["status"], measures=[part]))
        assoc = await engine.execute(_q("associate", dimensions=["status"], measures=[part]))
        assert _same_rows(default, assoc)
        assert _status(default, "cm") == ASSOC_SPEND_BY_STATUS


class TestModeInvariance:
    async def test_fully_attributable_query(self, engine) -> None:
        kw = {"dimensions": ["status", "customers.tier"],
              "measures": [ModelMeasure(formula="sum(amount)", name="amt")]}
        runs = [await engine.execute(_q(mode, **kw)) for mode in (None, "broadcast", "associate", "error")]
        assert all(_same_rows(r, runs[0]) for r in runs)
        for r in runs:
            assert not broadcast_warnings(r)
            assert not associated_warnings(r)
        total = await engine.execute(_q(None, measures=[ModelMeasure(formula="sum(amount)", name="amt")]))
        assert sum(float(r["orders.amt"]) for r in runs[0].data) == pytest.approx(float(total.data[0]["orders.amt"]))

    async def test_an_associated_measure_is_cardinality_neutral(self, engine) -> None:
        amt = ModelMeasure(formula="sum(amount)", name="amt")
        without = await engine.execute(_q(None, dimensions=["status"], measures=[amt]))
        with_cm = await engine.execute(_q(None, dimensions=["status"], measures=[amt, CM]))
        assert len(with_cm.data) == len(without.data)
        assert _status(with_cm, "amt") == _status(without, "amt")


class TestRowSupply:
    async def test_the_population_keeps_only_its_own_tuples(self, engine) -> None:
        resp = await engine.execute(_q(None, dimensions=["status"], measures=[COUNT]))
        assert _status(resp, "n") == ASSOC_COUNT_BY_STATUS

    async def test_the_orderless_entity_reaches_the_null_cell(self, null_status_engine) -> None:
        resp = await null_status_engine.execute(_q(None, dimensions=["status"], measures=[COUNT]))
        assert _status(resp, "n")[None] == 2.0


class TestBroadcastFallback:
    async def test_first_last_broadcasts_with_its_reason(self, engine) -> None:
        default = await engine.execute(_q(None, dimensions=["status"], measures=[LAST]))
        bcast = await engine.execute(_q("broadcast", dimensions=["status"], measures=[LAST]))
        assert _same_rows(default, bcast)
        assert len(set(_status(default, "ls").values())) == 1
        (w,) = _fallbacks(default)
        assert "ls" in w.measure
        assert [d.dimension for d in w.dimensions] == ["status"]
        assert FIRST_LAST.search(w.association_unsupported or "")
        assert w.hint != DICE_SLICE_HINT
        assert FIRST_LAST.search(w.hint)
        assert REMEDY.search(w.hint)
        assert not associated_warnings(default)

    async def test_window_broadcasts_with_its_reason(self, engine) -> None:
        kw = {"dimensions": ["status"], "time_dimensions": SIGNUP_MONTH, "measures": [WINDOWED]}
        default = await engine.execute(_q(None, **kw))
        bcast = await engine.execute(_q("broadcast", **kw))
        assert _same_rows(default, bcast)
        by_month: dict[str, set] = {}
        for row in default.data:
            by_month.setdefault(month_key(row["orders.customers.signup_at"]), set()).add(row["orders.win"])
        assert all(len(values) == 1 for values in by_month.values())
        (w,) = _fallbacks(default)
        assert WINDOW.search(w.association_unsupported or "")
        assert w.hint != DICE_SLICE_HINT
        assert WINDOW.search(w.hint)
        assert REMEDY.search(w.hint)

    async def test_each_reason_is_reported(self, engine) -> None:
        resp = await engine.execute(_q(
            None, dimensions=["status"], time_dimensions=SIGNUP_MONTH, measures=[MIXED]))
        reasons = [w.association_unsupported or "" for w in _fallbacks(resp)]
        assert any(FIRST_LAST.search(r) for r in reasons)
        assert any(WINDOW.search(r) for r in reasons)
        assert all(w.hint != DICE_SLICE_HINT for w in _fallbacks(resp))

    @pytest.mark.parametrize("shape", INELIGIBLE)
    async def test_explicit_associate_keeps_the_typed_error(self, engine, shape) -> None:
        kw, measure = INELIGIBLE[shape]
        query = _q("associate", measures=[measure], **kw)
        with pytest.raises(AssociationError):
            await engine.execute(query)

    @pytest.mark.parametrize("shape", INELIGIBLE)
    async def test_explicit_broadcast_warns_ordinarily(self, engine, shape) -> None:
        kw, measure = INELIGIBLE[shape]
        resp = await engine.execute(_q("broadcast", measures=[measure], **kw))
        (w,) = broadcast_warnings(resp)
        assert w.association_unsupported is None
        assert w.hint == DICE_SLICE_HINT

    @pytest.mark.parametrize("shape", INELIGIBLE)
    async def test_error_mode_refuses(self, engine, shape) -> None:
        kw, measure = INELIGIBLE[shape]
        query = _q("error", measures=[measure], **kw)
        with pytest.raises(SlayerError):
            await engine.execute(query)

    async def test_a_stages_error_mode_refuses_under_a_default_root(self, engine) -> None:
        stages = [_q("error", name="s", dimensions=["status"], measures=[CM]),
                  _q(None, source_model="s", measures=[STAGE_ROWS])]
        with pytest.raises(SlayerError):
            await engine.execute(query=stages)

    async def test_a_root_error_mode_spares_a_default_stages_fallback(self, engine) -> None:
        stage = _q(None, name="s", dimensions=["status"], measures=[LAST])
        resp = await engine.execute(query=[stage, _q("error", source_model="s", measures=[STAGE_ROWS])])
        assert resp.data[0]["s.k"] == len(ASSOC_SPEND_BY_STATUS)
        assert _fallbacks(resp)

    async def test_requested_grain_is_never_broadcast(self, engine) -> None:
        part = ModelMeasure(formula="last(customers.spend, customers.signup_at, partition_by=status)", name="lp")
        default, associate = (_q(mode, dimensions=["status"], measures=[part]) for mode in (None, "associate"))
        with pytest.raises(AssociationError) as by_default:
            await engine.execute(default)
        with pytest.raises(AssociationError) as by_associate:
            await engine.execute(associate)
        assert type(by_default.value) is type(by_associate.value)

    @pytest.mark.parametrize("mode", [None, "broadcast", "associate", "error"])
    async def test_input_safety_is_not_a_fallback(self, crossing_engine, mode) -> None:
        query = _q(mode, dimensions=["status"], measures=[ModelMeasure(formula="sum(customers.vip_spend)", name="vip")])
        with pytest.raises(UnsafeJoinInputError, match="unproven join hop"):
            await crossing_engine.execute(query)

    async def test_partition_key_fanning_from_its_host_fails_by_default(self, fanning_engine) -> None:
        query = _q(None, source_model="regions", dimensions=["bad_pop"],
                   measures=[ModelMeasure(formula="sum(pop, partition_by=bad_pop)", name="w")])
        with pytest.raises(PartitionKeyError, match="region_events"):
            await fanning_engine.execute(query)

    async def test_local_ranked_aggregate_never_raises(self, engine) -> None:
        kw = {"source_model": "customers", "dimensions": ["orders.status"],
              "measures": [ModelMeasure(formula="last(spend, signup_at)", name="l")]}
        default = await engine.execute(_q(None, **kw))
        bcast = await engine.execute(_q("broadcast", **kw))
        assert _same_rows(default, bcast)


class TestUngrainedReaggregationInner:
    @pytest.mark.parametrize("mode", MODES)
    async def test_plain_inner_associates(self, engine, mode) -> None:
        nested = ModelMeasure(formula="sum(sum(customers.spend))", name="ss")
        resp = await engine.execute(_q(mode, dimensions=["status"], measures=[nested]))
        assert _status(resp, "ss") == ASSOC_SPEND_BY_STATUS
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    async def test_plain_inner_under_broadcast_and_error(self, engine) -> None:
        nested = ModelMeasure(formula="sum(sum(customers.spend))", name="ss")
        resp = await engine.execute(_q("broadcast", dimensions=["status"], measures=[nested]))
        assert _status(resp, "ss") == dict.fromkeys(ASSOC_SPEND_BY_STATUS, BCAST_SPEND_CROSS)
        query = _q("error", dimensions=["status"], measures=[nested])
        with pytest.raises(SlayerError):
            await engine.execute(query)

    async def test_ranked_inner_falls_back_by_default(self, engine) -> None:
        nested = ModelMeasure(formula="sum(last(customers.spend, customers.signup_at))", name="sl")
        resp = await engine.execute(_q(None, dimensions=["status"], measures=[nested]))
        assert any(FIRST_LAST.search(w.association_unsupported or "") for w in _fallbacks(resp))

    async def test_ranked_inner_fails_under_associate(self, engine) -> None:
        query = _q("associate", dimensions=["status"],
                   measures=[ModelMeasure(formula="sum(last(customers.spend, customers.signup_at))", name="sl")])
        with pytest.raises(AssociationError):
            await engine.execute(query)
