"""Spine time dimensions factor out of population inference; a spine population is reported as ``time_spine × P``."""

from __future__ import annotations

import pytest

from tests._dev2015_fixtures import BACKENDS, JAN_MAR, TWO_FACTS, m, spine_engine, spine_query

PER_GROUP = [m("sum(orders.amount)", "o"), m("sum(returns.amount)", "r")]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with spine_engine(request.param) as eng:
        yield eng


def _per_group(**extra):
    return spine_query(measures=PER_GROUP, date_range=JAN_MAR, dimensions=["customers.region"], **extra)


class TestSpineFactorsOut:
    async def test_p_is_the_coarse_side_not_a_fact(self, engine) -> None:
        resp = await engine.execute(_per_group())
        assert resp.population == "time_spine × customers"
        assert resp.population_inferred is True

    async def test_explicit_p_reported_not_inferred(self, engine) -> None:
        resp = await engine.execute(_per_group(source_model="customers"))
        assert resp.population == "time_spine × customers"
        assert resp.population_inferred is False

    async def test_a_field_filter_still_participates(self, engine) -> None:
        resp = await engine.execute(_per_group(filters=["orders.amount > 60"]))
        assert resp.population == "time_spine × orders"


class TestSpineOnly:
    async def test_unit_p_raises_no_inference_error(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS))
        assert resp.population == "time_spine"
        assert resp.population_inferred is True

    async def test_named_spine_is_explicit(self, engine) -> None:
        resp = await engine.execute(spine_query(measures=TWO_FACTS, source_model="time_spine"))
        assert resp.population == "time_spine"
        assert resp.population_inferred is False


class TestReportedUniformly:
    async def test_dry_run_and_cache_hit(self, engine) -> None:
        query = _per_group()
        dry = await engine.execute(query, dry_run=True)
        await engine.execute(query, cache=True)
        hit = await engine.execute(query, cache=True)
        for resp in (dry, hit):
            assert resp.population == "time_spine × customers"
            assert resp.population_inferred is True
