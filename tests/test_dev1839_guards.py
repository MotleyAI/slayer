"""DEV-1839 guard surface — what still fails closed after the union-grain lift
(design D6/D7/D9), the guards that must NOT change, and the single-grain
locking shapes the new predicates must not regress (design D6, Context (c))."""

from __future__ import annotations

import re

import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1839_fixtures import (
    ModelMeasure,
    REGION_CUMSUM,
    REGION_MONTH_TOTAL,
    REGION_TOTAL,
    SAMEGRAIN_RANK,
    SAMEGRAIN_RANK_OF,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


class TestMixedWindowedRankedDeferred:
    """D6 — a mixed-grain transform with a windowed or first/last inner
    aggregate defers to DEV-1835 (the union would need the synthesized time
    bucket), never misgrains."""

    async def test_mixed_windowed_grain_deferred(self) -> None:
        band = (
            "rank(amount:sum(window='90d', partition_by=region) - "
            "amount:sum(partition_by=city))"
        )
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "x"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1835") as ei:
            await gen(query)
        assert re.search(r"(?i)window", str(ei.value))  # names the combination

    async def test_mixed_first_last_grain_deferred(self) -> None:
        band = (
            "rank(amount:last(partition_by=region) - "
            "amount:sum(partition_by=city))"
        )
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "x"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1835") as ei:
            await gen(query)
        assert re.search(r"(?i)first|last", str(ei.value))  # names the combination


class TestTransformKwargAgainstUnion:
    """D7 — keyword references resolve against the union grain."""

    async def test_kwarg_outside_union_fails_cleanly(self) -> None:
        band = (
            "rank(amount:sum(partition_by=region) - "
            "amount:sum(partition_by=city), partition_by=channel)"
        )
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "x"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert "channel" in str(ei.value)
        assert "__regroup__" not in str(ei.value)


class TestUnchangedGuards:
    async def test_cross_model_inner_source_still_rejected(self) -> None:
        band = (
            "rank(customers.spend:sum(partition_by=region) - "
            "amount:sum(partition_by=city))"
        )
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "x"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)cross-model", str(ei.value))
        assert "__regroup__" not in str(ei.value)

    async def test_bare_inner_aggregate_still_rejected(self) -> None:
        band = "rank(amount:sum - amount:sum(partition_by=city))"
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "x"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert "partition_by" in str(ei.value)


class TestTemporalAxisContainment:
    """D9 — a time-ordered transform in a dimension needs its time-ordering
    key inside its evaluation grain; today the single-grain form silently
    duplicates result rows instead (the defect this guard fails closed)."""

    @pytest.mark.parametrize("transform", [
        "cumsum({})", "lag({})", "time_shift({}, -1)",
    ])
    async def test_single_grain_missing_time_axis_fails_cleanly(
        self, transform: str,
    ) -> None:
        band = transform.format("amount:sum(partition_by=[region, city])")
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "cs"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)time", str(ei.value))
        assert "partition_by" in str(ei.value)

    async def test_mixed_grain_missing_time_axis_fails_cleanly(self) -> None:
        band = "cumsum(amount:sum(partition_by=region) - amount:sum(partition_by=city))"
        query = q(
            dimensions=["region", "city", {"expression": band, "name": "cs"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await gen(query)
        assert re.search(r"(?i)time", str(ei.value))
        assert "partition_by" in str(ei.value)

    async def test_time_axis_inside_grain_stays_legal(self, exec_backend) -> None:
        dialect, engine = exec_backend
        band = "cumsum(amount:sum(partition_by=[region, ordered_at]))"
        query = q(
            dimensions=["region", {"expression": band, "name": "cs"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"])):
                (float(r["orders.cs"]), float(r["orders.s"]))
            for r in resp.data
        }
        assert set(got) == set(REGION_MONTH_TOTAL)
        assert len(resp.data) == len(REGION_MONTH_TOTAL)
        for key, (cs, s) in got.items():
            assert cs == pytest.approx(REGION_CUMSUM[key]), f"{key}"
            assert s == pytest.approx(REGION_MONTH_TOTAL[key]), f"{key}"
        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)


class TestSameGrainMultiAggregateLocking:
    """Design Context (c) — D6's actual single-grain non-regression surface: a
    transform root over MULTIPLE same-grain plain aggregates stays legal (the
    union degenerates to that grain). CASE-wrapped windowed / first-last
    dimension shapes stay pinned by the DEV-1824 suites."""

    async def test_rank_over_two_same_grain_aggregates(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", {"expression": SAMEGRAIN_RANK, "name": "gr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.region")
        assert len(resp.data) == len(SAMEGRAIN_RANK_OF)
        got = {key[0]: int(r["orders.gr"]) for key, r in by.items()}
        assert got == SAMEGRAIN_RANK_OF
        for key, r in by.items():
            assert float(r["orders.s"]) == pytest.approx(REGION_TOTAL[key[0]])
        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)
