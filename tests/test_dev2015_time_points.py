"""Custom granularities as relative-token units and as ``whole_periods_only`` snapping granularities."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator
from datetime import datetime

import pytest

from slayer.core.errors import TimeLiteralError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1737_fixtures import seed_backend
from tests._dev2015_fixtures import BACKENDS, DS_GRANULARITIES, bucket_key, cg_engine, m, value
from tests._engine_helpers import seeded_exec_engine
from tests._time_points_fixtures import CUSTOMERS, EV, PinnedClock, all_models, ids, ids_in, tp_engine

D = datetime
FAR_PAST, FAR_FUTURE = D(1900, 1, 1), D(2100, 1, 1)


@contextlib.asynccontextmanager
async def _engine(backend: str) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(
        dialect=backend, seed=lambda p: seed_backend(backend, p, [EV, CUSTOMERS]), models=all_models(),
        clock=PinnedClock(), datasource_fields=DS_GRANULARITIES,
    ) as (engine, _db):
        yield engine


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with _engine(request.param) as eng:
        yield eng


class TestCustomUnits:
    @pytest.mark.parametrize(("token", "start", "end"), [
        ("this fiscal_year", D(2026, 4, 1), D(2027, 4, 1)),
        ("last fiscal_year", D(2025, 4, 1), D(2026, 4, 1)),
        ("last 2 fiscal_years", D(2024, 4, 1), D(2026, 4, 1)),
        ("1 fiscal_year ago", D(2025, 4, 1), D(2026, 4, 1)),
        ("last 2 quarter_hours", D(2026, 9, 29, 11, 30), D(2026, 9, 29, 12)),
    ])
    async def test_resolves_to_the_custom_period(self, engine, token, start, end) -> None:
        assert await ids(engine, f"ts in '{token}'") == ids_in(start, end)
        assert await ids(engine, f"ts >= '{token}'") == ids_in(start, FAR_FUTURE)
        assert await ids(engine, f"ts <= '{token}'") == ids_in(FAR_PAST, end)

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_unit_outside_its_datasource(self, backend) -> None:
        async with tp_engine(backend) as eng:
            with pytest.raises(TimeLiteralError):
                await ids(eng, "ts >= 'last fiscal_year'")

    async def test_sub_day_custom_unit_against_a_date_column(self, engine) -> None:
        with pytest.raises(TimeLiteralError) as exc:
            await ids(engine, "d >= 'last 2 quarter_hours'")
        assert "day resolution" in str(exc.value)


class TestWholePeriodsOnly:
    @pytest.fixture(params=BACKENDS)
    async def cg(self, request):
        async with cg_engine(request.param) as eng:
            yield eng

    async def test_snapping_to_custom_boundaries(self, cg) -> None:
        resp = await cg.execute(SlayerQuery.model_validate({
            "source_model": "orders", "measures": [m("sum(amount)", "s")], "whole_periods_only": True,
            "time_dimensions": [{"dimension": "order_date", "granularity": "fiscal_year",
                                 "date_range": ["2024-05-01", "2025-12-31"]}],
        }))
        assert {bucket_key(value(r, "order_date"), width=10): float(value(r, "s")) for r in resp.data} == {
            "2024-04-01": 50.0,
        }

    async def test_month_with_a_mid_month_custom_granularity_warns(self, cg) -> None:
        resp = await cg.execute(SlayerQuery.model_validate({
            "source_model": "events", "measures": [m("sum(amount)", "s")], "whole_periods_only": True,
            "time_dimensions": [{"dimension": "ts", "granularity": "month"},
                                {"dimension": "ts", "granularity": "billing_month"}],
            "filters": ["ts >= '2025-01-01'"],
        }))
        assert resp.data
        dumps = [w.model_dump_json() for w in resp.warnings]
        assert any("billing_month" in d and "month" in d and "whole_periods_only" in d for d in dumps), dumps
