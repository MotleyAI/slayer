"""DEV-1836 task 1.10 — cross-model ranked (first/last), windowed, and
partitioned aggregates at safe grains (SQLite + DuckDB).

Spec: openspec …/specs/queries/cross-model-aggregates — "Explicit grain and
window on cross-model aggregates". Every explicit key must be attributable
from the aggregate's root; a windowed cross-model aggregate requires the
query's active time dimension attributable from its root.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.errors import SlayerError
from slayer.core.query import ColumnRef, TimeDimension
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1836_fixtures import (
    ModelMeasure,
    SPEND_1Y_BY_SIGNUP_MONTH,
    SPEND_BY_TIER,
    SPEND_FIRST_BY_TIER,
    SPEND_LAST_BY_TIER,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)

RAISES = (SlayerError, ValueError, NotImplementedError)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")


def signup_month_td() -> list[TimeDimension]:
    return [TimeDimension(
        dimension=ColumnRef(name="customers.signup_at"),
        granularity=TimeGranularity.MONTH,
    )]


async def _dry_scope_closed(engine, query, dialect) -> None:
    dry = await engine.execute(query, dry_run=True)
    assert "__regroup__" not in dry.sql, f"placeholder leaked:\n{dry.sql}"
    assert_scope_closed(dry.sql, dialect=dialect)


class TestCrossModelPartitioned:
    async def test_partitioned_at_declared_grain(self, exec_backend):
        """The value is computed at exactly the declared (attributable) grain
        and broadcast to the query rows."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier", "status"],
            measures=[M, ModelMeasure(
                formula="customers.spend:sum(partition_by=customers.tier)",
                name="pt",
            )],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier", "orders.status")
        for (tier, _status), row in by.items():
            expected = SPEND_BY_TIER.get(tier)  # None for the NULL-tier row
            if expected is None:
                assert row["orders.pt"] is None, (tier, _status)
            else:
                assert float(row["orders.pt"]) == pytest.approx(expected), tier
        await _dry_scope_closed(engine, query, dialect)


class TestCrossModelRanked:
    async def test_first_last_by_target_time(self, exec_backend):
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"],
            measures=[
                M,
                ModelMeasure(formula="customers.spend:first(customers.signup_at)",
                             name="f"),
                ModelMeasure(formula="customers.spend:last(customers.signup_at)",
                             name="l"),
            ],
        )
        resp = await engine.execute(query)
        by = rows_by(resp, "orders.customers.tier")
        for tier in ("gold", "silver", "bronze"):
            assert float(by[(tier,)]["orders.f"]) == pytest.approx(
                SPEND_FIRST_BY_TIER[tier],
            ), f"first:{tier}"
            assert float(by[(tier,)]["orders.l"]) == pytest.approx(
                SPEND_LAST_BY_TIER[tier],
            ), f"last:{tier}"
        assert by[(None,)]["orders.f"] is None
        assert by[(None,)]["orders.l"] is None
        await _dry_scope_closed(engine, query, dialect)

    async def test_ranked_is_cardinality_neutral(self, exec_backend):
        _, engine = exec_backend
        solo = await engine.execute(q(dimensions=["customers.tier"], measures=[M]))
        both = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[M, ModelMeasure(
                formula="customers.spend:last(customers.signup_at)", name="l",
            )],
        ))
        solo_by = rows_by(solo, "orders.customers.tier")
        both_by = rows_by(both, "orders.customers.tier")
        assert set(solo_by) == set(both_by)
        for key, row in both_by.items():
            assert row["orders.m"] == solo_by[key]["orders.m"], key


class TestCrossModelWindowed:
    async def test_windowed_with_attributable_td(self, exec_backend):
        """Trailing-1y spend over the customers-level time dimension."""
        dialect, engine = exec_backend
        query = q(
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(formula="customers.spend:sum(window='1y')",
                                   name="w")],
        )
        resp = await engine.execute(query)
        rows = [r for r in resp.data
                if r["orders.customers.signup_at"] is not None]
        got = {month_key(r["orders.customers.signup_at"]): r["orders.w"]
               for r in rows}
        assert len(got) == len(rows), "duplicate result rows for one month"
        assert set(got) == set(SPEND_1Y_BY_SIGNUP_MONTH)
        for month, expected in SPEND_1Y_BY_SIGNUP_MONTH.items():
            assert float(got[month]) == pytest.approx(expected), month
        await _dry_scope_closed(engine, query, dialect)

    async def test_windowed_partitioned_by_attributable_key(self, exec_backend):
        dialect, engine = exec_backend
        query = q(
            dimensions=["customers.tier"],
            time_dimensions=signup_month_td(),
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='1y', partition_by=customers.tier)",
                name="w",
            )],
        )
        resp = await engine.execute(query)
        rows = [r for r in resp.data if r["orders.customers.tier"] is not None]
        got = {
            (r["orders.customers.tier"],
             month_key(r["orders.customers.signup_at"])): r["orders.w"]
            for r in rows
        }
        assert len(got) == len(rows), "duplicate result rows for one group"
        expected = {
            ("gold", "2024-01"): 100.0,
            ("silver", "2024-02"): 150.0,
            ("gold", "2024-03"): 160.0,
            ("bronze", "2024-03"): 40.0,
        }
        assert set(got) == set(expected)
        for key, value in expected.items():
            assert float(got[key]) == pytest.approx(value), key
        await _dry_scope_closed(engine, query, dialect)

    async def test_windowed_requires_attributable_td(self, exec_backend):
        """The query's active TD (orders.ordered_at) is not attributable from
        customers: clear error, not wrong numbers."""
        _, engine = exec_backend
        query = q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="customers.spend:sum(window='1y')", name="w",
            )],
        )
        with pytest.raises(RAISES) as ei:
            await engine.execute(query)
        message = str(ei.value)
        assert "ordered_at" in message or "time dimension" in message
        assert "__regroup__" not in message
