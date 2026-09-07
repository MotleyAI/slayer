"""DEV-1854 — ``consecutive_periods`` over null-test predicates (``is None`` /
``is not None``), executed on SQLite + DuckDB against
``tests/_dev1846_fixtures.py``. Store B's March ``hi_rev:sum`` is NULL (no row
clears the >15 filter); store A's never is.
"""

from __future__ import annotations

import pytest

from tests._dev1846_fixtures import (
    ModelMeasure,
    SlayerQuery,
    gen,
    make_exec_engine,
    month_key,
    month_td,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    """SQLite + DuckDB engines over the hand-computed dataset. Defined here so
    the fixture name is not a cross-module import ruff reads as shadowing the
    parameter (F811)."""
    async for engine in make_exec_engine(request):
        yield engine


def _q(*, measures, dimensions=None) -> SlayerQuery:
    kw = dict(source_model="sales", time_dimensions=month_td(), measures=measures)
    if dimensions is not None:
        kw["dimensions"] = dimensions
    return SlayerQuery(**kw)


def _by_store_month(resp) -> dict:
    return {
        (r["sales.store"], month_key(r["sales.ordered_at"])): r
        for r in resp.data
    }


async def _error(*, measures):
    """The (type-name, message) of the raise a query produces, or fail."""
    try:
        await gen(_q(measures=measures))
    except Exception as exc:  # noqa: BLE001 — the exception itself is the contract
        return type(exc).__name__, str(exc)
    raise AssertionError("expected the query to fail closed, but it generated SQL")


class TestNullTestExecution:
    async def test_top_level_is_not_null_by_store(self, exec_engine) -> None:
        """``hi_rev:sum is not None`` drives the streak: A never NULL → 1,2,3;
        B NULL in March → 1, 2, 0."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(hi_rev:sum is not None)",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-01")]["sales.streak"]) == 1
        assert int(by[("A", "2024-02")]["sales.streak"]) == 2
        assert int(by[("A", "2024-03")]["sales.streak"]) == 3
        assert int(by[("B", "2024-01")]["sales.streak"]) == 1
        assert int(by[("B", "2024-02")]["sales.streak"]) == 2
        assert int(by[("B", "2024-03")]["sales.streak"]) == 0

    async def test_top_level_is_null_by_store(self, exec_engine) -> None:
        """The complementary ``is None`` form: A → 0,0,0; B → 0,0,1."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(hi_rev:sum is None)",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-01")]["sales.streak"]) == 0
        assert int(by[("A", "2024-02")]["sales.streak"]) == 0
        assert int(by[("A", "2024-03")]["sales.streak"]) == 0
        assert int(by[("B", "2024-01")]["sales.streak"]) == 0
        assert int(by[("B", "2024-02")]["sales.streak"]) == 0
        assert int(by[("B", "2024-03")]["sales.streak"]) == 1

    async def test_is_not_null_under_and(self, exec_engine) -> None:
        """A null test under ``and`` (rejected by the gate pre-fix): cost:sum is
        always positive, so the conjunction equals the null test → A 1,2,3 /
        B 1,2,0."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(hi_rev:sum is not None and cost:sum > 0)",
                name="streak")],
        ))
        by = _by_store_month(resp)
        assert int(by[("A", "2024-01")]["sales.streak"]) == 1
        assert int(by[("A", "2024-02")]["sales.streak"]) == 2
        assert int(by[("A", "2024-03")]["sales.streak"]) == 3
        assert int(by[("B", "2024-01")]["sales.streak"]) == 1
        assert int(by[("B", "2024-02")]["sales.streak"]) == 2
        assert int(by[("B", "2024-03")]["sales.streak"]) == 0

    async def test_dimension_is_not_null(self, exec_engine) -> None:
        """A null test over a grouped dimension column: store is never NULL →
        both streaks 1, 2, 3."""
        resp = await exec_engine.execute(_q(
            dimensions=["store"],
            measures=[ModelMeasure(
                formula="consecutive_periods(store is not None)", name="streak")],
        ))
        by = _by_store_month(resp)
        for store in ("A", "B"):
            assert int(by[(store, "2024-01")]["sales.streak"]) == 1
            assert int(by[(store, "2024-02")]["sales.streak"]) == 2
            assert int(by[(store, "2024-03")]["sales.streak"]) == 3


class TestNullTestTypingContract:
    async def test_null_test_in_value_position_rejected(self) -> None:
        name, msg = await _error(measures=[ModelMeasure(
            formula="consecutive_periods((hi_rev:sum is None) + 1)", name="x")])
        assert name == "ValueError", (name, msg)
        low = msg.lower()
        assert "consecutive_periods" in low, msg
        assert "boolean" in low, msg
        assert "value position" in low, msg
