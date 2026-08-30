"""DEV-1837 filter placement across attach and transform phases (design D3/D9;
spec requirement "Filters compose across attach and transform phases").

Band-only monthly series (dims ``[band]`` + month, from the fixture rows):
band 0: Jan 30, Feb 30 · band 1: Jan 25, Feb 40, Mar 85 (rows 6+7).
time_shift(-1): (0,Feb)=30 · (1,Feb)=25 · (1,Mar)=40; change: (0,Feb)=0 ·
(1,Feb)=15 · (1,Mar)=45.

With ``status == 'ok'`` row-inherited into the producer the city totals become
CityA 30 / CityB 40 / NULL 30 / CityC 25 / CityD 60, so CityC's band drops to
0 and ``band == 1`` keeps only rows 3 and 7 → 100.
"""

from __future__ import annotations

import pytest
import sqlglot

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1837_fixtures import (
    BAND35,
    ModelMeasure,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
)
from tests._engine_helpers import _extract_cte_body


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


BAND = {"expression": BAND35, "name": "band"}


def _by_band_month(resp, *values):
    return {
        (int(r["orders.band"]), month_key(r["orders.ordered_at"])):
            tuple(r[f"orders.{v}"] for v in values)
        for r in resp.data
    }


class TestPostFilterOnShiftedValue:
    async def test_change_filter_keeps_only_grown_groups(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[BAND],
            time_dimensions=month_td(),
            filters=["change(amount:sum) > 0"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="change(amount:sum)", name="c"),
            ],
        ))
        got = _by_band_month(resp, "m", "c")
        assert set(got) == {(1, "2024-02"), (1, "2024-03")}
        assert float(got[(1, "2024-02")][0]) == pytest.approx(40.0)
        assert float(got[(1, "2024-02")][1]) == pytest.approx(15.0)
        assert float(got[(1, "2024-03")][0]) == pytest.approx(85.0)
        assert float(got[(1, "2024-03")][1]) == pytest.approx(45.0)

    async def test_no_placeholder_in_filtered_sql(self, exec_engine) -> None:
        dry = await exec_engine.execute(q(
            dimensions=[BAND],
            time_dimensions=month_td(),
            filters=["change(amount:sum) > 0"],
            measures=[ModelMeasure(formula="change(amount:sum)", name="c")],
        ), dry_run=True)
        assert dry.sql is not None
        assert "__regroup__" not in dry.sql, dry.sql


class TestConjunctionSplits:
    """D9 — top-level AND conjuncts of ONE filter string route independently,
    equal to the separate-strings form."""

    async def test_band_and_change_conjunction_splits(self, exec_engine) -> None:
        base_kwargs = dict(
            dimensions=[BAND],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="change(amount:sum)", name="c"),
            ],
        )
        anded = await exec_engine.execute(q(
            filters=["band == 1 and change(amount:sum) > 0"], **base_kwargs,
        ))
        separate = await exec_engine.execute(q(
            filters=["band == 1", "change(amount:sum) > 0"], **base_kwargs,
        ))
        got = _by_band_month(anded, "m", "c")
        assert set(got) == {(1, "2024-02"), (1, "2024-03")}
        assert float(got[(1, "2024-02")][1]) == pytest.approx(15.0)
        assert float(got[(1, "2024-03")][1]) == pytest.approx(45.0)
        assert got == _by_band_month(separate, "m", "c")

    async def test_band_and_row_predicate_conjunction_splits(self, exec_engine) -> None:
        base_kwargs = dict(
            dimensions=[BAND],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        )
        anded = await exec_engine.execute(q(
            filters=["band == 1 and status == 'ok'"], **base_kwargs,
        ))
        separate = await exec_engine.execute(q(
            filters=["band == 1", "status == 'ok'"], **base_kwargs,
        ))
        got = {int(r["orders.band"]): float(r["orders.m"]) for r in anded.data}
        assert got == {1: pytest.approx(100.0)}
        assert got == {
            int(r["orders.band"]): float(r["orders.m"]) for r in separate.data
        }

    async def test_mixed_or_still_fails_closed(self, exec_engine) -> None:
        """An OR across phases has no common scope — the split-the-filter
        directive survives the AND lift."""
        with pytest.raises(NotImplementedError, match=r"separate filters"):
            await exec_engine.execute(q(
                dimensions=[BAND],
                time_dimensions=month_td(),
                filters=["band == 1 or change(amount:sum) > 0"],
                measures=[ModelMeasure(formula="change(amount:sum)", name="c")],
            ))


class TestShiftedCteRegroupAwareness:
    """Design D3 — a row-lowered predicate over the computed dimension reaches
    the shifted CTE's WHERE, rendered against the producer column (never the
    placeholder), and the shifted FROM carries the producer join."""

    QUERY_KWARGS = dict(
        dimensions=[BAND],
        time_dimensions=month_td(),
        filters=["band == 1"],
        measures=[
            ModelMeasure(formula="amount:sum", name="m"),
            ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
        ],
    )

    async def test_band_filter_with_time_shift_executes(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(**self.QUERY_KWARGS))
        got = _by_band_month(resp, "m", "prev")
        assert set(got) == {(1, "2024-01"), (1, "2024-02"), (1, "2024-03")}
        assert got[(1, "2024-01")][1] is None
        assert float(got[(1, "2024-02")][1]) == pytest.approx(25.0)
        assert float(got[(1, "2024-03")][1]) == pytest.approx(40.0)

    async def test_shifted_cte_where_renders_against_producer(self) -> None:
        sql = await gen(q(**self.QUERY_KWARGS))
        assert "__regroup__" not in sql, sql
        assert_scope_closed(sql, dialect="duckdb")
        shifted = _extract_cte_body(sql, r"shifted_\w+")
        # The shifted FROM carries the producer join, and the lowered predicate
        # compares the PRODUCER's column (Codex F2 — not just any WHERE).
        assert "JOIN" in shifted.upper(), shifted
        assert "_cm_" in shifted, shifted
        where = sqlglot.parse_one(shifted, read="duckdb").args.get("where")
        assert where is not None, shifted
        where_sql = where.sql(dialect="duckdb")
        assert "_cm_" in where_sql, where_sql
        assert "__regroup__" not in where_sql, where_sql
        # Parity with base: the same predicate narrows the base CTE too.
        base = _extract_cte_body(sql, r"\bbase\b")
        base_where = sqlglot.parse_one(base, read="duckdb").args.get("where")
        assert base_where is not None, base
