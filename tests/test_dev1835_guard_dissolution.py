"""DEV-1835 guard dissolution (design D7/D8) — executed values for the shapes
the DEV-1504 G4/G5/G6/G7 guards, their post-projection twin, and the
``time_shift``-over-ranked guard reject today, plus the
``change``/``change_pct``-over-attached internal-error fix (fails today with
``RenderContextMissingFacilityError``).

Scenario coverage map (spec: openspec …/specs/queries/partitioned-aggregates):
  Transform over a bare windowed measure ............. TestWindowedTransforms
  Bare windowed measure inside arithmetic ............ TestWindowedComposites
  Filter-only reference to a bare windowed measure ... TestWindowedFilters
  One predicate mixing a bare windowed and a plain ... TestWindowedFilters
  Temporal transform over a bare first/last measure .. TestTemporalOverRanked
  Change over a partitioned aggregate executes ....... TestChangeOverAttached
"""

from __future__ import annotations

import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1835_fixtures import (
    COL_WM,
    CHANGE_OVER_LAST,
    CHANGE_PCT_OVER_LAST,
    CUMSUM_OVER_W90,
    ModelMeasure,
    TS_OVER_LAST,
    TS_OVER_W90,
    W90_RATIO,
    make_exec_engine,
    month_key,
    month_td,
    q,
    with_nulls,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _by_region_month(resp, col: str) -> dict:
    return {
        (r["orders.region"], month_key(r["orders.ordered_at"])): r[f"orders.{col}"]
        for r in resp.data
    }


def _assert_map(got: dict, expected: dict) -> None:
    assert set(got) == set(expected), sorted(map(str, got))
    for key, value in expected.items():
        if value is None:
            assert got[key] is None, f"{key}: expected NULL, got {got[key]!r}"
        else:
            assert got[key] is not None, f"{key}: expected {value}, got NULL"
            assert float(got[key]) == pytest.approx(value), f"{key}"


async def _run_x(engine, dialect, *, measures, filters=None, dims=("region",)):
    query = q(
        dimensions=list(dims), time_dimensions=month_td(),
        measures=measures, **({"filters": filters} if filters else {}),
    )
    resp = await engine.execute(query)
    dry = await engine.execute(query, dry_run=True)
    assert "__regroup__" not in dry.sql, dry.sql
    assert_scope_closed(dry.sql, dialect=dialect)
    return resp


class TestWindowedTransforms:
    """G4 dissolves: transforms accept a bare windowed input."""

    async def test_cumsum_over_bare_windowed(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="cumsum(amount:sum(window='90d'))", name="x"),
        ])
        _assert_map(_by_region_month(resp, "x"), CUMSUM_OVER_W90)

    async def test_time_shift_over_bare_windowed(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="time_shift(amount:sum(window='90d'), -1)", name="x"),
        ])
        expected = with_nulls(CUMSUM_OVER_W90.keys(), TS_OVER_W90)
        _assert_map(_by_region_month(resp, "x"), expected)


class TestWindowedComposites:
    """G5 dissolves: a bare windowed measure composes in arithmetic."""

    async def test_windowed_over_plain_ratio(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="amount:sum", name="m"),
            ModelMeasure(formula="amount:sum(window='90d') / amount:sum", name="x"),
        ])
        _assert_map(_by_region_month(resp, "x"), W90_RATIO)

    async def test_windowed_halved(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="amount:sum(window='1y') / 2", name="x"),
        ])
        _assert_map(
            _by_region_month(resp, "x"), {k: v / 2 for k, v in COL_WM.items()},
        )


class TestWindowedFilters:
    """G6/G7 and the post-projection twin dissolve; the OR that spans scopes
    keeps the no-common-scope directive (design D8)."""

    async def test_filter_only_windowed_reference(self, exec_backend) -> None:
        """G6 — hidden producer; qualifying rows keep their values and no
        internal name leaks into the projection."""
        dialect, engine = exec_backend
        resp = await _run_x(
            engine, dialect,
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            filters=["amount:sum(window='90d') > 40"],
        )
        _assert_map(_by_region_month(resp, "m"), {
            ("North", "2024-02"): 70.0, ("South", "2024-03"): 25.0,
            (None, "2024-03"): 60.0,
        })
        assert set(resp.data[0]) == {"orders.region", "orders.ordered_at", "orders.m"}

    async def test_one_predicate_windowed_vs_plain(self, exec_backend) -> None:
        """G7 — the whole predicate evaluates after attachment."""
        dialect, engine = exec_backend
        resp = await _run_x(
            engine, dialect,
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            filters=["amount:sum(window='90d') > amount:sum"],
        )
        _assert_map(_by_region_month(resp, "m"), {
            ("North", "2024-02"): 70.0, ("South", "2024-03"): 25.0,
        })

    async def test_and_conjuncts_split_windowed_and_row(self, exec_backend) -> None:
        """The post-projection twin dissolves for AND: the windowed conjunct
        routes post-attach, the row conjunct pre-aggregation."""
        dialect, engine = exec_backend
        resp = await _run_x(
            engine, dialect,
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            filters=["amount:sum(window='90d') > 40 and status = 'ok'"],
        )
        # OK rows only: South loses its March bucket; the windowed values are
        # computed over the OK population, then > 40 keeps (N,Feb) and (NULL,Mar).
        _assert_map(_by_region_month(resp, "m"), {
            ("North", "2024-02"): 70.0, (None, "2024-03"): 60.0,
        })

    async def test_separate_filters_match_the_split(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(
            engine, dialect,
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            filters=["amount:sum(window='90d') > 40", "status = 'ok'"],
        )
        _assert_map(_by_region_month(resp, "m"), {
            ("North", "2024-02"): 70.0, (None, "2024-03"): 60.0,
        })

    async def test_mixed_or_keeps_no_common_scope_directive(
        self, exec_backend,
    ) -> None:
        """An OR spanning the post-attach and row scopes cannot be split
        without changing meaning — the existing directive stays."""
        _, engine = exec_backend
        with pytest.raises((ValueError, NotImplementedError), match="no common scope"):
            await engine.execute(q(
                dimensions=["region"], time_dimensions=month_td(),
                filters=["amount:sum(window='90d') > 40 or status = 'ok'"],
                measures=[ModelMeasure(formula="amount:sum", name="m")],
            ))


class TestTemporalOverRanked:
    """The 7b.15e ``time_shift``-over-ranked guard dissolves."""

    async def test_time_shift_over_bare_last(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="time_shift(amount:last, -1)", name="x"),
        ])
        keys = {
            ("North", "2024-01"), ("North", "2024-02"), ("South", "2024-01"),
            ("South", "2024-03"), (None, "2024-03"),
        }
        _assert_map(_by_region_month(resp, "x"), with_nulls(keys, TS_OVER_LAST))

    async def test_change_over_bare_last(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="change(amount:last)", name="x"),
        ])
        keys = {
            ("North", "2024-01"), ("North", "2024-02"), ("South", "2024-01"),
            ("South", "2024-03"), (None, "2024-03"),
        }
        _assert_map(_by_region_month(resp, "x"), with_nulls(keys, CHANGE_OVER_LAST))

    async def test_change_pct_over_bare_last(self, exec_backend) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula="change_pct(amount:last)", name="x"),
        ])
        keys = {
            ("North", "2024-01"), ("North", "2024-02"), ("South", "2024-01"),
            ("South", "2024-03"), (None, "2024-03"),
        }
        _assert_map(
            _by_region_month(resp, "x"), with_nulls(keys, CHANGE_PCT_OVER_LAST),
        )


class TestChangeOverAttached:
    """``change``/``change_pct`` over an attached partition-grain value: the
    region total is constant across a region's buckets, so the only bucket with
    a previous calendar month sees a difference of exactly 0 — attached-value
    semantics, distinct from ``change(amount:sum)`` (which gives 40)."""

    @pytest.mark.parametrize(("op", "expected"), [
        ("change", {("North", "2024-02"): 0.0}),
        ("change_pct", {("North", "2024-02"): 0.0}),
    ])
    async def test_change_over_partitioned(self, exec_backend, op, expected) -> None:
        dialect, engine = exec_backend
        resp = await _run_x(engine, dialect, measures=[
            ModelMeasure(formula=f"{op}(amount:sum(partition_by=region))", name="x"),
        ])
        keys = {
            ("North", "2024-01"), ("North", "2024-02"), ("South", "2024-01"),
            ("South", "2024-03"), (None, "2024-03"),
        }
        _assert_map(_by_region_month(resp, "x"), with_nulls(keys, expected))
