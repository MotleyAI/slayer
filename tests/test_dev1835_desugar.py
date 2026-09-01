"""DEV-1835 desugar (design D1) — a bare windowed / first-last measure behaves
as a partitioned aggregate at the full projected grain, in measure, order, and
filter roles, with its established public keys and executed values.

Scenario coverage map (spec: openspec …/specs/queries/partitioned-aggregates):
  Bare and explicit partition twins are equivalent ... TestBareExplicitTwins
  Migrated families keep their executed values ....... TestMigratedFamilyValuePins
  A filtered measure is cardinality-neutral .......... TestFilteredMeasureNeutrality
(…/specs/queries/computed-dimensions):
  A dual-role aggregate coexists with a bare windowed  TestDualRoleWithBareWindowed

Value pins for already-supported shapes must pass BEFORE and after the
migration; the twin-combined, order-only-windowed, and dual-role cases fail
today (duplicate producers / silent window drop / the DEV-1835 arm) and flip
with the implementation.
"""

from __future__ import annotations

import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1835_fixtures import (
    CITY_LAST_RC,
    CITY_MONTH_LAST,
    CITY_WM,
    COL_WM,
    GRAND_TOTAL,
    ModelMeasure,
    OK_LAST,
    OK_W90,
    ORDER_BY_W90_DESC,
    REGION_FIRST,
    REGION_LAST,
    REGION_MONTH_TOTAL,
    REGION_TOTAL,
    TRAILING_45D_REGION,
    make_exec_engine,
    make_shipped_exec_engine,
    month_key,
    month_td,
    q,
    _by_region,
    _by_region_month,
    _assert_map,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def shipped_backend(request):
    async for engine in make_shipped_exec_engine(request):
        yield request.param, engine


async def _clean_dry(engine, query, dialect: str) -> str:
    dry = await engine.execute(query, dry_run=True)
    assert "__regroup__" not in dry.sql, dry.sql
    assert_scope_closed(dry.sql, dialect=dialect)
    return dry.sql


class TestBareExplicitTwins:
    """The bare form and an explicit ``partition_by=`` naming the full
    projected grain are the same aggregation (design D1). Producer-count
    assertions for the combined query live in tests/test_dev1835_dedup.py."""

    async def test_windowed_twin_queries_agree(self, exec_backend) -> None:
        _, engine = exec_backend
        bare = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='1y')", name="w")],
        ))
        explicit = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="amount:sum(window='1y', partition_by=region)", name="w",
            )],
        ))
        _assert_map(_by_region_month(bare, "w"), COL_WM)
        _assert_map(_by_region_month(explicit, "w"), COL_WM)

    async def test_ranked_twin_queries_agree(self, exec_backend) -> None:
        _, engine = exec_backend
        bare = await engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:last", name="l")],
        ))
        explicit = await engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="amount:last(partition_by=region)", name="l",
            )],
        ))
        _assert_map(_by_region(bare, "l"), REGION_LAST)
        _assert_map(_by_region(explicit, "l"), REGION_LAST)

    async def test_windowed_twins_combined_agree(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum(window='1y')", name="wb"),
                ModelMeasure(
                    formula="amount:sum(window='1y', partition_by=region)",
                    name="we",
                ),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region_month(resp, "wb"), COL_WM)
        _assert_map(_by_region_month(resp, "we"), COL_WM)
        await _clean_dry(engine, query, dialect)


class TestMigratedFamilyValuePins:
    """Executed values of the currently-supported bare shapes — identical
    before and after the renderer-arm deletion, on both engines."""

    async def test_bare_windowed_at_city_grain(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(window='1y')", name="w"),
            ],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], r["orders.city"],
             month_key(r["orders.ordered_at"])): r["orders.w"]
            for r in resp.data
        }
        _assert_map(got, CITY_WM)
        await _clean_dry(engine, query, dialect)

    async def test_bare_windowed_duration_sensitivity(self, exec_backend) -> None:
        """45d drops North's January rows from February's window (90 → 45d is
        a real duration change, not a running total)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='45d')", name="w")],
        ))
        _assert_map(_by_region_month(resp, "w"), TRAILING_45D_REGION)

    async def test_bare_last_at_city_grain(self, exec_backend) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:last", name="l")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], r["orders.city"]): r["orders.l"]
            for r in resp.data
        }
        _assert_map(got, CITY_LAST_RC)
        await _clean_dry(engine, query, dialect)

    async def test_bare_last_with_time_dimension(self, exec_backend) -> None:
        """The month bucket joins the ranked grain (every visible slot)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region", "city"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:last", name="l")],
        ))
        got = {
            (r["orders.region"], r["orders.city"],
             month_key(r["orders.ordered_at"])): r["orders.l"]
            for r in resp.data
        }
        _assert_map(got, CITY_MONTH_LAST)

    async def test_bare_first(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:first", name="f")],
        ))
        _assert_map(_by_region(resp, "f"), REGION_FIRST)

    async def test_bare_windowed_time_dimension_only(self, exec_backend) -> None:
        """Codex F6 — no ordinary dimensions: the producer grain is the time
        bucket alone (90d from March's end still reaches every January row)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="w")],
        ))
        got = {
            month_key(r["orders.ordered_at"]): r["orders.w"] for r in resp.data
        }
        _assert_map(got, {"2024-01": 55.0, "2024-02": 125.0, "2024-03": 210.0})

    async def test_keyless_last_is_single_row(self, exec_backend) -> None:
        """Empty grain — the producer degenerates to one row (rendered as a
        single-row CROSS JOIN attach post-migration)."""
        dialect, engine = exec_backend
        query = q(measures=[
            ModelMeasure(formula="amount:sum", name="m"),
            ModelMeasure(formula="amount:last", name="l"),
        ])
        resp = await engine.execute(query)
        assert len(resp.data) == 1
        assert float(resp.data[0]["orders.m"]) == pytest.approx(GRAND_TOTAL)
        assert float(resp.data[0]["orders.l"]) == pytest.approx(60.0)
        await _clean_dry(engine, query, dialect)


class TestOrderOnlyHiddenProducers:
    async def test_order_only_bare_windowed_orders_by_the_window(
        self, exec_backend,
    ) -> None:
        """An order-only ``amount:sum(window='90d')`` beside a declared plain
        ``amount:sum`` must order by the WINDOWED value. Today the order ref
        silently binds to the plain measure (the canonical column name
        ``amount_sum`` matches it and the window is dropped — no ``_wm_`` CTE
        is planned); the desugar routes the order role through a hidden
        producer instead (design D1), which moves (South, 2024-03) from last
        place (plain 25) to third (windowed 50)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            order=[{"column": "amount:sum(window='90d')", "direction": "desc"}],
        ))
        got = [
            (r["orders.region"], month_key(r["orders.ordered_at"]))
            for r in resp.data
        ]
        assert got == ORDER_BY_W90_DESC
        assert set(resp.data[0]) == {"orders.region", "orders.ordered_at", "orders.m"}

    async def test_order_only_bare_last_orders_by_the_ranked_value(
        self, exec_backend,
    ) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
            order=[{"column": "amount:last", "direction": "desc"}],
        ))
        # REGION_LAST desc: NULL 60, North 30, South 25.
        assert [r["orders.region"] for r in resp.data] == [None, "North", "South"]
        assert set(resp.data[0]) == {"orders.region", "orders.m"}

    async def test_filter_only_bare_last(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"],
            filters=["amount:last > 28"],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        ))
        _assert_map(_by_region(resp, "m"), {"North": 100.0, None: 60.0})


class TestFilteredMeasureNeutrality:
    """Measure-local filters restrict producer rows only (design D8)."""

    async def test_filtered_windowed_beside_unfiltered(self, exec_backend) -> None:
        _, engine = exec_backend
        base = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        ))
        plus = await engine.execute(q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="ok_amount:sum(window='90d')", name="w"),
            ],
        ))
        _assert_map(_by_region_month(plus, "m"), _by_region_month(base, "m"))
        _assert_map(_by_region_month(plus, "m"), REGION_MONTH_TOTAL)
        _assert_map(_by_region_month(plus, "w"), OK_W90)

    async def test_filtered_last_beside_unfiltered(self, exec_backend) -> None:
        """``ok_amount:last`` follows the latest OK row; the all-NULL
        ``nomatch:last`` discriminates the filter reaching the producer."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["region"],
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="ok_amount:last", name="ol"),
                ModelMeasure(formula="nomatch:last", name="nl"),
            ],
        ))
        assert len(resp.data) == 3
        _assert_map(_by_region(resp, "m"), REGION_TOTAL)
        _assert_map(_by_region(resp, "ol"), OK_LAST)
        assert all(r["orders.nl"] is None for r in resp.data)


class TestDerivedColumnDimension:
    """Codex F3 — the "every dimension kind" requirement names derived
    (``Column.sql``) columns; ``city_key = LOWER(city)`` on the shipped-model
    variant."""

    async def test_derived_dimension_with_bare_last(self, shipped_backend) -> None:
        _, engine = shipped_backend
        resp = await engine.execute(q(
            dimensions=["region", "city_key"],
            measures=[ModelMeasure(formula="amount:last", name="l")],
        ))
        got = {
            (r["orders.region"], r["orders.city_key"]): r["orders.l"]
            for r in resp.data
        }
        _assert_map(got, {
            ("North", "citya"): 20.0, ("North", "cityb"): 40.0,
            ("North", None): 30.0, ("South", "cityc"): 25.0,
            (None, "cityd"): 60.0,
        })

    async def test_derived_dimension_with_bare_windowed(
        self, shipped_backend,
    ) -> None:
        _, engine = shipped_backend
        resp = await engine.execute(q(
            dimensions=["region", "city_key"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum(window='1y')", name="w")],
        ))
        got = {
            (r["orders.region"], r["orders.city_key"],
             month_key(r["orders.ordered_at"])): r["orders.w"]
            for r in resp.data
        }
        _assert_map(got, {
            ("North", "citya", "2024-01"): 30.0,
            ("North", "cityb", "2024-02"): 40.0,
            ("North", None, "2024-02"): 30.0,
            ("South", "cityc", "2024-01"): 25.0,
            ("South", "cityc", "2024-03"): 50.0,
            (None, "cityd", "2024-03"): 60.0,
        })


class TestDualRoleWithBareWindowed:
    async def test_dual_role_aggregate_beside_bare_windowed(
        self, exec_backend,
    ) -> None:
        """D10 subsumption + the lifted coexistence arm in one query: the same
        region total as a banding dimension AND a measure, next to a bare
        windowed measure."""
        dialect, engine = exec_backend
        band = "CASE WHEN amount:sum(partition_by=region) > 55 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["region", {"expression": band, "name": "rband"}],
            time_dimensions=month_td(),
            measures=[
                ModelMeasure(formula="amount:sum", name="m"),
                ModelMeasure(formula="amount:sum(partition_by=region)", name="rt"),
                ModelMeasure(formula="amount:sum(window='1y')", name="w"),
            ],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], int(r["orders.rband"]),
             month_key(r["orders.ordered_at"])):
                (r["orders.m"], r["orders.rt"], r["orders.w"])
            for r in resp.data
        }
        expected = {
            ("North", 1, "2024-01"): (30.0, 100.0, 30.0),
            ("North", 1, "2024-02"): (70.0, 100.0, 100.0),
            ("South", 0, "2024-01"): (25.0, 50.0, 25.0),
            ("South", 0, "2024-03"): (25.0, 50.0, 50.0),
            (None, 1, "2024-03"): (60.0, 60.0, 60.0),
        }
        assert set(got) == set(expected)
        assert len(resp.data) == len(expected)
        for key, values in expected.items():
            for actual, wanted in zip(got[key], values):
                assert float(actual) == pytest.approx(wanted), f"{key}"
        await _clean_dry(engine, query, dialect)
