"""Attach-dedup (design D6) — one producer per structural identity,
serving every consuming position across both attach phases; producers that
differ in any input stay separate.

Producer counting is by CTE-name prefix: post-migration every aggregate
producer renders under the uniform ``_cm_`` naming (design D3) and the ``_wm_``
/ ``_rk_`` prefixes are gone — so the counts double as the naming-migration
check and fail today (duplicate producers under the old prefixes).

Scenario coverage map (spec: openspec …/specs/queries/partitioned-aggregates):
  Same aggregate in both roles shares one producer ... TestDedupPositive
  Bare and explicit partition twins … one shared producer  TestDedupPositive
  Different producer inputs stay separate ............ TestDedupNegative
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1835_fixtures import (
    COL_WM,
    LAST_BY_ORDERED,
    LAST_BY_SHIPPED,
    OK_W90,
    REGION_LAST,
    REGION_TOTAL,
    TRAILING_45D_REGION,
    TRAILING_90D_REGION,
    cte_aliases,
    make_exec_engine,
    make_shipped_exec_engine,
    month_key,
    month_td,
    q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def shipped_backend(request):
    async for engine in make_shipped_exec_engine(request):
        yield request.param, engine


def _by_region(resp, col: str) -> dict:
    return {r["orders.region"]: float(r[col]) for r in resp.data}


def _by_region_month(resp, col: str) -> dict:
    return {
        (r["orders.region"], month_key(r["orders.ordered_at"])):
            float(r[col])
        for r in resp.data
    }


def _assert_map(got: dict, expected: dict) -> None:
    assert set(got) == set(expected), sorted(map(str, got))
    for key, value in expected.items():
        assert got[key] == pytest.approx(value), f"{key}"


async def _producer_counts(engine, query, dialect: str) -> dict:
    dry = await engine.execute(query, dry_run=True)
    return {
        prefix: len(cte_aliases(dry.sql, prefix, dialect=dialect))
        for prefix in ("_cm_", "_wm_", "_rk_")
    }


class TestDedupPositive:
    async def test_dual_role_aggregate_shares_one_producer(
        self, exec_backend,
    ) -> None:
        """The same region total inside a computed dimension AND as a selected
        metric: one producer relation, both roles correct."""
        dialect, engine = exec_backend
        band = "CASE WHEN sum(orders.amount, partition_by=orders.region) > 55 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["orders.region", {"field": band, "alias": "rband"}],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
            ],
        )
        resp = await engine.execute(query)
        got = {
            r["orders.region"]: (int(r["rband"]), float(r["rt"]))
            for r in resp.data
        }
        assert got == {
            "North": (1, 100.0), "South": (0, 50.0), None: (1, 60.0),
        }
        dry = await engine.execute(query, dry_run=True)
        cm = cte_aliases(dry.sql, "_cm_", dialect=dialect)
        assert len(cm) == 1, cm
        # D6 — a dual-phase producer keeps the combined-phase naming regime.
        assert cm[0].startswith("_cm_orders__"), cm
        assert not cte_aliases(dry.sql, "_wm_", dialect=dialect)
        assert not cte_aliases(dry.sql, "_rk_", dialect=dialect)

    async def test_windowed_bare_explicit_twins_share_one_producer(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, window='1y')", alias="wb"),
                QueryMetric(
                    metric="sum(orders.amount, window='1y', partition_by=orders.region)",
                    alias="we",
                ),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region_month(resp, "wb"), COL_WM)
        _assert_map(_by_region_month(resp, "we"), COL_WM)
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 1, "_wm_": 0, "_rk_": 0}, counts

    async def test_ranked_bare_explicit_twins_share_one_producer(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["orders.region"],
            metrics=[
                QueryMetric(metric="last(orders.amount)", alias="lb"),
                QueryMetric(metric="last(orders.amount, partition_by=orders.region)", alias="le"),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region(resp, "lb"), REGION_LAST)
        _assert_map(_by_region(resp, "le"), REGION_LAST)
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 1, "_wm_": 0, "_rk_": 0}, counts

    async def test_dual_role_beside_bare_windowed_is_two_producers(
        self, exec_backend,
    ) -> None:
        """D10 subsumption: the dual-role total dedups to one producer, the
        bare windowed metric adds exactly one more."""
        dialect, engine = exec_backend
        band = "CASE WHEN sum(orders.amount, partition_by=orders.region) > 55 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["orders.region", {"field": band, "alias": "rband"}],
            time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, partition_by=orders.region)", alias="rt"),
                QueryMetric(metric="sum(orders.amount, window='1y')", alias="w"),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region_month(resp, "w"), COL_WM)
        for r in resp.data:
            assert float(r["rt"]) == pytest.approx(
                REGION_TOTAL[r["orders.region"]],
            )
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 2, "_wm_": 0, "_rk_": 0}, counts


class TestDedupNegative:
    async def test_different_window_durations_stay_separate(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, window='90d')", alias="w90"),
                QueryMetric(metric="sum(orders.amount, window='45d')", alias="w45"),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region_month(resp, "w90"), TRAILING_90D_REGION)
        _assert_map(_by_region_month(resp, "w45"), TRAILING_45D_REGION)
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 2, "_wm_": 0, "_rk_": 0}, counts

    async def test_different_measure_filters_stay_separate(
        self, exec_backend,
    ) -> None:
        dialect, engine = exec_backend
        query = q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[
                QueryMetric(metric="sum(orders.amount, window='90d')", alias="w"),
                QueryMetric(metric="sum(orders.ok_amount, window='90d')", alias="wok"),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region_month(resp, "w"), TRAILING_90D_REGION)
        _assert_map(_by_region_month(resp, "wok"), OK_W90)
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 2, "_wm_": 0, "_rk_": 0}, counts

    async def test_different_ranking_columns_stay_separate(
        self, shipped_backend,
    ) -> None:
        """``last(amount)`` (default ``ordered_at``) vs an explicit
        ``shipped_at`` ranking field — North's values differ (30 vs 10)."""
        dialect, engine = shipped_backend
        query = q(
            dimensions=["orders.region"],
            metrics=[
                QueryMetric(metric="last(orders.amount)", alias="lo"),
                QueryMetric(metric="last(orders.amount, orders.shipped_at)", alias="ls"),
            ],
        )
        resp = await engine.execute(query)
        _assert_map(_by_region(resp, "lo"), LAST_BY_ORDERED)
        _assert_map(_by_region(resp, "ls"), LAST_BY_SHIPPED)
        counts = await _producer_counts(engine, query, dialect)
        assert counts == {"_cm_": 2, "_wm_": 0, "_rk_": 0}, counts
