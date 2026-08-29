"""DEV-1824 executed ground truth (SQLite + DuckDB) for computed-dimension
expressions — the ``queries/computed-dimensions`` delta scenarios. Expectations
hand-computed in ``tests/_dev1824_fixtures.py``.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  Banded partitioned aggregate as a dimension ... TestBandedDimension
  Expression over two different partition sets .. TestBandedDimension
  Rank of partitions as a bandable dimension .... TestTransformInDimension
  Context grain distinguishes dimension/measure . TestTransformInDimension
  Latest timestamp per partition as a dimension . TestFirstLastInDimension
  Rolling partition total as a dimension ........ TestWindowInDimension
  Fails cleanly without a time dimension ........ TestWindowInDimension
  Bare aggregate in a dimension is rejected ..... TestDimensionErrorSurface
  Aggregate over an attached value is rejected .. TestDimensionErrorSurface
  Cross-model aggregate source is rejected ...... TestDimensionErrorSurface
(Stage-boundary scenarios → tests/test_dev1824_multistage_boundary.py.)
"""

from __future__ import annotations

import pytest

from tests._dev1824_fixtures import (
    BAND35,
    ModelMeasure,
    REGION_LAST_AT,
    make_exec_engine,
    month_key,
    month_td,
    q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


RANK_DIM = "rank(amount:sum(partition_by=region))"


class TestBandedDimension:
    async def test_banded_partitioned_aggregate_groups_and_aggregates(
        self, exec_engine,
    ) -> None:
        # Pre-DEV-1824 behavior (DEV-1740 B2) — pinned here as the regression
        # net under the generalized discovery.
        resp = await exec_engine.execute(q(
            dimensions=["region", {"expression": BAND35, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            (r["orders.region"], int(r["orders.band"])): float(r["orders.s"])
            for r in resp.data
        }
        assert got == {
            ("North", 0): pytest.approx(60.0), ("North", 1): pytest.approx(40.0),
            ("South", 1): pytest.approx(50.0), (None, 1): pytest.approx(60.0),
        }

    async def test_expression_over_two_partition_sets(self, exec_engine) -> None:
        gap = "amount:sum(partition_by=city) - amount:sum(partition_by=region)"
        resp = await exec_engine.execute(q(
            dimensions=["region", {"expression": gap, "name": "gap"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            (r["orders.region"], float(r["orders.gap"])): float(r["orders.s"])
            for r in resp.data
        }
        assert got == {
            ("North", -70.0): pytest.approx(60.0),
            ("North", -60.0): pytest.approx(40.0),
            ("South", 0.0): pytest.approx(50.0),
            (None, 0.0): pytest.approx(60.0),
        }


class TestTransformInDimension:
    async def test_rank_of_partitions_as_dimension(self, exec_engine) -> None:
        # Region totals 100/60/50 rank North=1, NULL=2, South=3 at REGION grain.
        resp = await exec_engine.execute(q(
            dimensions=["region", {"expression": RANK_DIM, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            r["orders.region"]: (int(r["orders.rr"]), float(r["orders.s"]))
            for r in resp.data
        }
        assert got == {
            "North": (1, pytest.approx(100.0)),
            None: (2, pytest.approx(60.0)),
            "South": (3, pytest.approx(50.0)),
        }

    async def test_banding_by_rank_is_legal(self, exec_engine) -> None:
        tier = f"CASE WHEN {RANK_DIM} <= 1 THEN 'top' ELSE 'rest' END"
        resp = await exec_engine.execute(q(
            dimensions=[{"expression": tier, "name": "tier"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {r["orders.tier"]: float(r["orders.s"]) for r in resp.data}
        assert got == {"top": pytest.approx(100.0), "rest": pytest.approx(110.0)}

    async def test_context_grain_dimension_vs_measure(self, exec_engine) -> None:
        # Dimension context ranks the three regions (1/2/3); measure context
        # ranks the five result rows by their attached totals (1/1/1/4/5).
        as_dim = await exec_engine.execute(q(
            dimensions=["region", "city", {"expression": RANK_DIM, "name": "rr"}],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        dim_ranks = {r["orders.region"]: int(r["orders.rr"]) for r in as_dim.data}
        assert len(as_dim.data) == 5
        assert dim_ranks == {"North": 1, None: 2, "South": 3}

        as_measure = await exec_engine.execute(q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=RANK_DIM, name="rr")],
        ))
        measure_ranks = sorted(int(r["orders.rr"]) for r in as_measure.data)
        assert measure_ranks == [1, 1, 1, 4, 5]


class TestFirstLastInDimension:
    async def test_region_latest_timestamp_as_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[
                "region",
                {"expression": "ordered_at:last(partition_by=region)", "name": "lt"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {r["orders.region"]: str(r["orders.lt"])[:10] for r in resp.data}
        assert got == REGION_LAST_AT

    async def test_region_last_value_as_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[
                "region",
                {"expression": "amount:last(partition_by=region)", "name": "la"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            r["orders.region"]: (float(r["orders.la"]), float(r["orders.s"]))
            for r in resp.data
        }
        assert got == {
            "North": (pytest.approx(30.0), pytest.approx(100.0)),
            "South": (pytest.approx(25.0), pytest.approx(50.0)),
            None: (pytest.approx(60.0), pytest.approx(60.0)),
        }


class TestWindowInDimension:
    WBAND = (
        "CASE WHEN amount:sum(window='90d', partition_by=region) > 50 "
        "THEN 1 ELSE 0 END"
    )

    async def test_rolling_partition_total_bands_rows(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region", {"expression": self.WBAND, "name": "wband"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"]),
             int(r["orders.wband"])): float(r["orders.s"])
            for r in resp.data
        }
        # Trailing-90d region totals as of each month: (N,Jan)=30 (N,Feb)=100
        # (S,Jan)=25 (S,Mar)=50 (NULL,Mar)=60 — banded at > 50.
        assert got == {
            ("North", "2024-01", 0): pytest.approx(30.0),
            ("North", "2024-02", 1): pytest.approx(70.0),
            ("South", "2024-01", 0): pytest.approx(25.0),
            ("South", "2024-03", 0): pytest.approx(25.0),
            (None, "2024-03", 1): pytest.approx(60.0),
        }

    async def test_fails_cleanly_without_time_dimension(self, exec_engine) -> None:
        with pytest.raises(ValueError, match=r"resolve its time dimension"):
            await exec_engine.execute(q(
                dimensions=["region", {"expression": self.WBAND, "name": "wband"}],
                measures=[ModelMeasure(formula="amount:sum", name="s")],
            ))


class TestDimensionErrorSurface:
    async def test_bare_aggregate_rejected_with_directive(self, exec_engine) -> None:
        with pytest.raises(ValueError, match=r"partition_by=") as ei:
            await exec_engine.execute(q(
                dimensions=[
                    "region",
                    {"expression": "CASE WHEN amount:sum > 50 THEN 1 ELSE 0 END",
                     "name": "b"},
                ],
                measures=[ModelMeasure(formula="amount:sum", name="s")],
            ))
        assert "__regroup__" not in str(ei.value)

    async def test_aggregate_over_attached_value_rejected(self, exec_engine) -> None:
        # `b2` aggregates at the grain of `band`, whose own value needs a row
        # attach first — the requires_nested_attach shape fails closed.
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await exec_engine.execute(q(
                dimensions=[
                    {"expression": BAND35, "name": "band"},
                    {"expression": "amount:sum(partition_by=band)", "name": "b2"},
                ],
                measures=[ModelMeasure(formula="amount:sum", name="s")],
            ))
        assert "not yet supported" in str(ei.value).lower()
        assert "__regroup__" not in str(ei.value)

    async def test_measure_partitioned_by_computed_dimension_rejected(
        self, exec_engine,
    ) -> None:
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await exec_engine.execute(q(
                dimensions=[{"expression": BAND35, "name": "band"}],
                measures=[ModelMeasure(
                    formula="amount:sum(partition_by=band)", name="bt",
                )],
            ))
        assert "not yet supported" in str(ei.value).lower()
        assert "__regroup__" not in str(ei.value)

    async def test_grain_circular_dimension_rejected(self, exec_engine) -> None:
        circular = "CASE WHEN amount:sum(partition_by=selfband) > 10 THEN 1 ELSE 0 END"
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await exec_engine.execute(q(
                dimensions=["region", {"expression": circular, "name": "selfband"}],
                measures=[ModelMeasure(formula="amount:sum", name="s")],
            ))
        assert "selfband" in str(ei.value)
        assert "__regroup__" not in str(ei.value)

    async def test_cross_model_aggregate_source_rejected(self, exec_engine) -> None:
        cband = (
            "CASE WHEN customers.spend:sum(partition_by=region) > 100 "
            "THEN 1 ELSE 0 END"
        )
        with pytest.raises(
            NotImplementedError,
            match=r"cross-model aggregate source inside a computed dimension",
        ):
            await exec_engine.execute(q(
                dimensions=["region", {"expression": cband, "name": "cband"}],
                measures=[ModelMeasure(formula="amount:sum", name="s")],
            ))
