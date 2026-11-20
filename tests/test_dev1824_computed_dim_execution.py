"""Executed ground truth (SQLite + DuckDB) for computed-dimension
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
  Unattributable partition key is rejected ...... TestDimensionErrorSurface
(Stage-boundary scenarios → tests/test_dev1824_multistage_boundary.py.)
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError

from slayer.core.query import QueryMetric

from tests._dev1824_fixtures import (
    BAND35,
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


RANK_DIM = "rank(sum(orders.amount, partition_by=orders.region), direction='desc')"


class TestBandedDimension:
    async def test_banded_partitioned_aggregate_groups_and_aggregates(
        self, exec_engine,
    ) -> None:
        # Pre-generalization behavior — pinned here as the regression
        # net under the generalized discovery.
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", {"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {
            (r["orders.region"], int(r["band"])): float(r["s"])
            for r in resp.data
        }
        assert got == {
            ("North", 0): pytest.approx(60.0), ("North", 1): pytest.approx(40.0),
            ("South", 1): pytest.approx(50.0), (None, 1): pytest.approx(60.0),
        }

    async def test_expression_over_two_partition_sets(self, exec_engine) -> None:
        gap = "sum(orders.amount, partition_by=orders.city) - sum(orders.amount, partition_by=orders.region)"
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", {"field": gap, "alias": "gap"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {
            (r["orders.region"], float(r["gap"])): float(r["s"])
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
            dimensions=["orders.region", {"field": RANK_DIM, "alias": "rr"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {
            r["orders.region"]: (int(r["rr"]), float(r["s"]))
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
            dimensions=[{"field": tier, "alias": "tier"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {r["tier"]: float(r["s"]) for r in resp.data}
        assert got == {"top": pytest.approx(100.0), "rest": pytest.approx(110.0)}

    async def test_context_grain_dimension_vs_measure(self, exec_engine) -> None:
        # Dimension context ranks the three regions (1/2/3); metric context
        # ranks the five result rows by their attached totals (1/1/1/4/5).
        as_dim = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city", {"field": RANK_DIM, "alias": "rr"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        dim_ranks = {r["orders.region"]: int(r["rr"]) for r in as_dim.data}
        assert len(as_dim.data) == 5
        assert dim_ranks == {"North": 1, None: 2, "South": 3}

        as_measure = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric=RANK_DIM, alias="rr")],
        ))
        measure_ranks = sorted(int(r["rr"]) for r in as_measure.data)
        assert measure_ranks == [1, 1, 1, 4, 5]


class TestFirstLastInDimension:
    async def test_region_latest_timestamp_as_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[
                "orders.region",
                {"field": "last(orders.ordered_at, partition_by=orders.region)", "alias": "lt"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {r["orders.region"]: str(r["lt"])[:10] for r in resp.data}
        assert got == REGION_LAST_AT

    async def test_region_last_value_as_dimension(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[
                "orders.region",
                {"field": "last(orders.amount, partition_by=orders.region)", "alias": "la"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {
            r["orders.region"]: (float(r["la"]), float(r["s"]))
            for r in resp.data
        }
        assert got == {
            "North": (pytest.approx(30.0), pytest.approx(100.0)),
            "South": (pytest.approx(25.0), pytest.approx(50.0)),
            None: (pytest.approx(60.0), pytest.approx(60.0)),
        }


class TestWindowInDimension:
    WBAND = (
        "CASE WHEN sum(orders.amount, window='90d', partition_by=orders.region) > 50 "
        "THEN 1 ELSE 0 END"
    )

    async def test_rolling_partition_total_bands_rows(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["orders.region", {"field": self.WBAND, "alias": "wband"}],
            time_dimensions=month_td(),
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"]),
             int(r["wband"])): float(r["s"])
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
        query = q(
            dimensions=["orders.region", {"field": self.WBAND, "alias": "wband"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with pytest.raises(ValueError, match=r"resolve its time dimension"):
            await exec_engine.execute(query)


class TestDimensionErrorSurface:
    async def test_bare_aggregate_rejected_with_directive(self, exec_engine) -> None:
        query = q(
            dimensions=[
                "orders.region",
                {"field": "CASE WHEN sum(orders.amount) > 50 THEN 1 ELSE 0 END",
                 "alias": "b"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with pytest.raises(ValueError, match=r"partition_by=") as ei:
            await exec_engine.execute(query)
        assert "__regroup__" not in str(ei.value)

    async def test_windowed_reaggregation_root_fails_closed(self, exec_engine) -> None:
        """window= on a re-aggregation's OUTER aggregation names the
        combination even when a time dimension is present."""
        query = q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[QueryMetric(
                metric="sum(sum(orders.amount, partition_by=[orders.region, orders.city]), "
                        "window='90d')",
                alias="w")])
        with pytest.raises(SlayerError) as ei:
            await exec_engine.execute(query)
        assert "window= on its outer aggregation" in str(ei.value)

    async def test_aggregate_over_attached_value_executes(self, exec_engine) -> None:
        # `b2` aggregates at the grain of `band`, whose own value needs a row
        # attach first — legal (shape B); value oracles live in
        # tests/test_dev1847_shape_b.py.
        query = q(
            dimensions=[
                {"field": BAND35, "alias": "band"},
                {"field": "sum(orders.amount, partition_by=band)", "alias": "b2"},
            ],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        resp = await exec_engine.execute(query)
        assert resp.data
        for row in resp.data:
            assert float(row["b2"]) == float(row["s"])

    async def test_measure_partitioned_by_computed_dimension_executes(
        self, exec_engine,
    ) -> None:
        query = q(
            dimensions=[{"field": BAND35, "alias": "band"}],
            metrics=[QueryMetric(
                metric="sum(orders.amount, partition_by=band)", alias="bt",
            )],
        )
        resp = await exec_engine.execute(query)
        by = {row["band"]: float(row["bt"]) for row in resp.data}
        assert by == {0: 60.0, 1: 150.0}

    async def test_grain_circular_dimension_rejected(self, exec_engine) -> None:
        circular = "CASE WHEN sum(orders.amount, partition_by=orders.selfband) > 10 THEN 1 ELSE 0 END"
        query = q(
            dimensions=["orders.region", {"field": circular, "alias": "selfband"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        with pytest.raises((NotImplementedError, ValueError)) as ei:
            await exec_engine.execute(query)
        assert "selfband" in str(ei.value)
        assert "__regroup__" not in str(ei.value)

    async def test_cross_model_unattributable_partition_rejected(
        self, exec_engine,
    ) -> None:
        # The blanket cross-dataset-source guard is lifted; a cross-dataset
        # aggregate whose partition key is unreachable from its root still fails
        # closed, now naming the key.
        cband = (
            "CASE WHEN sum(customers.spend, partition_by=orders.region) > 100 "
            "THEN 1 ELSE 0 END"
        )
        query = q(
            dimensions=["orders.region", {"field": cband, "alias": "cband"}],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
            to_many_handling="broadcast",
        )
        with pytest.raises(ValueError, match=r"partition_by") as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "region" in msg
        assert "__regroup__" not in msg
