"""Shapes that were guarded and now compile by construction.

Executed on SQLite + DuckDB. Values are asserted differentially against the
same query without the new filter (the spec's parity language), so the oracles
do not re-hand-compute the engine's arithmetic.

Formerly guarded: the cross-dataset partition filter raised a
``NotImplementedError``; the OR-mixing shapes raised the "separate
filters" ``NotImplementedError``.
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1865_fixtures import (
    BAND35,
    CITY_TOTAL,
    REGION_TOTAL,
    q,
    rows_by,
)

RK_REGION = "customers.regions.name"
RK_TIER = "customers.tier"


class TestCrossModelPartitionedFilter:
    """cross-model-aggregates: filter on a cross-dataset partitioned aggregate."""

    async def test_partition_key_among_dims_and_aggregate_not_selected(
        self, exec_engine,
    ) -> None:
        base = q(
            dimensions=["customers.regions.name", "customers.tier"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
        )
        unfiltered = rows_by(await exec_engine.execute(base), RK_REGION, RK_TIER)
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["customers.regions.name", "customers.tier"],
            filter=["sum(customers.spend, partition_by=customers.regions.name) > 100"],
            metrics=[QueryMetric(metric="sum(customers.spend)", alias="sp")],
        )), RK_REGION, RK_TIER)
        # RegN partition total 300 > 100 survives; RegS total 50 does not.
        assert {k[0] for k in filtered} == {"RegN"}
        assert set(filtered) <= set(unfiltered)
        for key, row in filtered.items():
            assert float(row["sp"]) == pytest.approx(
                float(unfiltered[key]["sp"])
            )

    async def test_aggregate_also_selected_is_value_preserving(
        self, exec_engine,
    ) -> None:
        base = q(
            dimensions=["customers.regions.name", "customers.tier"],
            metrics=[
                QueryMetric(metric="sum(customers.spend)", alias="sp"),
                QueryMetric(
                    metric="sum(customers.spend, partition_by=customers.regions.name)",
                    alias="rsp",
                ),
            ],
        )
        unfiltered = rows_by(await exec_engine.execute(base), RK_REGION, RK_TIER)
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["customers.regions.name", "customers.tier"],
            filter=["sum(customers.spend, partition_by=customers.regions.name) > 100"],
            metrics=[
                QueryMetric(metric="sum(customers.spend)", alias="sp"),
                QueryMetric(
                    metric="sum(customers.spend, partition_by=customers.regions.name)",
                    alias="rsp",
                ),
            ],
        )), RK_REGION, RK_TIER)
        assert {k[0] for k in filtered} == {"RegN"}
        for key, row in filtered.items():
            assert float(row["sp"]) == pytest.approx(
                float(unfiltered[key]["sp"])
            )
            assert float(row["rsp"]) == pytest.approx(
                float(unfiltered[key]["rsp"])
            )


class TestLocalPartitionedFilter:
    """partitioned-aggregates: the ordinary local metric-typed partitioned
    filter route (already legal; pinned here)."""

    async def test_keep_rows_whose_partition_total_qualifies(self, exec_engine) -> None:
        base = q(
            dimensions=["orders.region", "orders.city"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )
        unfiltered = rows_by(await exec_engine.execute(base), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        # North (100) and NULL-region (60) qualify; South (50) does not.
        assert {k for k in filtered} == {k for k in unfiltered if REGION_TOTAL[k[0]] > 50}
        for key, row in filtered.items():
            assert float(row["s"]) == pytest.approx(float(unfiltered[key]["s"]))

    async def test_conjunction_in_one_string_equals_two_filters(self, exec_engine) -> None:
        # Top-level AND inside one filter string splits and types each conjunct
        # independently — identical to giving the two predicates separately.
        def key(resp):
            return {(r["orders.region"], r["orders.city"]): float(r["s"]) for r in resp.data}

        combined = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50 and orders.city != 'CityB'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        separate = await exec_engine.execute(q(
            dimensions=["orders.region", "orders.city"],
            filter=["sum(orders.amount, partition_by=orders.region) > 50", "orders.city != 'CityB'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        ))
        assert key(combined) == key(separate)


class TestComputedDimAggregateMixing:
    """partitioned-aggregates lift: one predicate mixing a computed
    dimension's aggregate with another reference is legal."""

    async def test_mix_with_row_level_ref_types_as_field(self, exec_engine) -> None:
        # `band` is the attached (row-scope) value; OR-ing it with a base field
        # masks per base row before re-aggregation — a field-typed conjunct.
        dims = ["orders.region", "orders.city", {"field": BAND35, "alias": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filter=["band == 1 or orders.channel == 'app'"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        # Row mask keeps a row iff its city total > 35 OR its channel is app.
        assert set(filtered) <= set(unfiltered)
        # (North, NULL city) has only a web order in a below-band city → dropped.
        assert ("North", None) not in filtered
        # (North, CityA) is below-band; only its one app order (amount 20) survives.
        assert float(filtered[("North", "CityA")]["s"]) == pytest.approx(20.0)
        for key, row in filtered.items():
            assert float(row["s"]) <= float(unfiltered[key]["s"]) + 1e-9

    async def test_mix_with_plain_aggregate_types_as_measure(self, exec_engine) -> None:
        # OR-ing the computed-dim aggregate with a plain aggregate evaluates at
        # query grain — a metric-typed conjunct that prunes cells only.
        dims = ["orders.region", "orders.city", {"field": BAND35, "alias": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filter=["sum(orders.amount, partition_by=orders.city) > 45 or sum(orders.amount) > 55"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        expected = {k for k, v in CITY_TOTAL.items() if v > 45}
        assert set(filtered) == expected
        # Metric mask: surviving cells keep the unfiltered values.
        for key, row in filtered.items():
            assert float(row["s"]) == pytest.approx(
                float(unfiltered[key]["s"])
            )


class TestComputedDimOwnAggregateIsField:
    """positions: a computed dimension's own aggregate resolves to its attached
    value and types as field (unchanged from before this change)."""

    async def test_filter_on_own_partition_aggregate_row_masks(self, exec_engine) -> None:
        dims = ["orders.region", "orders.city", {"field": BAND35, "alias": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filter=["sum(orders.amount, partition_by=orders.city) > 45"],
            metrics=[QueryMetric(metric="sum(orders.amount)", alias="s")],
        )), "orders.region", "orders.city")
        expected = {k for k, v in CITY_TOTAL.items() if v > 45}
        assert set(filtered) == expected
        # A whole-city row mask keeps every row of a surviving city → full total.
        for key, row in filtered.items():
            assert float(row["s"]) == pytest.approx(float(unfiltered[key]["s"]))
