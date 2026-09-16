"""DEV-1865 — shapes that were guarded and now compile by construction.

Executed on SQLite + DuckDB. Values are asserted differentially against the
same query without the new filter (the spec's parity language), so the oracles
do not re-hand-compute the engine's arithmetic.

Currently-red: the cross-model partition filter raises the DEV-1824
``NotImplementedError``; the OR-mixing shapes raise the DEV-1825 "separate
filters" ``NotImplementedError``.
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import (
    BAND35,
    CITY_TOTAL,
    REGION_TOTAL,
    ModelMeasure,
    q,
    rows_by,
)

RK_REGION = "orders.customers.regions.name"
RK_TIER = "orders.customers.tier"


class TestCrossModelPartitionedFilter:
    """cross-model-aggregates: filter on a cross-model partitioned aggregate."""

    async def test_partition_key_among_dims_and_aggregate_not_selected(
        self, exec_engine,
    ) -> None:
        base = q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
        )
        unfiltered = rows_by(await exec_engine.execute(base), RK_REGION, RK_TIER)
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["customers.regions.name", "customers.tier"],
            filters=["customers.spend:sum(partition_by=customers.regions.name) > 100"],
            measures=[ModelMeasure(formula="customers.spend:sum", name="sp")],
        )), RK_REGION, RK_TIER)
        # RegN partition total 300 > 100 survives; RegS total 50 does not.
        assert {k[0] for k in filtered} == {"RegN"}
        assert set(filtered) <= set(unfiltered)
        for key, row in filtered.items():
            assert float(row["orders.sp"]) == pytest.approx(
                float(unfiltered[key]["orders.sp"])
            )

    async def test_aggregate_also_selected_is_value_preserving(
        self, exec_engine,
    ) -> None:
        base = q(
            dimensions=["customers.regions.name", "customers.tier"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(
                    formula="customers.spend:sum(partition_by=customers.regions.name)",
                    name="rsp",
                ),
            ],
        )
        unfiltered = rows_by(await exec_engine.execute(base), RK_REGION, RK_TIER)
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["customers.regions.name", "customers.tier"],
            filters=["customers.spend:sum(partition_by=customers.regions.name) > 100"],
            measures=[
                ModelMeasure(formula="customers.spend:sum", name="sp"),
                ModelMeasure(
                    formula="customers.spend:sum(partition_by=customers.regions.name)",
                    name="rsp",
                ),
            ],
        )), RK_REGION, RK_TIER)
        assert {k[0] for k in filtered} == {"RegN"}
        for key, row in filtered.items():
            assert float(row["orders.sp"]) == pytest.approx(
                float(unfiltered[key]["orders.sp"])
            )
            assert float(row["orders.rsp"]) == pytest.approx(
                float(unfiltered[key]["orders.rsp"])
            )


class TestLocalPartitionedFilter:
    """partitioned-aggregates: the ordinary local measure-typed partitioned
    filter route (already legal; pinned here in the DEV-1865 suite)."""

    async def test_keep_rows_whose_partition_total_qualifies(self, exec_engine) -> None:
        base = q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        unfiltered = rows_by(await exec_engine.execute(base), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        # North (100) and NULL-region (60) qualify; South (50) does not.
        assert {k for k in filtered} == {k for k in unfiltered if REGION_TOTAL[k[0]] > 50}
        for key, row in filtered.items():
            assert float(row["orders.s"]) == pytest.approx(float(unfiltered[key]["orders.s"]))

    async def test_conjunction_in_one_string_equals_two_filters(self, exec_engine) -> None:
        # Top-level AND inside one filter string splits and types each conjunct
        # independently — identical to giving the two predicates separately.
        def key(resp):
            return {(r["orders.region"], r["orders.city"]): float(r["orders.s"]) for r in resp.data}

        combined = await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50 and city != 'CityB'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        separate = await exec_engine.execute(q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50", "city != 'CityB'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert key(combined) == key(separate)


class TestComputedDimAggregateMixing:
    """partitioned-aggregates DEV-1825 lift: one predicate mixing a computed
    dimension's aggregate with another reference is legal."""

    async def test_mix_with_row_level_ref_types_as_field(self, exec_engine) -> None:
        # `band` is the attached (row-scope) value; OR-ing it with a base column
        # masks per base row before re-aggregation — a field-typed conjunct.
        dims = ["region", "city", {"expression": BAND35, "name": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filters=["band == 1 or channel == 'app'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        # Row mask keeps a row iff its city total > 35 OR its channel is app.
        assert set(filtered) <= set(unfiltered)
        # (North, NULL city) has only a web order in a below-band city → dropped.
        assert ("North", None) not in filtered
        # (North, CityA) is below-band; only its one app order (amount 20) survives.
        assert float(filtered[("North", "CityA")]["orders.s"]) == pytest.approx(20.0)
        for key, row in filtered.items():
            assert float(row["orders.s"]) <= float(unfiltered[key]["orders.s"]) + 1e-9

    async def test_mix_with_plain_aggregate_types_as_measure(self, exec_engine) -> None:
        # OR-ing the computed-dim aggregate with a plain aggregate evaluates at
        # query grain — a measure-typed conjunct that prunes cells only.
        dims = ["region", "city", {"expression": BAND35, "name": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filters=["amount:sum(partition_by=city) > 45 or amount:sum > 55"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        expected = {k for k, v in CITY_TOTAL.items() if v > 45}
        assert set(filtered) == expected
        # Measure mask: surviving cells keep the unfiltered values.
        for key, row in filtered.items():
            assert float(row["orders.s"]) == pytest.approx(
                float(unfiltered[key]["orders.s"])
            )


class TestComputedDimOwnAggregateIsField:
    """positions: a computed dimension's own aggregate resolves to its attached
    value and types as field (unchanged from before this change)."""

    async def test_filter_on_own_partition_aggregate_row_masks(self, exec_engine) -> None:
        dims = ["region", "city", {"expression": BAND35, "name": "band"}]
        unfiltered = rows_by(await exec_engine.execute(q(
            dimensions=dims, measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        filtered = rows_by(await exec_engine.execute(q(
            dimensions=dims,
            filters=["amount:sum(partition_by=city) > 45"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )), "orders.region", "orders.city")
        expected = {k for k, v in CITY_TOTAL.items() if v > 45}
        assert set(filtered) == expected
        # A whole-city row mask keeps every row of a surviving city → full total.
        for key, row in filtered.items():
            assert float(row["orders.s"]) == pytest.approx(float(unfiltered[key]["orders.s"]))
