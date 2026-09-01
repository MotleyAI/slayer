"""DEV-1829 (D5 / F3) — guard preservation across the combined migration.

The DEV-1824 deferred-shape guards (window= / first / last / nested-transform /
in-filter) must fire on the ORIGINAL value-key trees, BEFORE the generalized
regroup desugar substitutes partitioned aggregates away — otherwise a substituted
placeholder would hide the shape and the guard would silently pass. Pinned both
directions: a guarded partitioned-measure filter still raises even with a
legitimate computed-dimension regroup active, and a legitimate computed-dimension
filter is NOT wrongly rejected by the reordered guard.
"""

from __future__ import annotations

import pytest

from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1739_fixtures import cm_cte_bodies, gen, month_td

# A computed dimension over a city-partitioned aggregate → legitimate ROW attach.
BAND_CITY = "CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END"


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestDeferredMeasureShapesStillRaise:
    """Each deferred partitioned-MEASURE shape raises the pinned DEV-1824 error
    after the migration (the guard runs on the pre-substitution snapshot)."""

    async def test_window_plus_partition_lifted(self) -> None:
        # DEV-1824 (task 3.3) — a LOCAL window=+partition_by measure renders via a
        # windowed producer at the (partition ∪ active-TD) grain (D5); executed
        # values in test_dev1824_partitioned_execution.py::TestWindowPlusPartition.
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="amount:sum(window='90d', partition_by=region)",
            )],
        )
        assert "__regroup__" not in await gen(q)

    @pytest.mark.parametrize("agg", ["first", "last"])
    async def test_first_last_plus_partition_lifted(self, agg: str) -> None:
        # DEV-1824 (task 3.4) — a LOCAL first/last with partition_by renders: the
        # producer computes the ranked pick at the partition grain and attaches.
        # (Executed values: test_dev1824_partitioned_execution.py.)
        q = _q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=f"amount:{agg}(partition_by=region)")],
        )
        assert "__regroup__" not in await gen(q)

    async def test_nested_transform_lifted(self) -> None:
        # DEV-1824 (task 3.5) — a LOCAL partitioned aggregate nested in a
        # transform desugars into a combined regroup producer and the transform
        # runs at the query grain over the attached value; it no longer raises.
        # (Executed values: test_dev1824_partitioned_execution.py.)
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="cumsum(amount:sum(partition_by=region))",
            )],
        )
        assert "__regroup__" not in await gen(q)

    async def test_partitioned_measure_in_filter_lifted(self) -> None:
        # DEV-1824 (task 3.6) — a LOCAL partitioned aggregate filter renders via a
        # combined producer + outer WHERE (executed values in
        # tests/test_dev1824_partitioned_execution.py::TestFilterOnPartitioned).
        q = _q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        assert "__regroup__" not in await gen(q)


class TestSharedAggregateRowPlusCombined:
    """A partitioned aggregate used BOTH inside a computed dimension (row attach)
    AND as a selected measure (combined attach). DEV-1824 (task 3.2) lifts this
    coexistence: discovery keeps it in BOTH sets, and it ships as duplicate
    producers (D10 — cross-phase dedup is Stage 2)."""

    async def test_same_key_as_dimension_and_measure(self) -> None:
        band = "CASE WHEN amount:sum(partition_by=region) > 5000 THEN 1 ELSE 0 END"
        q = _q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="region_total",
            )],
        )
        # Renders with both a row and a combined attach; no placeholder leaks.
        assert "__regroup__" not in await gen(q)


class TestGrainGuardsPreserved:
    """The DEV-1739 grain guards (``ValueError``) must survive the migration —
    a partitioned MEASURE key stays STRICT (must be a query dimension), and the
    kept cross-model narrow path (D2) still rejects an out-of-grain partition."""

    async def test_non_dimension_partition_key_raises(self) -> None:
        # A measure's partition_by must be a query dimension; the generalized
        # discovery must NOT relax this to the computed-dimension leniency.
        q = _q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="amount:sum(partition_by=city)")],
        )
        with pytest.raises(
            ValueError, match=r"partition_by.*not a query dimension",
        ) as ei:
            await gen(q)
        assert "region" in str(ei.value)

    async def test_cross_model_partition_outside_grain_raises(self) -> None:
        # DEV-1836: an explicit partition key must be attributable from the
        # aggregate's root — `region` is a host column, unreachable from customers.
        q = _q(
            dimensions=["region", "customers.tier"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=region)",
            )],
        )
        with pytest.raises(ValueError, match=r"partition_by") as ei:
            await gen(q)
        msg = str(ei.value)
        assert "region" in msg
        assert "attributable from customers" in msg


class TestGuardOrderingWithActiveRegroup:
    """D5 — the guard snapshot fires correctly even when a legitimate
    computed-dimension regroup is present in the same query."""

    async def test_partitioned_filter_lifted_with_computed_dim(self) -> None:
        # DEV-1824 (tasks 3.2/3.6) — a computed dimension (row attach,
        # partition_by=city) coexists with a partitioned-aggregate FILTER
        # (combined producer, partition_by=region, outer WHERE); both render with
        # no placeholder leak.
        q = _q(
            dimensions=["region", {"expression": BAND_CITY, "name": "band"}],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        assert "__regroup__" not in await gen(q)

    async def test_legitimate_computed_dim_filter_not_wrongly_raised(self) -> None:
        # The reverse direction: a final-only filter over the computed dimension
        # is legitimate and must NOT be rejected by the reordered guard.
        q = _q(
            dimensions=["region", {"expression": BAND_CITY, "name": "band"}],
            filters=["band == 1"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        sql = await gen(q)
        assert "SELECT" in sql
        # The band threshold is applied after join-back, never in the producer.
        assert "5000" not in cm_cte_bodies(sql)
