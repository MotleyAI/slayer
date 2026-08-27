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

    async def test_window_plus_partition_raises(self) -> None:
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="amount:sum(window='90d', partition_by=region)",
            )],
        )
        with pytest.raises(NotImplementedError, match=r"window=.*DEV-1824"):
            await gen(q)

    @pytest.mark.parametrize("agg", ["first", "last"])
    async def test_first_last_plus_partition_raises(self, agg: str) -> None:
        q = _q(
            dimensions=["region", "city"],
            measures=[ModelMeasure(formula=f"amount:{agg}(partition_by=region)")],
        )
        with pytest.raises(NotImplementedError, match=r"first/last.*DEV-1824"):
            await gen(q)

    async def test_nested_transform_raises(self) -> None:
        q = _q(
            dimensions=["region"], time_dimensions=month_td(),
            measures=[ModelMeasure(
                formula="cumsum(amount:sum(partition_by=region))",
            )],
        )
        with pytest.raises(
            NotImplementedError, match=r"nested inside a transform.*DEV-1824",
        ):
            await gen(q)

    async def test_partitioned_measure_in_filter_raises(self) -> None:
        q = _q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(
            NotImplementedError,
            match=r"Filtering on a partition_by aggregate.*DEV-1824",
        ):
            await gen(q)


class TestSharedAggregateRowPlusCombined:
    """A partitioned aggregate used BOTH inside a computed dimension (row attach)
    AND as a selected measure (combined attach) is the deferred row+combined
    coexistence shape. Discovery must keep it in BOTH sets so the guard raises —
    excluding it from combined discovery would silently rewrite the measure to a
    row placeholder at the wrong aggregation boundary (CR)."""

    async def test_same_key_as_dimension_and_measure_raises(self) -> None:
        band = "CASE WHEN amount:sum(partition_by=region) > 5000 THEN 1 ELSE 0 END"
        q = _q(
            dimensions=["region", {"expression": band, "name": "band"}],
            measures=[ModelMeasure(
                formula="amount:sum(partition_by=region)", name="region_total",
            )],
        )
        with pytest.raises(
            NotImplementedError, match=r"row and a combined regroup attach.*DEV-1824",
        ):
            await gen(q)


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
        # D2 keeps the cross-model partitioned-measure path; its out-of-derived-
        # grain guard must not regress.
        q = _q(
            dimensions=["region", "customers.tier"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=region)",
            )],
        )
        with pytest.raises(ValueError, match=r"partition_by") as ei:
            await gen(q)
        assert "customers.tier" in str(ei.value)


class TestGuardOrderingWithActiveRegroup:
    """D5 — the guard snapshot fires correctly even when a legitimate
    computed-dimension regroup is present in the same query."""

    async def test_partitioned_filter_still_raises_with_computed_dim(self) -> None:
        # A legitimate computed dimension (row attach, partition_by=city) does
        # NOT mask a separately-guarded partitioned-aggregate FILTER — the
        # in-filter guard still fires with the exact DEV-1824 message.
        q = _q(
            dimensions=["region", {"expression": BAND_CITY, "name": "band"}],
            filters=["amount:sum(partition_by=region) > 50"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(
            NotImplementedError,
            match=r"Filtering on a partition_by aggregate.*DEV-1824",
        ):
            await gen(q)

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
