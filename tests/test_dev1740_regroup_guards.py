"""DEV-1740 Part B2 — guards. A dimension expression over an aggregate must be
explicit and in-scope; deferred shapes raise loudly (never degrade).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import DistinctDimensionValuesError
from slayer.core.query import ModelMeasure, SlayerQuery

from tests._dev1740_fixtures import gen, month_td


def _q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


class TestPartitionByRequired:
    async def test_aggregate_without_partition_by_raises_directive(self) -> None:
        q = _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum > 5000 THEN 1 ELSE 0 END",
                 "name": "band"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        with pytest.raises(ValueError, match=r"partition_by") as ei:
            await gen(q)
        # The message must show the fix — the partition_by syntax.
        assert "partition_by" in str(ei.value)


class TestAggregateReferencingDimGenerates:
    async def test_valid_partitioned_dim_desugars_to_regroup(self) -> None:
        # A plain partitioned aggregate inside a dimension is the valid
        # aggregate-then-regroup case (B2) — the DEV-1825 regroup primitive
        # isolates it in a `_cm_` producer CTE. (The DEV-1739 measures-only
        # "not a query dimension" guard must NOT fire — city is a legal finer
        # grain.)
        q = _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum(partition_by=city) > 5000 "
                               "THEN 1 ELSE 0 END", "name": "band"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        sql = await gen(q)
        assert "_cm_" in sql


class TestDeferredShapesRaiseDev1824:
    async def test_transform_in_dimension_expression(self) -> None:
        q = _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN cumsum(amount:sum) > 5000 THEN 1 ELSE 0 END",
                 "name": "x"},
            ],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    async def test_window_plus_partition_in_dimension_expression(self) -> None:
        q = _q(
            dimensions=[
                "region",
                {"expression": "CASE WHEN amount:sum(window='90d', partition_by=city) "
                               "> 5000 THEN 1 ELSE 0 END", "name": "x"},
            ],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)

    @pytest.mark.parametrize("agg", ["first", "last"])
    async def test_first_last_partition_in_dimension_expression(self, agg: str) -> None:
        q = _q(
            dimensions=[
                "region",
                {"expression": f"CASE WHEN amount:{agg}(partition_by=city) > 5000 "
                               "THEN 1 ELSE 0 END", "name": "x"},
            ],
            measures=[ModelMeasure(formula="amount:sum", name="t")],
        )
        with pytest.raises(NotImplementedError, match=r"DEV-1824"):
            await gen(q)


class TestDistinctDimensionValuesRejection:
    async def test_aggregate_dim_rejected_in_raw_rows_mode(self) -> None:
        q = _q(
            dimensions=[
                {"expression": "CASE WHEN amount:sum(partition_by=city) > 5000 "
                               "THEN 1 ELSE 0 END", "name": "band"},
            ],
            distinct_dimension_values=False,
        )
        with pytest.raises(DistinctDimensionValuesError):
            await gen(q)
