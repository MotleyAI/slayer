"""DEV-1865 — a typing failure names both failed typings.

An expression valid as neither field nor measure fails with a clear typing
error (never a deferred-shape ``NotImplementedError`` and never a silent wrong
result), stating why field typing failed (the aggregate) and why measure typing
failed (the unavailable row-level reference / missing measure position).
"""

from __future__ import annotations

import pytest

from tests._dev1865_fixtures import ModelMeasure, make_exec_engine, q


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestMixedGrainOr:
    async def test_partitioned_or_nondim_rowlevel_is_typing_error(self, exec_engine) -> None:
        # Field typing fails (the aggregate); measure typing fails (status is not
        # a query dimension) — neither scope holds the whole OR.
        query = q(
            dimensions=["region", "city"],
            filters=["amount:sum(partition_by=region) > 50 or status == 'ok'"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        )
        with pytest.raises(Exception) as ei:  # noqa: PT011 — asserted on message
            await exec_engine.execute(query)
        msg = str(ei.value).lower()
        assert not isinstance(ei.value, NotImplementedError)
        assert "field" in msg and "measure" in msg
        assert "status" in msg
        assert "partition" in msg or "amount:sum" in msg


class TestRawRowsHaveNoMeasurePosition:
    async def test_filter_on_measure_in_raw_rows_errors(self, exec_engine) -> None:
        with pytest.raises(Exception) as ei:  # noqa: PT011
            query = q(
                dimensions=["status"],
                distinct_dimension_values=False,
                filters=["amount:sum > 100"],
            )
            await exec_engine.execute(query)
        assert "measure" in str(ei.value).lower()

    async def test_order_on_measure_in_raw_rows_errors(self, exec_engine) -> None:
        # An aggregate order target is valid as neither field nor measure when the
        # query has no measure position — rejected, never silently unsorted.
        with pytest.raises(Exception) as ei:  # noqa: PT011
            query = q(
                dimensions=["status"],
                distinct_dimension_values=False,
                order=[{"column": "amount:sum", "direction": "desc"}],
            )
            await exec_engine.execute(query)
        assert "measure" in str(ei.value).lower()
