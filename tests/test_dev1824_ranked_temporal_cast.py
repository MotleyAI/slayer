"""DEV-1824 regression — a ``first`` / ``last`` VALUE of a TEMPORAL column must
not be CAST to its declared type in the ranked CTE.

The value IS the raw picked column (``MAX(CASE WHEN rn = 1 THEN col)``), so the
declared-type cast is redundant on every dialect and, on SQLite, actively wrong:
``CAST(<text date> AS TIMESTAMP)`` takes NUMERIC affinity and truncates the date
to its leading year (every value collapses to e.g. ``2024``). Pre-existing bug
surfaced by DEV-1824; fixed at the shared ranked renderer via
``_ranked_value_cast_type``. The regression under test here is a PLAIN
(non-partition) first/last — the partitioned form is covered by
``test_dev1824_computed_dim_execution.TestFirstLastInDimension``. Fails without
the fix (SQLite returns ``2024`` for every region); DuckDB was always correct.
"""

from __future__ import annotations

import pytest

from tests._dev1824_fixtures import (
    REGION_LAST_AT,
    ModelMeasure,
    make_exec_engine,
    q,
)

#: Earliest ordered_at per region (row 1 Jan 10 / row 5 Jan 25 / row 7 Mar 10).
REGION_FIRST_AT = {"North": "2024-01-10", "South": "2024-01-25", None: "2024-03-10"}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestRankedTemporalValueNotTruncated:
    async def test_plain_last_of_timestamp_returns_the_full_date(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="ordered_at:last", name="lt")],
        ))
        got = {r["orders.region"]: str(r["orders.lt"])[:10] for r in resp.data}
        assert got == REGION_LAST_AT

    async def test_plain_first_of_timestamp_returns_the_full_date(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="ordered_at:first", name="ft")],
        ))
        got = {r["orders.region"]: str(r["orders.ft"])[:10] for r in resp.data}
        assert got == REGION_FIRST_AT
