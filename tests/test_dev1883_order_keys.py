"""DEV-1883 — ``gran(col)`` order entries resolve to the projected time dimension's bucket.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (Functional granularity form as an order key).
"""
import pytest

from slayer.core.query import SlayerQuery
from tests import _dev1883_fixtures as fx


@pytest.fixture
async def exec_engine(tmp_path):
    return await fx.build_exec_engine(tmp_path)


def _month_query(order_column: str, direction: str = "desc") -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[{"formula": "amount:sum"}],
        order=[{"column": order_column, "direction": direction}],
    )


class TestOrderByProjectedBucket:
    async def test_functional_order_matches_column_name_order(self, exec_engine) -> None:
        by_functional = await exec_engine.execute(_month_query("month(created_at)"))
        by_column = await exec_engine.execute(_month_query("created_at"))
        assert by_functional.data == by_column.data
        assert [r["orders.created_at"] for r in by_functional.data] == [
            "2025-03-01", "2024-02-01", "2024-01-01",
        ]

    async def test_functional_order_over_functional_projection(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
            order=[{"column": "month(created_at)", "direction": "asc"}],
        ))
        assert [r["orders.created_at"] for r in resp.data] == [
            "2024-01-01", "2024-02-01", "2025-03-01",
        ]


class TestOrderWithoutMatchingTimeDimension:
    async def test_no_time_dimension_on_column_errors_with_remedy(
        self, exec_engine,
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum"}],
            order=[{"column": "month(created_at)", "direction": "desc"}],
        )
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "created_at" in msg
        assert "month" in msg
        assert "time_dimension" in msg or "time dimension" in msg.lower(), (
            f"error must name the projected-time-dimension remedy: {msg}"
        )

    async def test_granularity_mismatch_errors_with_remedy(self, exec_engine) -> None:
        query = _month_query("year(created_at)")
        with pytest.raises(ValueError) as ei:
            await exec_engine.execute(query)
        msg = str(ei.value)
        assert "created_at" in msg
        assert "year" in msg
        assert "time_dimension" in msg or "time dimension" in msg.lower(), (
            f"error must name the projected-time-dimension remedy: {msg}"
        )
