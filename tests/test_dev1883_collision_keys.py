"""DEV-1883 — result keys for same-column time dimensions; dedupe and metadata conflicts.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (Result keys disambiguate same-column time dimensions).
"""
import pydantic
import pytest

from slayer.core.query import SlayerQuery, TimeDimension
from tests import _dev1883_fixtures as fx


@pytest.fixture
async def engine(tmp_path):
    return await fx.build_engine(tmp_path)


@pytest.fixture
async def exec_engine(tmp_path):
    return await fx.build_exec_engine(tmp_path)


class TestGranularitySuffixedKeys:
    async def test_two_granularities_functional_execution(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)", "year(created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        assert resp.columns == [
            "orders.created_at.month",
            "orders.created_at.year",
            "orders.amount_sum",
        ]
        rows = {
            r["orders.created_at.month"]: (
                r["orders.created_at.year"], r["orders.amount_sum"]
            )
            for r in resp.data
        }
        assert rows == {
            "2024-01-01": ("2024-01-01", 10.0),
            "2024-02-01": ("2024-01-01", 60.0),
            "2025-03-01": ("2025-01-01", 5.0),
        }

    async def test_explicit_form_gets_the_same_keys(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[
                {"dimension": "created_at", "granularity": "month"},
                {"dimension": "created_at", "granularity": "year"},
            ],
            measures=[{"formula": "amount:sum"}],
        ))
        assert resp.columns == [
            "orders.created_at.month",
            "orders.created_at.year",
            "orders.amount_sum",
        ]

    async def test_mixed_functional_and_explicit_suffixed(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "year"}],
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        assert set(resp.columns) == {
            "orders.created_at.year",
            "orders.created_at.month",
            "orders.amount_sum",
        }

    async def test_single_time_dimension_keeps_unsuffixed_key(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        assert resp.columns == ["orders.created_at", "orders.amount_sum"]

    async def test_distinct_columns_keep_unsuffixed_keys(self, exec_engine) -> None:
        """Two TDs on different columns don't collide, so neither key is suffixed."""
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["year(created_at)", "month(customers.created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        assert set(resp.columns) == {
            "orders.created_at",
            "orders.customers.created_at",
            "orders.amount_sum",
        }


class TestDuplicateTimeDimensions:
    def test_exact_duplicates_dedupe_across_forms(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        )
        assert not q.dimensions, q.dimensions
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]

    def test_exact_duplicates_dedupe_within_dimensions(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)", "month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]

    def test_exact_explicit_duplicates_dedupe(self) -> None:
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[
                {"dimension": "created_at", "granularity": "month"},
                {"dimension": "created_at", "granularity": "month"},
            ],
            measures=[{"formula": "amount:sum"}],
        )
        assert q.time_dimensions == [
            TimeDimension(dimension="created_at", granularity="month")
        ]

    def test_duplicates_with_identical_metadata_dedupe(self) -> None:
        entry = {
            "dimension": "created_at", "granularity": "month",
            "date_range": ["2024-01-01", "2024-12-31"], "label": "Created",
        }
        q = SlayerQuery(
            source_model="orders",
            time_dimensions=[dict(entry), dict(entry)],
            measures=[{"formula": "amount:sum"}],
        )
        assert q.time_dimensions == [TimeDimension.model_validate(entry)]

    async def test_deduped_query_executes_as_single(self, exec_engine) -> None:
        resp = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        assert resp.columns == ["orders.created_at", "orders.amount_sum"]
        assert {
            r["orders.created_at"]: r["orders.amount_sum"] for r in resp.data
        } == fx.MONTH_SUMS

    @pytest.mark.parametrize("metadata", [
        {"label": "A"},
        {"date_range": ["2024-01-01", "2024-12-31"]},
    ])
    def test_same_column_granularity_differing_metadata_rejected(
        self, metadata: dict,
    ) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    {"dimension": "created_at", "granularity": "month"},
                    {"dimension": "created_at", "granularity": "month", **metadata},
                ],
                measures=[{"formula": "amount:sum"}],
            )
        msg = str(ei.value)
        assert "created_at" in msg
        assert "month" in msg
