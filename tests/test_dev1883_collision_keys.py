"""Result keys for same-column time dimensions; dedupe and metadata conflicts (spec: queries/time-dimensions)."""
import pydantic
import pytest

from slayer.core.query import TimeDimension
from tests import _dev1883_fixtures as fx
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture
async def engine(tmp_path):
    return await fx.build_engine(tmp_path)


@pytest.fixture
async def exec_engine(tmp_path):
    return await fx.build_exec_engine(tmp_path)


class TestGranularitySuffixedKeys:
    async def test_two_granularities_functional_execution(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(fx.q(
            population="orders",
            dimensions=["date_trunc('month', orders.created_at)", "date_trunc('year', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
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

    async def test_two_granularities_dotted_column_execution(self, exec_engine: SlayerQueryEngine) -> None:
        """Joined (dotted) same-column collisions suffix both the SQL alias and the
        result key, so rows carry distinct month/year values (not one collapsed bucket)."""
        resp = await exec_engine.execute(fx.q(
            population="orders",
            dimensions=[
                "date_trunc('month', customers.created_at)", "date_trunc('year', customers.created_at)",
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        ))
        assert resp.columns == [
            "customers.created_at.month",
            "customers.created_at.year",
            "orders.amount_sum",
        ]
        rows = {
            r["customers.created_at.month"]: (
                r["customers.created_at.year"], r["orders.amount_sum"]
            )
            for r in resp.data
        }
        assert rows == {
            "2023-05-01": ("2023-01-01", 50.0),
            "2024-07-01": ("2024-01-01", 25.0),
        }

    async def test_explicit_form_gets_the_same_keys(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "year"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        ))
        assert resp.columns == [
            "orders.created_at.month",
            "orders.created_at.year",
            "orders.amount_sum",
        ]

    async def test_mixed_functional_and_explicit_suffixed(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(fx.q(
            population="orders",
            time_dimensions=[{"field": "orders.created_at", "granularity": "year"}],
            dimensions=["date_trunc('month', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
        ))
        assert set(resp.columns) == {
            "orders.created_at.year",
            "orders.created_at.month",
            "orders.amount_sum",
        }

    async def test_single_time_dimension_keeps_unsuffixed_key(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(fx.q(
            population="orders",
            dimensions=["date_trunc('month', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
        ))
        assert resp.columns == ["orders.created_at", "orders.amount_sum"]

    async def test_distinct_columns_keep_unsuffixed_keys(self, exec_engine: SlayerQueryEngine) -> None:
        """Two TDs on different columns don't collide, so neither key is suffixed."""
        resp = await exec_engine.execute(fx.q(
            population="orders",
            dimensions=["date_trunc('year', orders.created_at)", "date_trunc('month', customers.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
        ))
        assert set(resp.columns) == {
            "orders.created_at",
            "customers.created_at",
            "orders.amount_sum",
        }


class TestDuplicateTimeDimensions:
    def test_exact_duplicates_dedupe_across_forms(self) -> None:
        q = fx.q(
            population="orders",
            time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
            dimensions=["date_trunc('month', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert not q.dimensions, q.dimensions
        assert q.time_dimensions == [
            fx.td(field="orders.created_at", granularity="month")
        ]

    def test_exact_duplicates_dedupe_within_dimensions(self) -> None:
        q = fx.q(
            population="orders",
            dimensions=["date_trunc('month', orders.created_at)", "date_trunc('month', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert q.time_dimensions == [
            fx.td(field="orders.created_at", granularity="month")
        ]

    def test_exact_explicit_duplicates_dedupe(self) -> None:
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "month"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert q.time_dimensions == [
            fx.td(field="orders.created_at", granularity="month")
        ]

    def test_duplicates_with_identical_metadata_dedupe(self) -> None:
        entry = {
            "field": "created_at", "granularity": "month",
            "date_range": ["2024-01-01", "2024-12-31"], "label": "Created",
        }
        q = fx.q(
            population="orders",
            time_dimensions=[dict(entry), dict(entry)],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert q.time_dimensions == [TimeDimension.model_validate(entry)]

    async def test_deduped_query_executes_as_single(self, exec_engine: SlayerQueryEngine) -> None:
        resp = await exec_engine.execute(fx.q(
            population="orders",
            time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
            dimensions=["date_trunc('month', orders.created_at)"],
            metrics=[{"metric": "sum(orders.amount)"}],
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
            fx.q(
                population="orders",
                time_dimensions=[
                    {"field": "orders.created_at", "granularity": "month"},
                    {"field": "orders.created_at", "granularity": "month", **metadata},
                ],
                metrics=[{"metric": "sum(orders.amount)"}],
            )
        msg = str(ei.value)
        assert "created_at" in msg
        assert "month" in msg

    def test_diff_spelling_same_metadata_dedupes(self) -> None:
        """``created_at`` and ``orders.created_at`` are the same column (source-model
        prefix), so an identical pair dedupes rather than colliding downstream."""
        q = fx.q(
            population="orders",
            time_dimensions=[
                {"field": "orders.created_at", "granularity": "month"},
                {"field": "orders.created_at", "granularity": "month"},
            ],
            metrics=[{"metric": "sum(orders.amount)"}],
        )
        assert q.time_dimensions == [
            fx.td(field="orders.created_at", granularity="month")
        ]

    def test_diff_spelling_conflicting_metadata_rejected(self) -> None:
        """Differently-spelled same column+granularity with conflicting metadata is
        rejected at construction with the clean conflict message."""
        with pytest.raises(pydantic.ValidationError, match="Conflicting"):
            fx.q(
                population="orders",
                time_dimensions=[
                    {"field": "orders.created_at", "granularity": "month", "label": "A"},
                    {"field": "orders.created_at", "granularity": "month", "label": "B"},
                ],
                metrics=[{"metric": "sum(orders.amount)"}],
            )
