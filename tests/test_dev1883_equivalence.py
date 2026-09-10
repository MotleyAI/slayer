"""DEV-1883 — the functional form is byte-identical downstream to explicit ``TimeDimension``.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (equivalence scenarios).
"""
import pytest

from slayer.core.query import SlayerQuery
from tests import _dev1883_fixtures as fx


@pytest.fixture
async def engine(tmp_path):
    return await fx.build_engine(tmp_path)


@pytest.fixture
async def exec_engine(tmp_path):
    return await fx.build_exec_engine(tmp_path)


def _functional() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        dimensions=["month(created_at)"],
        measures=[{"formula": "amount:sum"}],
    )


def _explicit() -> SlayerQuery:
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[{"formula": "amount:sum"}],
    )


class TestEquivalence:
    async def test_generated_sql_identical(self, engine) -> None:
        functional = await engine.execute(_functional(), dry_run=True)
        explicit = await engine.execute(_explicit(), dry_run=True)
        assert functional.sql == explicit.sql

    async def test_rows_and_result_key_identical(self, exec_engine) -> None:
        functional = await exec_engine.execute(_functional())
        explicit = await exec_engine.execute(_explicit())
        assert functional.columns == explicit.columns
        assert functional.data == explicit.data
        assert functional.columns == ["orders.created_at", "orders.amount_sum"]
        assert {
            r["orders.created_at"]: r["orders.amount_sum"] for r in functional.data
        } == fx.MONTH_SUMS

    async def test_string_time_dimensions_entry_sql_identical(self, engine) -> None:
        string_form = SlayerQuery(
            source_model="orders",
            time_dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
        )
        functional = await engine.execute(string_form, dry_run=True)
        explicit = await engine.execute(_explicit(), dry_run=True)
        assert functional.sql == explicit.sql

    async def test_transform_axis_and_main_time_dimension_identical(
        self, engine,
    ) -> None:
        """A transform keyed off the TD via ``main_time_dimension``, with a second
        TD present, generates identical SQL from the functional form."""
        functional = await engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)", "year(customers.created_at)"],
            main_time_dimension="created_at",
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
        ), dry_run=True)
        explicit = await engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[
                {"dimension": "created_at", "granularity": "month"},
                {"dimension": "customers.created_at", "granularity": "year"},
            ],
            main_time_dimension="created_at",
            measures=[{"formula": "cumsum(amount:sum)", "name": "cs"}],
        ), dry_run=True)
        assert functional.sql == explicit.sql

    async def test_whole_periods_only_identical(self, engine) -> None:
        functional = await engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(created_at)"],
            measures=[{"formula": "amount:sum"}],
            whole_periods_only=True,
        ), dry_run=True)
        explicit = await engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[{"formula": "amount:sum"}],
            whole_periods_only=True,
        ), dry_run=True)
        assert functional.sql == explicit.sql

    async def test_dotted_functional_execution(self, exec_engine) -> None:
        functional = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            dimensions=["month(customers.created_at)"],
            measures=[{"formula": "amount:sum"}],
        ))
        explicit = await exec_engine.execute(SlayerQuery(
            source_model="orders",
            time_dimensions=[
                {"dimension": "customers.created_at", "granularity": "month"}
            ],
            measures=[{"formula": "amount:sum"}],
        ))
        assert functional.columns == explicit.columns
        assert functional.data == explicit.data
        assert functional.columns[0] == "orders.customers.created_at"
