"""Granularity-call error surface in dimensions, plus non-regression.

Spec: openspec/changes/dev-1883-support-functional-time-granularity-form-monthcol-in-query/
specs/queries/time-dimensions (Granularity error surface in dimensions).
"""
import re

import pydantic
import pytest

from slayer.core import errors
from slayer.core.enums import TimeGranularity
from slayer.core.query import SlayerQuery
from tests import _dev1883_fixtures as fx

GRANULARITIES = [g.value for g in TimeGranularity]


def assert_names_functional_form(msg: str) -> None:
    """The error must show the ``gran(col)`` remedy shape."""
    assert "(col" in msg or re.search(r"\b\w+\(created_at\)", msg), (
        f"error must show the gran(col) form: {msg}"
    )

WRONG_SHAPES = [
    "month()",
    "month(a, b)",
    "month(upper(x))",
    "month(*)",
    "month(created_at, granularity=2)",
]


@pytest.fixture
async def exec_engine(tmp_path):
    return await fx.build_exec_engine(tmp_path)


def _q(dimensions: list) -> SlayerQuery:
    return fx.q(
        source_model="orders",
        dimensions=dimensions,
        measures=[{"formula": "*:count"}],
    )


class TestWrongShapeGranularityCalls:
    @pytest.mark.parametrize("entry", WRONG_SHAPES)
    def test_rejected_naming_shape_and_granularities(self, entry: str) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            _q([entry])
        msg = str(ei.value)
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name granularity {gran!r}: {msg}"
        assert_names_functional_form(msg)


class TestUnknownSingleColumnCall:
    async def test_typo_names_granularities_and_partition_by_hedge(self, exec_engine) -> None:
        query = _q(["mnth(created_at)"])  # accepted at construction; the callee resolves at binding
        with pytest.raises(errors.QueryTypeError) as ei:
            await exec_engine.execute(query, dry_run=True)
        msg = str(ei.value)
        for gran in GRANULARITIES:
            assert gran in msg, f"error must name granularity {gran!r}: {msg}"
        assert "partition_by" in msg

    def test_typed_error_class_exists(self) -> None:
        assert issubclass(errors.GranularityCallError, errors.SlayerError)
        assert issubclass(errors.GranularityCallError, ValueError)

    @pytest.mark.parametrize(
        "entry", ["stddev(amount)", "variance(amount)", "countdistinct(amount)"],
    )
    def test_builtin_alias_aggregate_not_typo_rejected(self, entry: str) -> None:
        """A bare builtin-alias aggregate stays a computed dimension (binding-time
        ``partition_by=`` error), not a construction-time granularity typo."""
        q = _q([entry])
        assert q.dimensions, q.dimensions


class TestNonRegression:
    async def test_scalar_computed_dimension_unaffected(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(["upper(status)"]))
        assert resp.columns == ["orders.upper_status", "orders._count"]
        assert {r["orders.upper_status"] for r in resp.data} == {"OPEN", "PAID"}

    async def test_partition_by_aggregate_dimension_unaffected(self, exec_engine) -> None:
        resp = await exec_engine.execute(
            _q(["status", "amount:sum(partition_by=status) > 20"])
        )
        assert "orders._count" in resp.columns
        assert len(resp.data) == 2

    async def test_bare_builtin_aggregate_keeps_binding_error(self, exec_engine) -> None:
        query = _q(["sum(amount)"])
        with pytest.raises(ValueError, match="partition_by"):
            await exec_engine.execute(query)

    @pytest.mark.parametrize(("bound", "expected"), [
        ("2024-01-01", {"paid": 2, "open": 2}),
        ("2024-01-15", {"paid": 1, "open": 2}),  # lowers to created_at >= 2024-02-01
    ])
    async def test_granularity_in_filter_is_recognised(
        self, exec_engine, bound, expected,
    ) -> None:
        query = fx.q(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "*:count"}],
            filters=[f"month(created_at) >= '{bound}'"],
        )
        resp = await exec_engine.execute(query)
        assert {r["orders.status"]: r["orders._count"] for r in resp.data} == expected

    async def test_granularity_in_measure_is_treated_like_any_row_level_scalar(
        self, exec_engine,
    ) -> None:
        """Position parity: a bare ``gran(col)`` measure gets the same treatment as another row-level temporal scalar."""
        async def outcome(formula: str):
            query = fx.q(source_model="orders", dimensions=["status"], measures=[{"formula": formula, "name": "v"}])
            try:
                resp = await exec_engine.execute(query)
            except Exception as exc:  # noqa: BLE001 — the raised class is the compared outcome
                assert "Unknown aggregation" not in str(exc), exc
                return type(exc)
            return sorted(r["orders.status"] for r in resp.data)

        assert await outcome("month(created_at)") == await outcome("date_add(created_at, 0, 'day')")
