"""``date_trunc('<unit>', col)`` with a datasource-granularity unit resolves at binding."""

from __future__ import annotations

import pytest

from pydantic import ValidationError

from slayer.core.errors import GranularityCallError, QueryTypeError, TimeDimensionColumnError
from slayer.core.query import SlayerQuery

from tests._dev2015_fixtures import BACKENDS, BUILT_IN_GRANULARITIES, GRANULARITY_NAMES, cg_engine, m

COUNT = [m("count(*)", "n")]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with cg_engine(request.param) as eng:
        yield eng


def _q(**fields) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", "measures": COUNT, **fields})


class TestDatasourceCallee:
    async def test_dimension_entry_equals_the_explicit_form(self, engine) -> None:
        functional = await engine.execute(_q(dimensions=["date_trunc('fiscal_year', order_date)"]))
        explicit = await engine.execute(_q(time_dimensions=[{"dimension": "order_date", "granularity": "fiscal_year"}]))
        assert functional.sql == explicit.sql
        assert sorted(map(repr, functional.data)) == sorted(map(repr, explicit.data))

    async def test_time_dimensions_string_equals_the_explicit_form(self, engine) -> None:
        functional = await engine.execute(_q(time_dimensions=["date_trunc('fiscal_year', order_date)"]))
        explicit = await engine.execute(_q(time_dimensions=[{"dimension": "order_date", "granularity": "fiscal_year"}]))
        assert functional.sql == explicit.sql

    async def test_case_insensitive_callee(self, engine) -> None:
        upper = await engine.execute(_q(dimensions=["DATE_TRUNC('FISCAL_YEAR', order_date)"]))
        lower = await engine.execute(_q(dimensions=["date_trunc('fiscal_year', order_date)"]))
        assert upper.sql == lower.sql


class TestUnknownCalleeAtBinding:
    @pytest.mark.parametrize(("fields", "names_partition_by"), [
        ({"time_dimensions": ["date_trunc('mnth', order_date)"]}, False),
        ({"dimensions": ["date_trunc('mnth', order_date)"]}, False),
        ({"dimensions": ["mnth(order_date)"]}, True),
    ], ids=["time-dimensions-string", "dimension", "dimension-bare-call"])
    async def test_fails_at_binding_listing_granularities(self, engine, fields, names_partition_by) -> None:
        query = _q(**fields)  # construction no longer rejects it
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        msg = str(exc.value)
        for name in (*BUILT_IN_GRANULARITIES, *GRANULARITY_NAMES):
            assert name in msg
        if names_partition_by:
            assert "partition_by" in msg

    @pytest.mark.parametrize("entry", [
        "date_trunc('fiscal_year')",
        "date_trunc('fiscal_year', order_date, amount)",
        "date_trunc('fiscal_year', *)",
        "date_trunc(id, order_date)",
    ])
    async def test_wrong_shape_datasource_unit(self, engine, entry) -> None:
        async def _validate_and_execute() -> None:
            await engine.execute(_q(dimensions=[entry]), dry_run=True)

        with pytest.raises((GranularityCallError, ValidationError, QueryTypeError)) as exc:
            await _validate_and_execute()
        msg = str(exc.value)
        assert "date_trunc(" in msg, msg
        assert "(col" not in msg, msg
        assert "gran(" not in msg, msg

    async def test_non_temporal_argument_is_a_time_dimension_type_error(self, engine) -> None:
        query = _q(dimensions=["date_trunc('fiscal_year', upper(id))"])
        with pytest.raises(TimeDimensionColumnError, match="TEXT"):
            await engine.execute(query, dry_run=True)

    def test_bare_column_string_still_rejected_at_construction(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"source_model": "orders", "time_dimensions": ["order_date"]})
        assert "month" in str(exc.value)
        assert "date_trunc(" in str(exc.value)
