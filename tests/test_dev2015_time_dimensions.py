"""Functional ``gran(col)`` forms with a datasource-granularity callee resolve at binding."""

from __future__ import annotations

import pytest

from slayer.core.errors import QueryTypeError
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
        functional = await engine.execute(_q(dimensions=["fiscal_year(order_date)"]))
        explicit = await engine.execute(_q(time_dimensions=[{"dimension": "order_date", "granularity": "fiscal_year"}]))
        assert functional.sql == explicit.sql
        assert sorted(map(repr, functional.data)) == sorted(map(repr, explicit.data))

    async def test_time_dimensions_string_equals_the_explicit_form(self, engine) -> None:
        functional = await engine.execute(_q(time_dimensions=["fiscal_year(order_date)"]))
        explicit = await engine.execute(_q(time_dimensions=[{"dimension": "order_date", "granularity": "fiscal_year"}]))
        assert functional.sql == explicit.sql

    async def test_case_insensitive_callee(self, engine) -> None:
        upper = await engine.execute(_q(dimensions=["FISCAL_YEAR(order_date)"]))
        lower = await engine.execute(_q(dimensions=["fiscal_year(order_date)"]))
        assert upper.sql == lower.sql


class TestUnknownCalleeAtBinding:
    @pytest.mark.parametrize("fields", [
        {"time_dimensions": ["mnth(order_date)"]},
        {"dimensions": ["mnth(order_date)"]},
    ], ids=["time-dimensions-string", "dimension"])
    async def test_fails_at_binding_listing_granularities(self, engine, fields) -> None:
        query = _q(**fields)  # construction no longer rejects it
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        msg = str(exc.value)
        for name in (*BUILT_IN_GRANULARITIES, *GRANULARITY_NAMES):
            assert name in msg
        if "dimensions" in fields:
            assert "partition_by" in msg

    @pytest.mark.parametrize("entry", ["fiscal_year()", "fiscal_year(order_date, amount)", "fiscal_year(upper(id))"])
    async def test_wrong_shape_datasource_callee(self, engine, entry) -> None:
        query = _q(dimensions=[entry])
        with pytest.raises(QueryTypeError) as exc:
            await engine.execute(query, dry_run=True)
        msg = str(exc.value)
        assert "fiscal_year" in msg
        assert "(col" in msg or "gran(" in msg

    def test_bare_column_string_still_rejected_at_construction(self) -> None:
        with pytest.raises(ValueError) as exc:
            SlayerQuery.model_validate({"source_model": "orders", "time_dimensions": ["order_date"]})
        assert "month" in str(exc.value)
