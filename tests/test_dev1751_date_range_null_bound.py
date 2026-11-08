"""A ``null`` ``date_range`` bound is an open side; ``[null, null]`` is rejected at construction.

Exercised through the public planning path on a plain model and on a
multi-stage ``source_queries`` model; a one-sided range must never render
``BETWEEN ... AND NULL``.
"""

from __future__ import annotations

import pydantic
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery, TimeDimension
from tests._engine_helpers import _engine_generate


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
        ],
    )


def _daily() -> SlayerModel:
    return SlayerModel(
        name="daily", data_source="test",
        source_queries=[
            SlayerQuery(
                source_model="orders",
                dimensions=["created_at"],
                measures=[{"formula": "amount:sum", "name": "rev"}],
            )
        ],
    )


def _query(*, source_model: str, measure: str, date_range: list) -> SlayerQuery:
    return SlayerQuery(
        source_model=source_model,
        time_dimensions=[TimeDimension(dimension="created_at", granularity="month", date_range=date_range)],
        measures=[{"formula": measure, "name": "m0"}],
    )


ONE_SIDED = [
    pytest.param(["2024-01-01", None], "2024-01-01", id="missing-upper"),
    pytest.param([None, "2024-12-31"], "2025-01-01", id="missing-lower"),
]


def _assert_one_sided(sql: str, bound: str) -> None:
    assert bound in sql, sql
    assert "NULL" not in sql.upper().replace("IS NOT NULL", ""), sql
    assert "BETWEEN" not in sql.upper(), sql


class TestOneSidedRangePlans:

    @pytest.mark.parametrize(("date_range", "bound"), ONE_SIDED)
    async def test_plain_model(self, date_range, bound) -> None:
        query = _query(source_model="orders", measure="amount:sum", date_range=date_range)
        _assert_one_sided(await _engine_generate(query=query, model=_orders()), bound)

    @pytest.mark.parametrize(("date_range", "bound"), ONE_SIDED)
    async def test_source_queries_model(self, date_range, bound) -> None:
        query = _query(source_model="daily", measure="rev:max", date_range=date_range)
        sql = await _engine_generate(query=query, model=_orders(), extra_models=[_daily()])
        _assert_one_sided(sql, bound)


class TestBothBoundsMissing:

    def test_rejected_at_construction_naming_the_dimension(self) -> None:
        with pytest.raises(pydantic.ValidationError) as exc_info:
            _query(source_model="orders", measure="amount:sum", date_range=[None, None])
        assert "created_at" in str(exc_info.value), exc_info.value


class TestWellFormedRangeStillPlans:

    async def test_two_string_bounds_emit_the_half_open_range(self) -> None:
        query = _query(
            source_model="orders", measure="amount:sum",
            date_range=["2024-01-01", "2024-12-31"],
        )
        sql = await _engine_generate(query=query, model=_orders())
        assert "2024-01-01" in sql
        assert "2025-01-01" in sql
