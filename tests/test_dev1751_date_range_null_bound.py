"""DEV-1751 — a 2-element ``date_range`` with a ``None`` bound must raise at
planning time instead of emitting ``BETWEEN ... AND NULL`` (never true, silent
zero rows). Exercised through the public planning path on a plain model and on
a multi-stage ``source_queries`` model.

``SlayerQuery`` construction rejects ``None`` elements, so the bad range is
assigned post-construction — exactly what the facade used to do.
"""

from __future__ import annotations

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
    q = SlayerQuery(
        source_model=source_model,
        time_dimensions=[TimeDimension(dimension="created_at", granularity="month")],
        measures=[{"formula": measure, "name": "m0"}],
    )
    q.time_dimensions[0].date_range = date_range  # type: ignore[assignment]
    return q


BAD_RANGES = [
    pytest.param(["2024-01-01", None], id="missing-upper"),
    pytest.param([None, "2024-12-31"], id="missing-lower"),
    pytest.param([None, None], id="both-missing"),
]


def _assert_names_dimension_range_and_fix(msg: str) -> None:
    # Contract: name the dimension and the received range, suggest the fix.
    assert "created_at" in msg, msg
    assert "None" in msg, msg
    assert "one-sided" in msg.lower(), msg


class TestNullBoundFailsClosed:

    @pytest.mark.parametrize("date_range", BAD_RANGES)
    async def test_plain_model_raises_naming_the_dimension(self, date_range) -> None:
        query = _query(
            source_model="orders", measure="amount:sum", date_range=date_range,
        )
        model = _orders()
        with pytest.raises(ValueError) as exc_info:
            await _engine_generate(query=query, model=model)
        _assert_names_dimension_range_and_fix(str(exc_info.value))

    @pytest.mark.parametrize("date_range", BAD_RANGES)
    async def test_source_queries_model_raises(self, date_range) -> None:
        query = _query(
            source_model="daily", measure="rev:max", date_range=date_range,
        )
        model, extra = _orders(), [_daily()]
        with pytest.raises(ValueError) as exc_info:
            await _engine_generate(query=query, model=model, extra_models=extra)
        _assert_names_dimension_range_and_fix(str(exc_info.value))


class TestWellFormedRangeStillPlans:

    async def test_two_string_bounds_emit_the_inclusive_range(self) -> None:
        query = _query(
            source_model="orders", measure="amount:sum",
            date_range=["2024-01-01", "2024-12-31"],
        )
        sql = await _engine_generate(query=query, model=_orders())
        assert "2024-01-01" in sql
        assert "2024-12-31" in sql
