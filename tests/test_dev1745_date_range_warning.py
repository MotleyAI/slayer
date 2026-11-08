"""``date_range`` shapes: no normalization warning; a single element is a period; ``[]`` / 3+ fail at construction."""

from __future__ import annotations

import pydantic
import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning
from slayer.engine.normalization import normalize_query

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


def _query(date_range) -> SlayerQuery:
    td: dict = {"dimension": "created_at", "granularity": "month"}
    if date_range is not None:
        td["date_range"] = date_range
    return SlayerQuery(
        source_model="orders",
        time_dimensions=[td],
        measures=[{"formula": "amount:sum", "name": "m0"}],
    )


def _warnings_for(date_range) -> list:
    """Normalization warnings whose rule concerns date_range."""
    result = normalize_query(query=_query(date_range))
    return [
        w for w in result.warnings
        if "date_range" in (w.rule_id or "").lower()
        or "date_range" in (w.original or "")
    ]


MALFORMED = [
    pytest.param([], id="empty"),
    pytest.param(["2024-01-01", "2024-06-30", "2024-12-31"], id="three"),
]


class TestMalformedDateRangeRejected:

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_construction_fails_naming_the_dimension(self, date_range) -> None:
        with pytest.raises(pydantic.ValidationError) as ei:
            _query(date_range)
        assert "created_at" in str(ei.value), ei.value


class TestNoDateRangeWarning:

    @pytest.mark.parametrize("date_range", [
        ["2024-01-01"], ["2024-01-01", "2024-12-31"], ["2024-01-01", None], "2024-Q1", None,
    ])
    def test_silent(self, date_range) -> None:
        assert _warnings_for(date_range) == []

    def test_a_genuine_rewrite_rule_still_says_rewrote(self) -> None:
        w = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG", original="count(*)",
            normalized="*:count", location="measures[0].formula",
        )
        assert w.rewritten is True
        assert "rewrote" in w.human_message().lower()


@pytest.mark.asyncio
class TestEmission:

    async def _sql(self, date_range) -> str:
        return await _engine_generate(
            query=_query(date_range), model=_orders(),
            dialect="postgres", validate=False,
        )

    async def test_single_element_is_a_period(self) -> None:
        sql = await self._sql(["2024-01-01"])
        assert "2024-01-01" in sql
        assert "2024-01-02" in sql

    async def test_well_formed_still_filters(self) -> None:
        sql = await self._sql(["2024-01-01", "2024-12-31"])
        assert "2024-01-01" in sql
        assert "2025-01-01" in sql

    async def test_absent_emits_no_date_filter(self) -> None:
        assert "2024" not in await self._sql(None)
