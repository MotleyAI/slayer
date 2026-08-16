"""DEV-1745 (W6) — P0 normalization warning for a malformed ``date_range``.

Keep-list item 1 is RATIFIED: a malformed ``date_range`` keeps its silent
no-op in the planner (``stage_planner.py``: ``if not td.date_range or
len(td.date_range) != 2: continue``). This adds a normalization WARNING so the
drop stops being invisible — **no behavior change**.

Trigger (D7): ``date_range is not None and len(date_range) != 2``. That is
exactly the planner's own drop condition, so the warning fires if and only if
the range is actually ignored — covering ``[]``, a single element, and 3+.
``date_range=None`` is legitimately absent and never warns.

``normalize_query`` does not inspect ``query.time_dimensions`` at all today, so
this is a new rule. It runs per stage, so nested stages are covered.
"""

from __future__ import annotations

import warnings

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning
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
    pytest.param(["2024-01-01"], id="single"),
    pytest.param(["2024-01-01", "2024-06-30", "2024-12-31"], id="three"),
]


class TestMalformedDateRangeWarns:

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_warns(self, date_range) -> None:
        assert _warnings_for(date_range), (
            f"no normalization warning for malformed date_range={date_range!r}"
        )

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_warns_exactly_once(self, date_range) -> None:
        assert len(_warnings_for(date_range)) == 1, (
            f"expected exactly one warning for date_range={date_range!r}"
        )

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_emits_on_the_python_warnings_channel(self, date_range) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            normalize_query(query=_query(date_range))
        assert any(
            issubclass(w.category, SlayerNormalizationWarning) for w in caught
        ), f"no SlayerNormalizationWarning for date_range={date_range!r}"


class TestWarningWordingDescribesTheNoOp:
    """DEV-1783 item 7 — MALFORMED_DATE_RANGE reports but never rewrites, so its
    message must not claim a rewrite. A real rewrite rule (FUNC_STYLE_AGG) still
    says "rewrote"."""

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_structured_message_does_not_claim_a_rewrite(self, date_range) -> None:
        [w] = _warnings_for(date_range)
        assert w.rewritten is False, w
        msg = w.human_message()
        assert "rewrote" not in msg.lower(), msg
        assert "→" not in msg, msg
        assert w.rule_id in msg, msg
        assert w.location in msg, msg

    @pytest.mark.parametrize("date_range", MALFORMED)
    def test_python_warning_carrier_does_not_claim_a_rewrite(self, date_range) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            normalize_query(query=_query(date_range))
        texts = [str(w.message) for w in caught if "date_range" in str(w.message)]
        assert texts, "no date_range warning surfaced on the Python channel"
        # Neither the verb "rewrote" nor the transform arrow — nothing was
        # rewritten, so the carrier must not imply it (single source of truth
        # with human_message).
        assert all("rewrote" not in t.lower() for t in texts), texts
        assert all("→" not in t for t in texts), texts

    def test_a_genuine_rewrite_rule_still_says_rewrote(self) -> None:
        w = NormalizationWarning(
            rule_id="FUNC_STYLE_AGG", original="count(*)",
            normalized="*:count", location="measures[0].formula",
        )
        assert w.rewritten is True
        assert "rewrote" in w.human_message().lower()


class TestWellFormedDateRangeIsSilent:

    def test_two_elements_does_not_warn(self) -> None:
        assert _warnings_for(["2024-01-01", "2024-12-31"]) == []

    def test_absent_date_range_does_not_warn(self) -> None:
        assert _warnings_for(None) == []


@pytest.mark.asyncio
class TestNoBehaviorChange:
    """The ratified silent no-op stays: a malformed range emits no filter, and
    a well-formed one still does."""

    async def _sql(self, date_range) -> str:
        return await _engine_generate(
            query=_query(date_range), model=_orders(),
            dialect="postgres", validate=False,
        )

    @pytest.mark.parametrize("date_range", MALFORMED)
    async def test_malformed_emits_no_date_filter(self, date_range) -> None:
        sql = await self._sql(date_range)
        assert "2024-01-01" not in sql, (
            f"malformed date_range must stay a no-op, got:\n{sql}"
        )

    async def test_well_formed_still_filters(self) -> None:
        sql = await self._sql(["2024-01-01", "2024-12-31"])
        assert "2024-01-01" in sql
        assert "2024-12-31" in sql

    async def test_absent_matches_empty_emission(self) -> None:
        assert await self._sql(None) == await self._sql([])
