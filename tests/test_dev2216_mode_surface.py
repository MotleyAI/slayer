"""The optional ``to_many_handling`` field, refinement inheritance, the broadcast warning's hint and
usage reporting (spec: queries/attribution-modes, queries/semantics › Loud degradation)."""

from __future__ import annotations

import re

import pytest

from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from slayer.core.warnings import DICE_SLICE_HINT, BroadcastDimension, BroadcastGrainWarningPayload
from slayer.usage_reporting.features import query_features


def _saved(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", "metrics": ["sum(orders.amount)"], **kw})


def _refined(base: SlayerQuery, **refinement) -> SlayerQuery:
    return refine_query(saved=base, refinement=QueryRefinement.model_validate(refinement))


class TestField:
    def test_omitted_is_none(self) -> None:
        assert _saved().to_many_handling is None

    def test_explicit_null_is_the_default(self) -> None:
        assert _saved(to_many_handling=None).to_many_handling is None

    def test_omitted_mode_is_not_dumped(self) -> None:
        assert "to_many_handling" not in _saved().model_dump(mode="json", exclude_none=True)

    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    def test_named_modes_are_kept(self, mode: str) -> None:
        assert _saved(to_many_handling=mode).model_dump(mode="json")["to_many_handling"] == mode


class TestRefinement:
    def test_null_refinement_is_accepted(self) -> None:
        assert QueryRefinement.model_validate({"to_many_handling": None}).to_many_handling is None

    @pytest.mark.parametrize("saved", [None, "broadcast", "associate", "error"])
    def test_unset_and_null_inherit(self, saved: str | None) -> None:
        base = _saved(to_many_handling=saved)
        assert _refined(base, filter=["amount > 1"]).to_many_handling == saved
        assert _refined(base, to_many_handling=None).to_many_handling == saved

    @pytest.mark.parametrize("saved", [None, "associate"])
    def test_named_refinement_replaces(self, saved: str | None) -> None:
        assert _refined(_saved(to_many_handling=saved), to_many_handling="broadcast").to_many_handling == "broadcast"


class TestBroadcastPayload:
    def test_hint_names_omitting_the_mode(self) -> None:
        payload = BroadcastGrainWarningPayload(
            metric="ls", location="query",
            dimensions=[BroadcastDimension(dimension="status", reason="crosses a fanning hop")])
        assert payload.hint == DICE_SLICE_HINT
        assert re.search(r"omit", DICE_SLICE_HINT, re.IGNORECASE)
        assert "to_many_handling" in DICE_SLICE_HINT


class TestUsageReporting:
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    def test_any_named_mode_is_flagged(self, mode: str) -> None:
        assert "to_many_handling" in query_features(_saved(to_many_handling=mode)).flags

    def test_omitted_mode_is_not_flagged(self) -> None:
        assert "to_many_handling" not in query_features(_saved()).flags
