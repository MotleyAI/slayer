"""The optional ``to_many_handling`` field, refinement inheritance, the fallback warning payload and
usage reporting (spec: queries/attribution-modes, queries/semantics › Loud degradation)."""

from __future__ import annotations

import re
import warnings

import pytest
from xdist.remote import serialize_warning_message
from xdist.workermanage import unserialize_warning_message

from slayer.core.errors import BroadcastGrainWarning
from slayer.core.query import QueryRefinement, SlayerQuery, refine_query
from slayer.core.warnings import DICE_SLICE_HINT, BroadcastDimension, BroadcastGrainWarningPayload
from slayer.usage_reporting.features import query_features
from tests._dev1841_fixtures import ModelMeasure, make_exec_engine

LAST = ModelMeasure(formula="last(customers.spend, customers.signup_at)", name="ls")


def _saved(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "orders", "measures": ["sum(amount)"], **kw})


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
        assert _refined(base, filters=["amount > 1"]).to_many_handling == saved
        assert _refined(base, to_many_handling=None).to_many_handling == saved

    @pytest.mark.parametrize("saved", [None, "associate"])
    def test_named_refinement_replaces(self, saved: str | None) -> None:
        assert _refined(_saved(to_many_handling=saved), to_many_handling="broadcast").to_many_handling == "broadcast"


class TestFallbackPayload:
    def _payload(self, **kw) -> BroadcastGrainWarningPayload:
        return BroadcastGrainWarningPayload(
            measure="ls", location="query",
            dimensions=[BroadcastDimension(dimension="status", reason="crosses a fanning hop")], **kw)

    def test_reason_defaults_to_none(self) -> None:
        assert self._payload().association_unsupported is None

    def test_reason_round_trips(self) -> None:
        payload = self._payload(association_unsupported="first/last")
        restored = BroadcastGrainWarningPayload.model_validate(payload.model_dump(mode="json"))
        assert restored == payload
        assert restored.association_unsupported == "first/last"

    def test_ordinary_hint_names_omitting_the_mode(self) -> None:
        assert self._payload().hint == DICE_SLICE_HINT
        assert re.search(r"omit", DICE_SLICE_HINT, re.IGNORECASE)
        assert "to_many_handling" in DICE_SLICE_HINT


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestFallbackPythonWarning:
    async def test_names_the_reason_and_survives_transport(self, engine) -> None:
        query = SlayerQuery(source_model="orders", dimensions=["status"], measures=[LAST])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            resp = await engine.execute(query)
        (payload,) = [w for w in resp.warnings or [] if getattr(w, "kind", None) == "broadcast"]
        assert payload.association_unsupported
        emitted = [c.message for c in caught if isinstance(c.message, BroadcastGrainWarning)]
        assert emitted
        w = emitted[0]
        assert re.search(r"first|last", str(w), re.IGNORECASE)
        assert str(type(w)(*w.args)) == str(w)
        wm = warnings.WarningMessage(message=w, category=type(w), filename="f.py", lineno=1)
        assert str(w) in str(unserialize_warning_message(serialize_warning_message(wm)).message)


class TestUsageReporting:
    @pytest.mark.parametrize("mode", ["broadcast", "associate", "error"])
    def test_any_named_mode_is_flagged(self, mode: str) -> None:
        assert "to_many_handling" in query_features(_saved(to_many_handling=mode)).flags

    def test_omitted_mode_is_not_flagged(self) -> None:
        assert "to_many_handling" not in query_features(_saved()).flags
