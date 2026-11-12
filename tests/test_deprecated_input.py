"""The deprecated-input marker: one payload-carrying ``FutureWarning`` per conversion, attributed to the caller.

Spec: compatibility/removal-tripwire › Deprecated input forms convert with a structured warning.
"""

from __future__ import annotations

import inspect

from pydantic import TypeAdapter

from slayer.core.deprecation import deprecated_input
from slayer.core.models import SlayerModel
from slayer.core.query import QueryRefinement, SlayerQuery
from slayer.core.warnings import AnySlayerWarning, DeprecatedInputWarning
from slayer.engine.query_engine import SlayerResponse
from tests._deprecated_input_helpers import deprecations, payload_of, recorded


def _next_line() -> int:
    frame = inspect.currentframe()
    assert frame is not None
    assert frame.f_back is not None
    return frame.f_back.f_lineno + 1


class TestMarker:
    def test_one_future_warning_carrying_the_payload(self) -> None:
        with recorded() as caught:
            line = _next_line()
            payload = deprecated_input("old_form", "new_form", removal="3.0")
        [w] = deprecations(caught)
        assert issubclass(w.category, FutureWarning)
        assert isinstance(payload, DeprecatedInputWarning)
        assert payload_of(w) == payload
        assert (payload.kind, payload.old, payload.new, payload.removal) == (
            "deprecated_input", "old_form", "new_form", "3.0",
        )
        assert str(w.message) == payload.human_message()
        assert (w.filename, w.lineno) == (__file__, line)

    def test_each_call_warns_once(self) -> None:
        with recorded() as caught:
            deprecated_input("a", "b", removal="3.0")
            deprecated_input("a", "b", removal="3.0")
        assert len(deprecations(caught)) == 2

    def test_payload_round_trips_through_the_warning_union(self) -> None:
        with recorded():
            payload = deprecated_input("old_form", "new_form", removal="3.0")
        assert TypeAdapter(AnySlayerWarning).validate_python(payload.model_dump()) == payload
        response = SlayerResponse(data=[], warnings=[payload])
        assert SlayerResponse.model_validate_json(response.model_dump_json()).warnings == [payload]


class TestAttribution:
    """A converted input form warns once, at the first line outside SLayer and pydantic."""

    def test_direct_construction(self) -> None:
        with recorded() as caught:
            line = _next_line()
            SlayerQuery(source_model="orders", strict=True)  # pyright: ignore[reportCallIssue]  # the deprecated form under test
        [w] = deprecations(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_model_validate(self) -> None:
        with recorded() as caught:
            line = _next_line()
            SlayerQuery.model_validate({"source_model": "orders", "strict": True})
        [w] = deprecations(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_nested_inside_a_model(self) -> None:
        stage = {"source_model": "orders", "measures": ["count(*)"], "strict": True}
        with recorded() as caught:
            line = _next_line()
            SlayerModel(name="m", source_queries=[stage])
        [w] = deprecations(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_refinement(self) -> None:
        with recorded() as caught:
            line = _next_line()
            QueryRefinement.model_validate({"strict": True})
        [w] = deprecations(caught)
        assert (w.filename, w.lineno) == (__file__, line)

    def test_payload_names_strict_its_replacement_and_removal(self) -> None:
        with recorded() as caught:
            SlayerQuery(source_model="orders", strict=True)  # pyright: ignore[reportCallIssue]  # the deprecated form under test
        [w] = deprecations(caught)
        payload = payload_of(w)
        assert "strict" in payload.old
        assert "to_many_handling" in payload.new
        assert payload.removal == "3.0"

    def test_a_false_payload_says_to_drop_strict(self) -> None:
        with recorded() as caught:
            SlayerQuery(source_model="orders", strict=False)  # pyright: ignore[reportCallIssue]  # the deprecated form under test
        [w] = deprecations(caught)
        payload = payload_of(w)
        assert "strict" in payload.old
        assert "to_many_handling" not in payload.new
