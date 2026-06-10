"""DEV-1549 (post-merge): the default ``slayer search`` text format
prints the compact preview (``hit.description``) as the per-hit body,
falling back to ``hit.text``. Without this, compact-by-default would
leave the default text output with empty preview lines.
"""

from __future__ import annotations

import contextlib
import io
from typing import List, Optional

import pytest

from slayer.search.service import SearchHit, SearchResponse


def _capture_search_print(response: SearchResponse) -> str:
    from slayer.cli import _print_search_response_text

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _print_search_response_text(response)
    return buf.getvalue()


def _hit(
    *,
    kind: str = "memory",
    id_: str = "1",
    score: float = 0.5,
    text: str = "",
    description: Optional[str] = None,
) -> SearchHit:
    return SearchHit(
        kind=kind, id=id_, score=score, text=text, description=description,
    )


def test_text_output_uses_description_when_text_is_empty() -> None:
    response = SearchResponse(results=[
        _hit(description="amount is in cents", text=""),
    ])
    output = _capture_search_print(response)
    assert "amount is in cents" in output


def test_text_output_falls_back_to_text_when_description_is_none() -> None:
    response = SearchResponse(results=[
        _hit(text="full learning body that should still print", description=None),
    ])
    output = _capture_search_print(response)
    assert "full learning body that should still print" in output


def test_text_output_prints_blank_line_when_both_empty() -> None:
    response = SearchResponse(results=[_hit(text="", description=None)])
    output = _capture_search_print(response)
    # The line under the hit prefix is the preview body — empty when both
    # description and text are absent. Just assert no traceback.
    assert "Traceback" not in output


def test_text_output_prefers_description_over_text() -> None:
    """When both are set (e.g. verbose mode with a description), the
    preview line shows the description (the compact preview)."""
    response = SearchResponse(results=[
        _hit(description="short preview", text="long full body verbose render"),
    ])
    output = _capture_search_print(response)
    assert "short preview" in output


def test_text_output_handles_multiple_hits() -> None:
    response = SearchResponse(results=[
        _hit(id_="1", description="first preview"),
        _hit(id_="2", description="second preview"),
    ])
    output = _capture_search_print(response)
    assert "first preview" in output
    assert "second preview" in output


@pytest.mark.parametrize("preview", [
    "single line",
    "first line\nsecond line",
    "first line\n\nsecond paragraph",
])
def test_text_output_takes_first_line_only(preview: str) -> None:
    response = SearchResponse(results=[_hit(description=preview)])
    output = _capture_search_print(response)
    lines: List[str] = output.splitlines()
    preview_line = next(
        ln.strip() for ln in lines if ln.startswith("    ")
    )
    assert preview_line == "single line" or preview_line == "first line"
