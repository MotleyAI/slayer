"""Tests for the `slayer.help` package — content discovery, rendering, and
static validity of every JSON snippet and formula string in the topic bodies.
"""

from __future__ import annotations

import json
import re
from typing import Any

import pytest

from slayer.core.formula import parse_filter, parse_formula
from slayer.core.query import SlayerQuery
from slayer.engine.enrichment import extract_filter_transforms
from slayer.help import (
    TOPIC_SUMMARY_LINE,
    available_topic_names,
    render_help,
)

EXPECTED_TOPICS = (
    "queries",
    "formulas",
    "aggregations",
    "transforms",
    "time",
    "filters",
    "joins",
    "models",
    "extending",
    "workflow",
)


# ---------------------------------------------------------------------------
# Topic-set invariants
# ---------------------------------------------------------------------------


def test_available_topic_names_matches_expected() -> None:
    assert available_topic_names() == EXPECTED_TOPICS


def test_topic_summary_line_lists_every_topic() -> None:
    for name in EXPECTED_TOPICS:
        assert name in TOPIC_SUMMARY_LINE
    assert TOPIC_SUMMARY_LINE.startswith("Available help topics:")


# ---------------------------------------------------------------------------
# render_help behavior
# ---------------------------------------------------------------------------


class TestNoArg:
    def test_returns_intro_mentioning_slayer(self) -> None:
        out = render_help()
        assert "SLayer" in out
        # The intro should carry at least one of the headline invariants, so
        # agents landing here see the non-obvious facts immediately.
        assert any(
            phrase in out
            for phrase in (
                "Measures are not aggregates",
                "Joined data is reached via dotted paths",
                "Filters on measures",
            )
        )

    @pytest.mark.parametrize("val", [None, "", "   ", "\n\t"])
    def test_whitespace_or_none_returns_intro(self, val: Any) -> None:
        assert render_help(val) == render_help()


class TestValidTopic:
    @pytest.mark.parametrize("topic", EXPECTED_TOPICS)
    def test_returns_nonempty_body(self, topic: str) -> None:
        body = render_help(topic)
        assert body.strip()
        # Each body should at least reference another topic so that agents can
        # fan out from any entry point.
        assert "help(topic=" in body

    def test_case_insensitive(self) -> None:
        assert render_help("QUERIES") == render_help("queries")

    def test_whitespace_trimmed(self) -> None:
        assert render_help(" queries ") == render_help("queries")


class TestInvalidTopic:
    @pytest.mark.parametrize("val", ["bogus", "Queries_Typo", "unknown"])
    def test_unknown_topic_lists_valid_ones(self, val: str) -> None:
        out = render_help(val)
        assert "Unknown help topic" in out
        assert val in out
        for name in EXPECTED_TOPICS:
            assert name in out


# ---------------------------------------------------------------------------
# Content hygiene
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("topic", EXPECTED_TOPICS)
def test_no_todo_or_fixme(topic: str) -> None:
    body = render_help(topic)
    assert "TODO" not in body
    assert "FIXME" not in body


def test_intro_has_no_todo_or_fixme() -> None:
    body = render_help()
    assert "TODO" not in body
    assert "FIXME" not in body


# ---------------------------------------------------------------------------
# Static validation of every JSON snippet and formula string in the bodies.
#
# This guards against docs drift — if a topic's example references a feature
# that no longer parses (e.g. `avg(sum(x))`), the test fails loudly.
# ---------------------------------------------------------------------------


_FENCE_RE = re.compile(r"```json\n(.*?)\n```", re.DOTALL)


def _json_snippets(body: str) -> list[tuple[int, str]]:
    """Return (offset, snippet) pairs for every ```json ... ``` block."""
    return [(m.start(), m.group(1)) for m in _FENCE_RE.finditer(body)]


def _all_topic_bodies() -> list[tuple[str, str]]:
    """Intro + every topic body, as (label, body) pairs."""
    out = [("intro", render_help())]
    out.extend((name, render_help(name)) for name in EXPECTED_TOPICS)
    return out


def test_every_json_snippet_parses_as_slayer_query() -> None:
    """Every JSON block should be a valid SlayerQuery or a list of them."""
    failures: list[str] = []
    for label, body in _all_topic_bodies():
        for offset, snippet in _json_snippets(body):
            try:
                data = json.loads(snippet)
            except json.JSONDecodeError as exc:
                failures.append(f"{label} @ {offset}: JSON decode failed — {exc}\n{snippet}")
                continue
            try:
                if isinstance(data, list):
                    for entry in data:
                        SlayerQuery.model_validate(entry)
                else:
                    SlayerQuery.model_validate(data)
            except Exception as exc:
                failures.append(f"{label} @ {offset}: SlayerQuery.model_validate failed — {exc}\n{snippet}")
    assert not failures, "Invalid JSON snippets:\n\n" + "\n\n".join(failures)


def _collect_field_formulas(query_dict: dict) -> list[str]:
    """Pull every formula string out of the `fields` list of a parsed query dict."""
    formulas: list[str] = []
    for field in query_dict.get("fields", []) or []:
        if isinstance(field, str):
            formulas.append(field)
        elif isinstance(field, dict) and "formula" in field:
            formulas.append(field["formula"])
    return formulas


def _collect_filter_strings(query_dict: dict) -> list[str]:
    filters = query_dict.get("filters", []) or []
    return [f for f in filters if isinstance(f, str)]


def test_every_field_formula_parses() -> None:
    """Every field formula in every snippet must parse via parse_formula."""
    failures: list[str] = []
    for label, body in _all_topic_bodies():
        for offset, snippet in _json_snippets(body):
            try:
                data = json.loads(snippet)
            except json.JSONDecodeError:
                continue  # Already reported by the JSON test above.
            queries = data if isinstance(data, list) else [data]
            for q in queries:
                if not isinstance(q, dict):
                    continue
                for formula in _collect_field_formulas(q):
                    try:
                        parse_formula(formula)
                    except Exception as exc:
                        failures.append(f"{label} @ {offset}: parse_formula({formula!r}) — {exc}")
    assert not failures, "Formulas that failed to parse:\n" + "\n".join(failures)


def test_every_filter_string_parses() -> None:
    """Every filter string must parse through the engine's full filter pipeline.

    The engine's ``extract_filter_transforms`` pulls inline transforms out
    first (rewriting e.g. ``"last(change(x)) < 0"`` to ``"ft_0 < 0"`` with an
    extra hidden field), then the rewritten condition is passed to
    ``parse_filter``. This mirrors what the engine does at query time.
    """
    failures: list[str] = []
    for label, body in _all_topic_bodies():
        for offset, snippet in _json_snippets(body):
            try:
                data = json.loads(snippet)
            except json.JSONDecodeError:
                continue
            queries = data if isinstance(data, list) else [data]
            for q in queries:
                if not isinstance(q, dict):
                    continue
                for filter_str in _collect_filter_strings(q):
                    try:
                        rewritten, _ = extract_filter_transforms(filter_str)
                        parse_filter(rewritten)
                    except Exception as exc:
                        failures.append(f"{label} @ {offset}: parse_filter({filter_str!r}) — {exc}")
    assert not failures, "Filter strings that failed to parse:\n" + "\n".join(failures)
