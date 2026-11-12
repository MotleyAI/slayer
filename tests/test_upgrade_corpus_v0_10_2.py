"""A store written by ``motley-slayer==0.10.2`` loads, re-saves, re-ingests and queries with 0.10.2's results."""

from __future__ import annotations

import re
from typing import Any

import pytest

from tests._upgrade_corpus import Corpus, OfflineChecks, OnlineChecks, load_all

CORPUS = Corpus.load(release="v0_10_2", datasources=("shop", "cube", "lite"),
                     data_files=("data.duckdb", "data.sqlite"))

_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_COLON_SPAN = re.compile(r"[\w*\"]:[A-Za-z_]")
#: Keys whose string values are Mode-B text.
_MODE_B_KEYS = frozenset({"formula", "expression", "raw_formula", "filters", "dimensions", "time_dimensions",
                          "dimension", "column", "order", "main_time_dimension"})


@pytest.fixture(params=sorted(CORPUS.expected))
def key(request) -> str:
    return request.param


@pytest.fixture(params=[
    pytest.param("shop", marks=pytest.mark.xfail(
        strict=True, reason="DEV-2057: false stale-reference error for m_rank over uncached status_rank")),
    "cube", "lite",
])
def data_source(request) -> str:
    return request.param


def _mode_b_texts(value: Any, *, mode_b: bool = False) -> list[str]:
    """Every Mode-B string inside a dumped model or query."""
    if isinstance(value, str):
        return [value] if mode_b else []
    if isinstance(value, list):
        return [t for v in value for t in _mode_b_texts(v, mode_b=mode_b)]
    if isinstance(value, dict):
        return [t for k, v in value.items() if k != "columns"
                for t in _mode_b_texts(v, mode_b=mode_b or k in _MODE_B_KEYS)]
    return []


def _colon_spans(text: str) -> list[str]:
    return _COLON_SPAN.findall(_STRING_LITERAL.sub("''", text))


class TestOnline(OnlineChecks):
    corpus = CORPUS


class TestOffline(OfflineChecks):
    corpus = CORPUS

    async def test_every_colon_formula_loads_functional(self, offline_store):
        models, memories = await load_all(self.corpus, offline_store)
        dumped = [m.model_dump(mode="json") for m in models] + [
            m.query.model_dump(mode="json") for m in memories if m.query is not None]
        texts = [t for d in dumped for t in _mode_b_texts(d)]
        assert any("sum(amount)" in t for t in texts)
        assert [t for t in texts if _colon_spans(t)] == []
